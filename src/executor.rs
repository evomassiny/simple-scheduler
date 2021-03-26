use nix::{
    libc::{exit, EXIT_SUCCESS, STDERR_FILENO, STDOUT_FILENO},
    unistd::{Pid, mkfifo, chdir, execv, fork, getpid, setsid, access, close, ForkResult, AccessFlags},
    fcntl::{open, OFlag},
    sys::{
        epoll::{epoll_create, epoll_ctl, epoll_wait, EpollOp, EpollEvent, EpollFlags},
        signalfd::{signalfd, SfdFlags, SIGNALFD_NEW},
        signal::{Signal, SigSet},
        stat::Mode,
        wait::waitpid,
    },
};
use rocket::tokio::task::spawn_blocking;
use std::{
    os::unix::io::{RawFd, IntoRawFd},
    ffi::CString,
    convert::TryInto,
};

use crate::models::Process;
use crate::pipe::Pipe;
use crate::process_utils::{
    reset_signal_handlers,
    assign_file_to_fd,
    close_everything_but_stderr_stdout_and_pipe,
    rename_current_process,
    block_sigchild,
    MonitorHandle,
};

/// This is the main monitor loop, it:
/// * creates 2 fifos:
///   * One to listen from the hypervisor commands (`handle.to_monitor`),
///   * One to write reponse to the hypervisor (`handle.from_monitor`)
/// * poll for 2 kind of event: the child termination or an hypervisor command
/// * respond to the command through the `handle.from_monitor` fifo.
fn monitor_loop(_child_pid: Pid, handle: &MonitorHandle) -> Result<(), Box<dyn std::error::Error>> {
    // TODO:
    // * write child status after its termination (into `handle.status`),
    // * notify the hypervisor when the child terminates using the `handle.from_monitor` fifo, 
    // * setup command parsing (if needed)

    let USER_RW_PERM: Mode = Mode::S_IRUSR | Mode::S_IWUSR;
    // create to_monitor and from_monitor fifo
    if let Err(_) = access(&handle.to_monitor, AccessFlags::F_OK) {
        mkfifo(&handle.to_monitor, USER_RW_PERM)?;
    }
    if let Err(_) = access(&handle.from_monitor, AccessFlags::F_OK) {
        mkfifo(&handle.from_monitor, USER_RW_PERM)?;
    }
    // open `from_monitor` fifo (write end) in READ/WRITE mode, otherwise it would 
    // if no one has is reading it. Also opens it in BLOCKINGLY.
    let write_end_fd: RawFd = open(&handle.from_monitor, OFlag::O_RDWR, USER_RW_PERM)?;
    // open `to_monitor` fifo, NON-BLOCKINGLY, in read only
    // (we will poll it along with the sigchild fd).
    let read_end_fd: RawFd = open(&handle.to_monitor, OFlag::O_NONBLOCK | OFlag::O_RDONLY, Mode::S_IRUSR)?;
    
    // open SIG_CHLD as a RawFd, so we can poll it
    let mut mask = SigSet::empty();
    mask.add(Signal::SIGCHLD);
    let sigchild_fd: RawFd = signalfd(SIGNALFD_NEW, &mask, SfdFlags::SFD_NONBLOCK)?;

    // setup epoll 
    let epoll_fd: RawFd = epoll_create()?;

    // submit `sigchild_fd`:
    // 1- Create a Event with: 
    //  * EpollEvent::EPOLLIN => listen on input
    //  * we associate the RawFd of `sigchild_event` to the event, so we know which 
    //  fd caused the wakes.
    // 2 - submit it using epoll_ctl
    let mut sigchild_event = EpollEvent::new(EpollFlags::EPOLLIN, sigchild_fd.try_into()?);
    epoll_ctl(epoll_fd, EpollOp::EpollCtlAdd, sigchild_fd, Some(&mut sigchild_event))?;
    // same process for `read_end_fd`
    let mut read_end_event = EpollEvent::new(EpollFlags::EPOLLIN, read_end_fd.try_into()?);
    epoll_ctl(epoll_fd, EpollOp::EpollCtlAdd, read_end_fd, Some(&mut read_end_event))?;

    // create a empty event array, that we will feed to epoll_wait()
    let mut events: Vec<EpollEvent> = (0..10)
        .map(|_| EpollEvent::empty())
        .collect();
    // start the event loop
    const WAIT_FOREVER_TIMEOUT: isize = -1;
    'event_loop: while let Ok(event_count) = epoll_wait(epoll_fd, &mut events, WAIT_FOREVER_TIMEOUT) {
        for event_idx in 0..event_count {
            // fetch the data we've associated with the event (file descriptors)
            let fd: RawFd = events[event_idx].data().try_into()?;
            // read the concerned file descriptor
            match fd {
                fd if fd == read_end_fd => { println!("Can read stuff from to_monitor fifo !") },
                fd if fd == sigchild_fd => {
                    println!("child returned");
                    break 'event_loop;
                },
                _ => panic!("unsuscribed ")
            }
        }
    }
    // Close all opened RawFds
    close(epoll_fd)?;
    close(sigchild_fd)?;
    close(read_end_fd)?;
    close(write_end_fd)?;

    Ok(())
}

impl Process {

    /**
     * Spawn a `command` in a dedicated daemon process.
     *
     * The daemon process will:
     *  * use '${PROCESS_DIR_ENV}/${id}/cwd' as its "Current Working Directory",
     *  * redirect its stdout to '${PROCESS_DIR_ENV}/${id}/stdout'
     *  * redirect its stderr to '${PROCESS_DIR_ENV}/${id}/stderr'
     *  * not be related to the caller process, it will be a direct child of PID 1
     *
     *  This function use blocking calls, and should not be called in an async
     *  runtime.
     *
     *  see https://www.freedesktop.org/software/systemd/man/daemon.html
     *
     *  SAFETY: This method uses unsafe unix calls to spawn the process
     *  **as a daemon process**, so it can outlive the caller process.
     */
    fn blockingly_spawn_daemon(cmd: &str, id: i32) -> Result<Self, Box<dyn std::error::Error>> {
        // create process handle (holds path to process related files)
        let handle = MonitorHandle::from_task_id(id)?;
        // create a pipe to send pid from grandchild to grandparent
        let mut pipe = Pipe::new()?;

        unsafe {
            // fork process
            match fork()? {
                // This process immediately forks and dies,
                // which so its child is affected as a child of PID by the kernel
                ForkResult::Child => {
                    match fork()? {
                        ForkResult::Child => {
                            // This process is forked into 2 process:
                            // * the worker process which run the command
                            // * the monitor process which watch the monitor and listen from
                            // commands
                            //
                            //  NOTE: Every error must panic here ! otherwise we will end up we
                            //  several instance of the Web server.
                            //
                            //  NOTE: Here we must "clean up" as much as possible
                            //  the process state, so the worker has no access to the caller
                            //  internals (including file descriptors, sigaction, CWD...)

                            // detach from any terminal and create an independent session.
                            let _ = setsid().expect("Failed to detach");

                            // Close all files but stdin/stdout and the pipe:
                            close_everything_but_stderr_stdout_and_pipe(&pipe)
                                .expect("Could not close fd");

                            // set STDERR and STDOUT to files
                            assign_file_to_fd(&handle.stderr, STDERR_FILENO)
                                .expect("Failed to redirect stderr");
                            assign_file_to_fd(&handle.stdout, STDOUT_FILENO)
                                .expect("Failed to redirect stdout");

                            // Change directory to '$cwd'
                            chdir(&handle.cwd).expect("could not change directory to '$cwd'. ");
                            
                            // block SIGCHLD signal, so we dont lose some before listening on them
                            // (might not be needed)
                            block_sigchild().expect("Could not block SIGCHLD"); 

                            // Build the command line
                            // * build command args
                            let args: Vec<CString> = vec![
                                CString::new("/bin/bash").expect("failed to parse cmd"),
                                CString::new("-c").expect("failed to parse cmd"),
                                CString::new(cmd).expect("failed to parse cmd"),
                            ];

                            // finally fork the process:
                            // * the child will execute the command
                            // * the parent will wait for its child to terminate,
                            // and communicate its return status into a file.
                            match fork()? {
                                // This process runs the command
                                ForkResult::Child => {
                                    // remove all signal masks
                                    reset_signal_handlers().expect("failed to reset signals");
                                    // Close pipe, at this point only stderr/stdout file should be open
                                    pipe.close().expect("Could not close pipe");
                                    let _ = execv(&args[0], &args);
                                    unreachable!();
                                }
                                // This process monitor the worker process
                                ForkResult::Parent { child, .. } => {
                                    // change process name to a distinct one (from the server)
                                    let monitor_name = format!("monitor {}", id);
                                    let _ = rename_current_process(&monitor_name);
                                    pipe.close_reader().expect("Could not close pipe");

                                    // send the monitor PID to the caller process
                                    let self_pid = getpid();
                                    let _ = pipe.send_int(self_pid.as_raw());
                                    pipe.close().expect("Could not close pipe");

                                    // wait for child completion, and listen for request
                                    if let Err(e) = monitor_loop(child, &handle) {
                                        eprintln!("Monitor: '{}' failed with '{:?}'", &monitor_name, e);
                                        panic!("Process monitoring Failed");
                                    }
                                    // once the child has terminated, we can exit safely
                                    exit(EXIT_SUCCESS);
                                }
                            }
                        }
                        // this process must exit right away, so its child is attached to PID 1
                        // this will de facto turn it into a daemon
                        ForkResult::Parent { .. } => {
                            exit(EXIT_SUCCESS);
                        }
                    }
                }
                // Caller process,
                // waits for grandchild's PID reception,
                // and returns a `Process` instance
                ForkResult::Parent { child, .. } => {
                    pipe.close_writer()?;
                    let grandchild: i32 = pipe.recv_int()?;
                    pipe.close()?;
                    // wait for child (so its not a zombie process)
                    let _ = waitpid(child, None);
                    return Ok(Process {
                        id: 0,
                        pid: grandchild,
                    });
                }
            };
        }
    }

    /**
     * Spawn a `command` in a dedicated daemon process.
     *
     * The daemon process will:
     *  * use '/' as its "Current Working Directory",
     *  * use a temporary file for stdout
     *  * use a temporary file for stderr
     *  * not be related to the caller process, it will be a direct child of PID 1
     *
     *  see https://www.freedesktop.org/software/systemd/man/daemon.html
     */
    pub async fn spawn_daemon(cmd: &str, id: i32) -> Result<Self, String> {
        let command: String = String::from(cmd);
        // wrap the blocking function into its own thread, to avoid messing
        // with the current executor
        spawn_blocking(move || {
            Self::blockingly_spawn_daemon(&command, id)
                .map_err(|e| format!("Could not spawn daemon task: '{}'", e))
        })
        .await
        .map_err(|e| format!("Could not spawn blocking task: '{}'", e))?
    }
}
