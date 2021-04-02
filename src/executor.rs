use nix::{
    libc::{self, exit, EXIT_SUCCESS, STDERR_FILENO, STDOUT_FILENO},
    unistd::{Pid, chdir, execv, fork, getpid, setsid, close, ForkResult},
    sys::{
        epoll::{epoll_create, epoll_ctl, epoll_wait, EpollOp, EpollEvent, EpollFlags},
        signalfd::{SignalFd, SfdFlags, siginfo, SIGNALFD_NEW},
        signal::{Signal, SigSet},
        wait::waitpid,
    },
};
use rocket::tokio::task::spawn_blocking;
use std::{
    os::unix::{
        net::{UnixListener, UnixStream},
        io::{RawFd, AsRawFd},
    },
    ffi::CString,
    convert::TryInto,
    collections::HashMap,
    io::Read,
};
use serde::{Serialize, Deserialize};
use serde_json;

use crate::models::Process;
use crate::pipe::Pipe;
use crate::process_utils::{
    reset_signal_handlers,
    assign_file_to_fd,
    close_everything_but,
    rename_current_process,
    block_sigchild,
    MonitorHandle,
};

/// Epoll will wait forever (unless an event happens) if this timout value is provided
const WAIT_FOREVER_TIMEOUT: isize = -1;

/// Represents all the states of a monitoree process
#[derive(Debug,Serialize,Deserialize)]
pub enum Status {
    Stopped,
    Killed,
    Failed,
    Succeed,
    Running,
}
impl Status {
    /// Build a Status from the siginfo struct returned by reading a SIGCHLD signalfd 
    /// based on `man 2 sigaction`
    pub fn from_siginfo(info: &siginfo) -> Result<Self, String> {
        // check the signal that was bound to the signalfd this siginfo is the result of.
        if info.ssi_signo != libc::SIGCHLD as u32 {
            return Err("not a SIG_CHLD siginfo".to_string());
        }
        match info.ssi_code {
            libc::CLD_EXITED => {
                match info.ssi_status {
                    libc::EXIT_SUCCESS => Ok(Self::Succeed),
                    libc::EXIT_FAILURE => Ok(Self::Failed),
                    unknown => Err(format!("Unkown return status code '{}' in siginfo", unknown)),
                }
            },
            libc::CLD_KILLED  => Ok(Self::Killed),
            libc::CLD_DUMPED => Ok(Self::Failed),
            libc::CLD_TRAPPED  => Ok(Self::Failed),
            libc::CLD_STOPPED => Ok(Self::Stopped),
            libc::CLD_CONTINUED => Ok(Self::Running),
            unknown => Err(format!("Unkown status code '{}' in siginfo", unknown)),
        }
    }

    /// returns either or not the process is still running.
    pub fn is_terminated(&self) -> bool {
        match *self {
            Self::Stopped => false,
            Self::Killed => true,
            Self::Failed => true,
            Self::Succeed => true,
            Self::Running => false,
        }
    }
}


fn write_return_status(status: &Status, handle: &MonitorHandle)  -> Result<(), Box<dyn std::error::Error>> {
    let mut tmp = handle.status.clone();
    tmp.set_file_name(".status-tmp");
    std::fs::write(&tmp, serde_json::to_string(status)?)
        .map_err(|e| format!("Could not write {:?}, {:?}", &tmp, e))?;
    // rename is atomic if both paths are in the same mount point.
    std::fs::rename(&tmp, &handle.status)
        .map_err(|e| format!("Could not create {:?}, {:?}", &handle.status, e))?;
    Ok(())
}

/// This is the main monitor loop.
///
/// It runs in a single thread while the process is running,
/// it listens on a Unix Domain Socket for commands, and quits when the process is terminated.
///
/// In details, it:
/// * creates an Unix Domain socket to listen for hypervisor commands (`handle.monitor_socket`),
/// * poll for 2 kind of event: the child termination or connection to the socket
/// * when a connection happens, polls the resulting stream as well
/// * read data from the stream and interprets it as a command.
/// * write reponse data into the same stream.
fn monitor_loop(child_pid: Pid, handle: &MonitorHandle) -> Result<(), Box<dyn std::error::Error>> {
    // TODO:
    // * write child status after its termination (into `handle.status`),
    // * notify the hypervisor when the child terminates using the `handle.monitor_socket`
    // * setup command parsing (if needed)

    // create the Unix socket
    let listener = UnixListener::bind(&handle.monitor_socket)
        .map_err(|e| format!("Could not create monitor socket: '{:?}'. '{:?}'", &handle.monitor_socket, e))?;
    listener.set_nonblocking(true)?;
    let listener_fd: RawFd = listener.as_raw_fd();

    // open SIG_CHLD as a RawFd, so we can poll it
    let mut mask = SigSet::empty();
    mask.add(Signal::SIGCHLD);
    let mut sigchild_reader = SignalFd::with_flags(&mask, SfdFlags::SFD_NONBLOCK)?;
    let sigchild_fd: RawFd = sigchild_reader.as_raw_fd();

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
    // same process for `listener_fd`
    let mut listener_event = EpollEvent::new(EpollFlags::EPOLLIN, listener_fd.try_into()?);
    epoll_ctl(epoll_fd, EpollOp::EpollCtlAdd, listener_fd, Some(&mut listener_event))?;

    // create a empty event array, that we will feed to epoll_wait()
    let mut events: Vec<EpollEvent> = (0..10)
        .map(|_| EpollEvent::empty())
        .collect();

    // stores connection streams
    let mut streams_by_fd: HashMap<RawFd, UnixStream> = HashMap::new();
    let mut streams = listener.incoming();
    // assume running status
    let mut process_status: Status = Status::Running;
    // start the event loop:
    'event_loop: while let Ok(event_count) = epoll_wait(epoll_fd, &mut events, WAIT_FOREVER_TIMEOUT) {
        for event_idx in 0..event_count {
            // fetch the data we've associated with the event (file descriptors)
            let fd: RawFd = events[event_idx].data().try_into()?;
            match fd {
                // the monitoree has terminated
                fd if fd == sigchild_fd => {
                    process_status = match sigchild_reader.read_signal() {
                        Ok(Some(siginfo)) => Status::from_siginfo(&siginfo).unwrap_or(Status::Failed),
                        _ => Status::Failed,  // assume failure, if nothing is returned
                    };
                    if process_status.is_terminated() {
                        println!("Child returned with status '{:?}'", &process_status);
                        write_return_status(&process_status, &handle)?;
                        break 'event_loop;
                    }
                },
                // a client connected to the socket
                fd if fd == listener_fd => { 
                    // won't block (epolled)
                    let stream = streams.next()
                        .ok_or(format!("No stream available (unreachable)"))??;
                    // register the stream to the epoll_fd
                    let stream_fd: RawFd = stream.as_raw_fd();
                    println!("Submitting fd: '{}' for listening", &stream_fd);
                    let mut event = EpollEvent::new(EpollFlags::EPOLLIN, stream_fd.try_into()?);
                    epoll_ctl(epoll_fd, EpollOp::EpollCtlAdd, stream_fd, Some(&mut event))?;
                    // store the stream
                    streams_by_fd.insert(stream_fd, stream);

                },
                // data is ready to read on one of the connection
                stream_fd => {
                    // fetch the stream for the fd, and process the request
                    let mut stream = streams_by_fd.remove(&stream_fd)
                        .ok_or(format!("No stream using this RawFD (unreachable)"))?;
                    // unregister the stream from the epoll
                    epoll_ctl(epoll_fd, EpollOp::EpollCtlDel, stream_fd, None)?;
                    // read data from the stream
                    let mut buffer = Vec::new();
                    stream.read_to_end(&mut buffer)?;
                    // TODO: read command, return reponce.
                    println!("reading from '{}': {:?}", &stream_fd, &buffer);
                }
            }
        }
    }
    // Close all opened RawFds
    close(epoll_fd)?;
    //close(sigchild_fd)?;
    // remove the socket file
    std::fs::remove_file(&handle.monitor_socket)?;

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
     *  see `<https://www.freedesktop.org/software/systemd/man/daemon.html>`
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
                            let fd_to_keep: [RawFd; 4] = [
                                STDERR_FILENO as RawFd,
                                STDOUT_FILENO as RawFd,
                                pipe.write_end_fd(),
                                pipe.read_end_fd(),
                            ];
                            close_everything_but(&fd_to_keep[..])
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
                },
                // Caller process,
                // waits for grandchild's PID reception,
                // and returns a `Process` instance
                ForkResult::Parent { child, .. } => {
                    let grandchild: i32 = pipe.recv_int()?;
                    pipe.close()?;
                    // wait for child (so its not a zombie process)
                    let _ = waitpid(child, None);
                    Ok(Process {
                        id: 0,
                        pid: grandchild,
                    })
                },
            }
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
