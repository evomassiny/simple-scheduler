use nix::{
    libc::{exit, EXIT_SUCCESS, STDERR_FILENO, STDOUT_FILENO},
    sys::wait::waitpid,
    unistd::{chdir, execv, fork, getpid, setsid, ForkResult},
};
use rocket::tokio::task::spawn_blocking;
use std::{ffi::CString, os::unix::io::RawFd, path::PathBuf};

use crate::tasks::handle::TaskHandle;
use crate::tasks::ipc::Barrier;
use crate::tasks::monitor::Monitor;
//use crate::tasks::task_status::TaskStatus;
use crate::messaging::TaskStatus;
use crate::tasks::utils::{
    assign_file_to_fd, block_sigchild, close_everything_but, rename_current_process,
    reset_signal_handlers,
};

impl TaskHandle {
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
    fn spawn_blocking(
        commands: Vec<CString>,
        id: i64,
        hypervisor_sock: Option<PathBuf>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // create process handle (holds path to process related files)
        let mut handle = Self::from_task_id(id);
        handle
            .create_directory()
            .map_err(|e| format!("can't create task process directory {:?}", e))?;
        // create fence to block the executor process, until the monitor
        // one decides to release it.
        let task_barrier = Barrier::new()?;
        let monitor_ready_barrier = Barrier::new()?;

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

                            // Close all files but stdin/stdout and pipes:
                            let fd_to_keep: [RawFd; 2] =
                                [STDERR_FILENO as RawFd, STDOUT_FILENO as RawFd];
                            close_everything_but(&fd_to_keep[..]).expect("Could not close fd");

                            // set STDERR and STDOUT to files
                            assign_file_to_fd(&handle.stderr_file(), STDERR_FILENO)
                                .expect("Failed to redirect stderr");
                            assign_file_to_fd(&handle.stdout_file(), STDOUT_FILENO)
                                .expect("Failed to redirect stdout");

                            // Change directory to '$cwd'
                            chdir(&handle.working_directory())
                                .expect("could not change directory to '$cwd'. ");

                            // block SIGCHLD signal, so we dont lose some before listening on them
                            // (might not be needed)
                            block_sigchild().expect("Could not block SIGCHLD");

                            // finally fork the process:
                            // * the child will execute the command
                            // * the parent will wait for its child to terminate,
                            // and communicate its return status into a file.
                            match fork()? {
                                // This process runs the command
                                ForkResult::Child => {
                                    // remove all signal masks
                                    reset_signal_handlers().expect("failed to reset signals");
                                    // write pid to file
                                    handle.save_pid(getpid()).expect("failed to write PID file");
                                    // wait for a Start command
                                    task_barrier.wait().expect("Waiting for 'GO/NO GO' failed.");
                                    let _ = execv(&commands[0], &commands);
                                    unreachable!();
                                }
                                // This process monitor the worker process
                                ForkResult::Parent { child, .. } => {
                                    // change process name to a distinct one (from the server)
                                    let monitor_name = format!("monitor {}", id);
                                    let _ = rename_current_process(&monitor_name);

                                    // wait for child completion, and listen for request
                                    let mut monitor = Monitor {
                                        task_barrier: Some(task_barrier),
                                        monitor_ready_barrier: Some(monitor_ready_barrier),
                                        task_pid: child,
                                        task_id: id,
                                        status: TaskStatus::Pending,
                                        update_message_count: 0,
                                        handle,
                                        hypervisor_socket: hypervisor_sock,
                                        update_period_in_sec: 5,
                                    };

                                    if let Err(e) = monitor.run() {
                                        eprintln!(
                                            "Monitor: '{}' failed with '{:?}'",
                                            &monitor_name, e
                                        );
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
                    // wait for child (so its not a zombie process)
                    let _ = waitpid(child, None);
                    // wait for the monitor to be ready before returning
                    monitor_ready_barrier.wait()?;
                    Ok(handle)
                }
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
    pub async fn spawn(
        cmd_args: Vec<String>,
        id: i64,
        hypervisor_sock: Option<PathBuf>,
    ) -> Result<Self, String> {
        let mut commands: Vec<CString> = vec![];

        for arg in cmd_args {
            commands.push(
                CString::new(arg)
                    .map_err(|_| "Could not format argument into CString".to_string())?,
            );
        }
        // wrap the blocking function into its own thread, to avoid messing
        // with the current executor
        spawn_blocking(move || {
            Self::spawn_blocking(commands, id, hypervisor_sock)
                .map_err(|e| format!("Could not spawn daemon task: '{}'", e))
        })
        .await
        .map_err(|e| format!("Could not spawn blocking task: '{}'", e))?
    }
}
