use nix::{
    libc::{exit, EXIT_SUCCESS, STDERR_FILENO, STDOUT_FILENO},
    unistd::{chdir, execv, fork, getpid, setsid, ForkResult},
    sys::{
        wait::waitpid,
    },
};
use rocket::tokio::task::spawn_blocking;
use std::{
    os::unix::io::{RawFd, IntoRawFd},
    ffi::CString,
};

use crate::models::Process;
use crate::pipe::Pipe;
use crate::process_utils::{
    reset_signal_handlers,
    assign_file_to_fd,
    close_everything_but_stderr_stdout_and_pipe,
    rename_current_process,
    MonitorHandle,
};

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

                            reset_signal_handlers().expect("failed to reset signals");

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
                                    // Close pipe, at this point only stderr/stdout file should be open
                                    pipe.close().expect("Could not close pipe");
                                    let _ = execv(&args[0], &args);
                                    unreachable!();
                                }
                                // This process monitor the worker process
                                ForkResult::Parent { child, .. } => {
                                    // change process name to a distinct one (from the server)
                                    let new_name = format!("monitor {}", id);
                                    let _ = rename_current_process(&new_name);
                                    pipe.close_reader().expect("Could not close pipe");
                                    // send the monitor PID to the caller process
                                    let self_pid = getpid();
                                    let _ = pipe.send_int(self_pid.as_raw());
                                    pipe.close().expect("Could not close pipe");
                                    // wait for child completion
                                    let _ = waitpid(child, None);
                                    // TODO: store the status of the child into
                                    // ${PROCESS_DIR_ENV_NAME}/${id}/status
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
