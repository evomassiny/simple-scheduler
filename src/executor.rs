use crate::models::Process;
use crate::pipe::Pipe;
use crate::process_utils::rename_current_process;
use nix::libc::{close, exit, EXIT_SUCCESS, STDERR_FILENO, STDOUT_FILENO};
use nix::sys::wait::waitpid;
use nix::unistd::{chdir, dup2, execv, fork, setsid, ForkResult};
use rocket::tokio::task::spawn_blocking;
use std::fs::File;
use std::os::unix::io::{AsRawFd, RawFd};
use std::{env, ffi::CString, fs, path::Path};

pub const PROCESS_DIR_ENV_NAME: &'static str = "PROCESS_DIR";
pub const PROCESS_OUTPUT_DIR_PREFIX: &'static str = "process-output-";

pub const PROCESS_STDOUT_FILE_NAME: &'static str = "stdout";
pub const PROCESS_STDERR_FILE_NAME: &'static str = "stderr";
pub const PROCESS_STATUS_FILE_NAME: &'static str = "status";
pub const PROCESS_CWD_DIR_NAME: &'static str = "cwd";

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
    fn blockingly_spawn_daemon(cmd: &str, id: i64) -> Result<Self, Box<dyn std::error::Error>> {
        // create temporary file for stdout
        // fetch $PROCESS_DIR_ENV variable
        let processes_dir: String = env::var(PROCESS_DIR_ENV_NAME)
            .unwrap_or_else(|_| env::temp_dir().to_string_lossy().to_string());

        // create process directory
        let output_dir =
            Path::new(&processes_dir).join(&format!("{}{}", PROCESS_OUTPUT_DIR_PREFIX, id));
        fs::create_dir_all(&output_dir)?;

        // create process CWD directory, the process will live inside
        let cwd = output_dir.join(&PROCESS_CWD_DIR_NAME);
        fs::create_dir_all(&cwd)?;

        // create process stdout/stderr file name
        let stdout_path = output_dir.join(&PROCESS_STDOUT_FILE_NAME);
        let stderr_path = output_dir.join(&PROCESS_STDERR_FILE_NAME);

        // create a pipe to send pid from grandchild to grandparent
        let mut pipe = Pipe::new()?;

        unsafe {
            // fork process
            match fork()? {
                ForkResult::Child => {
                    // detach from any terminal and create an independent session.
                    let _ = setsid().expect("Failed to detach");
                    // for this process, run the task in the child
                    // then send the child process to its grand parent (using the pipe)
                    // the quit.
                    match fork()? {
                        ForkResult::Child => {
                            // This is the main worker process.
                            // In this process, we:
                            //  * close the pipe, as we don't use it here
                            //  * iter /proc/self/fd and close everything but stdout/stderr
                            //  * set stdout and stderr to the temporary files defined above
                            //  * change directory to `$cwd so the process does not block any
                            //  unmounting action.
                            //  * call execv to run the task
                            //
                            //  NOTE: Every error must panic here ! otherwise we will end up we
                            //  several instance of the Web server.

                            // Close all files but stdin/stdout and the pipe:
                            // to do so: iter /proc/self/fd and close everything (but stdin / stdout)
                            let mut descriptors_to_close: Vec<i32> = Vec::new();
                            // first collect all descriptors,
                            {
                                let fd_entries = std::fs::read_dir("/proc/self/fd")
                                    .expect("could not read /proc/self/fd");
                                for entry in fd_entries {
                                    let file_name: String = entry
                                        .expect("Could not read entry")
                                        .file_name()
                                        .to_string_lossy()
                                        .to_string();
                                    let fd: i32 = file_name.parse()?;
                                    if fd != STDOUT_FILENO
                                        && fd != STDERR_FILENO
                                        && !pipe.is_write_fd(fd)
                                    {
                                        descriptors_to_close.push(fd);
                                    }
                                }
                            }
                            // then close them in a second step,
                            // (to avoid closing the /proc/self/fd as we're crawling it.)
                            for fd in descriptors_to_close {
                                let _ = close(fd);
                            }

                            // set STDERR and STDOUT to thetemporary files
                            // * STDERR:
                            let stderr =
                                File::create(stderr_path).expect("could not open stderr path");
                            let raw_stderr_fd: RawFd = stderr.as_raw_fd();
                            let _ = dup2(raw_stderr_fd, STDERR_FILENO)
                                .expect("Failed to redirect stderr");
                            // * STDOUT:
                            let stdout =
                                File::create(stdout_path).expect("could not open stdout path");
                            let raw_stdout_fd: RawFd = stdout.as_raw_fd();
                            let _ = dup2(raw_stdout_fd, STDOUT_FILENO)
                                .expect("Failed to redirect stdout");

                            // Change directory to '$cwd'
                            chdir(&cwd).expect("could not change directory to '$cwd'. ");

                            // Run the task
                            // * build command args
                            let args: Vec<CString> = vec![
                                CString::new("/bin/bash").expect("failed to parse cmd"),
                                CString::new("-c").expect("failed to parse cmd"),
                                CString::new(cmd).expect("failed to parse cmd"),
                            ];

                            // finally for the process:
                            // * the child will execute the command
                            // * the parent will wait for its child to terminate,
                            // and communicate its return status into a file.
                            match fork()? {
                                // This process runs the command
                                ForkResult::Child => {
                                    // Close pipe, at this point only stderr/stdout file should be open
                                    pipe.close();
                                    let _ = execv(&args[0], &args);
                                    unreachable!();
                                }
                                // This process monitor the worker process
                                ForkResult::Parent { child, .. } => {
                                    // change process name to a distinct one (from the server)
                                    let new_name = format!("monitor {}", id);
                                    let _ = rename_current_process(&new_name);
                                    // send the worker PID to the caller process
                                    pipe.close_reader();
                                    let _ = pipe.send_int(child.as_raw());
                                    pipe.close();
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
                ForkResult::Parent { child, .. } => {
                    pipe.close_writer();
                    let grandgrandchild: i32 = pipe.recv_int()?;
                    pipe.close();
                    // wait for child (so its not a zombie process)
                    let _ = waitpid(child, None);
                    return Ok(Process {
                        id: 0,
                        pid: grandgrandchild,
                        stderr: stderr_path.to_string_lossy().to_string(),
                        stdout: stdout_path.to_string_lossy().to_string(),
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
    pub async fn spawn_daemon(cmd: &str, id: i64) -> Result<Self, String> {
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
