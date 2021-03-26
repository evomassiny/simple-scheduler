use crate::models::Process;
use crate::pipe::Pipe;
use crate::process_utils::rename_current_process;
use nix::{
    libc::{exit, EXIT_SUCCESS, STDERR_FILENO, STDOUT_FILENO},
    unistd::{close, chdir, dup2, execv, fork, getpid, setsid, ForkResult},
    sys::{
        wait::waitpid,
        signal::{signal, SigHandler, Signal, sigprocmask, SigSet, SigmaskHow},
    },
};
use rocket::tokio::task::spawn_blocking;
use std::fs::File;
use std::os::unix::io::{RawFd, IntoRawFd};
use std::{env, ffi::CString, fs, path::Path};

pub const PROCESS_DIR_ENV_NAME: &'static str = "PROCESS_DIR";
pub const PROCESS_OUTPUT_DIR_PREFIX: &'static str = "process-output-";

pub const PROCESS_STDOUT_FILE_NAME: &'static str = "stdout";
pub const PROCESS_STDERR_FILE_NAME: &'static str = "stderr";
pub const PROCESS_STATUS_FILE_NAME: &'static str = "status";
pub const PROCESS_CWD_DIR_NAME: &'static str = "cwd";

/// reset signal handlers:
/// * unblock all signal mask
/// * reset all signal handler to default
fn reset_signals() -> Result<(), String> {
    // clear signal mask
    sigprocmask(SigmaskHow::SIG_UNBLOCK, Some(&SigSet::all()), None)
        .map_err(|_| "Failed to clean signal mask".to_string())?;
    // reset all sigaction to default
    for sig in Signal::iterator() {
        // SIGKILL and SIGSTOP cannot be altered on unix
        if sig == Signal::SIGKILL || sig == Signal::SIGSTOP {
            continue;
        }
        unsafe { 
            signal(sig, SigHandler::SigDfl)
                .map_err(|_| format!("Could not reset signal '{}' to default", sig.as_str()))?;
        }
    }
    Ok(())
}

/// use `dup2()` to open a file and assign it to a given File descriptor
/// (even if it already exists)
fn assign_file_to_fd(file_path: &Path, fd: RawFd) -> Result<(), String> {
    let file = File::create(file_path)
        .map_err(|_| format!("could not open '{}'", file_path.display()))?;
    let _ = dup2(file.into_raw_fd(), fd)
        .map_err(|_| "Failed to redirect stderr".to_string())?;
    Ok(())
}

/// Close all files openned by the caller process but stdin/stdout and the pipe:
/// NOTE: it iters `/proc/self/fd` and call `close()` on everything (but stdin / stdout)
fn close_everything_but_stderr_stdout_and_pipe(pipe: &Pipe) -> Result<(), String> {
    let mut descriptors_to_close: Vec<i32> = Vec::new();
    // first collect all descriptors,
    {
        let fd_entries = std::fs::read_dir("/proc/self/fd").expect("could not read /proc/self/fd");
        for entry in fd_entries {
            let file_name: String = entry
                .map_err(|_| "entry error".to_string())?
                .file_name()
                .to_string_lossy()
                .to_string();
            let fd: i32 = file_name.parse().map_err(|_| "parse error".to_string())?;
            if fd != STDOUT_FILENO && fd != STDERR_FILENO && !pipe.is_fd(fd) {
                descriptors_to_close.push(fd);
            }
        }
    }
    // then close them in a second step,
    // (to avoid closing the /proc/self/fd as we're crawling it.)
    for fd in descriptors_to_close {
        let _ = close(fd); // ignore failure
    }
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
                            assign_file_to_fd(&stderr_path, STDERR_FILENO)
                                .expect("Failed to redirect stderr");
                            assign_file_to_fd(&stdout_path, STDOUT_FILENO)
                                .expect("Failed to redirect stdout");

                            // Change directory to '$cwd'
                            chdir(&cwd).expect("could not change directory to '$cwd'. ");

                            reset_signals().expect("failed to reset signals");

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
