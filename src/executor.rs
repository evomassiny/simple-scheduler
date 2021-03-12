use crate::models::Process;
use nix::libc::{close, exit, EXIT_SUCCESS, STDERR_FILENO, STDOUT_FILENO};
use nix::sys::wait::waitpid;
use nix::unistd::{chdir, dup2, execv, fork, setsid, ForkResult};
use std::ffi::CString;
use std::fs::File;
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::Path;
use tempfile::NamedTempFile;
use rocket::tokio::task::spawn_blocking;
use crate::pipe::Pipe;


impl Process {

    /**
     * Spawn a `command` in a dedicated daemon process.
     *
     * The daemon process will:
     *  * use '/' as its "Current Working Directory",
     *  * use a temporary file for stdout 
     *  * use a temporary file for stderr 
     *  * not be related to the caller process, it will be a direct child of PID 1
     *
     *  This function use blocking calls, and should not be called in an async 
     *  runtime.
     *
     *  see https://www.freedesktop.org/software/systemd/man/daemon.html
     */
    fn blockingly_spawn_daemon(cmd: &str) -> Result<Self, Box<dyn std::error::Error>> {
        // create temporary file for stdout
        let stdout_file = NamedTempFile::new()?;
        let stdout_path = stdout_file.into_temp_path();
        let stdout_path = stdout_path.keep()?;

        // create temporary file for stderr
        let stderr_file = NamedTempFile::new()?;
        let stderr_path = stderr_file.into_temp_path();
        let stderr_path = stderr_path.keep()?;

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
                            //  * change directory to `/`, so the process does not block any
                            //  unmounting action.
                            //  * call execv to run the task
                            //
                            //  NOTE: Every error must panic here ! otherwise we will end up we
                            //  several instance of the Web server.

                            // Close pipe
                            pipe.close();

                            // Close all files but stdin/stdout:
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
                                    if fd != STDOUT_FILENO && fd != STDERR_FILENO {
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

                            // Change directory to '/'
                            let directory = Path::new("/");
                            chdir(directory).expect("could not change directory to '/'. ");

                            // Run the task
                            // * build command args
                            let args: Vec<CString> = vec![
                                CString::new("/bin/bash").expect("failed to parse cmd"),
                                CString::new("-c").expect("failed to parse cmd"),
                                CString::new(cmd).expect("failed to parse cmd"),
                            ];
                            // * run the function
                            let _ = execv(&args[0], &args);
                            unreachable!();
                        }
                        ForkResult::Parent { child, .. } => {
                            // this process will just send the PID of his child to its parent,
                            // then quit.
                            pipe.close_reader();
                            // TODO: here send child PID
                            let _ = pipe.send_int(child.as_raw());
                            pipe.close();
                            exit(EXIT_SUCCESS);
                        }
                    }
                }
                ForkResult::Parent { child, .. } => {
                    pipe.close_writer();
                    let grandchild: i32 = pipe.recv_int()?;
                    pipe.close();
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
    pub async fn spawn_daemon(cmd: &str) -> Result<Self, String> {
        let command: String = String::from(cmd);
        // wrap the blocking function into its own thread, to avoid messing
        // with the current executor
        spawn_blocking( move || {
            Self::blockingly_spawn_daemon(&command)
                .map_err(|e| format!("Could not spawn daemon task: '{}'", e))
            })
            .await
            .map_err(|e| format!("Could not spawn blocking task: '{}'", e))?
    }
}
