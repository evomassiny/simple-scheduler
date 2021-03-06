use crate::models::Process;
use nix::libc::{close, exit, EXIT_SUCCESS, STDERR_FILENO, STDOUT_FILENO};
use nix::sys::wait::waitpid;
use nix::unistd::{chdir, dup2, execv, fork, pipe, read, setsid, write, ForkResult};
use std::ffi::CString;
use std::fs::File;
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::Path;
use tempfile::NamedTempFile;

/// An Unix unamed pipe.
struct Pipe {
    read_end: Option<RawFd>,
    write_end: Option<RawFd>,
}

impl Pipe {
    /// Creates a unix pipe.
    pub fn new() -> Result<Self, String> {
        match pipe() {
            Ok((read_end, write_end)) => Ok(Self {
                read_end: Some(read_end),
                write_end: Some(write_end),
            }),
            Err(_) => Err("could nor create pipe".into()),
        }
    }

    /// Close writer end of the pipe
    pub fn close_writer(&mut self) {
        if let Some(raw_fd) = self.write_end {
            unsafe {
                close(raw_fd);
            }
        }
        self.write_end = None;
    }

    /// Close reader end of the pipe
    pub fn close_reader(&mut self) {
        if let Some(raw_fd) = self.read_end {
            unsafe {
                close(raw_fd);
            }
        }
        self.read_end = None;
    }

    /// Close pipe
    pub fn close(&mut self) {
        self.close_writer();
        self.close_reader();
    }

    /// send an integer to the pipe
    pub fn send_int(&self, value: i32) -> Result<(), String> {
        const INT_SIZE: usize = std::mem::size_of::<i32>();
        let mut bytes: [u8; INT_SIZE] = value.to_be_bytes();
        let mut total_written: usize = 0;
        while let Ok(written_count) = write(self.write_end.ok_or("write_end closed")?, &mut bytes) {
            total_written += written_count;
            if total_written >= INT_SIZE {
                break;
            }
        }
        return Ok(());
    }

    /// read an integer value from the pipe
    pub fn recv_int(&self) -> Result<i32, String> {
        const INT_SIZE: usize = std::mem::size_of::<i32>();
        let mut bytes: [u8; INT_SIZE] = [0; INT_SIZE];
        let mut total_read: usize = 0;
        while let Ok(read_count) = read(self.read_end.ok_or("read_end closed")?, &mut bytes) {
            total_read += read_count;
            if total_read >= INT_SIZE {
                break;
            }
        }
        return Ok(i32::from_be_bytes(bytes));
    }
}

impl Drop for Pipe {
    fn drop(&mut self) {
        self.close();
    }
}

impl Process {
    pub fn spawn(cmd: &str) -> Result<Self, Box<dyn std::error::Error>> {
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
                    // TODO: here receive and parse grand child PID
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
}
