use crate::models::Process;
use tempfile::NamedTempFile;
use nix::unistd::{fork, ForkResult, dup2, setsid, execv};
use nix::libc::{STDOUT_FILENO, STDERR_FILENO};
use nix::sys::wait::{waitpid, WaitPidFlag};
use std::fs::File;
use std::path::Path;
use std::os::unix::io::{AsRawFd, RawFd};
use std::ffi::{CString, CStr};

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



        unsafe {
            // fork process
            match fork()? {
                ForkResult::Child => {
                    /*
                     * TODO:
                     * fork again, so the grand child will not have any parent,
                     * and thus won't be a Zombie process once terminated.
                     * Then pass the grandchild PID to the grand parent through
                     * some kinf of IPC. Fifo should works.
                     */
                    // redirect process stderr to temp file
                    let stderr = File::create(stderr_path)?;
                    let raw_stderr_fd: RawFd = stderr.as_raw_fd();
                    let _ = dup2(raw_stderr_fd, STDERR_FILENO)
                        .expect("Failed to redirect stderr");

                    // redirect process stdout to temp file
                    let stdout = File::create(stdout_path)?;
                    let raw_stdout_fd: RawFd = stdout.as_raw_fd();
                    let _ = dup2(raw_stdout_fd, STDOUT_FILENO)
                        .expect("Failed to redirect stdout");

                    // detach from any terminal and create an independent session.
                    let _ = setsid().expect("Failed to detach");

                    // build command args
                    let args: Vec<CString> = vec![
                        CString::new("/bin/bash").expect("failed to parse cmd"),
                        CString::new("-c").expect("failed to parse cmd"),
                        CString::new(cmd).expect("failed to parse cmd"),
                    ];
                    let _ = execv(
                        &args[0],
                        &args,
                    );
                    unreachable!();
                },
                ForkResult::Parent{ child, .. } => {
                    //waitpid(child, Some(WaitPidFlag::WNOHANG));
                    return Ok( Process {
                            id: 0,
                            pid: child.as_raw(),
                            stderr: stderr_path.to_string_lossy().to_string(),
                            stdout: stdout_path.to_string_lossy().to_string(),
                        }
                    )
                },
            };
        }
    }

}
