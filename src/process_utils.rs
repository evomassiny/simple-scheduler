use nix::{
    libc::{ c_char, c_int, c_ulong, c_void, memset, prctl, strlen, strncpy, PR_SET_NAME, PT_NULL},
    unistd::{close, dup2},
    sys::{
        signal::{signal, SigHandler, Signal, sigprocmask, SigSet, SigmaskHow},
    },
};
use std::{
    fs::{self, File},
    os::unix::io::{RawFd, IntoRawFd},
    env,
    ffi::CString,
    path::{Path, PathBuf},
    convert::TryInto,
};

/** 
 * This file is dedicated to the implementation of `rename_current_process()`.
 *  It relies on a dirty hack to retreive the `argv[]` pointer:
 *  * it defines a function that will be placed in the ".init_array.00098" section of the program.
 *  * the glibc calls all functions of this section at the startup with the usual `argc` and `argv`
 *    arguments.
 *  * the previously defined functions stores those values into static variables.
 *  * the `rename_current_process()` uses those variables to directly write data into the `argv[0]` 
 *    array.
 *  
 *  This whole process is necessary because the std::env::vars() does not expose the argv[] pointer
 *  and as far I known, there is no other way to overwrite a process name (as shown in the output
 *  of `ps` or `top`).
 *
 * SAFETY: This uses a lot of C API, and raw pointer dereferencing, 
 * it cannot be implemented safely.
 */ 

  

// similar to C's argc main() argument
static mut ARGC: c_int = 0;
// similar to C's argv main() argument
static mut ARGV: *const *const u8 = PT_NULL as *const *const u8;

/// This function's only purpose is to store the values of `argc` and `argv`.
/// SAFETY: This function mutates static variables, so it is inherently to be unsafe,
/// but, because this function is only executed once (at startup), this should be safe.
extern "C" fn set_args(argc: c_int, argv: *const *const u8, _envp: *const *const u8) {
    unsafe {
        ARGC = argc;
        ARGV = argv;
    }
}

/// glibc passes argc, argv, and envp to functions in .init_array, as a non-standard extension.
/// (directly inspired from the rust std::sys::args crate, which defines .init_array.00099)
#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[used]
#[link_section = ".init_array.00098"]
static ARGV_SETTER: extern "C" fn(c_int, *const *const u8, *const *const u8) = set_args;

/// This function renames the caller process to `name`.
/// To do so it:
/// * call prctl() to change the name in /proc/${pid}/status
/// * directly write into `argv[0]` (change /proc/$[pid}/cmdline)
///
/// SAFETY: This uses a lot of C API, and raw pointer dereferencing, 
/// it cannot be implemented safely.
pub fn rename_current_process(name: &str) -> Result<(), ()> {
    let new_name = String::from(name);
    let name_as_c_string = CString::new(new_name.clone()).map_err(|_| ())?;
    unsafe {
        // 1 - call prctl syscall to set the new name,
        prctl(
            PR_SET_NAME,
            name_as_c_string.as_ptr() as c_ulong,
            PT_NULL,
            PT_NULL,
            PT_NULL,
        );
        // 2 - overwrite ARGV[0]
        if ARGV != (PT_NULL as *const *const u8) {
            // first get the old process name to retreive its length
            let old_len = strlen(*ARGV as *mut c_char);
            let new_len = std::cmp::min(old_len, new_name.len());
            memset(*ARGV as *mut c_void, '\0' as i32, old_len); // clear original string
            strncpy(*ARGV as *mut c_char, name_as_c_string.as_ptr(), new_len); // override with new content
        }
    }
    Ok(())
}

/// reset signal handlers:
/// * unblock all signal mask
/// * reset all signal handler to default
pub fn reset_signal_handlers() -> Result<(), String> {
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
pub fn assign_file_to_fd(file_path: &Path, fd: RawFd) -> Result<(), String> {
    let file = File::create(file_path)
        .map_err(|_| format!("could not open '{}'", file_path.display()))?;
    let _ = dup2(file.into_raw_fd(), fd)
        .map_err(|_| "Failed to redirect stderr".to_string())?;
    Ok(())
}

/// Close all files openned by the caller process but stdin/stdout and the pipe:
/// NOTE: it iters `/proc/self/fd` and call `close()` on everything (but stdin / stdout)
pub fn close_everything_but(file_descriptors: &[RawFd]) -> Result<(), String> {
    let mut descriptors_to_close: Vec<i32> = Vec::new();
    // first collect all descriptors,
    {
        let fd_entries = std::fs::read_dir("/proc/self/fd").expect("could not read /proc/self/fd");
        'directory_listing: for entry in fd_entries {
            let file_name: String = entry
                .map_err(|_| "entry error".to_string())?
                .file_name()
                .to_string_lossy()
                .to_string();
            let int_fd: i32 = file_name.parse().map_err(|_| "parse error".to_string())?;

            // check if the file is not in the black list
            let fd: RawFd = int_fd.try_into().map_err(|_| "parse error".to_string())?;
            for fd_to_keep in file_descriptors {
                if fd == *fd_to_keep {
                    continue 'directory_listing;
                }
            }
            descriptors_to_close.push(int_fd);
        }
    }

    // then close them in a second step,
    // (to avoid closing the /proc/self/fd as we're crawling it.)
    for fd in descriptors_to_close {
        let _ = close(fd); // ignore failure
    }
    Ok(())
}

/// mask SIGCHLD signals so they are stored in a queue by the kernel.
/// This should be done BEFORE forking, 
/// this way the parent won't miss any signals in-between the fork() 
/// and the reading of a SIGCHLD non-blocking Rawfd.
pub fn block_sigchild() -> Result<(), String> {
    let mut mask = SigSet::empty();
    mask.add(Signal::SIGCHLD);
    mask.thread_block().map_err(|_| "Could not mask SIGCHLD".to_string())?;
    Ok(())
}

/// Name of the environment var that holds a path to the process directory
pub const PROCESS_DIR_ENV_NAME: &'static str = "PROCESS_DIR";
/// Prefix of the process directory
pub const PROCESS_OUTPUT_DIR_PREFIX: &'static str = "process-output-";
/// stdout file name
pub const PROCESS_STDOUT_FILE_NAME: &'static str = "stdout";
/// stderr file name
pub const PROCESS_STDERR_FILE_NAME: &'static str = "stderr";
/// status file name
pub const PROCESS_STATUS_FILE_NAME: &'static str = "status";
/// Unix socket: hypervisor <-> monitor
pub const IPC_SOCKET: &'static str = "monitor.sock";
/// CWD directory name
pub const PROCESS_CWD_DIR_NAME: &'static str = "cwd";


/// holds path related to a monitor process
#[derive(Debug)]
pub struct MonitorHandle {
    pub stdout: PathBuf,
    pub stderr: PathBuf,
    pub status: PathBuf,
    pub cwd: PathBuf,
    pub monitor_socket: PathBuf,
}

impl MonitorHandle {
    pub fn from_task_id(task_id: i32) -> Result<Self, Box<dyn std::error::Error>> {
        // create temporary file for stdout
        // fetch $PROCESS_DIR_ENV variable
        let processes_dir: String = env::var(PROCESS_DIR_ENV_NAME)
            .unwrap_or_else(|_| env::temp_dir().to_string_lossy().to_string());

        // create process directory
        let output_dir = Path::new(&processes_dir).join(&format!("{}{}", PROCESS_OUTPUT_DIR_PREFIX, task_id));
        fs::create_dir_all(&output_dir)?;

        // create process CWD directory, the process will live inside
        let cwd = output_dir.join(&PROCESS_CWD_DIR_NAME);
        fs::create_dir_all(&cwd)?;

        // create process stdout/stderr file name
        let stdout_path = output_dir.join(&PROCESS_STDOUT_FILE_NAME);
        let stderr_path = output_dir.join(&PROCESS_STDERR_FILE_NAME);
        let status_path = output_dir.join(&PROCESS_STATUS_FILE_NAME);
        let monitor_socket = output_dir.join(&IPC_SOCKET);
        Ok(
            Self {
                stdout: stdout_path,
                stderr: stderr_path,
                status: status_path,
                cwd: cwd,
                monitor_socket: monitor_socket,

            }
        )
    }
}
