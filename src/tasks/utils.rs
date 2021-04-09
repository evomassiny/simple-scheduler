use nix::{
    libc::{ c_char, c_int, c_ulong, c_void, memset, prctl, strlen, strncpy, PR_SET_NAME, PT_NULL},
    unistd::{close, dup2},
    sys::{
        signal::{signal, SigHandler, Signal, sigprocmask, SigSet, SigmaskHow},
    },
};
use std::{
    fs::File,
    os::unix::io::{RawFd, IntoRawFd},
    ffi::CString,
    path::Path,
};

/** 
 * Those 2 statis variables file are dedicated to the implementation of `rename_current_process()`.
 *
 *  This function relies on a dirty hack to retreive the `argv[]` pointer:
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
/// similar to C's argv main() argument
static mut ARGV: Option<*const *const u8> = None;
/// similar to C's argc main() argument
static mut ARGC: Option<isize> = None;

/// This function's only purpose is to store the values of `argc` and `argv`.
extern "C" fn init_args(argc: c_int, argv: *const *const u8, _envp: *const *const u8) {
    // SAFETY: This function is executed only once at start-up, _before_ `main()`
    // `argc` and `argv` are garantied to live as long as the program.
    unsafe {
        ARGC = Some(argc as isize);
        ARGV = Some(argv);
    }
}

/// glibc passes argc, argv, and envp to functions in .init_array, as a non-standard extension.
/// (directly inspired from the rust std::sys::args crate, which defines .init_array.00099)
#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[used]
#[link_section = ".init_array.00098"]
static ARGV_SETTER: extern "C" fn(c_int, *const *const u8, *const *const u8) = init_args;

/// This function renames the caller process to `name`.
/// To do so it:
/// * call prctl() to change the name in /proc/${pid}/status
/// * directly write into `argv[0]` (change /proc/$[pid}/cmdline)
///
pub fn rename_current_process(name: &str) -> Result<(), ()> {
    let new_name = String::from(name);
    let name_as_c_string = CString::new(new_name.clone()).map_err(|_| ())?;
    // 1 - call prctl syscall to set the new name,
    // SAFETY: safe but rust does't know it (ffi)
    unsafe {
        prctl(
            PR_SET_NAME,
            name_as_c_string.as_ptr() as c_ulong,
            PT_NULL,
            PT_NULL,
            PT_NULL,
        );
    }
    // 2 - overwrite ARGV[0]
    // SAFETY: mutate **argv, it is safe as long as we don't write
    // more chars than strlen(**argv)
    unsafe {
        if let Some(argv) = ARGV {
            if argv != (PT_NULL as *const *const u8) && *argv != (PT_NULL as *const u8) {
                // first get the old process name to retreive its length
                let old_len = strlen(*argv as *mut c_char);
                let new_len = std::cmp::min(old_len, new_name.len());
                memset(*argv as *mut c_void, '\0' as i32, old_len); // clear original string
                strncpy(*argv as *mut c_char, name_as_c_string.as_ptr(), new_len); // override with new content
            }
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
            let fd: RawFd = file_name.parse().map_err(|_| "parse error".to_string())?;

            // check if the file is not in the black list
            for fd_to_keep in file_descriptors {
                if fd == *fd_to_keep {
                    continue 'directory_listing;
                }
            }
            descriptors_to_close.push(fd);
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
