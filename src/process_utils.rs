use nix::libc::{
    c_char, c_int, c_ulong, c_void, memset, prctl, strlen, strncpy, PR_SET_NAME, PT_NULL,
};
use std::ffi::CString;

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
        // first get the old process name to retreive its length
        let old_len = strlen(*ARGV as *mut c_char);
        let new_len = std::cmp::min(old_len, new_name.len());
        memset(*ARGV as *mut c_void, '\0' as i32, old_len); // clear original string
        strncpy(*ARGV as *mut c_char, name_as_c_string.as_ptr(), new_len); // override with new content
    }
    Ok(())
}
