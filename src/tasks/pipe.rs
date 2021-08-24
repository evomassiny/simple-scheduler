use std::io::{Read, Write};
use std::os::unix::io::{AsRawFd, RawFd};
use std::os::unix::net::UnixStream;
use nix::{
    libc::{self},
    sys::mman::{mmap, munmap, msync, ProtFlags, MapFlags, MsFlags},
    errno::{errno,Errno},
};

struct Pipe {
    pub read_end: UnixStream,
    pub write_end: UnixStream,
}
impl Pipe {
    pub fn new() -> Result<Self, std::io::Error> {
        let (rx, tx) = UnixStream::pair()?;
        rx.shutdown(std::net::Shutdown::Write)?;
        tx.shutdown(std::net::Shutdown::Read)?;
        Ok(Self {
            read_end: rx,
            write_end: tx,
        })
    }

    pub fn close(&self) -> Result<(), std::io::Error> {
        self.read_end.shutdown(std::net::Shutdown::Read)?;
        self.write_end.shutdown(std::net::Shutdown::Write)?;
        Ok(())
    }
}

impl Drop for Pipe {
    fn drop(&mut self) {
        let _ = self.close();
    }
}


/// Use this to block a process, until another one decides to release it.
pub struct Barrier {
    semaphore_ptr: *mut libc::sem_t,
}

impl Barrier {

    /// Build a `Barrier` semaphore.
    /// use it to block a processus until another releases it.
    ///
    /// # Usage:
    /// ```
    /// use nix::unistd::{fork,ForkResult};
    /// let barrier = Barrier::new()?;
    ///
    /// match fork()? {
    ///     ForkResult::Child => {
    ///         let _ = barrier.wait().expect("wait failed");
    ///         println!("B");
    ///     }
    ///     // This process monitor the worker process
    ///     ForkResult::Parent { .. } => {
    ///         println!("B");
    ///         let _ = barrier.release().expect("wait failed");
    ///     }
    /// }
    ///
    /// ```
    /// will allways print:
    /// > A
    /// > B
    /// 
    /// # NOTE:
    /// It uses a POSIX semaphore intialized on a piece of memory
    /// shared across processes.
    pub fn new() -> Result <Self, String> {
        // SAFETY: safe because
        // * semaphore is allocated through `mmap()`
        // * we never de-reference self.semaphore_ptr, the kernel does it itself
        //   through sem_wait() and sem_post()
        let semaphore_ptr: *mut libc::sem_t =  unsafe {
            // request the OS for a pointer to a piece of shared memory
            // big enough to hold a semaphore
            let semaphore_ptr = mmap(
                0 as * mut core::ffi::c_void,
                std::mem::size_of::<libc::sem_t>() as libc::size_t,
                ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
                MapFlags::MAP_ANONYMOUS | MapFlags::MAP_SHARED,
                0 as RawFd,
                0 as libc::off_t,
            ).map_err(|errno| format!("Failed to mmap semaphore: {:?}",  errno))?;
            // initialize the semaphore
            let code = libc::sem_init(
                semaphore_ptr as *mut libc::sem_t,
                1 as libc::c_int,               // 1 means that the semaphore is shared accross processes
                0 as libc::c_uint,
            );
            // check for semapthore initilization errors
            if code == -1 {
                return Err(format!("Failed to init semaphore: {:?}", Errno::from_i32(errno())));
            }
            semaphore_ptr as *mut libc::sem_t
        };

        Ok(Self { semaphore_ptr})
    }

    /// Block until a process calls ".release()"
    pub fn wait(&self) -> Result<(), String> {
        // SAFETY: safe because self.semaphore_ptr was initialzed
        let code = unsafe { libc::sem_wait(self.semaphore_ptr)};
        if code == -1 {
            return Err(format!("Failed to wait on semaphore: {:?}", Errno::from_i32(errno())));
        }
        Ok(())
    }
    
    /// release waiting processes
    pub fn release(&self) -> Result<(), String> {
        // SAFETY: safe because self.semaphore_ptr was initialzed
        let code = unsafe {libc::sem_post(self.semaphore_ptr)};
        if code == -1 {
            return Err(format!("Failed to release semaphore: {:?}", Errno::from_i32(errno())));
        }
        Ok(())
    }
}

impl Drop for Barrier {
    fn drop(&mut self) {
        // SAFETY: safe because self.semaphore_ptr was initialzed
        unsafe {
            // destroy the semaphore
            let _ = libc::sem_destroy(self.semaphore_ptr);
            // unmap the shared memory:
            let _ = munmap(self.semaphore_ptr as *mut libc::c_void, std::mem::size_of::<libc::sem_t>());
        }
    }
}

