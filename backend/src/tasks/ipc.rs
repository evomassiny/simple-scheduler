use nix::{
    errno::{errno, Errno},
    libc::{self},
    sys::mman::{mmap, munmap, MapFlags, ProtFlags},
};
use std::os::unix::io::RawFd;

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
    ///         println!("A");
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
    #[allow(clippy::unnecessary_cast)]
    pub fn new() -> Result<Self, String> {
        // SAFETY: safe because
        // * semaphore is allocated through `mmap()`
        // * we never de-reference self.semaphore_ptr, the kernel does it itself
        //   through sem_wait() and sem_post()
        let semaphore_ptr: *mut libc::sem_t = unsafe {
            let semaphore_size = std::num::NonZeroUsize::new_unchecked(std::mem::size_of::<libc::sem_t>());
            // request the OS for a pointer to a piece of shared memory
            // big enough to hold a semaphore
            let semaphore_ptr = mmap(
                None, 
                semaphore_size,
                ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
                MapFlags::MAP_ANONYMOUS | MapFlags::MAP_SHARED,
                0_i32 as RawFd,
                0 as libc::off_t,
            )
            .map_err(|errno| format!("Failed to mmap semaphore: {:?}", errno))?;
            // initialize the semaphore
            let code = libc::sem_init(
                semaphore_ptr as *mut libc::sem_t,
                1 as libc::c_int, // 1 means that the semaphore is shared accross processes
                0 as libc::c_uint,
            );
            // check for semapthore initilization errors
            if code == -1 {
                return Err(format!(
                    "Failed to init semaphore: {:?}",
                    Errno::from_i32(errno())
                ));
            }
            semaphore_ptr as *mut libc::sem_t
        };

        Ok(Self { semaphore_ptr })
    }

    /// Block until a process calls ".release()"
    pub fn wait(&self) -> Result<(), String> {
        // SAFETY: safe because self.semaphore_ptr was initialzed
        let code = unsafe { libc::sem_wait(self.semaphore_ptr) };
        if code == -1 {
            return Err(format!(
                "Failed to wait on semaphore: {:?}",
                Errno::from_i32(errno())
            ));
        }
        Ok(())
    }

    /// release waiting processes
    pub fn release(&self) -> Result<(), String> {
        // SAFETY: safe because self.semaphore_ptr was initialzed
        let code = unsafe { libc::sem_post(self.semaphore_ptr) };
        if code == -1 {
            return Err(format!(
                "Failed to release semaphore: {:?}",
                Errno::from_i32(errno())
            ));
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
            let _ = munmap(
                self.semaphore_ptr as *mut libc::c_void,
                std::mem::size_of::<libc::sem_t>(),
            );
        }
    }
}

#[test]
fn test_barrier() {
    use nix::unistd::{fork, ForkResult};
    use std::io::{Read, Write};
    use std::os::unix::net::UnixStream;
    use std::{thread, time::Duration};

    // build pipe
    let (mut rx, mut tx) = UnixStream::pair().expect("failed to build pipe");
    // make it unidirectional
    rx.shutdown(std::net::Shutdown::Write)
        .expect("closing failed");
    tx.shutdown(std::net::Shutdown::Read)
        .expect("closing failed");
    // remove any read time limit
    rx.set_read_timeout(None).expect("fail to set pipe timeout");

    // what we want to test
    let parent_has_written_barrier = Barrier::new().unwrap();
    let child_has_written_barrier = Barrier::new().unwrap();

    // fork into 2 processes,
    // then write data to the pipe from both processes
    // use barriers to assert a certain ordering,
    // and check the actual "sending" ordering by reading the pipe
    unsafe {
        match fork() {
            Ok(ForkResult::Parent { .. }) => {
                // sleep for a while (100 ms)
                thread::sleep(Duration::from_millis(100));
                // send "A"
                tx.write(b"A").unwrap();
                tx.flush().unwrap();
                // allow child to proceed
                parent_has_written_barrier.release().unwrap();
                // wait for child completion
                child_has_written_barrier.wait().unwrap();
                // read pipe content, should be A then B
                let mut data: [u8; 2] = [0u8; 2];
                rx.read_exact(&mut data).expect("Failed to read pipe");
                assert_eq!(&data, b"AB");
            }
            Ok(ForkResult::Child) => {
                // wait for parent
                parent_has_written_barrier.wait().unwrap();
                // send "B"
                tx.write(b"B").unwrap();
                tx.flush().unwrap();
                // release parent
                child_has_written_barrier.release().unwrap();
            }
            Err(e) => panic!("fork failed: {}", e),
        }
    }
}
