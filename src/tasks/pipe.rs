use std::io::{Read, Write};
use std::os::unix::io::{AsRawFd, RawFd};
use std::os::unix::net::UnixStream;

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
pub struct Fence {
    pipe: Pipe,
}
impl Fence {
    const RELASE_SIGNAL: u8 = 1;

    pub fn new() -> Result<Self, std::io::Error> {
        let pipe = Pipe::new()?;
        Ok(Self { pipe })
    }

    /// Send a signal to release waiters.
    /// NOTE: consumes the Fence
    pub fn release_waiter(mut self) -> Result<(), std::io::Error> {
        let signal: [u8; 1] = [Self::RELASE_SIGNAL];
        match self.pipe.write_end.write(&signal) {
            Ok(_) => self.pipe.write_end.flush()?,
            Err(_) => {} // for some reason, the reader always close the pipe first...
        }
        let _ = self.pipe.close();
        Ok(())
    }

    /// Blocks until release signal
    /// NOTE: consumes the Fence
    pub fn wait_for_signal(mut self) -> Result<(), std::io::Error> {
        let mut signal: Vec<u8> = Vec::new();
        while let Err(e) = self.pipe.read_end.read_to_end(&mut signal) {
            eprintln!("Failed to read Fence lock {:?}", e);
            continue;
        }
        let _ = self.pipe.close();
        Ok(())
    }

    pub fn read_end_fd(&self) -> RawFd {
        self.pipe.read_end.as_raw_fd()
    }
    pub fn write_end_fd(&self) -> RawFd {
        self.pipe.write_end.as_raw_fd()
    }
}
