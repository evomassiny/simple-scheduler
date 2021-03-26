use nix::unistd::{pipe, read, write, close};
use std::os::unix::io::RawFd;

/// An Unix unamed pipe.
pub struct Pipe {
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
    pub fn close_writer(&mut self) -> Result<(), String> {
        if let Some(raw_fd) = self.write_end {
            close(raw_fd).map_err(|_| "Could not close reader end")?;
        }
        self.write_end = None;
        Ok(())
    }

    /// Compare fd to self.
    pub fn is_fd(&self, external_fd: RawFd) -> bool {
        self.is_read_fd(external_fd) || self.is_write_fd(external_fd)
    }
    
    /// Compare fd to self.
    pub fn is_read_fd(&self, external_fd: RawFd) -> bool {
        match self.read_end {
            Some(fd) => fd == external_fd,
            None => false
        }
    }
    /// Compare fd to self.
    pub fn is_write_fd(&self, external_fd: RawFd) -> bool {
        match self.write_end {
            Some(fd) => fd == external_fd,
            None => false
        }
    }

    /// Close reader end of the pipe
    pub fn close_reader(&mut self) -> Result<(), String> {
        if let Some(raw_fd) = self.read_end {
            close(raw_fd).map_err(|_| "Could not close reader end")?;
        }
        self.read_end = None;
        Ok(())
    }

    /// Close pipe
    pub fn close(&mut self) -> Result<(), String> {
        self.close_writer()?;
        self.close_reader()
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
        let _ = self.close();
    }
}
