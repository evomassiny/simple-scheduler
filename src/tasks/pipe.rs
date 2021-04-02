use std::os::unix::io::{AsRawFd, RawFd};
use std::os::unix::net::{UnixStream};
use std::io::{Read,Write};

pub struct Pipe {
    read_end: UnixStream,
    write_end: UnixStream,
}
impl Pipe {

    pub fn new() -> Result<Self, std::io::Error> {
        let (rx, tx) = UnixStream::pair()?;
        rx.shutdown(std::net::Shutdown::Write)?;
        tx.shutdown(std::net::Shutdown::Read)?;
        Ok(Self { read_end: rx, write_end: tx })
    }

    /// send an integer to the pipe
    pub fn send_int(&mut self, value: i32) -> Result<(), std::io::Error> {
        const INT_SIZE: usize = std::mem::size_of::<i32>();
        let bytes: [u8; INT_SIZE] = value.to_be_bytes();
        self.write_end.write_all(&bytes[..])?;
        self.write_end.flush()?;
        Ok(())
    }
    
    /// read an integer value from the pipe
    pub fn recv_int(&mut self) -> Result<i32, std::io::Error> {
        const INT_SIZE: usize = std::mem::size_of::<i32>();
        let mut bytes: [u8; INT_SIZE] = [0; INT_SIZE];
        self.read_end.read_exact(&mut bytes)?;
        Ok(i32::from_be_bytes(bytes))
    }

    pub fn read_end_fd(&self) -> RawFd {
        self.read_end.as_raw_fd()
    }
    pub fn write_end_fd(&self) -> RawFd {
        self.write_end.as_raw_fd()
    }

    pub fn close(&self) -> Result<(), std::io::Error> {
        self.read_end.shutdown(std::net::Shutdown::Read)?;
        self.write_end.shutdown(std::net::Shutdown::Write)?;
        Ok(())
    }
}

