use crate::tasks::query::ByteSerializabe;
use nix::{libc, sys::signalfd::siginfo};
use rocket::tokio::{
    fs::File,
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
};
use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use std::path::Path;

/// Represents all the states of a monitoree process
#[derive(Debug, Serialize, Deserialize)]
pub enum TaskStatus {
    Pending,
    Stopped,
    Killed,
    Failed,
    Succeed,
    Running,
}
impl TaskStatus {
    /// Build a Status from the siginfo struct returned by reading a SIGCHLD signalfd
    /// based on `man 2 sigaction`
    pub fn from_siginfo(info: &siginfo) -> Result<Self, String> {
        // check the signal that was bound to the signalfd this siginfo is the result of.
        if info.ssi_signo != libc::SIGCHLD as u32 {
            return Err("not a SIG_CHLD siginfo".to_string());
        }
        match info.ssi_code {
            libc::CLD_EXITED => match info.ssi_status {
                libc::EXIT_SUCCESS => Ok(Self::Succeed),
                libc::EXIT_FAILURE => Ok(Self::Failed),
                unknown => Err(format!(
                    "Unkown return status code '{}' in siginfo",
                    unknown
                )),
            },
            libc::CLD_KILLED => Ok(Self::Killed),
            libc::CLD_DUMPED => Ok(Self::Failed),
            libc::CLD_TRAPPED => Ok(Self::Failed),
            libc::CLD_STOPPED => Ok(Self::Stopped),
            libc::CLD_CONTINUED => Ok(Self::Running),
            unknown => Err(format!("Unkown status code '{}' in siginfo", unknown)),
        }
    }

    /// returns either or not the process is still running.
    pub fn is_terminated(&self) -> bool {
        match *self {
            Self::Pending => false,
            Self::Stopped => false,
            Self::Killed => true,
            Self::Failed => true,
            Self::Succeed => true,
            Self::Running => false,
        }
    }

    /// Saves a Json representation of `&self` into a file.
    pub fn save_to_file(&self, path: &Path) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = path.with_file_name(".status-tmp");
        std::fs::write(&tmp, serde_json::to_string(self)?)
            .map_err(|e| format!("Could not write {:?}, {:?}", &tmp, e))?;
        // rename is atomic if both paths are in the same mount point.
        std::fs::rename(&tmp, path).map_err(|e| format!("Could not create {:?}, {:?}", path, e))?;
        Ok(())
    }

    /// Read status from a JSON file
    pub async fn async_from_file(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        // read status file
        let mut file = File::open(path).await?;
        let mut data: Vec<u8> = vec![];
        file.read_to_end(&mut data).await?;
        let status: Self = serde_json::from_slice(&data)?;
        Ok(status)
    }

    /// Reads one TaskStatus from an AsyncRead instance.
    pub async fn async_read_from<T: AsyncRead + Unpin>(
        reader: &mut T,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        const USIZE_SIZE: usize = std::mem::size_of::<usize>();
        let mut size_buf: [u8; USIZE_SIZE] = [0; USIZE_SIZE];

        // first read the content size
        let mut handle = reader.take(USIZE_SIZE.try_into()?);
        handle.read_exact(&mut size_buf).await?;
        let content_len: usize = usize::from_be_bytes(size_buf);

        // then read the content itself
        let mut data: Vec<u8> = Vec::with_capacity(content_len);
        // SAFETY: safe because we only read its content
        // if it has been overwritten by read_exact()
        unsafe {
            data.set_len(content_len);
        }
        handle = reader.take(content_len.try_into()?);
        handle.read_exact(&mut data).await?;
        let status = Self::from_bytes(&data)?;
        Ok(status)
    }

    /// Sends one TaskStatus to an AsyncWrite instance.
    pub async fn async_send_to<T: AsyncWrite + Unpin>(
        &self,
        writer: &mut T,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut bytes: Vec<u8> = self.to_bytes()?;
        let mut msg: Vec<u8> = Vec::new();
        msg.extend_from_slice(&bytes.len().to_be_bytes());
        msg.append(&mut bytes);
        writer.write_all(&msg).await?;
        writer.flush().await?;
        Ok(())
    }
}
