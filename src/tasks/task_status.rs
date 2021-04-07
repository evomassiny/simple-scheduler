use std::path::PathBuf;
use nix::{
    libc,
    sys::signalfd::siginfo,
};
use serde::{Serialize, Deserialize};
use serde_json;
use rocket::tokio::io::{AsyncRead,AsyncWrite,AsyncWriteExt,AsyncReadExt};
use crate::tasks::query::ByteSerializabe;


/// Represents all the states of a monitoree process
#[derive(Debug,Serialize,Deserialize)]
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
            libc::CLD_EXITED => {
                match info.ssi_status {
                    libc::EXIT_SUCCESS => Ok(Self::Succeed),
                    libc::EXIT_FAILURE => Ok(Self::Failed),
                    unknown => Err(format!("Unkown return status code '{}' in siginfo", unknown)),
                }
            },
            libc::CLD_KILLED  => Ok(Self::Killed),
            libc::CLD_DUMPED => Ok(Self::Failed),
            libc::CLD_TRAPPED  => Ok(Self::Failed),
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
    pub fn save_to_file(&self, path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
        let mut tmp = path.clone();
        tmp.set_file_name(".status-tmp");
        std::fs::write(&tmp, serde_json::to_string(self)?)
            .map_err(|e| format!("Could not write {:?}, {:?}", &tmp, e))?;
        // rename is atomic if both paths are in the same mount point.
        std::fs::rename(&tmp, path)
            .map_err(|e| format!("Could not create {:?}, {:?}", path, e))?;
        Ok(())
    }

    /// Reads one Query from an AsyncRead instance.
    pub async fn async_read_from<T: AsyncRead + Unpin>(reader: &mut T) -> Result<Self, Box<dyn std::error::Error>> {
        let mut data: Vec<u8> = Vec::new();
        reader.read_to_end(&mut data).await?;
        let sendable = Self::from_bytes(&data[..])?;
        Ok(sendable)
    }

    /// Sends one Query to an AsyncWrite instance.
    pub async fn async_send_to<T: AsyncWrite + Unpin>(&self, writer: &mut T) -> Result<(), Box<dyn std::error::Error>> {
        writer.write_all(&self.to_bytes()?).await?;
        Ok(())
    }

}
