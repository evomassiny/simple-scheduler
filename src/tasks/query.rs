use std::path::PathBuf;
use std::io::{Read, Write};
use std::marker::Sized;
use serde::{Serialize, Deserialize};
use serde::de::DeserializeOwned;
use async_trait::async_trait;
use rocket::tokio::io::{AsyncRead,AsyncWrite,AsyncWriteExt,AsyncReadExt};
use bincode;

#[derive(Debug, Serialize, Deserialize)]
pub enum Query {
    Start,
    GetStatus,
    Terminate,
    Kill,
    SetHypervisorSocket(PathBuf),
}

pub trait ByteSerializabe {
    fn from_bytes(bytes: &[u8]) -> Result<Self, String>
        where Self: Sized;
    fn to_bytes(&self) -> Result<Vec<u8>, String>;
}

impl<SD: Serialize + DeserializeOwned  + Sized > ByteSerializabe for SD {
    fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        bincode::deserialize(&bytes).map_err(|e| format!("cant parse query: {:?}", e))
    }

    fn to_bytes(&self) -> Result<Vec<u8>, String> {
        bincode::serialize(&self).map_err(|e| format!("cant serialize query: {:?}", e))
    }
}

#[async_trait]
pub trait Sendable {

    fn read_from<T: Read>(reader: &mut T) -> Result<Self, Box<dyn std::error::Error>>
        where Self: Sized;
    fn send_to<T: Write + Read>(&self, writer: &mut T) -> Result<(), Box<dyn std::error::Error>>;
}

impl<B: ByteSerializabe + Sized > Sendable for B {

    /// Reads one Query from a Reader
    fn read_from<T: Read>(reader: &mut T) -> Result<Self, Box<dyn std::error::Error>> {
        let mut data: Vec<u8> = Vec::new();
        reader.read_to_end(&mut data)?;
        let sendable = Self::from_bytes(&data[..])?;
        Ok(sendable)
    }

    /// Send one Query into a Writer
    fn send_to<T: Write + Read>(&self, writer: &mut T) -> Result<(), Box<dyn std::error::Error>> {
        writer.write_all(&self.to_bytes()?)?;
        Ok(())
    }
}

impl Query {

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
