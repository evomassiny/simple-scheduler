use rocket::tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use std::io::{Read, Write};
use std::marker::Sized;
use std::path::PathBuf;

#[derive(Debug, Serialize, Deserialize)]
pub enum Query {
    Start,
    Kill,
    Terminate,
    GetStatus,
    SetHypervisorSocket(Option<PathBuf>),
}

pub trait ByteSerializabe {
    fn from_bytes(bytes: &[u8]) -> Result<Self, String>
    where
        Self: Sized;
    fn to_bytes(&self) -> Result<Vec<u8>, String>;
}

impl<SD: Serialize + DeserializeOwned + Sized> ByteSerializabe for SD {
    fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        bincode::deserialize(&bytes).map_err(|e| format!("cant parse query: {:?}", e))
    }

    fn to_bytes(&self) -> Result<Vec<u8>, String> {
        bincode::serialize(&self).map_err(|e| format!("cant serialize query: {:?}", e))
    }
}

pub trait Sendable {
    fn read_from<T: Read>(reader: &mut T) -> Result<Self, Box<dyn std::error::Error>>
    where
        Self: Sized;
    fn send_to<T: Write + Read>(&self, writer: &mut T) -> Result<(), Box<dyn std::error::Error>>;
}

impl<B: ByteSerializabe + Sized> Sendable for B {
    /// Reads one Query from a Reader
    fn read_from<T: Read>(reader: &mut T) -> Result<Self, Box<dyn std::error::Error>> {
        const USIZE_SIZE: usize = std::mem::size_of::<usize>();
        let mut size_buf: [u8; USIZE_SIZE] = [0; USIZE_SIZE];

        let mut handle = reader.take(USIZE_SIZE.try_into()?);
        handle.read_exact(&mut size_buf)?;
        let content_len: usize = usize::from_be_bytes(size_buf);

        let mut data: Vec<u8> = vec![0; content_len];
        handle = reader.take(content_len.try_into()?);
        handle.read_exact(&mut data)?;
        let sendable = Self::from_bytes(&data)?;
        Ok(sendable)
    }

    /// Send one Query into a Writer
    fn send_to<T: Write + Read>(&self, writer: &mut T) -> Result<(), Box<dyn std::error::Error>> {
        let mut bytes: Vec<u8> = self.to_bytes()?;
        let mut msg: Vec<u8> = Vec::new();
        msg.extend_from_slice(&bytes.len().to_be_bytes());
        msg.append(&mut bytes);
        writer.write_all(&msg)?;
        writer.flush()?;
        Ok(())
    }
}

/// Read an `ByteSerializabe` struct from an AsyncRead instance.
pub async fn async_read_from<T: AsyncRead + Unpin, R: ByteSerializabe>(
        reader: &mut T,
    ) -> Result<R, Box<dyn std::error::Error>> {
    use std::mem::size_of;
    let mut size_buf: [u8; size_of::<usize>()] = [0; size_of::<usize>()];

    // first read the content size
    let mut handle = reader.take(size_of::<usize>().try_into()?);
    handle.read_exact(&mut size_buf).await?;
    let content_len: usize = usize::from_be_bytes(size_buf);

    // then read the content itself
    let mut data: Vec<u8> = vec![0; content_len];
    handle = reader.take(content_len.try_into()?);
    handle.read_exact(&mut data).await?;
    let sendable = R::from_bytes(&data)?;
    Ok(sendable)
}

/// Sends an `ByteSerializabe` struct to an AsyncWrite instance.
pub async fn async_send_to<T: AsyncWrite + Unpin, R: ByteSerializabe>(
    obj: &R,
    writer: &mut T,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut bytes: Vec<u8> = obj.to_bytes()?;
    let mut msg: Vec<u8> = Vec::new();
    msg.extend_from_slice(&bytes.len().to_be_bytes());
    msg.append(&mut bytes);
    writer.write_all(&msg).await?;
    writer.flush().await?;
    Ok(())
}

impl Query {
    /// Reads one Query from an AsyncRead instance.
    pub async fn async_read_from<T: AsyncRead + Unpin>(
        reader: &mut T,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        async_read_from(reader).await
    }

    /// Sends one Query to an AsyncWrite instance.
    pub async fn async_send_to<T: AsyncWrite + Unpin>(
        &self,
        writer: &mut T,
    ) -> Result<(), Box<dyn std::error::Error>> {
        async_send_to(self, writer).await
    }
}
