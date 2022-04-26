use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::convert::TryInto;
use std::io::{Read, Write};

use rocket::tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Objects that can be serialized into bytes and parsed from bytes.
pub trait ByteSerializabe {
    fn from_bytes(bytes: &[u8]) -> Result<Self, String>
    where
        Self: Sized;
    fn to_bytes(&self) -> Result<Vec<u8>, String>;
}

impl<SD: Serialize + DeserializeOwned + Sized> ByteSerializabe for SD {
    fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        bincode::deserialize(bytes).map_err(|e| format!("can't parse query: {:?}", e))
    }

    fn to_bytes(&self) -> Result<Vec<u8>, String> {
        bincode::serialize(&self).map_err(|e| format!("can't serialize query: {:?}", e))
    }
}

/// Object that can be read from or written to pipe/channel
pub trait Sendable {
    fn read_from<T: Read>(reader: &mut T) -> Result<Self, Box<dyn std::error::Error>>
    where
        Self: Sized;
    fn send_to<T: Write + Read>(&self, writer: &mut T) -> Result<(), Box<dyn std::error::Error>>;
}

impl<B: ByteSerializabe + Sized> Sendable for B {
    /// Reads one ExecutorQuery from a Reader
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

    /// Send one ExecutorQuery into a Writer
    fn send_to<T: Write + Read>(&self, writer: &mut T) -> Result<(), Box<dyn std::error::Error>> {
        // first read the msg object size, then the object itself.
        let mut bytes: Vec<u8> = self.to_bytes()?;
        let mut msg: Vec<u8> = Vec::new();
        msg.extend_from_slice(&bytes.len().to_be_bytes());
        msg.append(&mut bytes);
        writer.write_all(&msg)?;
        writer.flush()?;
        Ok(())
    }
}

/// Object that can be asynchronaly read from or written to pipe/channel
#[async_trait]
pub trait AsyncSendable {
    /// Reads one StatusUpdate from an AsyncRead instance.
    async fn async_read_from<T: AsyncRead + Unpin + Send>(
        reader: &mut T,
    ) -> Result<Self, Box<dyn std::error::Error>>
    where
        Self: Sized;
    /// Sends one StatusUpdate to an AsyncWrite instance.
    async fn async_send_to<T: AsyncWrite + Unpin + Send>(
        &self,
        writer: &mut T,
    ) -> Result<(), Box<dyn std::error::Error>>;
}

#[async_trait]
impl<B: ByteSerializabe + Sized + Send + Sync> AsyncSendable for B {
    /// Reads one ByteSerializabe from a AsyncReader
    async fn async_read_from<T: AsyncRead + Unpin + Send>(
        reader: &mut T,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // first read the msg object size, then the object itself.
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
        let sendable = Self::from_bytes(&data)?;
        Ok(sendable)
    }

    /// Send one ByteSerializabe into a AsyncWriter
    async fn async_send_to<T: AsyncWrite + Unpin + Send>(
        &self,
        writer: &mut T,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // write object size, then object
        let mut bytes: Vec<u8> = self.to_bytes()?;
        let mut msg: Vec<u8> = Vec::new();
        msg.extend_from_slice(&bytes.len().to_be_bytes());
        msg.append(&mut bytes);
        writer.write_all(&msg).await?;
        writer.flush().await?;
        Ok(())
    }
}
