use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use std::io::{Read, Write};
use std::marker::Sized;

use rocket::tokio::{
    fs::File,
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
};
use std::path::{Path, PathBuf};

use crate::messaging::sendable::{ByteSerializabe, AsyncSendable};

#[derive(Debug, Serialize, Deserialize)]
pub enum ExecutorQuery {
    Start,
    Kill,
    Terminate,
    GetStatus,
    SetHypervisorSocket(Option<PathBuf>),
}
