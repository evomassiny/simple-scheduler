use std::path::PathBuf;
use serde::{Serialize, Deserialize};
use bincode;

#[derive(Serialize, Deserialize)]
pub enum Query {
    GetStatus,
    Terminate,
    Kill,
    SetHypervisorSocket(PathBuf),
}

impl Query {

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        bincode::deserialize(&bytes[..]).map_err(|e| format!("cant parse query: {:?}", e))
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, String> {
        bincode::serialize(&self).map_err(|e| format!("cant serialize query: {:?}", e))
    }
}
