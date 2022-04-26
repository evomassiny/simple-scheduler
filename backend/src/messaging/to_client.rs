use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum RequestResult {
    Ok,
    Err(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ToClientMsg {
    RequestResult(RequestResult),
}
