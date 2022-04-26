use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Serialize, Deserialize)]
pub enum ExecutorQuery {
    Start,
    Kill,
    Terminate,
    GetStatus,
    SetHypervisorSocket(PathBuf),
    TerminateMonitor,
    RequestStatusNotification,
    Ok,
}
