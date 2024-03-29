use crate::models::TaskId;
use rocket::tokio::{fs::File, io::AsyncReadExt};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Represents all the states of a monitoree process
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Pending,
    Stopped,
    Killed,
    Failed,
    Succeed,
    Running,
}
impl TaskStatus {
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
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MonitorMsg {
    StatusBroadcast {
        task_id: TaskId,
        status: TaskStatus,
        update_version: usize,
    },
    SuicideNote {
        task_id: TaskId,
    },
    Ok,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ToSchedulerMsg {
    // TODO: rename
    KillJob(i64),
    JobAppended,
    Ok,
}
