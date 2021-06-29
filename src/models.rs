use chrono::NaiveDateTime;
use crate::workflows::{WorkflowError, WorkFlowTask, WorkFlowGraph};
use crate::tasks::TaskStatus;

/// State of job/task
#[derive(Debug)]
pub enum Status {
    Pending,
    Stopped,
    Killed,
    Failed,
    Succeed,
    Running,
}

impl Status {

    pub fn from_TaskStatus(status: &TaskStatus) -> Self {
        match *status {
            TaskStatus::Pending => Self::Pending,
            TaskStatus::Stopped => Self::Stopped,
            TaskStatus::Killed => Self::Killed,
            TaskStatus::Failed => Self::Failed,
            TaskStatus::Succeed => Self::Succeed,
            TaskStatus::Running => Self::Running,
        }
    }

    pub fn from_u8(value: u8) -> Result<Self, String> {
        match value {
            0 => Ok(Self::Pending),
            1 => Ok(Self::Stopped),
            2 => Ok(Self::Killed),
            3 => Ok(Self::Failed),
            4 => Ok(Self::Succeed),
            5 => Ok(Self::Running),
            v => Err(format!("'{:}' cannot be converted to a Status", v))
        }
    }

    pub fn as_u8(&self) -> u8 {
        match *self {
            Self::Pending => 0,
            Self::Stopped => 1,
            Self::Killed => 2,
            Self::Failed => 3,
            Self::Succeed => 4,
            Self::Running => 5,
        }
    }
}

#[derive(Debug)]
pub struct Job {
    pub id: i32,
    pub name: String,
    pub submit_time: NaiveDateTime,
    pub status: Status,
}

#[derive(Debug)]
pub struct Task {
    pub id: i32,
    pub name: String,
    pub status: Status,
    pub command: String,
    pub job: i32,
}

#[derive(Debug)]
pub struct TaskDependency {
    pub id: i32,
    pub child: i32,
    pub parent: i32,
}
