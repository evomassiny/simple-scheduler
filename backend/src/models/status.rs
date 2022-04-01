use crate::messaging::TaskStatus;


/// State of job/task
#[derive(Debug, Clone, Copy)]
pub enum Status {
    /// waiting to be scheduled
    Pending,
    /// started then stopped
    Stopped,
    /// killed (sigkill)
    Killed,
    /// returned error status
    Failed,
    /// ran successfully
    Succeed,
    /// currently scheduled
    Running,
    /// killed before being scheduled
    Canceled,
}

impl Status {
    pub fn from_task_status(status: &TaskStatus) -> Self {
        match *status {
            TaskStatus::Pending => Self::Pending,
            TaskStatus::Stopped => Self::Stopped,
            TaskStatus::Killed => Self::Killed,
            TaskStatus::Failed => Self::Failed,
            TaskStatus::Succeed => Self::Succeed,
            TaskStatus::Running => Self::Running,
        }
    }

    pub fn is_failure(&self) -> bool {
        match *self {
            Self::Pending => false,
            Self::Stopped => true,
            Self::Killed => true,
            Self::Failed => true,
            Self::Succeed => false,
            Self::Running => false,
            Self::Canceled => true,
        }
    }

    pub fn is_pending(&self) -> bool {
        matches!(*self, Self::Pending)
    }

    pub fn is_finished(&self) -> bool {
        match *self {
            Self::Pending => false,
            Self::Stopped => true,
            Self::Killed => true,
            Self::Failed => true,
            Self::Succeed => true,
            Self::Running => false,
            Self::Canceled => true,
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
            6 => Ok(Self::Canceled),
            v => Err(format!("'{:}' cannot be converted to a Status", v)),
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
            Self::Canceled => 6,
        }
    }

    pub fn as_proactive_string(&self) -> String {
        match *self {
            Self::Pending => "PENDING".to_string(),
            Self::Stopped => "PAUSED".to_string(),
            Self::Killed => "KILLED".to_string(),
            Self::Failed => "FAILED".to_string(),
            Self::Succeed => "FINISHED".to_string(),
            Self::Running => "RUNNING".to_string(),
            Self::Canceled => "CANCELED".to_string(),
        }
    }
}
