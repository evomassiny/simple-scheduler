mod task_status;
mod pipe;
mod monitor;
mod monitor_handle;
mod utils;
mod executor;
mod query;

pub use crate::tasks::monitor_handle::MonitorHandle;
pub use crate::tasks::task_status::TaskStatus;

#[derive(Debug)]
pub struct TaskProcess {
    pub id: i32,
    pub pid: i32,
    pub handle: MonitorHandle,
}


