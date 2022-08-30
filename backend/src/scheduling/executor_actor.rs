use crate::models::{TaskId};

pub trait ExecutorHandle {
    fn kill(&mut self, task: TaskId);
    fn spawn(&mut self, task: TaskId);
}
