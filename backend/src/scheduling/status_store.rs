use crate::models::{JobId, TaskId};
use rocket::tokio::{
    self,
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        oneshot::Sender,
    },
};
use std::collections::{HashMap, HashSet};

/// TODO: move into "task status actor"
pub trait StoreHandle {
    fn cancel(&mut self, task: TaskId);
}


#[derive(Debug)]
pub enum StoreError {
    UnknownTask(TaskId),
}

pub enum RunningTaskNotif {
    HasStatus {
        status: Status,
        age: usize,
    }
}

pub enum TaskListResponse {
    NotInCache(TaskId),
    Cached(TaskId, Status),
},

pub enum StoreRequests {
    ListTask {
        tasks: Vec<TaskId>,
        from: Sender<Vec<TaskListResponse>>
    }
}
