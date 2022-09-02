use crate::models::{Task, TaskCommandArgs, TaskId};
use crate::tasks::TaskHandle;
use rocket::tokio::{
    self,
    sync::mpsc::{unbounded_channel, UnboundedSender},
};
use sqlx::sqlite::SqlitePool;
use std::error::Error;
use std::path::PathBuf;

/// Messages exchanged from the handle to the executor actor.
enum ExecutorMsg {
    Kill(TaskId),
    Spawn(TaskId),
}

/// Must be implemented by the Handle
pub trait ExecutorHandle {
    fn kill(&mut self, task: TaskId);
    fn spawn(&mut self, task: TaskId);
}

/// This actor handle spawning and killing tasks,
/// it should be manipulated through an `ExecutorActorHandle`.
///
/// It reads the database to retreive attributes of tasks.
pub struct ExecutorActor {
    db_read_pool: SqlitePool,
    /// path to the Unix socket, the "task status receiver" listens on.
    /// (passed down to spawned task monitor processes)
    hypervisor_socket: PathBuf,
}

impl ExecutorActor {
    /// submit a task
    async fn spawn_task(&self, task: TaskId) -> Result<(), Box<dyn Error>> {
        let mut read_conn = self.db_read_pool.acquire().await?;
        let commands: Vec<String> = TaskCommandArgs::select_by_task(task, &mut read_conn)
            .await?
            .into_iter()
            .map(|arg| arg.argument)
            .collect();
        let _handle =
            TaskHandle::spawn(commands, task, Some(self.hypervisor_socket.clone())).await?;
        Ok(())
    }

    /// kill a task
    async fn kill_task(&self, task: TaskId) -> Result<(), Box<dyn Error>> {
        // submit task
        let mut read_conn = self.db_read_pool.acquire().await?;
        let handle = Task::<TaskId>::get_handle_by_task_id(&mut read_conn, task).await?;
        handle.kill().await?;
        Ok(())
    }
}

/// Handle to an ExecutorActorHandle.
pub struct ExecutorActorHandle {
    to_executor: UnboundedSender<ExecutorMsg>,
}

impl ExecutorHandle for ExecutorActorHandle {
    fn kill(&mut self, task: TaskId) {
        let _ = self.to_executor.send(ExecutorMsg::Kill(task));
    }

    fn spawn(&mut self, task: TaskId) {
        let _ = self.to_executor.send(ExecutorMsg::Kill(task));
    }
}

pub fn spawn_executor_actor(
    db_read_pool: SqlitePool,
    hypervisor_socket: PathBuf,
) -> ExecutorActorHandle {
    let executor_actor = ExecutorActor {
        db_read_pool,
        hypervisor_socket,
    };
    let (to_executor, mut from_handle) = unbounded_channel::<ExecutorMsg>();
    let handle = ExecutorActorHandle { to_executor };
    tokio::spawn(async move {
        loop {
            let _ = match from_handle.recv().await {
                Some(ExecutorMsg::Kill(task)) => executor_actor.kill_task(task).await,
                Some(ExecutorMsg::Spawn(task)) => executor_actor.spawn_task(task).await,
                None => break,
            };
        }
    });
    handle
}
