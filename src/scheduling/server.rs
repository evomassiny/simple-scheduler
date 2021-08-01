use rocket::tokio::{
    net::{UnixListener, UnixStream},
    self,
};
use crate::messaging::{ToSchedulerMsg, AsyncSendable};
use crate::models::{Task,Job,Status};
use sqlx::sqlite::SqlitePool;
use std::path::PathBuf;
use std::error::Error;

pub use crate::scheduling::client::SchedulerClient;

pub struct SchedulerServer {
    socket: PathBuf,
    pool: SqlitePool,
    /// max number number of parallel running tasks
    max_capacity: usize,
}

impl SchedulerServer {

    pub fn new(socket: PathBuf, pool: SqlitePool) -> Self {
        Self { socket, pool, max_capacity: 40 }
    }

    /// Check if any job/task is pending, if so, launch them.
    async fn update_work_queue(&self) -> Result<(), Box<dyn Error>> {
        let mut conn = self.pool.acquire().await?;
        let pendings = Job::select_by_status(&Status::Pending, &mut conn)
            .await?;
        for job in pendings {
            ;
        }
        // TODO:
        // select all pending/running job
        // for each, select all associated tasks
        //  * if all are finished set job as Finished
        //  * launch task with met dependancies

        Ok(())
    }

    /// Read a task status update from a monitor process through `stream`.
    async fn process_msg(&self, stream: &mut UnixStream) -> Result<(), Box<dyn Error>> {
        let msg = ToSchedulerMsg::async_read_from(stream).await?;
        match msg {
            // update task status
            ToSchedulerMsg::StatusUpdate { task_handle, status } => {
                let task_handle: String = task_handle
                    .into_os_string()
                    .to_string_lossy()
                    .to_string();
                let mut conn = self.pool.acquire().await?;
                let _ = Task::select_by_handle_and_set_status(&task_handle, &status, &mut conn)
                    .await
                    .map_err(|e| format!("update error: {:?}", e))?;
                // a new task ended, means we could potentially launch the ones that
                // depended of it.
                let _ = self.update_work_queue().await?;
            },
            // Update work queue.
            ToSchedulerMsg::JobAppended => {
                let _ = self.update_work_queue().await?;
            },
        }
        Ok(())
    }

    pub fn client(&self) -> SchedulerClient {
        SchedulerClient { 
            socket: self.socket.clone(),
            pool: self.pool.clone(),
        }
    }

    /// Listen on a unix domain socket for monitors status update messages.
    /// Updates the Task status.
    /// 
    /// NOTE: Failure are only logged, this loop should live as long as the web server.
    pub fn start(self) {
        tokio::task::spawn(async move {
            // remove the socket file
            let _ = std::fs::remove_file(&self.socket);
            let listener = UnixListener::bind(&self.socket).expect("Can bind to hypervisor socket.");

            loop {
                match listener.accept().await {
                    Ok((mut stream, _addr)) => {
                        if let Err(e) = self.process_msg(&mut stream).await {
                            eprintln!("Error while processing update msg: {:?}", e);
                        }
                    }
                    Err(e) => { 
                        eprintln!(
                            "Connection to hypervisor socket '{:?}' failed: {:?}",
                            &self.socket,
                            e,
                        );
                    }
                }
            }
        });
    }

}
