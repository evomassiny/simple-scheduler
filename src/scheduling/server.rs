use crate::messaging::{AsyncSendable, TaskStatus, ToClientMsg, RequestResult, ToSchedulerMsg};
use crate::models::Model;
use crate::models::{Batch, Job, Status, Task};
use crate::tasks::TaskHandle;
use rocket::tokio::{
    self,
    net::{UnixListener, UnixStream},
};
use sqlx::sqlite::SqlitePool;
use std::error::Error;
use std::path::PathBuf;

pub use crate::scheduling::client::SchedulerClient;

#[derive(Debug)]
pub enum SchedulerServerError {
    NoSuchTask,
}
impl std::fmt::Display for SchedulerServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SchedulerServerError {:?}", &self)
    }
}
impl std::error::Error for SchedulerServerError {}

pub struct SchedulerServer {
    socket: PathBuf,
    pool: SqlitePool,
    /// max number number of parallel running tasks
    max_capacity: usize,
}

impl SchedulerServer {
    pub fn new(socket: PathBuf, pool: SqlitePool) -> Self {
        Self {
            socket,
            pool,
            max_capacity: 40,
        }
    }

    /// Check if any job/task is pending, if so, launch them.
    async fn update_work_queue(&self) -> Result<(), Box<dyn Error>> {
        // select all pending/running job
        // for each, select all associated tasks
        //  * if all are finished set job as Finished
        //  * launch task with met dependancies
        let mut conn = self.pool.acquire().await?;
        // get running and pending jobs
        let mut jobs = Job::select_by_status(&Status::Running, &mut conn).await?;
        let pendings = Job::select_by_status(&Status::Pending, &mut conn).await?;
        jobs.extend(pendings);
        // get the first one with available task
        for job in jobs {
            let mut batch = Batch::from_job(job, &mut conn).await?;
            if let Some(task) = batch.next_ready_task().await? {
                // submit task
                let task_id = task.id.ok_or(SchedulerServerError::NoSuchTask)?;
                let handle =
                    TaskHandle::spawn(&task.command, task_id, Some(self.socket.clone())).await?;
                // update task
                let mut running_task: Task = task.to_owned();
                running_task.handle = handle.handle_string();
                running_task.status = Status::Running;
                let _ = running_task.update(&mut conn).await?;
                // update job
                batch.job.status = Status::Running;
                let _ = batch.job.update(&mut conn).await?;
                // start task
                let _ = handle.start().await?;
            }
        }

        Ok(())
    }

    /// fetch tasks by its handle, set its status, update its job status as well
    async fn update_task_status(
        &self,
        handle: &str,
        task_status: &TaskStatus,
    ) -> Result<(), Box<dyn Error>> {
        let mut conn = self.pool.acquire().await?;
        let mut task = Task::get_by_handle(handle, &mut conn).await?;
        task.status = Status::from_task_status(task_status);
        let _ = task.update(&mut conn).await?;

        let mut job = Job::get_by_id(task.job, &mut conn).await?;
        if task.status.is_failure() {
            job.status = task.status;
            let _ = job.update(&mut conn).await?;
        } else {
            for task in Task::select_by_job(task.job, &mut conn).await? {
                if !task.status.is_finished() {
                    // if there is remaining tasks
                    // no need to update job staus
                    return Ok(());
                }
            }
            job.status = task.status;
            let _ = job.update(&mut conn).await?;
        }
        Ok(())
    }

    async fn kill_job(&self, job_id: i64) -> Result<(), Box<dyn std::error::Error>> {
        let mut conn = self.pool.acquire().await?;
        // update job status
        let mut job = Job::get_by_id(job_id, &mut conn).await?;
        job.status = Status::Killed;
        let _ = job.update(&mut conn).await?;
        
        // iter all job task,
        // kill the running ones and mark the status of
        // the others as "Canceled"
        for mut task in Task::select_by_job(job_id, &mut conn).await? {
            match task.status {
                Status::Running => {
                    let handle = task.handle();
                    handle.kill().await?;
                    // do not update task status,
                    // this will be done after the monitor process sends
                    // its status update.
                }
                Status::Pending => {
                    task.status = Status::Canceled;
                    task.update(&mut conn).await?;
                }
                _ => continue,
            }
        }

        Ok(())
    }

    /// Read a task status update from a monitor process through `stream`.
    async fn process_msg(&self, stream: &mut UnixStream) -> Result<(), Box<dyn Error>> {
        let msg = ToSchedulerMsg::async_read_from(stream).await?;
        match msg {
            // update task status
            ToSchedulerMsg::StatusUpdate {
                task_handle,
                status,
            } => {
                let task_handle: String =
                    task_handle.into_os_string().to_string_lossy().to_string();
                // update task and job status
                let _ = self
                    .update_task_status(&task_handle, &status)
                    .await
                    .map_err(|e| format!("update error: {:?}", e))?;
                // a new task ended, means we could potentially launch the ones that
                // depended of it.
                let _ = self.update_work_queue().await?;
            }
            // Update work queue.
            ToSchedulerMsg::JobAppended => {
                let _ = self.update_work_queue().await?;
            }
            // Kill very task of a job
            ToSchedulerMsg::KillJob(job_id) => {
                // kill job
                let res: ToClientMsg = match self.kill_job(job_id).await {
                    Ok(_) => ToClientMsg::RequestResult(RequestResult::Ok),
                    Err(error) => ToClientMsg::RequestResult(RequestResult::Err(error.to_string())),
                };
                // send murder status back to client
                res.async_send_to(stream).await?;
            }
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
            let listener =
                UnixListener::bind(&self.socket).expect("Can bind to hypervisor socket.");

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
                            &self.socket, e,
                        );
                    }
                }
            }
        });
    }
}
