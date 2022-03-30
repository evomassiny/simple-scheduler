use crate::messaging::{AsyncSendable, RequestResult, TaskStatus, ToClientMsg, ToSchedulerMsg};
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

/// Hypervisor state
pub struct SchedulerServer {
    /// path to the UNIX socket this hypervisor is bound to
    socket: PathBuf,
    /// Database pool (contains tasks/jobs states)
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
        let mut conn = self.pool.acquire().await?;
        let mut running_task_count: usize =
            Task::count_by_status(&Status::Running, &mut conn).await?;
        // select all pending/running job
        // for each, select all associated tasks
        //  * if all are finished set job as Finished
        //  * launch task with met dependancies
        let mut jobs = Job::select_by_status(&Status::Running, &mut conn).await?;
        let pendings = Job::select_by_status(&Status::Pending, &mut conn).await?;
        jobs.extend(pendings);
        'job_loop: for job in jobs {
            let mut batch = Batch::from_job(job, &mut conn).await?;
            'task_loop: loop {
                // bail if all running slots are taken
                if running_task_count == self.max_capacity {
                    break 'job_loop;
                }
                if let Some(task) = batch.next_ready_task().await? {
                    // submit task
                    let task_id = task.id.ok_or(SchedulerServerError::NoSuchTask)?;
                    let commands: Vec<String> = task.command_args.iter().map(|arg| arg.argument.clone()).collect();
                    let handle =
                        TaskHandle::spawn(commands, task_id, Some(self.socket.clone()))
                            .await?;
                    // update task
                    task.handle = handle.handle_string();
                    task.status = Status::Running;
                    let _ = task.update(&mut conn).await?;
                    batch.job.status = Status::Running;
                    let _ = batch.job.update(&mut conn).await?;
                    // start task
                    let _ = handle.start().await?;
                    // update number of running tasks
                    running_task_count += 1;
                } else {
                    break 'task_loop;
                }
            }
        }
        Ok(())
    }

    /// fetch task by its handle, then:
    /// * set its status,
    /// * update its job status as well (if no other task are remaining)
    /// * if the task is finished, load its stderr/stdout, then remove its handle directory
    async fn update_task_state(
        &self,
        handle: &str,
        task_status: &TaskStatus,
    ) -> Result<(), Box<dyn Error>> {
        let mut conn = self.pool.acquire().await?;
        let mut task = Task::get_by_handle(handle, &mut conn).await?;
        task.status = Status::from_task_status(task_status);

        if task.status.is_finished() {
            let task_handle = task.handle();
            // read and store stderr, and stdout
            task.stderr = Some(task_handle.read_stderr().await?);
            task.stdout = Some(task_handle.read_stdout().await?);
            // Clean-up file system
            task_handle.cleanup().await?;
        }
        let _ = task.update(&mut conn).await?;

        let mut job = Job::get_by_id(task.job, &mut conn).await?;

        for task in Task::select_by_job(task.job, &mut conn).await? {
            if !task.status.is_finished() {
                // if there is remaining tasks
                // no need to update job staus
                return Ok(());
            }
        }

        let tasks = Task::select_by_job(task.job, &mut conn).await?;

        // job status:
        // * If all task failed => failure
        // * If ONE task canceled => Canceled
        // * If ONE task Stopped => Stopped
        // * If ONE task Killed => killed
        // * if mix failure/succed => succeed
        let mut job_status = Status::Failed;
        for task in &tasks {
            match task.status {
                Status::Stopped => {
                    job_status = Status::Stopped;
                    break;
                },
                Status::Canceled => {
                    job_status = Status::Canceled;
                    break;
                },
                Status::Killed => {
                    job_status = Status::Killed;
                    break;
                },
                Status::Succeed => {
                    job_status = Status::Succeed;
                },
                _ => {}
            }
        }
        job.status = job_status;
        let _ = job.update(&mut conn).await?;
        Ok(())
    }

    /// Ask each monitor process to SIGKILL its monitoree task
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
                    eprintln!(">>>>>>>>>>>>>>>>>>>>>>>>>");
                    handle.kill().await?;
                    eprintln!("<<<<<<<<<<<<<<<<<<<<<<<<<");
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
            // update task status from the task monitor
            ToSchedulerMsg::StatusUpdate {
                task_handle,
                status,
            } => {
                let task_handle: String =
                    task_handle.into_os_string().to_string_lossy().to_string();
                // update task and job status
                let _ = self
                    .update_task_state(&task_handle, &status)
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

    /// Build a `SchedulerClient`
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
                UnixListener::bind(&self.socket).expect("Cant bind to hypervisor socket.");

            loop {
                match listener.accept().await {
                    Ok((mut stream, _addr)) => {
                        if let Err(e) = self.process_msg(&mut stream).await {
                            // TODO: use syslog or rocket's own logging utility
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
