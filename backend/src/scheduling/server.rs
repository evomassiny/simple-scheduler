use crate::messaging::{AsyncSendable, RequestResult, TaskStatus, ToClientMsg, ToSchedulerMsg};
use crate::models::Model;
use crate::models::{Batch, Job, Status, Task};
use crate::tasks::TaskHandle;
use rocket::tokio::{
    self,
    net::{UnixListener, UnixStream},
    time::{self, Duration, Instant},
};
use sqlx::sqlite::SqlitePool;
use std::error::Error;
use std::path::PathBuf;
use std::collections::HashSet;

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

        if task.status.is_finished() {
            // If the current stored status is already finished,
            // we already set the state of the current task through another mean
            // (eg: by crawling the monitors files directly)
            //
            return Ok(());
        }

        task.status = Status::from_task_status(task_status);
        let task_handle = task.handle();

        if task.status.is_finished() {
            // read and store stderr + stdout
            if let Ok(stderr) = task_handle.read_stderr().await {
                task.stderr = Some(stderr);
            }
            if let Ok(stdout) = task_handle.read_stdout().await {
                task.stdout = Some(stdout);
            }
        }
        let _ = task.update(&mut conn).await?;

        let mut job = Job::get_by_id(task.job, &mut conn).await?;

        let _ = job.update_state_from_task_ones(&mut conn).await?;
        // Clean-up file system
        task_handle.cleanup().await?;
        Ok(())
    }

    /// Check for all unfinished tasks if a status file does not exists,
    /// if so, read it and update the corresponding tasks.
    ///
    /// This is usefull if for whatever reason the connection between a monitor process and 
    /// the scheduler server fails. We can crawl the file system to 
    /// fetch the task status.
    /// 
    /// If everything went well, return the number of updated tasks
    async fn scan_disk_for_finished_tasks(&self) -> Result<usize, Box<dyn Error>> {
        let mut conn = self.pool.acquire().await?;
        let non_terminated_tasks = Task::select_non_terminated(&mut conn).await?;

        let mut count = 0;
        let mut jobs: HashSet<i64> = HashSet::new();
        for task in &non_terminated_tasks {
            let monitor_handle = task.handle();

            if !monitor_handle.has_status_file().await {
                continue;
            }
            let task_status = monitor_handle.get_status().await?;
            let _= self.update_task_state(&task.handle, &task_status).await?;
            // store job id, update it in a later pass
            jobs.insert(task.job);
            // count the number of crawled tasks
            count += 1;
        }

        // update related jobs state
        for job_id in jobs {
            let mut job = Job::get_by_id(job_id, &mut conn).await?;
            let _ = job.update_state_from_task_ones(&mut conn).await?;

        }

        Ok(count)
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

            // setup recurrent timer for "garbage collector"
            let interval = Duration::from_secs(60);
            let timer = time::sleep(interval);
            tokio::pin!(timer);
            
            // start by checking for any already finished tasks
            if let Err(e) = self.scan_disk_for_finished_tasks().await {
                eprintln!(
                    "failed to scan finished tasks {:?}",
                    e,
                );
            }

            loop {
                tokio::select! {
                    // Periodically check for finished but un-handled tasks
                    // if some are found, collect their status and outputs
                    () = &mut timer => {
                        if let Err(e) = self.scan_disk_for_finished_tasks().await {
                            eprintln!(
                                "failed to scan finished tasks {:?}",
                                e,
                            );
                        }
                        if let Err(e) = self.update_work_queue().await {
                            eprintln!(
                                "failed to update work queue {:?}",
                                e,
                            );
                        }
                        // reset timer
                        timer.as_mut().reset(Instant::now() + interval);
                    },
                    // also listen for messages, either from the web app or from a monitor process.
                    connection = listener.accept() => {
                        match connection {
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
                }
            }
        });
    }

}
