use crate::messaging::{AsyncSendable, RequestResult, TaskStatus, ToClientMsg, ToSchedulerMsg};
use crate::models::{Batch, Job, JobId, Status, Task, TaskView};
use crate::tasks::TaskHandle;
use rocket::tokio::io::AsyncWriteExt;
use rocket::tokio::sync::mpsc::UnboundedSender;
use rocket::tokio::{
    self,
    net::{UnixListener, UnixStream},
    time::Duration,
};
use sqlx::sqlite::SqlitePool;
use std::collections::HashMap;
use std::error::Error;
use std::path::PathBuf;

pub use crate::scheduling::client::SchedulerClient;

/// Messages sent to the Work Queue updater
#[derive(Debug)]
pub enum WorkQueueMsg {
    UpdateTaskStatus {
        task_handle: PathBuf,
        status: TaskStatus,
        version: i64,
    },
    UpdateWorkQueue,
}

/// Hypervisor state
#[derive(Clone)]
pub struct SchedulerServer {
    /// path to the UNIX socket this hypervisor is bound to
    socket: PathBuf,
    /// Database pool (contains tasks/jobs states)
    read_pool: SqlitePool,
    write_pool: SqlitePool,
    /// max number number of parallel running tasks
    nb_of_workers: usize,
}

impl SchedulerServer {
    pub fn new(
        socket: PathBuf,
        read_pool: SqlitePool,
        write_pool: SqlitePool,
        nb_of_workers: usize,
    ) -> Self {
        Self {
            socket,
            read_pool,
            write_pool,
            nb_of_workers,
        }
    }

    /// Check if any job/task is pending, if so, launch them.
    /// prioritize small jobs.
    async fn update_work_queue(&self) -> Result<(), Box<dyn Error>> {
        let mut read_conn = self.read_pool.acquire().await?;
        let mut running_task_count: usize =
            Task::count_by_status(&Status::Running, &mut read_conn).await?;
        // bail if all running slots are taken
        if running_task_count >= self.nb_of_workers {
            return Ok(());
        }
        // select all pending/running job
        let mut jobs = Job::select_by_status(&Status::Running, &mut read_conn).await?;
        let pendings = Job::select_by_status(&Status::Pending, &mut read_conn).await?;
        jobs.extend(pendings);

        // sort the job by their number of tasks, so small jobs get higher priority,
        // this prevets huge jobs from clogging the queue.
        let mut task_count_by_job: HashMap<JobId, usize> = HashMap::new();
        for job in &jobs {
            let count = job.task_count(&mut read_conn).await?;
            task_count_by_job.insert(job.id, count);
        }
        // unwrap is safe because `jobs` was not mutated
        jobs.sort_by_key(|job| *task_count_by_job.get(&job.id).unwrap());

        // for job, select all associated tasks
        //  * if all are finished set job as Finished
        //  * launch task with met dependencies
        'job_loop: for job in jobs {
            let mut batch = Batch::from_job(job, &mut read_conn).await?;
            'task_loop: loop {
                // bail if all running slots are taken
                if running_task_count >= self.nb_of_workers {
                    break 'job_loop;
                }
                if let Some(task) = batch.next_ready_task().await? {
                    // submit task
                    let commands: Vec<String> = task
                        .command_args(&mut read_conn)
                        .await?
                        .iter()
                        .map(|arg| arg.argument.clone())
                        .collect();
                    let handle =
                        TaskHandle::spawn(commands, task.id, Some(self.socket.clone())).await?;
                    // update task
                    task.handle = handle.handle_string();
                    task.status = Status::Running;

                    let _ = {
                        let mut write_conn = self.write_pool.acquire().await?;
                        Task::update_status_and_handle(
                            task.id,
                            &task.status,
                            &task.handle,
                            &mut write_conn,
                        )
                        .await?
                    };
                    batch.job.status = Status::Running;
                    let _ = {
                        let mut write_conn = self.write_pool.acquire().await?;
                        batch.job.save(&mut write_conn).await?
                    };
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
    /// * set its status if the current status version stored is lower than `update_version`,
    /// * update its job status as well (if no other task are remaining)
    /// * if the task is finished, load its stderr/stdout, then remove its handle directory
    async fn update_task_state(
        &self,
        task_id: i64,
        task_status: &TaskStatus,
        update_version: Option<i64>,
        work_queue: &UnboundedSender<WorkQueueMsg>,
    ) -> Result<(), Box<dyn Error>> {
        let status = Status::from_task_status(task_status);

        let version_updated = {
            let mut write_conn = self
                .write_pool
                .acquire()
                .await
                .map_err(|e| format!("acquiring write_pool {:?}", e))?;
            Task::try_update_status(&mut write_conn, task_id, &status, update_version).await?
        };
        if !version_updated {
            return Ok(());
        }
        println!(
            "Update status: {}, {:?}, status version: {:?}",
            &task_id, &*task_status, update_version
        );

        let mut read_conn = self.read_pool.acquire().await?;
        let mut task = Task::get_by_id(task_id, &mut read_conn).await?;

        let task_handle = task.handle();

        if task.status.is_finished() {
            // read and store stderr + stdout
            if let Ok(stderr) = task_handle.read_stderr().await {
                task.stderr = Some(stderr);
            }
            if let Ok(stdout) = task_handle.read_stdout().await {
                task.stdout = Some(stdout);
            }
            // update task state
            let _ = {
                let mut write_conn = self
                    .write_pool
                    .acquire()
                    .await
                    .map_err(|e| format!("acquiring write_pool 2 {:?}", e))?;
                task.save(&mut write_conn).await?
            };
            // Ask monitor to clean-up file system and quit
            if let Err(error) = task_handle.terminate_monitor().await {
                eprintln!("Failed to terminate monitor process {:?}", error);
            }
        }

        let _ = {
            let mut write_conn = self
                .write_pool
                .acquire()
                .await
                .map_err(|e| format!("acquiring write_pool 2 {:?}", e))?;
            let mut read_conn = self
                .read_pool
                .acquire()
                .await
                .map_err(|e| format!("acquiring read_pool 2 {:?}", e))?;
            Job::update_job_state_from_task_ones(&mut read_conn, &mut write_conn, task.job).await?
        };

        work_queue.send(WorkQueueMsg::UpdateWorkQueue)?;
        Ok(())
    }

    async fn update_task_state_by_handle(
        &self,
        task_handle: &str,
        task_status: &TaskStatus,
        update_version: Option<i64>,
        work_queue: &UnboundedSender<WorkQueueMsg>,
    ) -> Result<(), Box<dyn Error>> {
        let mut read_conn = self.read_pool.acquire().await?;
        let task_id = Task::get_task_id_by_handle(&mut read_conn, task_handle).await?;
        self.update_task_state(task_id, task_status, update_version, work_queue)
            .await
    }

    /// Ask all running monitor processes for a status update.
    async fn request_tasks_status_update(
        &self,
        work_queue: &UnboundedSender<WorkQueueMsg>,
    ) -> Result<(), Box<dyn Error>> {
        let mut read_conn = self.read_pool.acquire().await?;
        let non_terminated_tasks =
            TaskView::select_by_status(&Status::Running, &mut *read_conn).await?;

        if !non_terminated_tasks.is_empty() {
            println!(
                "Found {} unfinished tasks. Requesting status...",
                non_terminated_tasks.len()
            );
        }
        for task in &non_terminated_tasks {
            let monitor_handle = &task.handle;

            // request a status notification, will be processed as other ones,
            // (eg: through `self.socket`)
            if let Err(error) = monitor_handle.request_status_notification().await {
                eprintln!(
                    "monitor {:?} did not send status update: {:?}",
                    &monitor_handle.directory, error
                );
            } else {
                continue;
            }
            // This weird pattern avoids "status used accross an await point." error.
            let status;
            {
                status = match monitor_handle.get_status_from_file().await {
                    Ok(status) => status,
                    Err(error) => {
                        eprintln!(
                            "Failed to get status from file, {:?}, assuming that it failed.",
                            error
                        );
                        TaskStatus::Failed
                    }
                };
            }
            // Update task with the same update version as it was, this way,
            // if the task status was set legitimaly while we were requesting those notification,
            // we won't overwrite it.
            let _ = self
                .update_task_state(task.id, &status, task.last_update_version, work_queue)
                .await?;
        }

        Ok(())
    }

    /// Ask each monitor process to SIGKILL its monitoree task
    async fn kill_job(&self, job_id: i64) -> Result<(), Box<dyn std::error::Error>> {
        let mut read_conn = self.read_pool.acquire().await?;
        // update job status
        let mut job = Job::get_by_id(job_id, &mut read_conn).await?;
        job.status = Status::Killed;
        let _ = {
            let mut write_conn = self.write_pool.acquire().await?;
            job.save(&mut write_conn).await?
        };

        // iter all job task,
        // kill the running ones and mark the status of
        // the others as "Canceled"
        for mut task in Task::select_by_job(job_id, &mut read_conn).await? {
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
                    let mut write_conn = self.write_pool.acquire().await?;
                    task.save(&mut write_conn).await?;
                }
                _ => continue,
            }
        }

        Ok(())
    }

    /// Read a task status update from a monitor process through `stream`.
    async fn process_msg(
        &self,
        stream: &mut UnixStream,
        work_queue: UnboundedSender<WorkQueueMsg>,
    ) -> Result<(), Box<dyn Error>> {
        let msg = ToSchedulerMsg::async_read_from(stream).await?;
        match msg {
            // update task status from the task monitor
            ToSchedulerMsg::StatusUpdate {
                task_handle,
                status,
                update_version,
            } => {
                // close connection with monitor process
                use crate::messaging::ExecutorQuery;
                let _ = ExecutorQuery::Ok.async_send_to(&mut *stream).await;
                let _ = stream.shutdown().await;

                // send status message to dedicated actor
                work_queue.send(WorkQueueMsg::UpdateTaskStatus {
                    task_handle,
                    status,
                    version: update_version,
                })?;
            }
            // Update work queue.
            ToSchedulerMsg::JobAppended => {
                // send update request message to dedicated actor
                work_queue.send(WorkQueueMsg::UpdateWorkQueue)?;
            }
            // Kill every tasks of a job
            ToSchedulerMsg::KillJob(job_id) => {
                // kill job
                let res: ToClientMsg = match self.kill_job(job_id).await {
                    Ok(_) => ToClientMsg::RequestResult(RequestResult::Ok),
                    Err(error) => ToClientMsg::RequestResult(RequestResult::Err(error.to_string())),
                };
                // send murder status back to client
                res.async_send_to(stream).await?;
            }
            ToSchedulerMsg::Ok => { /* nothing to do */ }
        }
        Ok(())
    }

    /// reconfigure already running monitor processes
    pub async fn try_re_configure_monitors(&self) -> Result<(), Box<dyn Error>> {
        let mut read_conn = self.read_pool.acquire().await?;
        let non_terminated_tasks =
            TaskView::select_by_status(&Status::Running, &mut read_conn).await?;

        for task in &non_terminated_tasks {
            if let Err(e) = &task
                .handle
                .configure_hypervisor_socket(self.socket.clone())
                .await
            {
                eprintln!(
                    "Failed to re-configure monitor process for task {:?}, {:?}",
                    task.id, e
                );
            }
        }
        Ok(())
    }

    /// Build a `SchedulerClient`
    pub fn client(&self) -> SchedulerClient {
        SchedulerClient {
            socket: self.socket.clone(),
            read_pool: self.read_pool.clone(),
            write_pool: self.write_pool.clone(),
        }
    }

    /// Spawn 3 daemon tasks:
    /// * one to process requests from the scheduler client and monitors process
    /// * one to update the work queue and task status
    /// * one to periodically request the running monitors for status updates
    ///
    /// NOTE: Failure are only logged, this loop should live as long as the web server.
    pub fn start(self) {
        let (work_queue_tx, mut work_queue_rx) =
            tokio::sync::mpsc::unbounded_channel::<WorkQueueMsg>();

        // Spawn actor responsible for processing task status updates
        // This asserts that their is no concurrent writes to the database.
        let server = self.clone();
        let work_queue_handle = work_queue_tx.clone();
        tokio::task::spawn(async move {
            loop {
                match work_queue_rx.recv().await {
                    Some(WorkQueueMsg::UpdateTaskStatus {
                        task_handle,
                        status,
                        version,
                    }) => {
                        // update task and job status
                        let task_handle =
                            task_handle.into_os_string().to_string_lossy().to_string();
                        if let Err(error) = server
                            .update_task_state_by_handle(
                                &task_handle,
                                &status,
                                Some(version),
                                &work_queue_handle,
                            )
                            .await
                        {
                            eprintln!("Error while processing update status message, {:?}", error);
                        }
                    }
                    Some(WorkQueueMsg::UpdateWorkQueue) => {
                        println!("Update work queue");
                        if let Err(e) = server.update_work_queue().await {
                            eprintln!("failed to update work queue {:?}", e);
                        }
                    }
                    None => eprintln!("Could not process update msg"),
                }
            }
        });

        // Spawn actor responsible for periodic monitor status polling
        let server = self.clone();
        let work_queue_handle = work_queue_tx.clone();
        tokio::task::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));

            loop {
                let _ = interval.tick().await;
                println!("Polling task status");
                if let Err(e) = server.request_tasks_status_update(&work_queue_handle).await {
                    eprintln!("failed to poll task statuses {:?}", e);
                }
            }
        });

        // spawn actor responsible for:
        // * responding to SchedulerClient requests (web server)
        // * responding to Monitors requests, (delegate status update to dedicated actor)
        tokio::task::spawn(async move {
            // remove the socket file
            let _ = std::fs::remove_file(&self.socket);
            let listener =
                UnixListener::bind(&self.socket).expect("Cant bind to hypervisor socket.");

            // reconfigure already runnning monitors processes, to use the new hypervisor socket
            if let Err(e) = self.try_re_configure_monitors().await {
                eprintln!("failed to re-configure monitors {:?}", e,);
            }

            loop {
                tokio::select! {
                    // also listen for messages, either from the web app or from a monitor process.
                    connection = listener.accept() => {

                        match connection {
                            Ok((mut stream, _addr)) => {
                                let server = self.clone();
                                let work_queue_tx = work_queue_tx.clone();
                                tokio::task::spawn( async move {
                                    if let Err(e) = server.process_msg(&mut stream, work_queue_tx).await {
                                        eprintln!("Error while processing update msg: {:?}", e);
                                    }
                                });
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
