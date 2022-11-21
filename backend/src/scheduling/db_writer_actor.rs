//!
//! This actor is responsible for:
//! * writting status update to the database,
//! * fetching output from finished tasks,
//! * writting task outputs to the database,
//! * writting new workflows into the database
//!

use crate::models::{Batch, Job, JobId, Status, Task, TaskId, UserId};
use crate::workflows::WorkFlowGraph;
use rocket::tokio::{
    self,
    sync::{
        mpsc::{unbounded_channel, UnboundedSender},
        oneshot::{channel, Sender},
    },
};
use sqlx::sqlite::SqlitePool;

/// Represents the whole set of database write requests
#[derive(Debug)]
pub enum DbWriteRequest {
    /// store a task status,
    /// if the status happens to be a final one, tries to load
    /// the task stderr/stdout
    SetTaskStatus { task_id: TaskId, status: Status },
    /// store a job status
    SetJobStatus { job_id: JobId, status: Status },
    /// add a Job to the database
    InsertJob {
        job: WorkFlowGraph,
        user: UserId,
        resp_channel: Sender<(JobId, Vec<TaskId>, Vec<(TaskId, TaskId)>)>,
    },
}

pub struct DbWriterActor {
    db_pool: SqlitePool,
}

impl DbWriterActor {
    pub fn new(db_pool: SqlitePool) -> Self {
        Self { db_pool }
    }

    /// Create a job from a `WorkFlowGraph`,
    /// and store it inside the database, (along with all the relates models).
    /// Returns a tuple of 3 elements:
    /// 0. the job id,
    /// 1. the vec of all IDs of each tasks
    /// 3. a Vec of all dependencies between tasks,
    ///    each element of this vec being a tuple of the ID of the parent and child task.
    pub async fn insert_job(
        &mut self,
        graph: WorkFlowGraph,
        user: UserId,
    ) -> Result<(JobId, Vec<TaskId>, Vec<(TaskId, TaskId)>), Box<dyn std::error::Error>> {
        let mut conn = self.db_pool.acquire().await?;
        let batch = Batch::from_graph(&graph, user, &mut conn).await?;
        let tasks: Vec<TaskId> = batch.tasks.iter().map(|task| task.id).collect();
        let dependencies: Vec<(TaskId, TaskId)> = batch
            .dependencies
            .iter()
            .map(|dep| (dep.parent, dep.child))
            .collect();
        Ok((batch.job.id, tasks, dependencies))
    }

    pub async fn set_job_status(
        &mut self,
        job: JobId,
        status: Status,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut conn = self.db_pool.acquire().await?;
        let _ = Job::<JobId>::set_status(&mut conn, job, &status).await?;
        Ok(())
    }

    pub async fn set_task_status(
        &mut self,
        task: TaskId,
        status: Status,
    ) -> Result<(), Box<dyn std::error::Error>> {
        println!("db: setting {task} to {status:?}");
        let mut conn = self.db_pool.acquire().await?;
        let _ = Task::<TaskId>::set_status(&mut conn, task, &status).await?;

        if status.is_finished() && !matches!(status, Status::Canceled) {
            let mut task = Task::get_by_id(task, &mut conn).await?;
            let task_handle = task.handle();

            // read and store stderr + stdout
            if let Ok(stderr) = task_handle.read_stderr().await {
                task.stderr = Some(stderr);
            }
            if let Ok(stdout) = task_handle.read_stdout().await {
                task.stdout = Some(stdout);
            }
            // update task state
            let _ = task.save(&mut conn).await?;

            // Ask monitor to clean-up file system and quit
            if let Err(error) = task_handle.terminate_monitor().await {
                eprintln!(
                    "db_writer: (task: {:?}) Failed to terminate monitor process {:?}",
                    task, error
                );
            }
        }
        Ok(())
    }
}

/// Handle to the DataBase writer actor.
#[derive(Clone)]
pub struct DbWriterHandle(pub(crate) UnboundedSender<DbWriteRequest>);

impl DbWriterHandle {
    /// Ask the db_writer_actor to store a `WorkFlowGraph` as a job,
    /// (along with all the relates models).
    ///
    /// Returns a tuple of 3 elements:
    /// 0. the job id,
    /// 1. the vec of all IDs of each tasks
    /// 3. a Vec of all dependencies between tasks,
    ///    each element of this vec being a tuple of the ID of the parent and child task.
    pub async fn insert_job(
        &self,
        job: WorkFlowGraph,
        user: UserId,
    ) -> Option<(JobId, Vec<TaskId>, Vec<(TaskId, TaskId)>)> {
        let (tx, rx) = channel::<(JobId, Vec<TaskId>, Vec<(TaskId, TaskId)>)>();
        let request = DbWriteRequest::InsertJob {
            job,
            user,
            resp_channel: tx,
        };
        if let Err(e) = self.0.send(request) {
            eprintln!("db_writer handle: failed to send InsertJob msg: {:?}", e);
            return None;
        }
        match rx.await {
            Ok(job_summary) => Some(job_summary),
            Err(e) => {
                eprintln!("db_writer handle: InsertJob error: {:?}", e);
                None
            }
        }
    }

    /// Ask the db_writer_actor to update the status of a task
    pub fn set_task_status(&self, task: TaskId, status: Status) {
        if let Err(e) = self.0.send(DbWriteRequest::SetTaskStatus {
            task_id: task,
            status,
        }) {
            eprintln!("db_writer handle: Error while sending write request: {e}");
        }
    }

    /// Ask the db_writer_actor to update the status of a job
    pub fn set_job_status(&self, job: JobId, status: Status) {
        if let Err(e) = self.0.send(DbWriteRequest::SetJobStatus {
            job_id: job,
            status,
        }) {
            eprintln!("db_writer handle: Error while sending write request: {e}");
        };
    }
}

/// Spawn an actor responsible for writting into the SQLite database.
/// Using this actor, and _only_ this actor for database writting
/// avoid race condition when writting data concurrently.
///
/// This function returns a `DbWriterHandle`, use it to comunicate with
/// the actor.
pub fn spawn_db_writer_actor(db_write_pool: SqlitePool) -> DbWriterHandle {
    let mut writer_actor = DbWriterActor {
        db_pool: db_write_pool,
    };
    let (to_writer, mut from_handle) = unbounded_channel::<DbWriteRequest>();

    tokio::spawn(async move {
        loop {
            match from_handle.recv().await {
                Some(DbWriteRequest::SetTaskStatus { task_id, status }) => {
                    if let Err(e) = writer_actor.set_task_status(task_id, status).await {
                        eprintln!("db_writer: Error while setting state {status:?} for task {task_id}: {e:?}");
                    }
                }
                Some(DbWriteRequest::SetJobStatus { job_id, status }) => {
                    if let Err(e) = writer_actor.set_job_status(job_id, status).await {
                        eprintln!("db_writer: Error while setting state {status:?} for job {job_id}: {e:?}");
                    }
                }
                // read job insertion request, return the newly inserted job id
                Some(DbWriteRequest::InsertJob {
                    job,
                    user,
                    resp_channel,
                }) => match writer_actor.insert_job(job, user).await {
                    Ok((job, tasks, dependencies)) => {
                        if let Err(e) = resp_channel.send((job, tasks, dependencies)) {
                            eprintln!("db_writer: failed to send job to db_writer {:?}", e);
                        }
                    }
                    Err(e) => {
                        eprintln!("db_writer: InsertJob error: {:?}", e);
                    }
                },
                None => {
                    continue;
                }
            };
        }
    });
    DbWriterHandle(to_writer)
}
