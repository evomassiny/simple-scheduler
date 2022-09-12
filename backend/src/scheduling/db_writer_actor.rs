//!
//! This actor is responsible for:
//! * writting status update to the database,
//! * fetching output from finished tasks,
//! * writting task outputs to the database,
//! * writting new workflows into the database
//!

use crate::models::{Batch, Job, JobId, Status, Task, TaskId, User, UserId};
use crate::workflows::WorkFlowGraph;
use rocket::tokio::{
    self,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        oneshot::{channel, Sender},
    },
};
use sqlx::sqlite::SqlitePool;

/// Should represents the whole set of database write requests
pub enum WriteRequest {
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
        println!("setting {task} to {status:?}");
        let mut conn = self.db_pool.acquire().await?;
        let _ = Task::<TaskId>::set_status(&mut conn, task, &status).await?;

        if status.is_finished() {
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
                eprintln!("Failed to terminate monitor process {:?}", error);
            }
        }
        Ok(())
    }
}

/// Handle to the DataBase writer actor.
#[derive(Clone)]
pub struct DbWriterHandle(UnboundedSender<WriteRequest>);

impl DbWriterHandle {
    pub async fn insert_job(&self, job: WorkFlowGraph, user: UserId) -> Option<(JobId, Vec<TaskId>, Vec<(TaskId, TaskId)>)> {
        let (tx, rx) = channel::<(JobId, Vec<TaskId>, Vec<(TaskId, TaskId)>)>();
        self.0.send(WriteRequest::InsertJob {
            job,
            user,
            resp_channel: tx,
        });
        match rx.await {
            Ok(job_summary) => Some(job_summary),
            Err(e) => {
                eprintln!("InsertJob error: {:?}", e);
                None
            },
        }
    }

    pub fn set_task_status(&self, task: TaskId, status: Status) {
        if let Err(e) = self.0.send(WriteRequest::SetTaskStatus {
            task_id: task,
            status,
        }) {
            eprintln!("Error while sending write request: {e}");
        }
    }

    pub fn set_job_status(&self, job: JobId, status: Status) {
        if let Err(e) = self.0.send(WriteRequest::SetJobStatus {
            job_id: job,
            status,
        }) {
            eprintln!("Error while sending write request: {e}");
        }
;
    }
}

pub fn spawn_db_writer_actor(db_write_pool: SqlitePool) -> DbWriterHandle {

    let mut writer_actor = DbWriterActor {
        db_pool: db_write_pool,
    };
    let (to_writer, mut from_handle) = unbounded_channel::<WriteRequest>();

    tokio::spawn(async move {
        loop {
            match from_handle.recv().await {
                Some(WriteRequest::SetTaskStatus { task_id, status }) => {
                    if let Err(e) = writer_actor.set_task_status(task_id, status).await {
                        eprintln!("Error while setting state {status:?} for task {task_id}: {e:?}");
                    }
                }
                Some(WriteRequest::SetJobStatus { job_id, status }) => {
                    if let Err(e) = writer_actor.set_job_status(job_id, status).await {
                        eprintln!("Error while setting state {status:?} for job {job_id}: {e:?}");
                    }
                }
                // read job insertion request, return the newly inserted job id
                Some(WriteRequest::InsertJob {
                    job,
                    user,
                    resp_channel,
                }) => {
                    match writer_actor.insert_job(job, user).await {
                        Ok((job, tasks, dependencies)) => {
                            resp_channel.send((job, tasks, dependencies));
                        },
                        Err(e) => {
                            eprintln!("db_writer: InsertJob error: {:?}", e);
                        },
                    }
                }
                None => {
                    continue;
                },
            };
        }
    });
    DbWriterHandle(to_writer)
}
