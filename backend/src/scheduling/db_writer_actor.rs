//!
//! This actor is responsible for:
//! * writting status update to the database,
//! * fetching output from finished tasks,
//! * writting task outputs to the database,
//! * writting new workflows into the database
//!

use crate::models::{Batch, JobId, User, UserId};
use crate::models::{JobId, Status, TaskId};
use crate::workflows::WorkFlowGraph;
use sqlx::sqlite::SqlitePool;
use rocket::tokio::{
    self,
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        oneshot::Sender,
    },
};

/// Should represents the whole set of database write requests
pub enum WriteRequest {
    /// store a task status,
    /// if the status happens to be a final one, tries to load 
    /// the task stderr/stdout
    SetTaskStatus { task_id: TaskId, status: Status },
    /// store a job status
    SetJobStatus { job_id: JobId, status: Status },
    /// add a Job to the database
    AddJob { job: WorkFlowGraph, user: UserId,  },
}

pub struct DBWriterActor {
    db_pool: SqlitePool,
}

impl DBWriterActor {
    pub fn new(db_pool: SqlitePool) -> Self {
        Self { db_pool }
    }

    pub async fn add_job(
        &mut self,
        graph: WorkFlowGraph,
        user: UserId,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let batch = Batch::from_graph(&graph, user, &mut self.db_pool).await?;
        Ok(())
    }

    pub async fn set_job_status(
        &mut self,
        task: TaskId,
        status: Status,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // TODO:
        // * fetch stderr/stdout if status is terminated
        // * save status
        todo!();
    }

    pub async fn set_task_status(
        &mut self,
        task: TaskId,
        status: Status,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // TODO:
        // * fetch stderr/stdout if status is terminated
        // * save status
        todo!();
    }
}
