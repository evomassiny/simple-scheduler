use crate::messaging::{AsyncSendable, RequestResult, ToClientMsg, ToSchedulerMsg};
use crate::models::{Batch, ModelError};
use crate::tokio::net::UnixStream;
use crate::workflows::WorkFlowGraph;
use rocket::fs::TempFile as RocketTempFile;
use rocket::tokio::{self, fs::File, io::AsyncReadExt};
use sqlx::sqlite::SqlitePool;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use tempfile::NamedTempFile;

#[derive(Debug)]
pub enum SchedulerClientError {
    KillFailed(String),
    SchedulerConnectionError,
    RequestSendingError,
}
impl std::fmt::Display for SchedulerClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SchedulerClientError {:?}", &self)
    }
}
impl std::error::Error for SchedulerClientError {}

/// Struct handling handling communication
/// with the scheduler (eg: hypervisor).
pub struct SchedulerClient {
    pub socket: PathBuf,
    pub pool: SqlitePool,
}

impl SchedulerClient {
    pub async fn connect_to_scheduler(&self) -> Result<UnixStream, SchedulerClientError> {
        UnixStream::connect(&self.socket)
            .await
            .map_err(|_| SchedulerClientError::SchedulerConnectionError)
    }

    /// Submit a Job with a single task: a shell command
    pub async fn submit_command_job(
        &self,
        job_name: &str,
        task_name: &str,
        cmd: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut conn = self.pool.acquire().await?;
        let batch = Batch::from_shell_command(job_name, task_name, cmd, &mut conn).await;
        println!("batch: {:?}", &batch);

        let mut to_hypervisor = self.connect_to_scheduler().await?;
        let _ = ToSchedulerMsg::JobAppended
            .async_send_to(&mut to_hypervisor)
            .await?;
        Ok(())
    }

    /// parse a workflow file, and submit the parsed job
    pub async fn submit_workflow(
        &self,
        workflow_path: &Path,
    ) -> Result<i64, Box<dyn std::error::Error>> {
        let mut conn = self.pool.acquire().await?;

        // read file content
        let mut file = File::open(workflow_path).await?;
        let mut content: String = String::new();
        file.read_to_string(&mut content).await?;

        // parse as workflow
        let graph: WorkFlowGraph = WorkFlowGraph::from_str(&content)?;

        // Save to DB
        let batch = Batch::from_graph(&graph, &mut conn).await?;

        // warn hypervisor that new jobs are available
        let mut to_hypervisor = self.connect_to_scheduler().await?;
        let _ = ToSchedulerMsg::JobAppended
            .async_send_to(&mut to_hypervisor)
            .await?;

        // return job id
        let job_id = batch.job.id.ok_or(ModelError::ModelNotFound)?;
        Ok(job_id)
    }

    /// parse a workflow file from uploaded data, then submit it
    pub async fn submit_from_tempfile(
        &self,
        uploaded_file: &mut RocketTempFile<'_>,
    ) -> Result<i64, Box<dyn std::error::Error>> {
        // first persist the file
        // * use a NamedtempFile to get a free path
        let file = tokio::task::spawn_blocking(NamedTempFile::new)
            .await
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "spawn_block panic")
            })??;
        // * move the tempfile content to it
        uploaded_file.persist_to(file.path()).await?;

        let job_id = self.submit_workflow(file.path()).await?;
        Ok(job_id)
    }

    pub async fn kill_job(&self, job_id: i64) -> Result<(), SchedulerClientError> {
        let mut to_hypervisor = self
            .connect_to_scheduler()
            .await
            .map_err(|_| SchedulerClientError::SchedulerConnectionError)?;
        let _ = ToSchedulerMsg::KillJob(job_id)
            .async_send_to(&mut to_hypervisor)
            .await
            .map_err(|_| SchedulerClientError::RequestSendingError)?;
        let request_result = ToClientMsg::async_read_from(&mut to_hypervisor)
            .await
            .map_err(|_| SchedulerClientError::SchedulerConnectionError)?;
        match request_result {
            ToClientMsg::RequestResult(rr) => match rr {
                RequestResult::Ok => Ok(()),
                RequestResult::Err(error) => {
                    Err(SchedulerClientError::KillFailed(error.to_string()))
                }
            },
            _ => Err(SchedulerClientError::KillFailed(
                "unexpected query result".to_string(),
            )),
        }
    }
}
