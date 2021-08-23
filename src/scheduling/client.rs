use sqlx::sqlite::SqlitePool;
use std::path::{PathBuf,Path};
use crate::models::{Batch,ModelError};
use crate::messaging::{ToSchedulerMsg, AsyncSendable};
use crate::tokio::net::UnixStream;
use crate::workflows::WorkFlowGraph;
use rocket::tokio::{
    self,
    fs::File,
    io::AsyncReadExt,
};
use rocket::fs::TempFile as RocketTempFile;
use tempfile::NamedTempFile;

pub struct SchedulerClient {
    pub socket: PathBuf,
    pub pool: SqlitePool,
}

impl SchedulerClient {

    pub async fn connect_to_scheduler(&self) -> Result<UnixStream, String> {
        UnixStream::connect(&self.socket)
            .await
            .map_err(|_| format!("Could not open hypervisor socket {:?}", &self.socket))
    }

    /// Submit a Job with a single task: a shell command
    pub async fn submit_command_job(&self, job_name: &str, task_name: &str, cmd: &str) 
    -> Result<(), Box<dyn std::error::Error>> {
        let mut conn = self.pool.acquire().await?;
        let batch = Batch::from_shell_command(
            job_name,
            task_name,
            cmd,
            &mut conn,
        ).await;
        println!("batch: {:?}", &batch);

        let mut to_hypervisor = self.connect_to_scheduler().await?;
        let _ = ToSchedulerMsg::JobAppended.async_send_to(&mut to_hypervisor).await?;
        Ok(())
    }

    /// parse a workflow file, and submit the parsed job
    pub async fn submit_workflow(&self, workflow_path: &Path) -> Result<i64, Box<dyn std::error::Error>> {
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
        let _ = ToSchedulerMsg::JobAppended.async_send_to(&mut to_hypervisor).await?;

        // return job id
        let job_id = batch.job.id.ok_or(ModelError::ModelNotFound)?;
        Ok(job_id)
    }

    /// parse a workflow file from uploaded data, then submit it
    pub async fn submit_from_tempfile(&self, uploaded_file: &mut RocketTempFile<'_>)
        -> Result<i64, Box<dyn std::error::Error>> { 
        // first persist the file 
        // * use a NamedtempFile to get a free path
        let file = tokio::task::spawn_blocking(move || NamedTempFile::new() ).await.map_err(
            |_| { std::io::Error::new(std::io::ErrorKind::BrokenPipe, "spawn_block panic")
        })??;
        // * move the tempfile content to it
        uploaded_file.persist_to(file.path()).await?;

        let job_id = self.submit_workflow(file.path()).await?;
        Ok(job_id)

    }
}

