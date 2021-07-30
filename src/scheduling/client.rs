use sqlx::sqlite::SqlitePool;
use std::path::PathBuf;
use crate::models::Batch;
use crate::messaging::{ToSchedulerMsg, AsyncSendable};
use crate::tokio::net::UnixStream;

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

    pub async fn submit_command_job(&self, job_name: &str, task_name: &str, cmd: &str) 
    -> Result<(), Box<std::error::Error>> {
        let mut conn = self.pool.acquire().await?;
        let batch = Batch::from_shell_command(
            job_name,
            task_name,
            cmd,
            &mut conn,
        );

        let mut to_hypervisor = self.connect_to_scheduler().await?;
        let _ = ToSchedulerMsg::JobAppended.async_send_to(&mut to_hypervisor).await?;
        Ok(())
    }
}

