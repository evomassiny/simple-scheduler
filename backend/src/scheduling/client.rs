use crate::messaging::{AsyncSendable, RequestResult, ToClientMsg, ToSchedulerMsg};
use crate::models::{Batch, Existing, ModelError, User};
use crate::workflows::WorkFlowGraph;
use rocket::fs::TempFile as RocketTempFile;
use rocket::http::ContentType;
use rocket::tokio::net::UnixStream;
use rocket::tokio::{self, fs::File, io::AsyncReadExt};
use sqlx::sqlite::SqlitePool;
use std::path::PathBuf;
use std::str::FromStr;
use tempfile::NamedTempFile;

#[derive(Debug)]
pub enum SchedulerClientError {
    KillFailed(String),
    SchedulerConnectionError,
    RequestSendingError,
    BadInputFile,
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
    /// absolute path to the UNIX socket the hypervisor is listening on
    pub socket: PathBuf,
    pub pool: SqlitePool,
}

impl SchedulerClient {
    async fn connect_to_scheduler(&self) -> Result<UnixStream, SchedulerClientError> {
        UnixStream::connect(&self.socket)
            .await
            .map_err(|_| SchedulerClientError::SchedulerConnectionError)
    }

    /// parse a workflow file, and submit the parsed job
    pub async fn submit_workflow(
        &self,
        workflow_xml: &str,
        user: &User<Existing>,
    ) -> Result<i64, Box<dyn std::error::Error>> {
        let mut conn = self.pool.acquire().await?;
        // parse as workflow
        let graph: WorkFlowGraph = WorkFlowGraph::from_str(workflow_xml)?;

        // Save to DB
        let batch = Batch::from_graph(&graph, &user, &mut conn).await?;

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
        user: &User<Existing>,
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

        // Content-type prepended with "application"
        // some app use "xml", some use "application/xml"
        let xml_type: ContentType = ContentType::new("application", "xml");
        // same for zip
        let zip_type: ContentType = ContentType::new("application", "zip");

        // check if the file is a ZIP archive, if so, unzip it first
        let file_content: String = match uploaded_file.content_type() {
            Some(content_type)
                if *content_type == ContentType::XML || *content_type == xml_type =>
            {
                // read file content
                let mut file = File::open(&file.path()).await?; // because file.path() == uploaded_file.path().unwrap()
                let mut content: String = String::new();
                file.read_to_string(&mut content).await?;
                content
            }
            Some(content_type)
                if *content_type == ContentType::ZIP || *content_type == zip_type =>
            {
                // unzip and read content
                let unzipped_content = tokio::task::spawn_blocking(
                    move || -> Result<String, Box<SchedulerClientError>> {
                        use std::io::Read;
                        let zipped_file = std::fs::File::open(&file.path())
                            .map_err(|_| SchedulerClientError::BadInputFile)?;
                        let mut archive = zip::ZipArchive::new(zipped_file)
                            .map_err(|_| SchedulerClientError::BadInputFile)?;
                        let mut file = archive
                            .by_index(0)
                            .map_err(|_| SchedulerClientError::BadInputFile)?;

                        let mut content = String::new();
                        file.read_to_string(&mut content)
                            .map_err(|_| SchedulerClientError::BadInputFile)?;
                        Ok(content)
                    },
                )
                .await??;
                unzipped_content
            }
            Some(content_type) => {
                println!("{:?}", content_type);
                return Err(Box::new(SchedulerClientError::BadInputFile));
            }
            _ => return Err(Box::new(SchedulerClientError::BadInputFile)),
        };

        let job_id = self.submit_workflow(&file_content, &user).await?;
        Ok(job_id)
    }

    /// Tells the hypervisor to SIGKILL the job `job_id`
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
                RequestResult::Err(error) => Err(SchedulerClientError::KillFailed(error)),
            },
            _ => Err(SchedulerClientError::KillFailed(
                "unexpected query result".to_string(),
            )),
        }
    }
}
