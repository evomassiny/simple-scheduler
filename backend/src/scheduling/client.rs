use crate::messaging::{AsyncSendable, RequestResult, ToClientMsg, ToSchedulerMsg};
use crate::models::{Batch, JobId, User, UserId};
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

type DbWriterHandle = ();

/// Struct handling handling communication
/// with the scheduler (eg: hypervisor).
pub struct SchedulerClient {
    ///// absolute path to the UNIX socket the hypervisor is listening on
    //pub socket: PathBuf,
    //pub read_pool: SqlitePool,
    //pub write_pool: SqlitePool,
    pub db_writer_handle: DbWriterHandle,
    pub status_cache_handle: StatusCacheHandle,
    pub queue_handle: QueueHandle,
}

impl SchedulerClient {

    /// parse a workflow file, and submit the parsed job
    pub async fn _submit_workflow(
        &self,
        workflow_xml: &str,
        user: &User<UserId>,
    ) -> Result<JobId, Box<dyn std::error::Error>> {
        let mut write_conn = self.write_pool.acquire().await?;
        // parse as workflow
        let graph: WorkFlowGraph = WorkFlowGraph::from_str(workflow_xml)?;

        // Save to DB
        let batch = Batch::from_graph(&graph, user.id, &mut write_conn).await?;

        // warn hypervisor that new jobs are available
        let mut to_hypervisor = self.connect_to_scheduler().await?;
        let _ = ToSchedulerMsg::JobAppended
            .async_send_to(&mut to_hypervisor)
            .await?;

        // return job id
        Ok(batch.job.id)
    }

    /// parse a workflow file, and submit the parsed job
    pub async fn submit_workflow(
        &self,
        workflow_xml: &str,
        user: &User<UserId>,
    ) -> Result<JobId, Box<dyn std::error::Error>> {

        // parse as workflow
        let graph: WorkFlowGraph = WorkFlowGraph::from_str(workflow_xml)?;

        // save it into the database
        let job = self.db_writer_handle.save_job(graph, user).await?;

        // schedule it
        self.queue_handle.add_job(&job).await?;
        Ok(job.id)
    }

    /// parse a workflow file from uploaded data, then submit it
    pub async fn submit_from_tempfile(
        &self,
        uploaded_file: &mut RocketTempFile<'_>,
        user: &User<UserId>,
    ) -> Result<i64, Box<dyn std::error::Error>> {
        // TODO: move to workflows::WorkFlowGraph

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

        let job_id = self.submit_workflow(&file_content, user).await?;
        Ok(job_id)
    }

    /// Tells the hypervisor to SIGKILL the job `job_id`
    pub async fn kill_job(&self, job_id: i64) -> Result<(), SchedulerClientError> {
        match self.queue_handle.unschedule(job_id).await {
            Ok(()) => Ok(()),
            _ => Err(SchedulerClientError::KillFailed(
                "unexpected query result".to_string(),
            )),
        }
    }
    
    /// 
    pub async fn get_job_status(&self, job_id: i64) -> Result<(), SchedulerClientError> {
        match self.status_cache_handle.get_job_status(job_id).await {
            Some(_) => {
                todo!();
                // return job status + tasks statuses
            },
            None => {
                // request status Database
                // if results: 
                //      write it to cache (async),
                // return job status + tasks statuses
                todo!()
            }
        }
    }
}
