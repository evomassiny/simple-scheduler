use super::cache_actor::{
    CacheReadHandle, CacheReader, CacheWriteHandle, CacheWriter, JobStatusDetail,
};
use super::db_writer_actor::DbWriterHandle;
use super::queue_actor::{QueueSubmissionClient, QueueSubmissionHandle};
use crate::messaging::{AsyncSendable, RequestResult, ToClientMsg, ToSchedulerMsg};
use crate::models::{Batch, JobId, Status, TaskId, User, UserId};
use crate::rocket::futures::TryStreamExt;
use crate::sqlx::Row;
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
    JobCreationError,
    UnknownJob,
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
/// with the scheduler (eg: the swarm of actors that compose the hypervisor).
pub struct SchedulerClient {
    pub read_pool: SqlitePool,
    db_writer_handle: DbWriterHandle,
    status_cache_reader: CacheReader,
    status_cache_writer: CacheWriter,
    submission_handle: QueueSubmissionClient,
}

impl SchedulerClient {
    pub fn new(
        read_pool: SqlitePool,
        db_writer_handle: DbWriterHandle,
        status_cache_reader: CacheReader,
        status_cache_writer: CacheWriter,
        submission_handle: QueueSubmissionClient,
    ) -> Self {
        Self {
            read_pool,
            db_writer_handle,
            status_cache_reader,
            status_cache_writer,
            submission_handle,
        }
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
        let (job, tasks, dependencies) = self
            .db_writer_handle
            .insert_job(graph, user.id)
            .await
            .ok_or(SchedulerClientError::JobCreationError)?;

        self.submission_handle
            .add_job_to_queue(job, tasks, dependencies);
        Ok(job)
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
                return Err(Box::new(SchedulerClientError::BadInputFile));
            }
            _ => return Err(Box::new(SchedulerClientError::BadInputFile)),
        };

        let job_id = self.submit_workflow(&file_content, user).await?;
        Ok(job_id)
    }

    /// Tells the hypervisor to SIGKILL the job `job_id`
    pub async fn kill_job(&self, job_id: i64) -> Result<(), SchedulerClientError> {
        self.submission_handle.remove_job_from_queue(job_id);
        Ok(())
    }

    async fn get_job_status_from_db(
        &self,
        job_id: JobId,
    ) -> Result<JobStatusDetail, Box<dyn std::error::Error>> {
        let mut conn = self.read_pool.acquire().await?;

        // fetch status for each tasks
        let mut row = sqlx::query("SELECT status FROM jobs WHERE id = ?")
            .bind(&job_id)
            .fetch_one(&mut conn)
            .await?;
        let job_status: Status = Status::from_u8(row.try_get("status")?)?;
        let mut rows = sqlx::query("SELECT id, status FROM tasks WHERE job = ?")
            .bind(&job_id)
            .fetch(&mut conn);

        let mut task_statuses: Vec<(TaskId, Status)> = Vec::new();
        while let Some(row) = rows.try_next().await? {
            // fetch name
            let task_id: TaskId = row.try_get("id")?;
            // fetch status, build a string from it
            let status_code: u8 = row.try_get("status")?;
            let status: Status = Status::from_u8(status_code)?;
            task_statuses.push((task_id, status));
        }
        Ok(JobStatusDetail {
            id: job_id,
            status: job_status,
            task_statuses,
        })
    }

    /// Try to fetch job status from cache, if not present,
    /// try from the db instead (and update db).
    pub async fn get_job_status(
        &self,
        job_id: JobId,
    ) -> Result<JobStatusDetail, SchedulerClientError> {
        match self.status_cache_reader.get_job_status(job_id).await {
            Some(job_status) => Ok(job_status),
            None => {
                match self.get_job_status_from_db(job_id).await {
                    Ok(job_status) => {
                        // insert job into cache
                        self.status_cache_writer.add_job(job_status.clone());
                        Ok(job_status)
                    }
                    Err(e) => {
                        eprintln!("Error while trying to fetch {job_id} status from DB");
                        Err(SchedulerClientError::UnknownJob)
                    }
                }
            }
        }
    }
}
