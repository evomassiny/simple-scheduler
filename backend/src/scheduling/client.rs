use super::cache_actor::{
    CacheReadHandle, CacheReader, CacheWriteHandle, CacheWriter, JobStatusDetail,
};
use super::db_writer_actor::DbWriterHandle;
use super::queue_actor::{QueueSubmissionClient, QueueSubmissionHandle};
use crate::auth::{AuthToken, Credentials};

use crate::models::{
    JobId, ModelError, Status, Task, TaskDepId, TaskDependency, TaskId, User, UserId,
};
use crate::rocket::futures::TryStreamExt;
use crate::sqlx::Row;
use crate::workflows::WorkFlowGraph;
use rocket::fs::TempFile as RocketTempFile;
use rocket::http::ContentType;

use rocket::tokio::{self, fs::File, io::AsyncReadExt};
use sqlx::sqlite::SqlitePool;

use std::str::FromStr;
use tempfile::NamedTempFile;

pub struct TaskOutput {
    pub status: Status,
    pub stderr: String,
    pub stdout: String,
}

#[derive(Debug)]
pub enum SchedulerClientError {
    KillFailed(String),
    JobCreationError,
    DbError(String),
    UnknownTask,
    UnknownJob,
    UnknownUser,
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
    /// database pool, only used for reads
    read_pool: SqlitePool,
    /// handle to the DbWriter actoro
    db_writer_handle: DbWriterHandle,
    /// handle to the Cache actor
    status_cache_reader: CacheReader,
    /// handle to the Cache actor
    status_cache_writer: CacheWriter,
    /// handle to the queue actor
    submission_handle: QueueSubmissionClient,
}

impl SchedulerClient {
    /// build a new scheduler client
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

    /// parse a workflow file, and schedule the parsed job
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

        // Add job to cache (not mandatory)
        let job_status = JobStatusDetail {
            id: job,
            status: Status::Pending,
            task_statuses: tasks.iter().map(|id| (*id, Status::Pending)).collect(),
        };
        self.status_cache_writer.add_job(job_status);

        // schedule job
        self.submission_handle
            .add_job_to_queue(job, tasks, dependencies);
        Ok(job)
    }

    /// parse a workflow file from uploaded data, then schedule it
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
            Some(_content_type) => {
                return Err(Box::new(SchedulerClientError::BadInputFile));
            }
            _ => return Err(Box::new(SchedulerClientError::BadInputFile)),
        };

        let job_id = self.submit_workflow(&file_content, user).await?;
        Ok(job_id)
    }

    /// Tells the hypervisor to SIGKILL the job `job_id`
    pub async fn kill_job(&self, job_id: JobId) -> Result<(), SchedulerClientError> {
        self.submission_handle.remove_job_from_queue(job_id);
        Ok(())
    }

    /// ask Database for last known state of Job `job_id` and its sibling tasks
    async fn get_job_status_from_db(
        &self,
        job_id: JobId,
    ) -> Result<JobStatusDetail, Box<dyn std::error::Error>> {
        let mut conn = self.read_pool.acquire().await?;

        // fetch status for each tasks
        let row = sqlx::query("SELECT status FROM jobs WHERE id = ?")
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

    /// Try to fetch job status from cache actor, if not present,
    /// try from the db instead (and update cache).
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
                    Err(_e) => {
                        eprintln!("Error while trying to fetch {job_id} status from DB");
                        Err(SchedulerClientError::UnknownJob)
                    }
                }
            }
        }
    }

    /// Parse an AuthToken into a User<UserId> model
    pub async fn fetch_user(
        &self,
        auth: &AuthToken,
    ) -> Result<User<UserId>, Box<dyn std::error::Error>> {
        let mut conn = self.read_pool.acquire().await?;
        auth.fetch_user(&mut conn)
            .await
            .ok_or(SchedulerClientError::UnknownUser.into())
    }

    /// Use `credentials` to fetch an existing user from the database
    pub async fn get_user_from_credentials(
        &self,
        credentials: &Credentials,
    ) -> Option<User<UserId>> {
        let mut conn = self.read_pool.acquire().await.ok()?;
        credentials.get_user(&mut conn).await
    }

    /// fetch task stderr/stdout/status
    pub async fn get_task_outputs(
        &self,
        task: TaskId,
        user: UserId,
    ) -> Result<TaskOutput, SchedulerClientError> {
        let mut conn = self
            .read_pool
            .acquire()
            .await
            .map_err(|e| SchedulerClientError::DbError(e.to_string()))?;

        // fetch stderr/stdout/status from tasks,
        // (filter tasks that do not belong to the user).
        let row = sqlx::query("SELECT tasks.status, tasks.stdout, tasks.stderr FROM tasks LEFT JOIN jobs on tasks.job = jobs.id WHERE tasks.id = ? and jobs.user = ?")
            .bind(&task)
            .bind(&user)
            .fetch_one(&mut conn)
            .await
            .map_err(|e| SchedulerClientError::DbError(e.to_string()))?;
        let stderr: String = row
            .try_get("stderr")
            .map_err(|_| SchedulerClientError::UnknownTask)?;
        let stdout: String = row
            .try_get("stdout")
            .map_err(|_| SchedulerClientError::UnknownTask)?;
        let status_code: u8 = row
            .try_get("status")
            .map_err(|_| SchedulerClientError::UnknownTask)?;
        let status: Status = Status::from_u8(status_code)
            .map_err(|_| SchedulerClientError::DbError("bad status code".to_string()))?;
        Ok(TaskOutput {
            stderr,
            stdout,
            status,
        })
    }

    /// Relauch PENDING and RUNNING jobs
    pub async fn relaunch(&self) -> Result<(), SchedulerClientError> {
        let mut conn = self
            .read_pool
            .acquire()
            .await
            .map_err(|e| SchedulerClientError::DbError(e.to_string()))?;

        let mut rows = sqlx::query("SELECT id, status FROM jobs where status = ? or status = ?")
            .bind(Status::Pending.as_u8())
            .bind(Status::Running.as_u8())
            .fetch(&mut conn);

        let mut jobs: Vec<(JobId, Status)> = Vec::new();
        while let Some(row) = rows
            .try_next()
            .await
            .map_err(|e| SchedulerClientError::DbError(e.to_string()))?
        {
            let job_id: JobId = row
                .try_get("id")
                .map_err(|_| SchedulerClientError::DbError("id".to_string()))?;
            let status_code: u8 = row
                .try_get("status")
                .map_err(|_| SchedulerClientError::DbError("status".to_string()))?;
            let job_status = Status::from_u8(status_code)
                .map_err(|_| SchedulerClientError::DbError("status code".to_string()))?;

            jobs.push((job_id, job_status));
        }
        drop(rows);

        for (job_id, job_status) in jobs {
            let tasks = Task::<TaskId>::select_by_job(job_id, &mut conn)
                .await
                .map_err(|e| SchedulerClientError::DbError(e.to_string()))?;
            // schedule job
            let dependencies = TaskDependency::<TaskDepId>::select_by_job(job_id, &mut conn)
                .await
                .map_err(|e| SchedulerClientError::DbError(e.to_string()))?;

            // Add job to cache
            let job_status = JobStatusDetail {
                id: job_id,
                status: job_status,
                task_statuses: tasks.iter().map(|task| (task.id, task.status)).collect(),
            };
            self.status_cache_writer.add_job(job_status);

            let dependency_ids: Vec<(TaskId, TaskId)> = dependencies
                .iter()
                .map(|dep| (dep.child, dep.parent))
                .collect();
            let task_ids: Vec<TaskId> = tasks.iter().map(|task| task.id).collect();
            self.submission_handle
                .add_job_to_queue(job_id, task_ids, dependency_ids);
        }
        Ok(())
    }
}
