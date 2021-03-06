use crate::models::{ModelError, Status, Task, User, UserId};
use crate::rocket::futures::TryStreamExt;
use crate::sqlx::Row;
use chrono::{NaiveDateTime, Utc};
use sqlx::sqlite::SqliteConnection;

/// A newly created Jobn not save in db
pub struct NewJob;
/// Id of an existing record in db
pub type JobId = i64;

/// The `Job` struct implements abstraction over the `jobs` SQL table,
/// defined as such:
///
/// ```sql
/// CREATE TABLE IF NOT EXISTS jobs (
///      id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
///      name VARCHAR(256) NOT NULL,
///      submit_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
///      status TINYINT NOT NULL DEFAULT 0 CHECK (status in (0, 1, 2, 3, 4, 5, 6)),
///      user_id INTEGER,
///      FOREIGN KEY(user_id) REFERENCES users(id) -- tasks pk constraint
/// );
/// ```
///
/// The `Job` model holds common information about a set of tasks and their dependancies,
/// the `job.id` primary key is used to group the `tasks` and `task_dependencies` sql table,
/// and their related `crate::models::Task` and `crate::models::TaskDependancy` models.
///
#[derive(Debug, Clone)]
pub struct Job<Id> {
    /// Id of a job
    pub id: Id,
    /// name of the job
    pub name: String,
    /// When the user submitted it
    pub submit_time: NaiveDateTime,
    /// completion status
    pub status: Status,
    /// ID of the user that submitted the job
    pub user: UserId,
}

impl Job<NewJob> {
    /// Build a pending Job, use the current time
    /// as `submit_time`.
    /// The returned instance is not saved in the database.
    pub fn new(name: &str, user: UserId) -> Self {
        Job {
            id: NewJob,
            name: name.to_string(),
            submit_time: NaiveDateTime::from_timestamp(Utc::now().timestamp(), 0),
            status: Status::Pending,
            user,
        }
    }

    pub async fn save(self, conn: &mut SqliteConnection) -> Result<Job<JobId>, ModelError> {
        let query_result =
            sqlx::query("INSERT INTO jobs (name, submit_time, status, user) VALUES (?, ?, ?, ?)")
                .bind(&self.name)
                .bind(&self.submit_time.format("%Y-%m-%d %H:%M:%S").to_string())
                .bind(self.status.as_u8())
                .bind(self.user)
                .execute(conn)
                .await
                .map_err(|e| ModelError::DbError(format!("{:?}", e)))?;
        let id: JobId = query_result.last_insert_rowid();
        let job = Job {
            id,
            name: self.name,
            submit_time: self.submit_time,
            status: self.status,
            user: self.user,
        };
        Ok(job)
    }
}

impl Job<JobId> {
    pub async fn save(&mut self, conn: &mut SqliteConnection) -> Result<(), ModelError> {
        let _query_result = sqlx::query(
            "UPDATE jobs SET name = ?, submit_time = ?, status = ?, user = ? WHERE id = ?",
        )
        .bind(&self.name)
        .bind(&self.submit_time.format("%Y-%m-%d %H:%M:%S").to_string())
        .bind(self.status.as_u8())
        .bind(self.user)
        .bind(self.id)
        .execute(conn)
        .await
        .map_err(|e| ModelError::DbError(format!("{:?}", e)))?;
        Ok(())
    }

    /// query database and return all jobs with a status set to `status`
    pub async fn select_by_status(
        status: &Status,
        conn: &mut SqliteConnection,
    ) -> Result<Vec<Self>, ModelError> {
        let mut jobs: Vec<Self> = Vec::new();
        let mut rows = sqlx::query(
            "SELECT id, name, submit_time, user FROM jobs WHERE status = ? ORDER BY submit_time",
        )
        .bind(status.as_u8())
        .fetch(conn);

        while let Some(row) = rows
            .try_next()
            .await
            .map_err(|e| ModelError::DbError(e.to_string()))?
        {
            jobs.push(Self {
                id: row
                    .try_get("id")
                    .map_err(|_| ModelError::ColumnError("id".to_string()))?,
                name: row
                    .try_get("name")
                    .map_err(|_| ModelError::ColumnError("name".to_string()))?,
                status: *status,
                submit_time: row
                    .try_get("submit_time")
                    .map_err(|_| ModelError::ColumnError("submit_time".to_string()))?,
                user: row
                    .try_get("user")
                    .map_err(|_| ModelError::ColumnError("user".to_string()))?,
            });
        }
        Ok(jobs)
    }

    /// Select a Job by its id
    pub async fn get_by_id(id: JobId, conn: &mut SqliteConnection) -> Result<Self, ModelError> {
        let row = sqlx::query("SELECT id, name, status, submit_time, user FROM jobs WHERE id = ?")
            .bind(&id)
            .fetch_one(conn)
            .await
            .map_err(|_| ModelError::ModelNotFound)?;

        let status_code: u8 = row
            .try_get("status")
            .map_err(|_| ModelError::ColumnError("status".to_string()))?;
        let status = Status::from_u8(status_code)
            .map_err(|_| ModelError::ColumnError("status".to_string()))?;
        Ok(Self {
            id: row
                .try_get("id")
                .map_err(|_| ModelError::ColumnError("id".to_string()))?,
            name: row
                .try_get("name")
                .map_err(|_| ModelError::ColumnError("name".to_string()))?,
            status,
            submit_time: row
                .try_get("submit_time")
                .map_err(|_| ModelError::ColumnError("submit_time".to_string()))?,
            user: row
                .try_get("user")
                .map_err(|_| ModelError::ColumnError("user".to_string()))?,
        })
    }

    pub async fn get_job_status_by_id(
        id: JobId,
        conn: &mut SqliteConnection,
    ) -> Result<Status, ModelError> {
        let row = sqlx::query("SELECT status FROM jobs WHERE id = ?")
            .bind(id)
            .fetch_one(conn)
            .await
            .map_err(|_| ModelError::ModelNotFound)?;
        let status_code: u8 = row
            .try_get("status")
            .map_err(|_| ModelError::ColumnError("status".to_string()))?;
        let status = Status::from_u8(status_code)
            .map_err(|_| ModelError::ColumnError("status".to_string()))?;
        Ok(status)
    }

    /// Lookup state of all tasks composing the job, and update self.status accordingly.
    /// (commit changes in DB).
    ///
    /// * If all task failed => failure
    /// * If ONE task canceled => Canceled
    /// * If ONE task Stopped => Stopped
    /// * If ONE task Killed => killed
    /// * if mix failure/succed => succeed
    pub async fn update_job_state_from_task_ones(
        read_conn: &mut SqliteConnection,
        write_conn: &mut SqliteConnection,
        job_id: JobId,
    ) -> Result<(), ModelError> {
        let task_statuses = Task::select_statuses_by_job(job_id, &mut *read_conn).await?;

        for status in &task_statuses {
            if !status.is_finished() {
                // if there is remaining tasks
                // no need to update job staus
                return Ok(());
            }
        }
        let mut job_status = Status::Failed;
        for status in &task_statuses {
            match status {
                Status::Stopped => {
                    job_status = Status::Stopped;
                    break;
                }
                Status::Canceled => {
                    job_status = Status::Canceled;
                    break;
                }
                Status::Killed => {
                    job_status = Status::Killed;
                    break;
                }
                Status::Succeed => {
                    job_status = Status::Succeed;
                }
                _ => {}
            }
        }
        let _query_result = sqlx::query("UPDATE jobs SET status = ? WHERE id = ?")
            .bind(&job_status.as_u8())
            .bind(&job_id)
            .execute(&mut *write_conn)
            .await
            .map_err(|e| ModelError::DbError(format!("{:?}", e)))?;
        Ok(())
    }

    pub async fn task_count(&self, read_conn: &mut SqliteConnection) -> Result<usize, ModelError> {
        let (count,): (u32,) = sqlx::query_as("SELECT COUNT(id) FROM tasks WHERE job = ?")
            .bind(self.id)
            .fetch_one(read_conn)
            .await
            .map_err(|e| ModelError::DbError(format!("{:?}", e)))?;
        Ok(count as usize)
    }
}
