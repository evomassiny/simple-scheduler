use crate::models::{ModelError, Status, UserId};

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

    /// Set new status for job identified by `job_id`
    pub async fn set_status(
        conn: &mut SqliteConnection,
        job_id: JobId,
        status: &Status,
    ) -> Result<(), ModelError> {
        sqlx::query("UPDATE jobs SET status = ? WHERE id = ?")
            .bind(&status.as_u8())
            .bind(&job_id)
            .execute(&mut *conn)
            .await
            .map_err(|e| ModelError::DbError(format!("{:?}", e)))?;
        Ok(())
    }
}
