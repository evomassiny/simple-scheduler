use crate::models::{
    Model,
    ModelError,
    Status
};
use chrono::{NaiveDateTime, Utc};
use crate::sqlx::Row;
use sqlx::sqlite::SqliteConnection;
use crate::rocket::futures::TryStreamExt;
use async_trait::async_trait;


/// The `Job` struct implements abstraction over the `jobs` SQL table, 
/// defined as such:
///
/// ```sql
/// CREATE TABLE IF NOT EXISTS jobs (
///       id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
///       name VARCHAR(256) NOT NULL,
///       submit_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
///       status TINYINT NOT NULL DEFAULT 0 CHECK (status in (0, 1, 2, 3, 4, 5, 6))
/// );
/// ```
///
/// The `Job` model holds common information about a set of tasks and their dependancies,
/// the `job.id` primary key is used to group the `tasks` and `task_dependencies` sql table,
/// and their related `crate::models::Task` and `crate::models::TaskDependancy` models.
///
/// It implements the `crate::model::Model` trait.
#[derive(Debug, Clone)]
pub struct Job {
    /// Id of a job
    pub id: Option<i64>,
    /// name of the job
    pub name: String,
    /// When the user submitted it
    pub submit_time: NaiveDateTime,
    /// completion status
    pub status: Status,
}

#[async_trait]
impl Model for Job {
    async fn update(&mut self, conn: &mut SqliteConnection) -> Result<(), ModelError> {
        let _query_result =
            sqlx::query("UPDATE jobs SET name = ?, submit_time = ?, status = ? WHERE id = ?")
                .bind(&self.name)
                .bind(&self.submit_time.format("%Y-%m-%d %H:%M:%S").to_string())
                .bind(self.status.as_u8())
                .bind(self.id.ok_or(ModelError::ModelNotFound)?)
                .execute(conn)
                .await
                .map_err(|e| ModelError::DbError(format!("{:?}", e)))?;
        Ok(())
    }

    async fn create(&mut self, conn: &mut SqliteConnection) -> Result<(), ModelError> {
        let query_result =
            sqlx::query("INSERT INTO jobs (name, submit_time, status) VALUES (?, ?, ?)")
                .bind(&self.name)
                .bind(&self.submit_time.format("%Y-%m-%d %H:%M:%S").to_string())
                .bind(self.status.as_u8())
                .execute(conn)
                .await
                .map_err(|e| ModelError::DbError(format!("{:?}", e)))?;
        let id = query_result.last_insert_rowid();
        self.id = Some(id);
        Ok(())
    }

    fn id(&self) -> Option<i64> {
        self.id
    }
}

impl Job {
    /// Build a pending Job, use the current time
    /// as `submit_time`.
    /// The returned instance is not saved in the database.
    pub fn new(name: &str) -> Self {
        Job {
            id: None,
            name: name.to_string(),
            submit_time: NaiveDateTime::from_timestamp(Utc::now().timestamp(), 0),
            status: Status::Pending,
        }
    }

    /// query database and return all jobs with a status set to `status`
    pub async fn select_by_status(
        status: &Status,
        conn: &mut SqliteConnection,
    ) -> Result<Vec<Self>, ModelError> {
        let mut jobs: Vec<Self> = Vec::new();
        let mut rows = sqlx::query(
            "SELECT id, name, submit_time FROM jobs WHERE status = ? ORDER BY submit_time",
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
            });
        }
        Ok(jobs)
    }

    /// Select a Job by its id
    pub async fn get_by_id(id: i64, conn: &mut SqliteConnection) -> Result<Self, ModelError> {
        let row = sqlx::query("SELECT id, name, status, submit_time FROM jobs WHERE id = ?")
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
        })
    }

    pub async fn get_job_status_by_id(
        id: i64,
        conn: &mut SqliteConnection,
    ) -> Result<Status, ModelError> {
        let row = sqlx::query("SELECT status FROM jobs WHERE id = ?")
            .bind(&id)
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
}

