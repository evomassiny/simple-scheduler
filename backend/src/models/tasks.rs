use crate::models::{
    Model,
    ModelError,
    Status
};
use crate::tasks::TaskHandle;
use crate::sqlx::Row;
use sqlx::sqlite::SqliteConnection;
use crate::rocket::futures::TryStreamExt;
use async_trait::async_trait;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Task {
    pub id: Option<i64>,
    pub name: String,
    pub status: Status,
    pub handle: String,
    pub command_args: Vec<TaskCommandArgs>,
    pub stderr: Option<String>,
    pub stdout: Option<String>,
    pub job: i64,
}

#[async_trait]
impl Model for Task {
    async fn update(&mut self, conn: &mut SqliteConnection) -> Result<(), ModelError> {
        let _query_result = sqlx::query(
            "UPDATE tasks \
            SET name = ?, status = ?, handle = ?, \
            job = ?, stderr = ?, stdout = ? \
            WHERE id = ?",
        )
        .bind(&self.name)
        .bind(self.status.as_u8())
        .bind(&self.handle)
        .bind(&self.job)
        .bind(&self.stderr)
        .bind(&self.stdout)
        .bind(self.id.ok_or(ModelError::ModelNotFound)?)
        .execute(&mut *conn)
        .await
        .map_err(|e| ModelError::DbError(format!("{:?}", e)))?;

        for command in self.command_args.iter_mut() {
            command.update(&mut *conn).await?;
        }

        Ok(())
    }

    async fn create(&mut self, conn: &mut SqliteConnection) -> Result<(), ModelError> {
        let query_result = sqlx::query(
            "INSERT INTO tasks (name, status, handle, job) \
            VALUES (?, ?, ?, ?)",
        )
        .bind(&self.name)
        .bind(self.status.as_u8())
        .bind(&self.handle)
        .bind(&self.job)
        .execute(&mut *conn)
        .await
        .map_err(|e| ModelError::DbError(format!("{:?}", e)))?;

        let id = query_result.last_insert_rowid();
        self.id = Some(id);

        for command in self.command_args.iter_mut() {
            command.task = Some(id);
            command.create(&mut *conn).await?;
        }
        Ok(())
    }

    fn id(&self) -> Option<i64> {
        self.id
    }
}

impl Task {
    /// Select a Task by its handle
    pub async fn get_by_handle(
        handle: &str,
        conn: &mut SqliteConnection,
    ) -> Result<Self, ModelError> {
        let row = sqlx::query(
            "SELECT id, name, status, handle, job, stderr, stdout \
            FROM tasks WHERE handle = ?",
        )
        .bind(&handle)
        .fetch_one(&mut *conn)
        .await
        .map_err(|_| ModelError::ModelNotFound)?;

        let status_code: u8 = row
            .try_get("status")
            .map_err(|_| ModelError::ColumnError("status".to_string()))?;
        let status = Status::from_u8(status_code)
            .map_err(|_| ModelError::ColumnError("status".to_string()))?;

        let task_id: i64 = row
            .try_get("id")
            .map_err(|_| ModelError::ColumnError("id".to_string()))?;

        let task = Self {
            id: Some(task_id),
            name: row
                .try_get("name")
                .map_err(|_| ModelError::ColumnError("name".to_string()))?,
            status,
            handle: row
                .try_get("handle")
                .map_err(|_| ModelError::ColumnError("handle".to_string()))?,
            job: row
                .try_get("job")
                .map_err(|_| ModelError::ColumnError("job".to_string()))?,
            stderr: row
                .try_get("stderr")
                .map_err(|_| ModelError::ColumnError("job".to_string()))?,
            stdout: row
                .try_get("stdout")
                .map_err(|_| ModelError::ColumnError("job".to_string()))?,
            command_args: TaskCommandArgs::select_by_task(task_id, &mut *conn).await?,
        };
        Ok(task)
    }

    /// query database and return all tasks belonging to `job_id`
    pub async fn select_by_job(
        job_id: i64,
        conn: &mut SqliteConnection,
    ) -> Result<Vec<Self>, ModelError> {
        let mut tasks: Vec<Task> = Vec::new();
        let mut rows = sqlx::query(
            "SELECT id, name, status, handle, stderr, stdout \
                FROM tasks WHERE job = ?",
        )
        .bind(&job_id)
        .fetch(&mut *conn);

        while let Some(row) = rows
            .try_next()
            .await
            .map_err(|e| ModelError::DbError(e.to_string()))?
        {
            let status_code = row
                .try_get("status")
                .map_err(|_| ModelError::ColumnError("status".to_string()))?;
            let status = Status::from_u8(status_code)
                .map_err(|_| ModelError::ColumnError("status code".to_string()))?;

            let task_id: i64 = row
                .try_get("id")
                .map_err(|_| ModelError::ColumnError("id".to_string()))?;

            tasks.push(Self {
                id: Some(task_id),
                name: row
                    .try_get("name")
                    .map_err(|_| ModelError::ColumnError("name".to_string()))?,
                handle: row
                    .try_get("handle")
                    .map_err(|_| ModelError::ColumnError("handle".to_string()))?,
                job: job_id,
                status,
                stderr: row
                    .try_get("stderr")
                    .map_err(|_| ModelError::ColumnError("command".to_string()))?,
                stdout: row
                    .try_get("stdout")
                    .map_err(|_| ModelError::ColumnError("command".to_string()))?,
                command_args: Vec::new(),
            });
        }
        drop(rows);

        for task in tasks.iter_mut() {
            task.command_args.extend(
                TaskCommandArgs::select_by_task(
                    task.id.ok_or(ModelError::ModelNotFound)?,
                    &mut *conn,
                )
                .await?
            );
        }
        Ok(tasks)
    }

    /// Return an handle to the process task represented by self.
    pub fn handle(&self) -> TaskHandle {
        TaskHandle {
            directory: PathBuf::from(&self.handle),
        }
    }

    pub async fn count_by_status(
        status: &Status,
        conn: &mut SqliteConnection,
    ) -> Result<usize, ModelError> {
        let (count,): (i32,) = sqlx::query_as("SELECT COUNT(*) FROM tasks WHERE status = ?")
            .bind(status.as_u8())
            .fetch_one(conn)
            .await
            .map_err(|e| ModelError::DbError(e.to_string()))?;
        // sqlite only support isize
        Ok(count as usize)
    }
}

#[derive(Debug, Clone)]
pub struct TaskDependency {
    pub id: Option<i64>,
    pub job: i64,
    pub child: i64,
    pub parent: i64,
}

#[async_trait]
impl Model for TaskDependency {
    async fn update(&mut self, conn: &mut SqliteConnection) -> Result<(), ModelError> {
        let _query_result =
            sqlx::query("UPDATE task_dependencies SET child = ?, parent = ?, job = ? WHERE id = ?")
                .bind(&self.child)
                .bind(&self.parent)
                .bind(&self.job)
                .bind(self.id.ok_or(ModelError::ModelNotFound)?)
                .execute(conn)
                .await
                .map_err(|e| ModelError::DbError(format!("{:?}", e)))?;
        Ok(())
    }

    async fn create(&mut self, conn: &mut SqliteConnection) -> Result<(), ModelError> {
        let query_result =
            sqlx::query("INSERT INTO task_dependencies (job, child, parent) VALUES (?, ?, ?)")
                .bind(&self.job)
                .bind(&self.child)
                .bind(&self.parent)
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

impl TaskDependency {
    /// query database and return all task dependencies belonging to `job_id`
    pub async fn select_by_job(
        job_id: i64,
        conn: &mut SqliteConnection,
    ) -> Result<Vec<Self>, ModelError> {
        let mut dependencies: Vec<Self> = Vec::new();
        let mut rows = sqlx::query("SELECT id, child, parent FROM task_dependencies WHERE job = ?")
            .bind(&job_id)
            .fetch(conn);

        while let Some(row) = rows
            .try_next()
            .await
            .map_err(|e| ModelError::DbError(e.to_string()))?
        {
            dependencies.push(Self {
                id: row
                    .try_get("id")
                    .map_err(|_| ModelError::ColumnError("id".to_string()))?,
                child: row
                    .try_get("child")
                    .map_err(|_| ModelError::ColumnError("name".to_string()))?,
                parent: row
                    .try_get("parent")
                    .map_err(|_| ModelError::ColumnError("handle".to_string()))?,
                job: job_id,
            });
        }
        Ok(dependencies)
    }
}

#[derive(Debug, Clone)]
pub struct TaskCommandArgs {
    pub id: Option<i64>,
    pub argument: String,
    pub position: i64,
    pub task: Option<i64>,
}

#[async_trait]
impl Model for TaskCommandArgs {
    async fn update(&mut self, conn: &mut SqliteConnection) -> Result<(), ModelError> {
        let _query_result = sqlx::query(
            "UPDATE task_command_arguments \
            SET argument = ?, position = ?, task = ? \
            WHERE id = ?",
        )
        .bind(&self.argument)
        .bind(&self.position)
        .bind(&self.task.ok_or(ModelError::ModelNotFound)?)
        .bind(self.id.ok_or(ModelError::ModelNotFound)?)
        .execute(conn)
        .await
        .map_err(|e| ModelError::DbError(format!("{:?}", e)))?;
        Ok(())
    }

    async fn create(&mut self, conn: &mut SqliteConnection) -> Result<(), ModelError> {
        let query_result = sqlx::query(
            "INSERT INTO task_command_arguments (argument, position, task) \
            VALUES (?, ?, ?)",
        )
        .bind(&self.argument)
        .bind(self.position)
        .bind(&self.task)
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

impl TaskCommandArgs {
    async fn select_by_task(
        task_id: i64,
        conn: &mut SqliteConnection,
    ) -> Result<Vec<Self>, ModelError> {
        // collect command args
        let mut rows = sqlx::query(
            "SELECT id, argument, position FROM task_command_arguments WHERE task = ? ORDER BY position",
        )
        .bind(task_id)
        .fetch(&mut *conn);

        let mut command_args: Vec<TaskCommandArgs> = Vec::new();
        while let Some(row) = rows
            .try_next()
            .await
            .map_err(|e| ModelError::DbError(e.to_string()))?
        {
            command_args.push(
                 Self {
                task: Some(task_id),
                position: row
                    .try_get("position")
                    .map_err(|_| ModelError::ColumnError("position".to_string()))?,
                argument: row
                    .try_get("argument")
                    .map_err(|_| ModelError::ColumnError("argument".to_string()))?,
                id: row
                    .try_get("id")
                    .map_err(|_| ModelError::ColumnError("id".to_string()))?,
            });
        }
        Ok(command_args)
    }

    pub fn from_strings(command_args: Vec<String>) -> Vec<Self> {
        let mut args: Vec<Self> = Vec::new();
        for (i, arg) in command_args.into_iter().enumerate() {
            args.push( Self {
                task: None,
                position: i as i64,
                argument: arg,
                id: None,
            });
        }
        args
    }
}

