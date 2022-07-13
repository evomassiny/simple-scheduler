use crate::messaging::TaskStatus;
use crate::models::{ModelError, Status, JobId};
use crate::rocket::futures::TryStreamExt;
use crate::sqlx::Row;
use crate::tasks::TaskHandle;
use sqlx::sqlite::SqliteConnection;
use sqlx::Connection;
use std::path::PathBuf;

/// newly created task, not existing in db yet
pub struct NewTask;
/// id (primary key) of a task
pub type TaskId = i64;

/// This `Task` struct implements abstraction over the `tasks` SQL table,
/// defined as such:
/// ```sql
/// CREATE TABLE IF NOT EXISTS tasks (
///       id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
///       name VARCHAR(256) NOT NULL DEFAULT "",
///       handle VARCHAR(512) NOT NULL DEFAULT "",
///       status TINYINT NOT NULL DEFAULT 0 CHECK (status in (0, 1, 2, 3, 4, 5, 6)),
///       last_update_version INTEGER,
///       stderr TEXT DEFAULT NULL,
///       stdout TEXT DEFAULT NULL,
///       job INTEGER,
///       FOREIGN KEY(job) REFERENCES jobs(id) -- jobs pk constraint
/// );
/// ```
/// This each record represent a native executable command to execute,
/// the actual commands arguments are stored in a separated table `task_command_arguments`.
///
/// Each `Task` is linked to one `crate::models::Job` (many to one relationship) through their `job` attribute.
///
/// `Task` have dependencies, ie: some stask can only be launched after the success of another one,
/// those dependencies are stored in a separated table `task_dependencies`.
#[derive(Debug, Clone)]
pub struct Task<Id> {
    pub id: Id,
    pub name: String,
    /// Compeletion status
    pub status: Status,
    /// last status message version number, (auto incremented by the monitor process)
    pub last_update_version: Option<i64>,
    /// path to a unix socket (fifo), which can be used to communicate with
    /// the process monitoring this task.
    pub handle: String,
    /// what the task spat out in stdout while runnning
    pub stderr: Option<String>,
    /// what the task spat out in stderr while runnning
    pub stdout: Option<String>,
    /// id of  the related `Job` model
    pub job: JobId,
}

impl Task<NewTask> {
    pub async fn save(self, conn: &mut SqliteConnection) -> Result<Task<TaskId>, ModelError> {
        let query_result = sqlx::query(
            "INSERT INTO tasks (name, status, last_update_version, handle, job) \
            VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&self.name)
        .bind(self.status.as_u8())
        .bind(&self.last_update_version)
        .bind(&self.handle)
        .bind(&self.job)
        .execute(&mut *conn)
        .await
        .map_err(|e| ModelError::DbError(format!("{:?}", e)))?;

        let id: TaskId = query_result.last_insert_rowid();

        Ok( Task {
            id,
            name: self.name,
            status: self.status,
            last_update_version: self.last_update_version,
            handle: self.handle,
            stderr: self.stderr,
            stdout: self.stdout,
            job: self.job,
        })
    }
}
impl Task<TaskId> {

    pub async fn save(&mut self, conn: &mut SqliteConnection) -> Result<(), ModelError> {
        let _query_result = sqlx::query(
            "UPDATE tasks \
            SET name = ?, status = ?, last_update_version = ?, handle = ?, \
            job = ?, stderr = ?, stdout = ? \
            WHERE id = ?",
        )
        .bind(&self.name)
        .bind(self.status.as_u8())
        .bind(&self.last_update_version)
        .bind(&self.handle)
        .bind(&self.job)
        .bind(&self.stderr)
        .bind(&self.stdout)
        .bind(self.id)
        .execute(&mut *conn)
        .await
        .map_err(|e| ModelError::DbError(format!("{:?}", e)))?;

        Ok(())
    }

    pub async fn update_status_and_handle(
        task_id: TaskId,
        status: &Status,
        handle: &str,
        conn: &mut SqliteConnection,
    ) -> Result<(), ModelError> {
        let _ = sqlx::query("UPDATE tasks SET status = ?, handle = ? WHERE id = ?")
            .bind(&status.as_u8())
            .bind(handle)
            .bind(&task_id)
            .execute(&mut *conn)
            .await
            .map_err(|e| ModelError::DbError(format!("{:?}", e)))?;
        Ok(())
    }

    /// Compare `new_status_version` to the stored one for the task `task_id`,
    /// if `new_status_version` is greater of equal, update the task status with `new_status`.
    ///
    /// # Note
    /// This occurs in a single transaction, otherwise we might end up with cached states.
    ///
    /// # Return
    /// return true if the task status was changed.
    pub async fn try_update_status(
        conn: &mut SqliteConnection,
        task_id: TaskId,
        new_status: &Status,
        new_status_version: Option<i64>,
    ) -> Result<bool, ModelError> {
        let mut transaction = conn
            .begin()
            .await
            .map_err(|e| ModelError::DbError(format!("{:?}", e)))?;
        let row = sqlx::query(
            "SELECT status, last_update_version \
            FROM tasks WHERE id = ?",
        )
        .bind(&task_id)
        .fetch_one(&mut transaction)
        .await
        .map_err(|_| ModelError::ModelNotFound)?;

        let current_status: u8 = row
            .try_get("status")
            .map_err(|_| ModelError::ColumnError("status".to_string()))?;
        let current_version: Option<i64> = row
            .try_get("last_update_version")
            .map_err(|_| ModelError::ColumnError("last_update_version".to_string()))?;

        let mut status_updated = false;
        if new_status_version.unwrap_or(-1) >= current_version.unwrap_or(-1) {
            sqlx::query("UPDATE tasks SET status = ?, last_update_version = ? WHERE id = ?")
                .bind(&new_status.as_u8())
                .bind(&new_status_version)
                .bind(&task_id)
                .execute(&mut transaction)
                .await
                .map_err(|e| ModelError::DbError(format!("{:?}", e)))?;

            status_updated = new_status.as_u8() != current_status;
        }
        transaction
            .commit()
            .await
            .map_err(|e| ModelError::DbError(format!("{:?}", e)))?;

        Ok(status_updated)
    }

    /// Select a Task by its handle
    pub async fn get_task_id_by_handle(
        conn: &mut SqliteConnection,
        handle: &str,
    ) -> Result<TaskId, ModelError> {
        let row = sqlx::query("SELECT id FROM tasks WHERE handle = ?")
            .bind(&handle)
            .fetch_one(&mut *conn)
            .await
            .map_err(|_| ModelError::ModelNotFound)?;
        let task_id: TaskId = row
            .try_get("id")
            .map_err(|_| ModelError::ColumnError("id".to_string()))?;
        Ok(task_id)
    }

    /// Select a Task by its id
    pub async fn get_by_id(task_id: TaskId, conn: &mut SqliteConnection) -> Result<Self, ModelError> {
        let row = sqlx::query(
            "SELECT name, status, last_update_version, handle, job, stderr, stdout \
            FROM tasks WHERE id = ?",
        )
        .bind(&task_id)
        .fetch_one(&mut *conn)
        .await
        .map_err(|_| ModelError::ModelNotFound)?;

        let status_code: u8 = row
            .try_get("status")
            .map_err(|_| ModelError::ColumnError("status".to_string()))?;
        let status = Status::from_u8(status_code)
            .map_err(|_| ModelError::ColumnError("status".to_string()))?;

        let task = Self {
            id: task_id,
            name: row
                .try_get("name")
                .map_err(|_| ModelError::ColumnError("name".to_string()))?,
            status,
            last_update_version: row
                .try_get("last_update_version")
                .map_err(|_| ModelError::ColumnError("last_update_version".to_string()))?,
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
        };
        Ok(task)
    }

    /// return the list of DISTINCT statuses of all tasks belonging to a single job.
    pub async fn select_statuses_by_job(
        job_id: JobId,
        conn: &mut SqliteConnection,
    ) -> Result<Vec<Status>, ModelError> {
        let mut statuses: Vec<Status> = Vec::new();
        let mut rows = sqlx::query("SELECT distinct(status) FROM tasks WHERE job = ?")
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

            statuses.push(status);
        }
        Ok(statuses)
    }

    /// query database and return all tasks belonging to `job_id`
    pub async fn select_by_job(
        job_id: JobId,
        conn: &mut SqliteConnection,
    ) -> Result<Vec<Self>, ModelError> {
        let mut tasks: Vec<Task<TaskId>> = Vec::new();
        let mut rows = sqlx::query(
            "SELECT id, name, status, last_update_version, handle, stderr, stdout \
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
                id: task_id,
                name: row
                    .try_get("name")
                    .map_err(|_| ModelError::ColumnError("name".to_string()))?,
                handle: row
                    .try_get("handle")
                    .map_err(|_| ModelError::ColumnError("handle".to_string()))?,
                job: job_id,
                status,
                last_update_version: row
                    .try_get("last_update_version")
                    .map_err(|_| ModelError::ColumnError("last_update_version".to_string()))?,
                stderr: row
                    .try_get("stderr")
                    .map_err(|_| ModelError::ColumnError("stderr".to_string()))?,
                stdout: row
                    .try_get("stdout")
                    .map_err(|_| ModelError::ColumnError("stdout".to_string()))?,
            });
        }

        Ok(tasks)
    }

    pub async fn command_args(&self, conn: &mut SqliteConnection) -> Result<Vec<TaskCommandArgs<ArgId>>, ModelError> {
        TaskCommandArgs::select_by_task(self.id, conn).await
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

/// newly created task dependency, not existing in db yet
pub struct NewTaskDep;
/// id (primary key) of a task
pub type TaskDepId = i64;

/// This struct is an abstraction over the `task_dependencies` SQL table,
/// defined as follow:
/// ```sql
/// CREATE TABLE IF NOT EXISTS task_dependencies (
///       id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
///       child INTEGER,
///       parent INTEGER,
///       job INTEGER,
///       FOREIGN KEY(child) REFERENCES tasks(id), -- tasks pk constraint
///       FOREIGN KEY(parent) REFERENCES tasks(id) -- tasks pk constraint
///       FOREIGN KEY(job) REFERENCES jobs(id) -- tasks pk constraint
/// );
/// ```
///
/// It represents the dependency between two tasks, basically an edge in the
/// whole execution dependencies graph.
#[derive(Debug, Clone)]
pub struct TaskDependency<Id> {
    pub id: Id,
    /// id of the job that each of the 2 concerned tasks are belonging to.
    pub job: JobId,
    /// the task id that must be run AFTER `self.parent`
    pub child: TaskId,
    /// the task id that must be run BEFORE `self.child`
    pub parent: TaskId,
}

impl TaskDependency<NewTaskDep> {

    pub async fn save(self, conn: &mut SqliteConnection) -> Result<TaskDependency<TaskDepId>, ModelError> {
        let query_result =
            sqlx::query("INSERT INTO task_dependencies (job, child, parent) VALUES (?, ?, ?)")
                .bind(&self.job)
                .bind(&self.child)
                .bind(&self.parent)
                .execute(conn)
                .await
                .map_err(|e| ModelError::DbError(format!("{:?}", e)))?;
        let id: TaskDepId = query_result.last_insert_rowid();
        Ok(
            TaskDependency {
                id,
                job: self.job,
                child: self.child,
                parent: self.parent,
            }
        )
    }
}

impl TaskDependency<TaskDepId> {

    pub async fn save(&mut self, conn: &mut SqliteConnection) -> Result<(), ModelError> {
        let _query_result =
            sqlx::query("UPDATE task_dependencies SET child = ?, parent = ?, job = ? WHERE id = ?")
                .bind(&self.child)
                .bind(&self.parent)
                .bind(&self.job)
                .bind(self.id)
                .execute(conn)
                .await
                .map_err(|e| ModelError::DbError(format!("{:?}", e)))?;
        Ok(())
    }

    /// query database and return all task dependencies belonging to `job_id`
    pub async fn select_by_job(
        job_id: JobId,
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


/// newly created task task command line arg, not existing in db yet
pub struct NewArg;
/// id (primary key) of a task command line argument
pub type ArgId = i64;

/// Struct representing a task command argument, this is an
/// abstraction over the `task_command_arguments` SQl table, defined as follow:
///
/// ```sql
/// CREATE TABLE IF NOT EXISTS task_command_arguments (
///       id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
///       argument TEXT NOT NULL,
///       position INTEGER,
///       task INTEGER,
///       FOREIGN KEY(task) REFERENCES tasks(id) -- tasks pk constraint
/// );
/// ```
#[derive(Debug, Clone)]
pub struct TaskCommandArgs<Id> {
    pub id: Id,
    /// the executable argument
    pub argument: String,
    /// the argument position
    pub position: i64,
    /// the ID o the related task
    pub task: TaskId,
}

impl TaskCommandArgs<NewArg> {

    pub async fn save(self, conn: &mut SqliteConnection) -> Result<TaskCommandArgs<ArgId>, ModelError> {
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
        let id: ArgId = query_result.last_insert_rowid();

        Ok(
            TaskCommandArgs {
                id,
                argument: self.argument,
                position: self.position,
                task: self.task,
            }
        )
    }

    pub fn from_strings(command_args: Vec<String>, task_id: TaskId) -> Vec<Self> {
        let mut args: Vec<Self> = Vec::new();
        for (i, arg) in command_args.into_iter().enumerate() {
            args.push(Self {
                task: task_id,
                position: i as i64,
                argument: arg,
                id: NewArg,
            });
        }
        args
    }
}

impl TaskCommandArgs<ArgId> {

    pub async fn save(&mut self, conn: &mut SqliteConnection) -> Result<(), ModelError> {
        let _query_result = sqlx::query(
            "UPDATE task_command_arguments \
            SET argument = ?, position = ?, task = ? \
            WHERE id = ?",
        )
        .bind(&self.argument)
        .bind(&self.position)
        .bind(&self.task)
        .bind(self.id)
        .execute(conn)
        .await
        .map_err(|e| ModelError::DbError(format!("{:?}", e)))?;
        Ok(())
    }

    async fn select_by_task(
        task_id: TaskId,
        conn: &mut SqliteConnection,
    ) -> Result<Vec<Self>, ModelError> {
        // collect command args
        let mut rows = sqlx::query(
            "SELECT id, argument, position FROM task_command_arguments WHERE task = ? ORDER BY position",
        )
        .bind(task_id)
        .fetch(&mut *conn);

        let mut command_args: Vec<TaskCommandArgs<ArgId>> = Vec::new();
        while let Some(row) = rows
            .try_next()
            .await
            .map_err(|e| ModelError::DbError(e.to_string()))?
        {
            command_args.push(Self {
                task: task_id,
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

}

/// a small subset of a "tasks" record.
pub struct TaskView {
    pub id: TaskId,
    pub status: Status,
    pub last_update_version: Option<i64>,
    pub handle: TaskHandle,
}
impl TaskView {
    /// query database and return tasks by status
    pub async fn select_by_status(
        status: &Status,
        conn: &mut SqliteConnection,
    ) -> Result<Vec<TaskView>, ModelError> {
        let mut tasks: Vec<TaskView> = Vec::new();
        let mut rows = sqlx::query(
            "SELECT id, status, last_update_version, handle FROM tasks WHERE status = ?", // 0 => Pending, 5 => Running
        )
        .bind(status.as_u8())
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

            let handle_string: String = row
                .try_get("handle")
                .map_err(|_| ModelError::ColumnError("handle".to_string()))?;
            let handle_path = PathBuf::from(&handle_string);

            tasks.push(TaskView {
                id: task_id,
                handle: TaskHandle {
                    directory: handle_path,
                },
                status,
                last_update_version: row
                    .try_get("last_update_version")
                    .map_err(|_| ModelError::ColumnError("last_update_version".to_string()))?,
            });
        }
        Ok(tasks)
    }
}
