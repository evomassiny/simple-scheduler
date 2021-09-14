use crate::messaging::TaskStatus;
use crate::rocket::futures::TryStreamExt;
use crate::sqlx::Row;
use crate::tasks::TaskHandle;
use crate::workflows::WorkFlowGraph;
use async_trait::async_trait;
use chrono::{NaiveDateTime, Utc};
use sqlx::sqlite::SqliteConnection;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug)]
pub enum ModelError {
    InvalidTaskId,
    InvalidTaskName,
    DependencyCycle,
    ModelNotFound,
    DbError(String),
    ColumnError(String),
}
impl std::fmt::Display for ModelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ModelError {:?}", &self)
    }
}
impl std::error::Error for ModelError {}

/// trait shared by Database models,
/// allow easier database manipulation.
#[async_trait]
pub trait Model {
    async fn update(&mut self, conn: &mut SqliteConnection) -> Result<(), ModelError>;
    async fn create(&mut self, conn: &mut SqliteConnection) -> Result<(), ModelError>;
    fn id(&self) -> Option<i64>;

    //async fn save(&mut self) -> Result<(), ModelError>;
    async fn save(&mut self, conn: &mut SqliteConnection) -> Result<(), ModelError> {
        let id = self.id();
        if id.is_some() {
            self.update(conn).await?;
        } else {
            self.create(conn).await?;
        }
        Ok(())
    }
}

/// State of job/task
#[derive(Debug, Clone, Copy)]
pub enum Status {
    /// waiting to be scheduled
    Pending,
    /// started then stopped
    Stopped,
    /// killed (sigkill)
    Killed,
    /// returned error status
    Failed,
    /// ran successfully
    Succeed,
    /// currently scheduled
    Running,
    /// killed before being scheduled
    Canceled,
}

impl Status {
    pub fn from_task_status(status: &TaskStatus) -> Self {
        match *status {
            TaskStatus::Pending => Self::Pending,
            TaskStatus::Stopped => Self::Stopped,
            TaskStatus::Killed => Self::Killed,
            TaskStatus::Failed => Self::Failed,
            TaskStatus::Succeed => Self::Succeed,
            TaskStatus::Running => Self::Running,
        }
    }

    pub fn is_failure(&self) -> bool {
        match *self {
            Self::Pending => false,
            Self::Stopped => true,
            Self::Killed => true,
            Self::Failed => true,
            Self::Succeed => false,
            Self::Running => false,
            Self::Canceled => true,
        }
    }

    pub fn is_pending(&self) -> bool {
        matches!(*self, Self::Pending)
    }

    pub fn is_finished(&self) -> bool {
        match *self {
            Self::Pending => false,
            Self::Stopped => true,
            Self::Killed => true,
            Self::Failed => true,
            Self::Succeed => true,
            Self::Running => false,
            Self::Canceled => true,
        }
    }

    pub fn from_u8(value: u8) -> Result<Self, String> {
        match value {
            0 => Ok(Self::Pending),
            1 => Ok(Self::Stopped),
            2 => Ok(Self::Killed),
            3 => Ok(Self::Failed),
            4 => Ok(Self::Succeed),
            5 => Ok(Self::Running),
            6 => Ok(Self::Canceled),
            v => Err(format!("'{:}' cannot be converted to a Status", v)),
        }
    }

    pub fn as_u8(&self) -> u8 {
        match *self {
            Self::Pending => 0,
            Self::Stopped => 1,
            Self::Killed => 2,
            Self::Failed => 3,
            Self::Succeed => 4,
            Self::Running => 5,
            Self::Canceled => 6,
        }
    }

    pub fn as_proactive_string(&self) -> String {
        match *self {
            Self::Pending => "PENDING".to_string(),
            Self::Stopped => "PAUSED".to_string(),
            Self::Killed => "KILLED".to_string(),
            Self::Failed => "FAILED".to_string(),
            Self::Succeed => "FINISHED".to_string(),
            Self::Running => "RUNNING".to_string(),
            Self::Canceled => "CANCELED".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Job {
    pub id: Option<i64>,
    pub name: String,
    pub submit_time: NaiveDateTime,
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
    fn new(name: &str) -> Self {
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

#[derive(Debug, Clone)]
pub struct Task {
    pub id: Option<i64>,
    pub name: String,
    pub status: Status,
    pub handle: String,
    pub command: String,
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
            command = ?, job = ?, stderr = ?, stdout = ? \
            WHERE id = ?",
        )
        .bind(&self.name)
        .bind(self.status.as_u8())
        .bind(&self.handle)
        .bind(&self.command)
        .bind(&self.job)
        .bind(&self.stderr)
        .bind(&self.stdout)
        .bind(self.id.ok_or(ModelError::ModelNotFound)?)
        .execute(conn)
        .await
        .map_err(|e| ModelError::DbError(format!("{:?}", e)))?;
        Ok(())
    }

    async fn create(&mut self, conn: &mut SqliteConnection) -> Result<(), ModelError> {
        let query_result = sqlx::query(
            "INSERT INTO tasks (name, status, handle, command, job) \
            VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&self.name)
        .bind(self.status.as_u8())
        .bind(&self.handle)
        .bind(&self.command)
        .bind(&self.job)
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

impl Task {
    /// Select a Task by its handle
    pub async fn get_by_handle(
        handle: &str,
        conn: &mut SqliteConnection,
    ) -> Result<Self, ModelError> {
        let row = sqlx::query(
            "SELECT id, name, status, handle, command, job, stderr, stdout \
            FROM tasks WHERE handle = ?",
        )
        .bind(&handle)
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
            handle: row
                .try_get("handle")
                .map_err(|_| ModelError::ColumnError("handle".to_string()))?,
            command: row
                .try_get("command")
                .map_err(|_| ModelError::ColumnError("command".to_string()))?,
            job: row
                .try_get("job")
                .map_err(|_| ModelError::ColumnError("job".to_string()))?,
            stderr: row
                .try_get("stderr")
                .map_err(|_| ModelError::ColumnError("job".to_string()))?,
            stdout: row
                .try_get("stdout")
                .map_err(|_| ModelError::ColumnError("job".to_string()))?,
        })
    }

    /// query database and return all tasks belonging to `job_id`
    pub async fn select_by_job(
        job_id: i64,
        conn: &mut SqliteConnection,
    ) -> Result<Vec<Self>, ModelError> {
        let mut tasks: Vec<Task> = Vec::new();
        let mut rows = sqlx::query(
            "SELECT id, name, status, handle, command, stderr, stdout \
                FROM tasks WHERE job = ?",
        )
        .bind(&job_id)
        .fetch(conn);

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

            tasks.push(Self {
                id: row
                    .try_get("id")
                    .map_err(|_| ModelError::ColumnError("id".to_string()))?,
                name: row
                    .try_get("name")
                    .map_err(|_| ModelError::ColumnError("name".to_string()))?,
                handle: row
                    .try_get("handle")
                    .map_err(|_| ModelError::ColumnError("handle".to_string()))?,
                command: row
                    .try_get("command")
                    .map_err(|_| ModelError::ColumnError("command".to_string()))?,
                job: job_id,
                status,
                stderr: row
                    .try_get("stderr")
                    .map_err(|_| ModelError::ColumnError("command".to_string()))?,
                stdout: row
                    .try_get("stdout")
                    .map_err(|_| ModelError::ColumnError("command".to_string()))?,
            });
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

/// A `Batch` is a graph of task to be executed.
/// It is a composition of 3 kinds of models:
/// * one `Job`: metadata about the whole Batch
/// * a collections of `Task`s, (commands to be executed)
/// * a collections of `TaskDependency`s, which define the ordering
/// constraints of the whole batch execution.
#[derive(Debug)]
pub struct Batch {
    pub job: Job,
    /// One task == one bash command to execute == one node in the execution graph
    pub tasks: Vec<Task>,
    /// execution graph edge
    pub dependencies: Vec<TaskDependency>,
}

impl Batch {
    pub async fn from_shell_command(
        job_name: &str,
        task_name: &str,
        cmd: &str,
        conn: &mut SqliteConnection,
    ) -> Result<Self, ModelError> {
        let mut job = Job::new(&job_name);
        let _ = job.save(conn).await?;
        let job_id: i64 = job.id.ok_or(ModelError::ModelNotFound)?;

        let mut task = Task {
            id: None,
            name: task_name.to_string(),
            status: Status::Pending,
            handle: "".to_string(),
            command: cmd.to_string(),
            job: job_id,
            stdout: None,
            stderr: None,
        };
        let _ = task.save(conn).await?;

        Ok(Self {
            job,
            tasks: vec![task],
            dependencies: vec![],
        })
    }

    pub async fn from_graph(
        workflow: &WorkFlowGraph,
        conn: &mut SqliteConnection,
    ) -> Result<Self, ModelError> {
        // validate input
        if !workflow.are_task_names_unique() {
            return Err(ModelError::InvalidTaskName);
        }
        if !workflow.is_cycle_free() {
            return Err(ModelError::DependencyCycle);
        }

        // Create and save Job
        let mut job = Job::new(&workflow.name);
        let _ = job.save(conn).await?;
        let job_id: i64 = job.id.ok_or(ModelError::ModelNotFound)?;

        // create and save all tasks
        let mut tasks: Vec<Task> = Vec::new();
        for graph_task in &workflow.tasks {
            let mut task = Task {
                id: None,
                name: graph_task.name.clone(),
                status: Status::Pending,
                handle: "".to_string(),
                command: graph_task.command(),
                job: job_id,
                stdout: None,
                stderr: None,
            };
            let _ = task.save(conn).await?;
            tasks.push(task);
        }

        // create and save TaskDependency
        let mut dependencies: Vec<TaskDependency> = Vec::new();
        for (task_idx, deps_ids) in workflow.dependency_indices.iter().enumerate() {
            let child: i64 = tasks[task_idx].id().ok_or(ModelError::ModelNotFound)?;
            for dep_idx in deps_ids {
                let parent: i64 = tasks[*dep_idx].id().ok_or(ModelError::ModelNotFound)?;
                let mut dependency = TaskDependency {
                    id: None,
                    child,
                    parent,
                    job: job_id,
                };
                let _ = dependency.save(conn).await?;
                dependencies.push(dependency);
            }
        }
        Ok(Self {
            job,
            tasks,
            dependencies,
        })
    }

    pub async fn from_job(job: Job, conn: &mut SqliteConnection) -> Result<Self, ModelError> {
        // select tasks by job id
        let tasks = Task::select_by_job(job.id.ok_or(ModelError::ModelNotFound)?, conn).await?;
        let dependencies =
            TaskDependency::select_by_job(job.id.ok_or(ModelError::ModelNotFound)?, conn).await?;
        Ok(Self {
            job,
            tasks,
            dependencies,
        })
    }

    pub async fn next_ready_task<'a, 'b>(&'a mut self) -> Result<Option<&'b mut Task>, ModelError>
    where
        'a: 'b,
    {
        // build task index
        let mut task_index: HashMap<i64, usize> = HashMap::new();
        for (idx, task) in self.tasks.iter().enumerate() {
            task_index.insert(task.id.ok_or(ModelError::ModelNotFound)?, idx);
        }
        // build task dependencies index
        let mut dependencies_index: HashMap<i64, Vec<i64>> = HashMap::new();
        for dependency in self.dependencies.iter() {
            match dependencies_index.get_mut(&dependency.child) {
                // append to existing vec
                Some(deps) => deps.push(dependency.parent),
                // insert new vec
                None => {
                    let _ = dependencies_index.insert(dependency.child, vec![dependency.parent]);
                }
            }
        }
        let mut ready_idx: Option<usize> = None;
        // find task with fulfilled dependencies
        'ready_task_lookup: for (idx, task) in self.tasks.iter().enumerate() {
            // ignore running, of finished tasks
            if !task.status.is_pending() {
                continue 'ready_task_lookup;
            }
            let task_id = task.id.ok_or(ModelError::InvalidTaskId)?;
            // iter dependencies
            if let Some(parent_ids) = dependencies_index.get(&task_id) {
                for parent_id in parent_ids {
                    let parent_idx = task_index.get(parent_id).ok_or(ModelError::InvalidTaskId)?;
                    let parent = &self.tasks[*parent_idx];
                    /* TODO handle failed states */
                    if !parent.status.is_finished() {
                        continue 'ready_task_lookup;
                    }
                }
            }
            ready_idx = Some(idx);
            break;
        }
        match ready_idx {
            Some(idx) => Ok(self.tasks.get_mut(idx)),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::models::Batch;
    use crate::workflows::{WorkFlowGraph, WorkFlowTask};
    use rocket::tokio;
    use sqlx::{Connection, Executor, SqliteConnection};
    use std::collections::HashMap;

    /// create an in-memory database, and execute the initial migration.
    async fn setup_in_memory_database() -> Result<SqliteConnection, Box<dyn std::error::Error>> {
        let mut conn = SqliteConnection::connect("sqlite::memory:").await?;
        conn.execute(include_str!("../migrations/20210402155322_creation.sql"))
            .await?;
        Ok(conn)
    }

    /// build a test graph With 2 tasks A and B, with B depending on A
    fn build_test_workflow_graph() -> WorkFlowGraph {
        WorkFlowGraph {
            name: "test-job".to_string(),
            tasks: vec![
                WorkFlowTask {
                    name: "A".to_string(),
                    cluster_name: None,
                    node_count: 1,
                    executable: "touch".to_string(),
                    executable_arguments: vec!["/tmp/tmp-empty-file".to_string()],
                },
                WorkFlowTask {
                    name: "B".to_string(),
                    cluster_name: None,
                    node_count: 1,
                    executable: "rm".to_string(),
                    executable_arguments: vec!["/tmp/tmp-empty-file".to_string()],
                },
            ],
            dependency_indices: vec![vec![], vec![0]],
            name_to_idx: {
                let mut name_to_idx: HashMap<String, usize> = HashMap::new();
                name_to_idx.insert("A".to_string(), 0);
                name_to_idx.insert("B".to_string(), 1);
                name_to_idx
            },
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_graph_persistence() {
        // build DB
        let mut conn = setup_in_memory_database().await.unwrap();
        // build graph
        let graph = build_test_workflow_graph();
        assert!(graph.is_cycle_free());
        assert!(graph.are_task_names_unique());

        // test batch creation
        let batch = Batch::from_graph(&graph, &mut conn)
            .await
            .expect("Failed to build Batch");
        assert_eq!(&batch.job.name, "test-job");

        // test (loosely) the model creation in the database
        let (job_name,): (String,) = sqlx::query_as("SELECT name FROM jobs")
            .fetch_one(&mut conn)
            .await
            .expect("failed to count tasks");
        assert_eq!(&job_name, &batch.job.name);

        let (task_count,): (i64,) = sqlx::query_as("SELECT count(id) FROM tasks")
            .fetch_one(&mut conn)
            .await
            .expect("failed to count tasks");
        assert_eq!(task_count, 2);

        let (dep_count,): (i64,) = sqlx::query_as("SELECT count(id) FROM task_dependencies")
            .fetch_one(&mut conn)
            .await
            .expect("failed to count tasks");
        assert_eq!(dep_count, 1);
    }
}
