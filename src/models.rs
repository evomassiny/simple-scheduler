use crate::workflows::{WorkflowError, WorkFlowTask, WorkFlowGraph};
use crate::tasks::TaskStatus;
use async_trait::async_trait;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions, SqliteConnection};
use chrono::{DateTime, TimeZone, NaiveDateTime, Utc};
use sqlx::Row;

#[derive(Debug)]
pub enum ModelError {
    InvalidTaskName,
    DependencyCycle,
    ModelNotFound,
    DBError,
}

/// trait shared by Database models,
/// allow easier database manipulation.
#[async_trait]
pub trait Model {
    async fn update(&mut self, conn: &mut SqliteConnection) -> Result<(), ModelError>;
    async fn create(&mut self, conn: &mut SqliteConnection) -> Result<(), ModelError>;
    fn id(&self) -> Option<i64>;

    //async fn save(&mut self) -> Result<(), ModelError>;
    async fn save(&mut self, conn: &mut SqliteConnection) -> Result<(), ModelError>{
        let id = self.id();
        if id.is_some() {
            self.update(conn).await?;
        } else {
            self.create(conn).await?;
        }
        Ok(())
    }
}

//#[async_trait]
//impl Model {
//}

/// State of job/task
#[derive(Debug)]
pub enum Status {
    Pending,
    Stopped,
    Killed,
    Failed,
    Succeed,
    Running,
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

    pub fn from_u8(value: u8) -> Result<Self, String> {
        match value {
            0 => Ok(Self::Pending),
            1 => Ok(Self::Stopped),
            2 => Ok(Self::Killed),
            3 => Ok(Self::Failed),
            4 => Ok(Self::Succeed),
            5 => Ok(Self::Running),
            v => Err(format!("'{:}' cannot be converted to a Status", v))
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
        }
    }
}

#[derive(Debug)]
pub struct Job {
    pub id: Option<i64>,
    pub name: String,
    pub submit_time: NaiveDateTime,
    pub status: Status,
}

#[async_trait]
impl Model for Job {
    async fn update(&mut self, conn: &mut SqliteConnection) -> Result<(), ModelError> {
        let _query_result = sqlx::query("UPDATE jobs VALUES name = ?, submit_time = ?, status = ? WHERE id = ?")
            .bind(&self.name)
            .bind(&self.submit_time.format("%Y-%m-%d %H:%M:%S").to_string())
            .bind(self.status.as_u8())
            .bind(self.id.ok_or(ModelError::ModelNotFound)?)
            .execute(conn)
            .await
            .map_err(|_| ModelError::DBError)?;
        Ok(())
    }

    async fn create(&mut self, conn: &mut SqliteConnection) -> Result<(), ModelError>{
        let query_result = sqlx::query("INSERT INTO jobs (name, submit_time, status) VALUES (?, ?, ?)")
            .bind(&self.name)
            .bind(&self.submit_time.format("%Y-%m-%d %H:%M:%S").to_string())
            .bind(self.status.as_u8())
            .execute(conn)
            .await
            .map_err(|_| ModelError::DBError)?;
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
            submit_time: NaiveDateTime::from_timestamp(
                Utc::now().timestamp(), 
                0,
            ),
            status: Status::Pending,
        }
    }

}

#[derive(Debug)]
pub struct Task {
    pub id: Option<i64>,
    pub name: String,
    pub status: Status,
    pub handle: String,
    pub command: String,
    pub job: i64,
}

#[async_trait]
impl Model for Task {
    async fn update(&mut self, conn: &mut SqliteConnection) -> Result<(), ModelError> {
        let _query_result = sqlx::query("UPDATE tasks VALUES name = ?, status = ?, handle = ?, command = ?, job = ? WHERE id = ?")
            .bind(&self.name)
            .bind(self.status.as_u8())
            .bind(&self.handle)
            .bind(&self.command)
            .bind(&self.job)
            .bind(self.id.ok_or(ModelError::ModelNotFound)?)
            .execute(conn)
            .await
            .map_err(|_| ModelError::DBError)?;
        Ok(())
    }

    async fn create(&mut self, conn: &mut SqliteConnection) -> Result<(), ModelError>{
        let query_result = sqlx::query("INSERT INTO jobs (name, status, handle, command, job) VALUES (?, ?, ?, ?, ?)")
            .bind(&self.name)
            .bind(self.status.as_u8())
            .bind(&self.handle)
            .bind(&self.command)
            .bind(&self.job)
            .execute(conn)
            .await
            .map_err(|_| ModelError::DBError)?;
        let id = query_result.last_insert_rowid();
        self.id = Some(id);
        Ok(())
    }

    fn id(&self) -> Option<i64> {
        self.id
    }

}

impl Task {

    /// Select a Task by its handle, and update its status
    pub async fn select_by_handle_and_set_status(handle: &str, status: &TaskStatus, conn: &mut SqliteConnection) -> Result<(), ModelError> {
        let status_code: u8 = Status::from_task_status(status).as_u8();
        sqlx::query("UPDATE tasks SET status = ? WHERE handle = ?")
            .bind(status_code)
            .bind(handle)
            .execute(conn)
            .await
            .map_err(|_| ModelError::ModelNotFound)?;
        Ok(())
    }
}



#[derive(Debug)]
pub struct TaskDependency {
    pub id: Option<i64>,
    pub child: i64,
    pub parent: i64,
}

#[async_trait]
impl Model for TaskDependency {
    async fn update(&mut self, conn: &mut SqliteConnection) -> Result<(), ModelError> {
        let _query_result = sqlx::query("UPDATE task_dependencies VALUES child = ?, parent = ? WHERE id = ?")
            .bind(&self.child)
            .bind(&self.parent)
            .bind(self.id.ok_or(ModelError::ModelNotFound)?)
            .execute(conn)
            .await
            .map_err(|_| ModelError::DBError)?;
        Ok(())
    }

    async fn create(&mut self, conn: &mut SqliteConnection) -> Result<(), ModelError>{
        let query_result = sqlx::query("INSERT INTO task_dependencies (child, parent) VALUES (?, ?)")
            .bind(&self.child)
            .bind(&self.parent)
            .execute(conn)
            .await
            .map_err(|_| ModelError::DBError)?;
        let id = query_result.last_insert_rowid();
        self.id = Some(id);
        Ok(())
    }

    fn id(&self) -> Option<i64> {
        self.id
    }

}


/// A `Batch` is a graph of task to be executed.
/// It is a composition of 3 kinds of models:
/// * one `Job`: metadata about the whole Batch 
/// * a collections of `Task`s, (commands to be executed)
/// * a collections of `TaskDependency`s, which define the ordering
/// constraints of the whole batch execution.
pub struct Batch {
    pub job: Job,
    /// One task == one bash command to execute == one node in the execution graph
    pub tasks: Vec<Task>,
    /// execution graph edge
    pub dependencies: Vec<TaskDependency>,
}


impl Batch {
    pub fn from_graph(workflow: &WorkFlowGraph, conn: &mut SqliteConnection) -> Result<Self, ModelError> {
        // validate input
        if !workflow.are_task_names_unique() {
            return Err(ModelError::InvalidTaskName);
        }
        if !workflow.is_cycle_free() {
            return Err(ModelError::DependencyCycle);
        }

        // Create and save Job
        let mut job = Job::new(&workflow.name);
        job.save(conn);
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
            };
            task.save(conn);
            tasks.push(task);
        }

        // create and save TaskDependency
        let mut dependencies: Vec<TaskDependency> = Vec::new();
        for (task_idx, deps_ids) in workflow.dependency_indices.iter().enumerate() {
            let child: i64 = tasks[task_idx].id().ok_or(ModelError::ModelNotFound)?;
            for dep_idx in deps_ids {
                let parent: i64 = tasks[*dep_idx].id().ok_or(ModelError::ModelNotFound)?;
                let mut dependency = TaskDependency { id: None, child, parent };
                dependency.save(conn);
                dependencies.push(dependency);
            }
        }
        Ok(Batch { job, tasks, dependencies })
    }

}

