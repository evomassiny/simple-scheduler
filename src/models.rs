use chrono::NaiveDateTime;
use crate::workflows::{WorkflowError, WorkFlowTask, WorkFlowGraph};
use crate::tasks::TaskStatus;
use async_trait::async_trait;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions, SqliteConnection};
use sqlx::Row;

#[derive(Debug)]
pub enum ModelError {
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

pub struct Workflow {
    pub job: Job,
    pub tasks: Vec<Task>,
    pub dependencies: Vec<TaskDependency>,
}


impl Workflow {
    pub fn from_graph(workflows: &WorkFlowGraph) -> Self {
        unimplemented!();
    }
}

