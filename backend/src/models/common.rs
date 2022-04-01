use async_trait::async_trait;
use sqlx::sqlite::SqliteConnection;

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

