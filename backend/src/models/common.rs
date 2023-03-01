#[derive(Debug)]
pub enum ModelError {
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
