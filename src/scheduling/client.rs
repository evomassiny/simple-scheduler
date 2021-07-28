use sqlx::sqlite::SqlitePool;
use std::path::PathBuf;

pub struct SchedulerClient {
    pub socket: PathBuf,
    pub pool: SqlitePool,
}

