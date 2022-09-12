use rocket::serde::Deserialize;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use sqlx::{Connection, SqliteConnection};

#[derive(Deserialize, Debug)]
pub struct Config {
    pub database_url: String,
    pub process_directory: String,
    pub hypervisor_socket_path: String,
    pub private_key_path: String,
    pub public_key_path: String,
    pub nb_of_workers: usize,
}

impl Config {
    pub async fn database_pool(&self) -> Result<SqlitePool, String> {
        // Build database connection pool
        let pool = SqlitePoolOptions::new()
            .min_connections(16)
            .max_connections(16)
            .connect(&self.database_url)
            .await
            .map_err(|e| format!("failed to get database pool: {:?}", e))?;

        Ok(pool)
    }

    pub async fn database_connection(&self) -> Result<SqliteConnection, String> {
        SqliteConnection::connect(&self.database_url)
            .await
            .map_err(|e| format!("failed to connect to database {:?}", e))
    }
}
