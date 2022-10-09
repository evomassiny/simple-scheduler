use rocket::serde::Deserialize;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions, SqliteConnectOptions};
use sqlx::{Connection, SqliteConnection, ConnectOptions};
use std::str::FromStr;

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
        let mut options = SqliteConnectOptions::from_str(&self.database_url)
            .map_err(|e| format!("failed to build database options: {:?}", e))?;
        options.disable_statement_logging();
        // Build database connection pool
        let pool = SqlitePoolOptions::new()
            .min_connections(16)
            .max_connections(16)
            .connect_with(options)
            .await
            .map_err(|e| format!("failed to get database pool: {:?}", e))?;

        Ok(pool)
    }

    pub async fn database_connection(&self) -> Result<SqliteConnection, String> {
        let conn = SqliteConnectOptions::from_str(&self.database_url)
            .map_err(|e| format!("failed to build database options: {:?}", e))?
            .disable_statement_logging()
            .connect()
            .await
            .map_err(|e| format!("failed to connect to database: {:?}", e))?;
        Ok(conn)
    }
}
