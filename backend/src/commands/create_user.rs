use crate::config::Config;
use crate::models::create_or_update_user;

/// Create (or update) user credentials.
pub async fn create_user(name: &str, password: &str, config: &Config) -> Result<(), String> {
    let conn = &mut config
        .database_connection()
        .await
        .map_err(|e| format!("failed to get database pool: {:?}", e))?;
    create_or_update_user(name, password, conn)
        .await
        .map_err(|e| format!("failed to set user: {:?}", e))?;
    Ok(())
}
