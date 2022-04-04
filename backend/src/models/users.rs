use crate::models::{Model, ModelError};
use scrypt::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Scrypt,
};
use sqlx::SqliteConnection;

/// Abstraction over the `users` SQl table, defined as such:
/// ```sql
/// CREATE TABLE IF NOT EXISTS users (
///       id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
///       name VARCHAR(256) NOT NULL,
///       password_hash VARCHAR(256) NOT NULL,
/// );
/// ```
///
/// This table stores the name and password hash (+salt) of a user.
#[derive(Debug)]
pub struct User {
    /// User name
    name: String,
    /// PHC string, contains
    /// * hash algorithm name
    /// * salt
    /// * hashed password
    password_hash: String,
    /// database index, or None if unsaved.
    id: Option<i64>,
}

#[async_trait]
impl Model for User {
    async fn update(&mut self, conn: &mut SqliteConnection) -> Result<(), ModelError> {
        let _query_result = sqlx::query(
            "UPDATE users SET name = ?, password_hash = ?) \
            WHERE id = ?",
        )
        .bind(&self.name)
        .bind(&self.password_hash)
        .bind(&self.id.ok_or(ModelError::ModelNotFound)?)
        .execute(conn)
        .await
        .map_err(|e| ModelError::DbError(format!("{:?}", e)))?;
        Ok(())
    }

    async fn create(&mut self, conn: &mut SqliteConnection) -> Result<(), ModelError> {
        let query_result = sqlx::query(
            "INSERT INTO users (name, password_hash) \
            VALUES (?, ?)",
        )
        .bind(&self.name)
        .bind(&self.password_hash)
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

impl User {
    /// Create a new User struct (hash the provided password)
    pub fn new(name: &str, password: &str) -> Result<Self, String> {
        let salt = SaltString::generate(&mut OsRng);

        let password_hash = Scrypt
            .hash_password(password.as_bytes(), &salt)
            .map_err(|e| format!("Error while creating user model {:?}", e))?
            .to_string();
        Ok(Self {
            name: name.to_owned(),
            password_hash,
            id: None,
        })
    }

    /// Verify password against self.password_hash
    pub fn verify_password(&self, password: &str) -> Result<bool, String> {
        let parsed_hash = PasswordHash::new(&self.password_hash)
            .map_err(|e| format!("Error while parsing user PHC string {:?}", e))?;
        Ok(Scrypt
            .verify_password(password.as_bytes(), &parsed_hash)
            .is_ok())
    }

    pub async fn get_from_name(
        name: &str,
        conn: &mut SqliteConnection,
    ) -> Option<Self> {
        let row: (i64, String) =
            sqlx::query_as("SELECT id, password_hash FROM users WHERE name = ?")
                .bind(&name)
                .fetch_one(conn)
                .await
                .ok()?;

        Some(Self {
            name: name.to_owned(),
            password_hash: row.1,
            id: Some(row.0),
        })
    }

    pub async fn get_from_id(id: i64, conn: &mut SqliteConnection) -> Option<Self> {
        let row: (String, String) =
            sqlx::query_as("SELECT name, password_hash FROM users WHERE id = ?")
                .bind(id)
                .fetch_one(conn)
                .await
                .ok()?;

        Some(Self {
            name: row.0,
            password_hash: row.1,
            id: Some(id),
        })
    }
}




#[cfg(test)]
mod tests {
    use rocket::tokio;
    use sqlx::{Connection, Executor, SqliteConnection};
    use crate::models::{User, Model};

    #[test]
    fn test_password_verification() {
        let user = User::new("debug-user", "debug-password").expect("Failed to build User struct");

        assert_eq!(user.verify_password("debug-password"), Ok(true));
        assert_eq!(user.verify_password("not-good-password"), Ok(false));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_user_login() {
        // build DB
        let mut conn = SqliteConnection::connect("sqlite::memory:")
            .await
            .expect("Could not create in-memory test db.");
        conn.execute(include_str!("../../migrations/20210402155322_creation.sql"))
            .await.expect("Could not build test database.");

        let mut user = User::new("debug-user", "debug-password").unwrap();
        user.save(&mut conn).await.unwrap();

        let user = User::get_from_name("debug-user", &mut conn)
            .await
            .expect("Could not fetch User struct");
        assert_eq!(user.verify_password("debug-password"), Ok(true));
    }
}
