use crate::models::{Existing, Model, ModelError, New};
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
pub struct User<State> {
    /// User name
    pub name: String,
    /// PHC string, contains
    /// * hash algorithm name
    /// * salt
    /// * hashed password
    pub password_hash: String,
    /// database index, or None if unsaved.
    pub state: State,
}

impl<T> User<T> {
    /// Create a new User struct (hash the provided password)
    pub fn new(name: &str, password: &str) -> Result<User<New>, String> {
        Ok(User {
            name: name.to_owned(),
            password_hash: build_password_hash(password)?,
            state: New,
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
}

impl User<New> {
    pub async fn create(self, conn: &mut SqliteConnection) -> Result<User<Existing>, ModelError> {
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
        Ok(User {
            name: self.name,
            password_hash: self.password_hash,
            state: Existing { id },
        })
    }
}

impl User<Existing> {
    pub async fn get_from_name(name: &str, conn: &mut SqliteConnection) -> Option<Self> {
        let row: (i64, String) =
            sqlx::query_as("SELECT id, password_hash FROM users WHERE name = ?")
                .bind(&name)
                .fetch_one(conn)
                .await
                .ok()?;

        Some(User {
            name: name.to_owned(),
            password_hash: row.1,
            state: Existing { id: row.0 },
        })
    }

    pub async fn get_from_id(id: i64, conn: &mut SqliteConnection) -> Option<Self> {
        let row: (String, String) =
            sqlx::query_as("SELECT name, password_hash FROM users WHERE id = ?")
                .bind(id)
                .fetch_one(conn)
                .await
                .ok()?;

        Some(User {
            name: row.0,
            password_hash: row.1,
            state: Existing { id },
        })
    }

    pub async fn update(&self, conn: &mut SqliteConnection) -> Result<(), ModelError> {
        let query_result = sqlx::query(
            "UPDATE users SET name = ? , password_hash = ? \
            WHERE id = ?",
        )
        .bind(&self.name)
        .bind(&self.password_hash)
        .bind(&self.state.id)
        .execute(conn)
        .await
        .map_err(|e| ModelError::DbError(format!("{:?}", e)))?;
        Ok(())
    }
}

fn build_password_hash(password: &str) -> Result<String, String> {
    let salt = SaltString::generate(&mut OsRng);

    let password_hash = Scrypt
        .hash_password(password.as_bytes(), &salt)
        .map_err(|e| format!("Error while creating user model {:?}", e))?
        .to_string();
    Ok(password_hash)
}

pub async fn create_or_update_user(
    name: &str,
    password: &str,
    conn: &mut SqliteConnection,
) -> Result<User<Existing>, String> {
    match User::<Existing>::get_from_name(&name, &mut *conn).await {
        Some(mut user) => {
            user.password_hash = build_password_hash(&password)
                .map_err(|e| format!("Failed to hash new password {:?}", e))?;

            user.update(&mut *conn)
                .await
                .map_err(|e| format!("Failed to set new password, {:?}", e))?;
            Ok(user)
        }
        None => {
            let new_user = User::<New>::new(&name, &password)
                .map_err(|e| format!("failed to build user object: {:?}", e))?;
            let user = new_user
                .create(&mut *conn)
                .await
                .map_err(|e| format!("failed to set new password: {:?}", e))?;
            Ok(user)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::models::{New, User};
    use rocket::tokio;
    use sqlx::{Connection, Executor, SqliteConnection};

    #[test]
    fn test_password_verification() {
        let user =
            User::<New>::new("debug-user", "debug-password").expect("Failed to build User struct");

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
            .await
            .expect("Could not build test database.");

        let new_user = User::<New>::new("debug-user", "debug-password").unwrap();

        let user = new_user.create(&mut conn).await.unwrap();

        let same_user = User::get_from_name("debug-user", &mut conn)
            .await
            .expect("Could not fetch User struct");
        assert_eq!(user.verify_password("debug-password"), Ok(true));
        assert_eq!(same_user.verify_password("debug-password"), Ok(true));
        assert_eq!(same_user.state.id, user.state.id);
    }
}
