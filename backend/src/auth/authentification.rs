use crate::scheduling::SchedulerClient;
use crate::auth::{KeyPair, Credentials};
use tempfile::NamedTempFile;
use rocket::{form::Form, fs::TempFile, State};
use rocket::tokio::{self, fs::File, io::AsyncReadExt};
use sqlx::sqlite::SqliteConnection;

#[derive(FromForm)]
pub struct CredentialFileForm<'r> {
    credential: TempFile<'r>,
}
impl <'r> CredentialFileForm<'r> {

    pub async fn read_content(&mut self) -> Result<String, Box<dyn std::error::Error>> {
        // persist the temporary file
        // * use a NamedtempFile to get a free path
        let file = tokio::task::spawn_blocking(NamedTempFile::new)
            .await
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "spawn_block panic")
            })??;
        // * move the tempfile content to it
        self.credential.persist_to(file.path()).await?;
        // read the file content
        let mut file = File::open(&file.path()).await?;
        let mut content: String = String::new();
        file.read_to_string(&mut content).await?;
        Ok(content)
    }
}

#[post("/login", data = "<credential>")]
pub async fn login(key_pair: &State<KeyPair>, mut credential: Form<CredentialFileForm<'_>>) -> &'static str {

    if let Ok(content) = credential.read_content().await {
        if let Ok(cred) = key_pair.decode_credentials(&content) {
            return if cred.is_allowed() { "yes" } else { "no" };
        }
    }
    "failed to connect"
}

//pub async fn create_user(db_conn: &mut SqliteConnection, login: String, password: String, key_pair: &KeyPair) -> Result<(), ()> {
    
//}
