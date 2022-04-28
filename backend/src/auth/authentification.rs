use crate::auth::AuthToken;
use crate::auth::KeyPair;
use crate::scheduling::SchedulerClient;
use rocket::http::{CookieJar, Status};
use rocket::tokio::{self, fs::File, io::AsyncReadExt};
use rocket::{form::Form, fs::TempFile, State};
use tempfile::NamedTempFile;

/// A Token file, from the client,
/// (contains an encrypted serialized java object which contains credential infos).
#[derive(FromForm)]
pub struct CredentialFileForm<'r> {
    credential: TempFile<'r>,
}
impl<'r> CredentialFileForm<'r> {
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

/// parse any provided credential file, if the
/// referenced user is valid, stored an authentification token,
/// (client side, in a cookie).
///
/// This token will then be used to discriminate authorized users.
#[post("/login", data = "<credential>")]
pub async fn login(
    key_pair: &State<KeyPair>,
    scheduler: &State<SchedulerClient>,
    cookies: &CookieJar<'_>,
    mut credential: Form<CredentialFileForm<'_>>,
) -> (Status, &'static str) {
    let mut read_conn = match scheduler.inner().read_pool.acquire().await {
        Ok(conn) => conn,
        Err(_) => return (Status::BadRequest, "Failed to acquire db handle."),
    };

    let maybe_credential = match credential.read_content().await {
        Ok(content) => key_pair.decode_credentials(&content).ok(),
        Err(_) => return (Status::BadRequest, "Failed to parse credentials."),
    };

    if let Some(credential) = maybe_credential {
        match credential.get_user(&mut read_conn).await {
            Some(user) => {
                // safe to unwrap because get_user() asserts that user exists.
                let token = AuthToken::new(user).unwrap();
                let cookie = token.as_cookie().into_owned();
                cookies.add_private(cookie);
                return (Status::Ok, "success");
            }
            None => return (Status::Forbidden, "wrong user"),
        }
    }
    (Status::Forbidden, "wrong credentials")
}
