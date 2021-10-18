use crate::scheduling::SchedulerClient;
use crate::sqlx::Row;
use rocket::{form::Form, fs::TempFile, State};
use sqlx::sqlite::SqliteConnection;
use std::io::{Error as IOError, ErrorKind};

#[derive(FromForm)]
pub struct CredentialFileForm<'r> {
    credential: TempFile<'r>,
}

#[post("/login", data = "<credential>")]
pub async fn login(mut credential: Form<CredentialFileForm<'_>>) -> &'static str {
    "TODO"
}
