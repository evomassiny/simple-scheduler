use crate::scheduling::SchedulerClient;
use crate::sqlx::Row;
use rocket::{
    fs::TempFile,
    State,
    form::Form,
};
use sqlx::sqlite::SqliteConnection;

#[derive(FromForm)]
pub struct CredentialFileForm<'r> {
    credential: TempFile<'r>,
}

#[post("/login", data = "<credential>")]
pub async fn login(mut credential: Form<CredentialFileForm<'_>>,) -> &'static str {
    "TODO"
}

