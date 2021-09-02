use crate::scheduling::SchedulerClient;
use crate::sqlx::Row;
use rocket::{
    fs::TempFile,
    State,
};
use sqlx::sqlite::SqliteConnection;

/// Parse a Workflow file from an uploaded XML,
/// submit it as a new (batch) job,
/// and return its ID.
#[post("/submit", format = "text/plain", data = "<cred>")]
pub async fn login(
    _scheduler: &State<SchedulerClient>,
    mut cred: TempFile<'_>,
) -> &'static str {
    "TODO"
}

