use crate::models::{Job, Status};
use crate::scheduling::SchedulerClient;
use crate::sqlx::Row;
use rocket::{
    fs::TempFile,
    http::Status as HttpStatus,
    response::status::{Custom, NotFound},
    serde::json::{json, Value as JsonValue},
    State,
};
use sqlx::sqlite::SqliteConnection;

/// Parse a Workflow file from an uploaded XML,
/// submit it as a new (batch) job,
/// and return its ID.
#[post("/submit", format = "application/xml", data = "<uploaded_file>")]
pub async fn submit_job(
    scheduler: &State<SchedulerClient>,
    mut uploaded_file: TempFile<'_>,
) -> Result<JsonValue, Custom<String>> {
    let scheduler = scheduler.inner();
    // submit job
    let job_id = scheduler
        .submit_from_tempfile(&mut uploaded_file)
        .await
        .map_err(|e| Custom(HttpStatus::InternalServerError, e.to_string()))?;

    Ok(json!({
        "jobId": job_id,
    }))
}

/// return status of jobs and related task
/// (agglomerated sum of running, pending and succeed task).
#[get("/jobs/<job_id>")]
pub async fn job_status(
    scheduler: &State<SchedulerClient>,
    job_id: i64,
) -> Result<JsonValue, NotFound<String>> {
    let scheduler = scheduler.inner();

    let mut conn = scheduler
        .pool
        .acquire()
        .await
        .map_err(|e| NotFound(e.to_string()))?;

    let status: &str = match Job::get_job_status_by_id(job_id, &mut conn)
        .await
        .map_err(|e| NotFound(e.to_string()))?
    {
        Status::Pending => "PENDING",
        Status::Stopped => "PAUSED",
        Status::Killed => "KILLED",
        Status::Failed => "FAILED",
        Status::Succeed => "FINISHED",
        Status::Running => "RUNNING",
    };

    let row = sqlx::query(
        "SELECT COUNT(*) AS total_count, \
            SUM(CASE WHEN status = ? then 1 ELSE 0 END) as succeed_count, \
            SUM(CASE WHEN status = ? then 1 ELSE 0 END) as pending_count, \
            SUM(CASE WHEN status = ? then 1 ELSE 0 END) as running_count \
            FROM tasks WHERE job = ?",
    )
    .bind(Status::Succeed.as_u8())
    .bind(Status::Pending.as_u8())
    .bind(Status::Running.as_u8())
    .bind(&job_id)
    .fetch_one(&mut conn)
    .await
    .map_err(|e| NotFound(e.to_string()))?;

    let total_count: i32 = row
        .try_get("total_count")
        .map_err(|e| NotFound(e.to_string()))?;
    let succeed_tasks_count: i32 = row
        .try_get("succeed_count")
        .map_err(|e| NotFound(e.to_string()))?;
    let pending_tasks_count: i32 = row
        .try_get("pending_count")
        .map_err(|e| NotFound(e.to_string()))?;
    let running_tasks_count: i32 = row
        .try_get("running_count")
        .map_err(|e| NotFound(e.to_string()))?;

    // mimicking proactive responce
    Ok(json!({
        "jobInfo": {
            "jobId": job_id,
            "status": status,
            "numberOfFinishedTasks": succeed_tasks_count,
            "numberOfPendingTasks": pending_tasks_count,
            "numberOfRunningTasks": running_tasks_count,
            "totalNumberOfTasks": total_count,
        }
    }))
}

#[get("/spawn")]
pub async fn debug_spawn(scheduler: &State<SchedulerClient>) -> String {
    // build task DB object and get its id
    let cmd: String = "sleep 30 && echo $(date)".to_string();
    let scheduler = scheduler.inner();
    scheduler
        .submit_command_job("command_job", "task", &cmd)
        .await
        .expect("failed");
    String::from("task successfully launched")
}
