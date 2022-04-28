use crate::auth::AuthToken;
use crate::models::{Job, Status};
use crate::scheduling::SchedulerClient;
use rocket::{
    form::Form,
    fs::TempFile,
    futures::TryStreamExt,
    http::Status as HttpStatus,
    response::status::{Custom, NotFound},
    serde::json::{json, Value as JsonValue},
    State,
};
use sqlx::Row;
use std::collections::HashMap;

#[derive(FromForm)]
pub struct WorkflowForm<'r> {
    file: TempFile<'r>,
}

/// Parse a Workflow file from an uploaded XML,
/// submit it as a new (batch) job,
/// and return its ID.
#[post("/submit", format = "multipart/form-data", data = "<uploaded_file>")]
pub async fn submit_job(
    scheduler: &State<SchedulerClient>,
    auth: AuthToken,
    mut uploaded_file: Form<WorkflowForm<'_>>,
) -> Result<JsonValue, Custom<String>> {
    let scheduler = scheduler.inner();
    let mut read_conn = scheduler
        .read_pool
        .acquire()
        .await
        .map_err(|e| Custom(HttpStatus::InternalServerError, e.to_string()))?;
    let user = auth.fetch_user(&mut read_conn).await.ok_or(Custom(
        HttpStatus::InternalServerError,
        "unknown user".to_string(),
    ))?;
    // submit job
    let job_id = scheduler
        .submit_from_tempfile(&mut uploaded_file.file, &user)
        .await
        .map_err(|e| Custom(HttpStatus::InternalServerError, e.to_string()))?;

    Ok(json!({
        "id": job_id,
    }))
}

/// return status of jobs and related task
/// (agglomerated sum of running, pending and succeed task).
#[get("/jobs/<job_id>")]
pub async fn job_status(
    scheduler: &State<SchedulerClient>,
    _auth: AuthToken,
    job_id: i64,
) -> Result<JsonValue, NotFound<String>> {
    let scheduler = scheduler.inner();

    let mut read_conn = scheduler
        .read_pool
        .acquire()
        .await
        .map_err(|e| NotFound(e.to_string()))?;

    let status: String = Job::get_job_status_by_id(job_id, &mut read_conn)
        .await
        .map_err(|e| NotFound(e.to_string()))?
        .as_proactive_string();

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
    .fetch_one(&mut read_conn)
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

    // fetch status for each tasks
    let mut rows = sqlx::query("SELECT name, status FROM tasks WHERE job = ?")
        .bind(&job_id)
        .fetch(&mut read_conn);
    let mut task_details: HashMap<String, JsonValue> = HashMap::new();

    while let Some(row) = rows.try_next().await.map_err(|e| NotFound(e.to_string()))? {
        // fetch name
        let name: String = row.try_get("name").map_err(|e| NotFound(e.to_string()))?;
        // fetch status, build a string from it
        let status_code: u8 = row.try_get("status").map_err(|e| NotFound(e.to_string()))?;
        let status: String = Status::from_u8(status_code)
            .map_err(NotFound)?
            .as_proactive_string();
        task_details.insert(
            name,
            json!({
                    "taskInfo": { "taskStatus": status }
            }),
        );
    }

    // mimicks proactive API
    Ok(json!({
        "jobInfo": {
            "jobId": job_id,
            "status": status,
            "numberOfFinishedTasks": succeed_tasks_count,
            "numberOfPendingTasks": pending_tasks_count,
            "numberOfRunningTasks": running_tasks_count,
            "totalNumberOfTasks": total_count,
        },
        "tasks": task_details,
    }))
}

/// Request to kill job.
#[put("/jobs/<job_id>/kill")]
pub async fn kill_job(
    scheduler: &State<SchedulerClient>,
    _auth: AuthToken,
    job_id: i64,
) -> Result<JsonValue, Custom<String>> {
    let scheduler = scheduler.inner();

    scheduler
        .kill_job(job_id)
        .await
        .map_err(|e| Custom(HttpStatus::InternalServerError, e.to_string()))?;

    // mimicks proactive API
    Ok(json!({
        "jobInfo": {
            "jobId": job_id,
            "status": Status::Killed.as_proactive_string(),
        },
    }))
}
