use crate::auth::AuthToken;
use crate::models::{JobId, Status, TaskId};
use crate::scheduling::{JobStatusDetail, SchedulerClient};
use rocket::{
    form::Form,
    fs::TempFile,
    futures::TryStreamExt,
    http::Status as HttpStatus,
    response::status::{Custom, NotFound},
    serde::json::{json, Value as JsonValue},
    State,
};
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

    let user = scheduler
        .fetch_user(&auth)
        .await
        .map_err(|_e| Custom(HttpStatus::InternalServerError, "unknown user".to_string()))?;

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
    job_id: JobId,
) -> Result<JsonValue, NotFound<String>> {
    let scheduler = scheduler.inner();

    let JobStatusDetail {
        id,
        status,
        task_statuses,
    } = scheduler
        .get_job_status(job_id)
        .await
        .map_err(|e| NotFound(e.to_string()))?;

    let mut pending_count: usize = 0;
    let mut running_count: usize = 0;
    let mut finished_count: usize = 0;
    let total_count = task_statuses.len();

    let mut task_details: HashMap<TaskId, JsonValue> = HashMap::new();
    for (task_id, task_status) in task_statuses {
        match &task_status {
            &Status::Pending => pending_count += 1,
            &Status::Running => running_count += 1,
            _ => {}
        }
        if task_status.is_finished() {
            finished_count += 1;
        }
        task_details.insert(
            task_id,
            json!({
                    "taskInfo": { "taskStatus": task_status.as_proactive_string() }
            }),
        );
    }
    // mimicks proactive API
    Ok(json!({
        "jobInfo": {
            "jobId": id,
            "status": status.as_proactive_string(),
            "numberOfFinishedTasks": finished_count,
            "numberOfPendingTasks": pending_count,
            "numberOfRunningTasks": running_count,
            "totalNumberOfTasks": total_count,
        },
        "tasks": task_details,
    }))
}

/// return list of tasks related to `job_id`
#[get("/jobs/<job_id>/tasks")]
pub async fn list_job_tasks(
    scheduler: &State<SchedulerClient>,
    _auth: AuthToken,
    job_id: JobId,
) -> Result<JsonValue, NotFound<String>> {
    let scheduler = scheduler.inner();

    let JobStatusDetail { task_statuses, .. } = scheduler
        .get_job_status(job_id)
        .await
        .map_err(|e| NotFound(e.to_string()))?;

    let task_ids: Vec<TaskId> = task_statuses.into_iter().map(|(id, _)| id).collect();
    Ok(json!({ "tasks": task_ids }))
}

/// Request to kill job.
#[put("/jobs/<job_id>/kill")]
pub async fn kill_job(
    scheduler: &State<SchedulerClient>,
    _auth: AuthToken,
    job_id: JobId,
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
