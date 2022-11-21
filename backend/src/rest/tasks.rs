use crate::auth::AuthToken;
use crate::models::TaskId;
use crate::scheduling::SchedulerClient;
use rocket::{
    response::status::NotFound,
    serde::json::{json, Value as JsonValue},
    State,
};

/// return output of given tasks
#[get("/tasks/<task_id>")]
pub async fn task_outputs(
    scheduler: &State<SchedulerClient>,
    auth: AuthToken,
    task_id: TaskId,
) -> Result<JsonValue, NotFound<String>> {
    let scheduler = scheduler.inner();

    let output = scheduler
        .get_task_outputs(task_id, auth.user_id)
        .await
        .map_err(|e| NotFound(e.to_string()))?;

    Ok(json!({
        "status": output.status.as_proactive_string(),
        "stderr": output.stderr,
        "stdout": output.stdout,
    }))
}
