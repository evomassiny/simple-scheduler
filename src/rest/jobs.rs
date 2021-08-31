use crate::scheduling::SchedulerClient;
use rocket::{fs::TempFile, State};

#[post("/submit", format = "application/xml", data = "<uploaded_file>")]
pub async fn submit_job(
    scheduler: &State<SchedulerClient>,
    mut uploaded_file: TempFile<'_>,
) -> String {
    let scheduler = scheduler.inner();
    match scheduler.submit_from_tempfile(&mut uploaded_file).await {
        Ok(job_id) => format!("Job {:?} successfully created", &job_id),
        Err(error) => format!("Failed to create job: {:?}", error),
    }
}

#[get("/submit/<job_id>")]
pub async fn job_status(scheduler: &State<SchedulerClient>, job_id: i64) -> String {
    let scheduler = scheduler.inner();
    // TODO
    // output must contain
    // { "jobInfo": { "status": status } }
    // with status in { "PENDING", "RUNNING", "STALLED"
    // with status in {PENDING, RUNNING, STALLED, FINISHED,
    // PAUSED, CANCELED, FAILED, KILLED}
    unimplemented!()
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
