#[macro_use]
extern crate rocket;
extern crate chrono;
extern crate dotenv;
extern crate nix;
extern crate serde;
extern crate serde_json;
extern crate sqlx;
extern crate tempfile;

mod messaging;
mod models;
mod scheduling;
pub mod tasks;
pub mod workflows;

use std::path::{Path, PathBuf};

use dotenv::dotenv;
use rocket::{fs::TempFile, tokio, State};
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};

use scheduling::{SchedulerClient, SchedulerServer};
use tasks::TaskHandle;

#[get("/")]
async fn index() -> &'static str {
    "Hello, world!"
}

#[get("/spawn")]
async fn spawn(scheduler: &State<SchedulerClient>) -> String {
    // build task DB object and get its id
    let cmd: String = "sleep 30 && echo $(date)".to_string();
    let scheduler = scheduler.inner();
    scheduler
        .submit_command_job("command_job", "task", &cmd)
        .await
        .expect("failed");
    String::from("task successfully launched")
}

#[post("/submit", format = "application/xml", data = "<uploaded_file>")]
async fn submit(scheduler: &State<SchedulerClient>, mut uploaded_file: TempFile<'_>) -> String {
    let scheduler = scheduler.inner();
    match scheduler.submit_from_tempfile(&mut uploaded_file).await {
        Ok(job_id) => format!("Job {:?} successfully created", &job_id),
        Err(error) => format!("Failed to create job: {:?}", error),
    }
}

#[rocket::main]
async fn main() {
    dotenv().expect("Failed reading .env");
    // Build database connection pool
    let url = std::env::var("DATABASE_URL").expect("No DATABASE_URL environment variable set");
    let pool = SqlitePoolOptions::new()
        .max_connections(16)
        .connect(&url)
        .await
        .expect("Could not connect to database.");

    // launch process update listener loop
    let hypervisor_socket = std::env::var("HYPERVISOR_SOCKET_PATH")
        .expect("No HYPERVISOR_SOCKET_PATH environment variable set");
    let socket_path = Path::new(&hypervisor_socket).to_path_buf();
    let pool_clone = pool.clone();
    let scheduler_server = SchedulerServer::new(socket_path, pool_clone);
    let scheduler_client = scheduler_server.client();
    scheduler_server.start();

    // launch HTTP server
    let socket_path = Path::new(&hypervisor_socket).to_path_buf();
    let result = rocket::build()
        .manage(pool)
        .manage(socket_path)
        .manage(scheduler_client)
        .mount("/rest/scheduler/", routes![spawn, submit])
        .mount("/", routes![index])
        .launch()
        .await;
    assert!(result.is_ok());
}
