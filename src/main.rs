#[macro_use]
extern crate rocket;
extern crate chrono;
extern crate dotenv;
extern crate nix;
extern crate serde;
extern crate serde_json;
extern crate sqlx;

pub mod tasks;
pub mod workflows;
mod scheduling;
//mod listener;
mod models;

use std::path::{Path,PathBuf};

use dotenv::dotenv;
use rocket::{State,tokio};
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};

use tasks::TaskHandle;
use scheduling::{SchedulerServer,SchedulerClient};

#[get("/")]
async fn index() -> &'static str {
    "Hello, world!"
}

#[get("/spawn")]
async fn spawn(pool: &State<SqlitePool>, hypervisor_sock: &State<PathBuf>) -> String {
    // fetch hypervisor socket path
    let sock = Some(hypervisor_sock.inner().clone());
    // fetch database connection
    let mut conn = pool.acquire().await.unwrap();

    // build task DB object and get its id
    let cmd: String = "sleep 10 && echo $(date)".to_string();
    let query_result = sqlx::query("INSERT INTO tasks (command) VALUES (?)")
        .bind(&cmd)
        .execute(&mut conn)
        .await
        .expect("Could not send request");
    let task_id = query_result.last_insert_rowid();

    // spawn the task
    match TaskHandle::spawn(&cmd, task_id, sock).await {
        Ok(task) => {
            // set the task handle
            let _ = sqlx::query("INSERT INTO tasks (handle) VALUES (?) WHERE id = ?")
                .bind(&task.directory.clone().into_os_string().to_string_lossy().to_string())
                .bind(&task_id)
                .execute(&mut conn)
                .await;
            let _ = task.start().await.expect("Could not start task");
            let status = task.get_status().await.expect("Could not get status");
            let pid = task.get_pid().await.expect("could not get pid");
            format!("task '{:?}', spawned with PID: {}, {:?}", &task.directory, pid, status)
        }
        Err(error) => format!("Failed: {:?}", error),
    }
}

//#[post("/submit")]
//async fn submit(pool: &State<SqlitePool>, hypervisor_sock: &State<PathBuf>) -> String {

//}

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
        .mount("/", routes![index, spawn])
        //.mount("/rest", routes![submit])
        .launch()
        .await;
    assert!(result.is_ok());
}
