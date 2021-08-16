#[macro_use]
extern crate rocket;
extern crate chrono;
extern crate dotenv;
extern crate nix;
extern crate serde;
extern crate serde_json;
extern crate sqlx;

mod messaging;
pub mod tasks;
pub mod workflows;
mod scheduling;
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
async fn spawn(scheduler: &State<SchedulerClient>) -> String {
    // build task DB object and get its id
    let cmd: String = "sleep 30 && echo $(date)".to_string();
    let scheduler = scheduler.inner();
    scheduler.submit_command_job("command_job", "task", &cmd).await.expect("failed");
    String::from("task successfully launched")
}

//#[post("/submit")]
//async fn submit(scheduler: &State<SchedulerClient> ) -> String { }

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
        .mount("/", routes![index, spawn])
        //.mount("/rest", routes![submit])
        .launch()
        .await;
    assert!(result.is_ok());
}
