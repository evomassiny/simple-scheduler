#[macro_use] extern crate rocket;
#[macro_use] extern crate jaded;
extern crate chrono;
extern crate dotenv;
extern crate nix;
extern crate serde;
extern crate serde_json;
extern crate sqlx;
extern crate tempfile;
extern crate zip;
extern crate rsa;
extern crate aes;
extern crate base64;

mod auth;
mod messaging;
mod models;
mod rest;
mod scheduling;
mod tasks;
mod workflows;

use std::path::Path;

use dotenv::dotenv;
use rocket::tokio;
use sqlx::sqlite::SqlitePoolOptions;

use scheduling::SchedulerServer;

#[get("/")]
async fn index() -> &'static str {
    "Hello, world! there is no front-end. sry"
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
    let scheduler_server = SchedulerServer::new(socket_path, pool);
    let scheduler_client = scheduler_server.client();
    scheduler_server.start();

    // Load RSA key pair (for auth)
    let key_pair = auth::KeyPair::load_from(
        std::env::var("PUBLIC_KEY_PATH").expect("No 'PUBLIC_KEY_PATH' set."),
        std::env::var("PRIVATE_KEY_PATH").expect("No 'PRIVATE_KEY_PATH' set."),
    ).await.expect("Could not read key pair");

    // launch HTTP server
    let result = rocket::build()
        .manage(scheduler_client)
        .manage(key_pair)
        .mount(
            "/rest/scheduler/",
            routes![
                rest::job_status,
                rest::submit_job,
                rest::kill_job,
                rest::debug_spawn,
                auth::login
            ],
        )
        .mount("/", routes![index])
        .launch()
        .await;
    assert!(result.is_ok());
}
