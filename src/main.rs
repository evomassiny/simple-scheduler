#![feature(proc_macro_hygiene, decl_macro)]
#[macro_use] extern crate rocket;
#[macro_use] extern crate rocket_contrib;
extern crate chrono;
extern crate tempfile;
extern crate nix;
extern crate sqlx;
extern crate dotenv;

pub mod models;
pub mod executor;
pub mod pipe;
pub mod process_utils;

use sqlx::sqlite::{SqlitePoolOptions,SqlitePool};
use rocket::State;
use dotenv::dotenv;


#[get("/")]
async fn index() -> &'static str {
    "Hello, world!"
}

#[get("/spawn")]
async fn spawn(pool: State<'_, SqlitePool>) -> String {
    if let Ok(process) = models::Process::spawn_daemon("sleep 60 && echo yes", 1).await {
        return format!("Spawned PID: {}", process.pid);
    }
    format!("Failed")
}


#[rocket::main]
async fn main() {
    dotenv().ok().expect("Failed reading .env");
    // Build database connection pool
    let pool = SqlitePoolOptions::new()
        .max_connections(16)
        .connect("sqlite:database.sqlite")
        .await
        .expect("Could not connect to database.");
    let result = rocket::ignite()
        .manage(pool)
        .mount("/", routes![index, spawn])
        .launch().await;
    assert!(result.is_ok());
}
