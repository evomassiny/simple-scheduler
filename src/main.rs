#![feature(proc_macro_hygiene, decl_macro)]
#[macro_use] extern crate rocket;
#[macro_use] extern crate rocket_contrib;
extern crate chrono;
extern crate tempfile;
extern crate nix;
extern crate sqlx;

pub mod models;
pub mod executor;
pub mod pipe;

use sqlx::sqlite::SqlitePoolOptions;
use rocket::State;

#[get("/")]
async fn index() -> &'static str {
    "Hello, world!"
}

#[get("/spawn")]
async fn spawn(pool: State<'_, SqlitePoolOptions>) -> String {
    if let Ok(process) = models::Process::spawn_daemon("sleep 60 && echo yes").await {
        return format!("Spawned PID: {}", process.pid);
    }
    format!("Failed")
}


#[rocket::main]
async fn main() {
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
