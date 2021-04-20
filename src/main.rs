//#![feature(proc_macro_hygiene, decl_macro)]
#[macro_use]
extern crate rocket;
extern crate rocket_contrib;
extern crate chrono;
extern crate dotenv;
extern crate nix;
extern crate serde;
extern crate serde_json;
extern crate sqlx;

pub mod tasks;

use dotenv::dotenv;
use rocket::State;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};

use tasks::TaskHandle;

#[get("/")]
async fn index() -> &'static str {
    "Hello, world!"
}

#[get("/spawn")]
async fn spawn(_pool: State<'_, SqlitePool>) -> String {
    match TaskHandle::spawn("sleep 10 && echo $(date)", 1).await {
        Ok(task) => {
            let _ = task.start().await;
            let status = task.get_status().await.unwrap();
            let pid = task.get_pid().await.unwrap();
            format!("Spawned PID: {}, {:?}", pid, status)
        }
        Err(error) => format!("Failed: {:?}", error),
    }
}

#[rocket::main]
async fn main() {
    dotenv().expect("Failed reading .env");
    let url = std::env::var("DATABASE_URL").expect("No DATABASE_URL environment variable set");
    // Build database connection pool
    let pool = SqlitePoolOptions::new()
        .max_connections(16)
        .connect(&url)
        .await
        .expect("Could not connect to database.");
    let result = rocket::build()
        .manage(pool)
        .mount("/", routes![index, spawn])
        .launch()
        .await;
    assert!(result.is_ok());
}
