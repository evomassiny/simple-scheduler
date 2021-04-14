//#![feature(proc_macro_hygiene, decl_macro)]
#[macro_use] extern crate rocket;
#[macro_use] extern crate rocket_contrib;
extern crate chrono;
extern crate nix;
extern crate sqlx;
extern crate dotenv;
extern crate serde;
extern crate serde_json;

pub mod tasks;

use sqlx::{
    sqlite::{SqlitePoolOptions, SqlitePool},
};
use rocket::State;
use dotenv::dotenv;

use tasks::{TaskProcess, Sendable, TaskStatus};

#[get("/")]
async fn index() -> &'static str {
    "Hello, world!"
}

#[get("/spawn")]
async fn spawn(pool: State<'_, SqlitePool>) -> String {
    match TaskProcess::spawn("sleep 10 && echo $(date)", 1).await {
        Ok(process) => {
            process.handle.start().await;

            let status = process.handle.get_status().await.unwrap();
            format!("Spawned PID: {}, {:?}", process.pid, status)
            //format!("Spawned PID: {}", process.pid)
        },
        Err(error) => format!("Failed: {:?}", error),
    }
}


#[rocket::main]
async fn main() {
    dotenv().expect("Failed reading .env");
    let url = std::env::var("DATABASE_URL")
        .expect("No DATABASE_URL environment variable set");
    // Build database connection pool
    let pool = SqlitePoolOptions::new()
        .max_connections(16)
        .connect(&url)
        .await
        .expect("Could not connect to database.");
    let result = rocket::build()
        .manage(pool)
        .mount("/", routes![index, spawn])
        .launch().await;
    assert!(result.is_ok());
}
