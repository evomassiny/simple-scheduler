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
async fn spawn(pool: State<'_, SqlitePool>) -> String {
    let cmd: String = "sleep 60 && echo $(date)".to_string();
    let mut conn = pool.acquire().await.unwrap();
    let query_result = sqlx::query("INSERT INTO tasks (name, command) VALUES (?, ?)")
        .bind("")
        .bind(&cmd)
        .execute(&mut conn)
        .await
        .expect("Could not send request");

    match TaskHandle::spawn(&cmd, query_result.last_insert_rowid()).await {
        Ok(task) => {
            let _ = task.start().await.expect("Could not start task");
            let status = task.get_status().await.expect("Could not get status");
            let pid = task.get_pid().await.expect("could not get pid");
            format!("task '{:?}', spawned with PID: {}, {:?}", &task.directory, pid, status)
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
