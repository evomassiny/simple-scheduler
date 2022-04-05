#[macro_use]
extern crate rocket;
extern crate aes;
extern crate base64;
extern crate chrono;
extern crate clap;
extern crate dotenv;
extern crate jaded;
extern crate nix;
extern crate rsa;
extern crate scrypt;
extern crate serde;
extern crate serde_json;
extern crate sqlx;
extern crate tempfile;
extern crate zip;

mod auth;
mod messaging;
mod models;
mod rest;
mod scheduling;
mod tasks;
mod workflows;

use crate::models::create_or_update_user;
use std::path::Path;

use clap::{Parser, Subcommand};
use dotenv::dotenv;
use rocket::tokio;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};

use scheduling::SchedulerServer;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the Web Server
    RunServer,
    /// Create a new user
    CreateUser { name: String, password: String },
}

#[rocket::main]
async fn main() {
    dotenv().expect("Failed reading .env");
    let db_url = std::env::var("DATABASE_URL").expect("No DATABASE_URL environment variable set");

    let cli = Cli::parse();

    match cli.command {
        Commands::CreateUser { name, password } => {
            use crate::sqlx::Connection;
            use sqlx::SqliteConnection;
            let mut conn = SqliteConnection::connect(&db_url)
                .await
                .expect("Could not connect with db.");
            create_or_update_user(&name, &password, &mut conn)
                .await
                .expect("Failed to set user.");
        }
        Commands::RunServer => {
            let pool = build_database_pool(db_url)
                .await
                .expect("Failed to get database pool");
            start_server(pool).await;
        }
    }
}

async fn build_database_pool(db_url: String) -> Result<SqlitePool, String> {
    // Build database connection pool
    let pool = SqlitePoolOptions::new()
        .max_connections(16)
        .connect(&db_url)
        .await
        .map_err(|e| format!("failed to get database pool: {:?}", e))?;
    Ok(pool)
}

/// Start Scheduler hypervisor service
/// and Web server.
/// Both tied to `pool`.
async fn start_server(pool: SqlitePool) {
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
    )
    .await
    .expect("Could not read key pair");

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
                auth::login
            ],
        )
        .mount("/", routes![index])
        .launch()
        .await;
    println!("{:?}", result);
}

#[get("/")]
async fn index() -> &'static str {
    "Hello, world! there is no front-end. sry"
}
