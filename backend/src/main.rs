#[macro_use] extern crate rocket;
#[macro_use] extern crate clap;
extern crate jaded;
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
extern crate scrypt;

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
use sqlx::sqlite::{SqlitePoolOptions, SqlitePool};
use clap::{Parser, Subcommand};

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
    /// Adds files to myapp
    RunServer,
    CreateUser { name: String, password: String  },
}


#[rocket::main]
async fn main() {
    dotenv().expect("Failed reading .env");
    let url = std::env::var("DATABASE_URL").expect("No DATABASE_URL environment variable set");

    let cli = Cli::parse();

    match cli.command {
        Commands::CreateUser { name, password } => {
            create_user(name, password, url).await;
        },
        Commands::RunServer => {
            let pool = database_pool(url)
                .await
                .expect("Failed to get database pool");
            start_server(pool).await;
        },
    }

}

async fn create_user(name: String, password: String, url: String) {
    use crate::models::{User,Model};
    use sqlx::SqliteConnection;
    use crate::sqlx::Connection;
    let mut conn = SqliteConnection::connect(&url)
        .await
        .expect("Could not connect with db.");

    let mut user = User::new(&name, &password).expect("failed to build user object");
    user.save(&mut conn).await;
}

async fn database_pool(url: String) -> Result<SqlitePool, String> {
    // Build database connection pool
    let pool = SqlitePoolOptions::new()
        .max_connections(16)
        .connect(&url)
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

#[get("/")]
async fn index() -> &'static str {
    "Hello, world! there is no front-end. sry"
}

