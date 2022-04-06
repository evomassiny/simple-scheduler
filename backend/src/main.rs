#[macro_use]
extern crate rocket;
extern crate aes;
extern crate base64;
extern crate chrono;
extern crate clap;
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
mod config;
mod messaging;
mod models;
mod rest;
mod scheduling;
mod tasks;
mod workflows;

use crate::config::Config;
use crate::models::create_or_update_user;
use crate::scheduling::SchedulerServer;

use clap::{Parser, Subcommand};
use rocket::{tokio, Build, Rocket};

use std::path::Path;

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
    let rocket = rocket::build();
    let config: Config = rocket.figment().extract().expect("Failed to read config.");

    let cli = Cli::parse();

    match cli.command {
        // create a new user, store its credentials into the database
        Commands::CreateUser { name, password } => {
            let mut conn = &mut config
                .database_connection()
                .await
                .expect("failed to connect to db");
            create_or_update_user(&name, &password, &mut conn)
                .await
                .expect("Failed to set user.");
        }
        // starts the web server
        Commands::RunServer => {
            start_server(rocket, &config).await;
        }
    }
}

/// Start Scheduler hypervisor service
/// and Web server.
/// Both tied to `pool`.
async fn start_server(rocket: Rocket<Build>, config: &Config) {
    // launch process update listener loop
    let socket_path = Path::new(&config.hypervisor_socket_path).to_path_buf();
    let pool = config.database_pool().await.expect("Failed to build pool.");
    let scheduler_server = SchedulerServer::new(socket_path, pool);
    let scheduler_client = scheduler_server.client();
    scheduler_server.start();

    // Load RSA key pair (for auth)
    let key_pair = auth::KeyPair::load_from(&config.public_key_path, &config.private_key_path)
        .await
        .expect("Could not read key pair");

    let result = rocket
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
