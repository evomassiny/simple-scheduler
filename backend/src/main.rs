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
mod commands;
mod config;
mod messaging;
mod models;
mod rest;
mod scheduling;
mod tasks;
mod workflows;

use crate::config::Config;

use clap::{Parser, Subcommand};

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
    /// Create a Token use to authentifiate the user
    CreateCredentialToken { name: String, password: String },
}

#[rocket::main]
async fn main() {
    let rocket = rocket::build();
    let config: Config = rocket.figment().extract().expect("Failed to read config.");

    let cli = Cli::parse();

    match cli.command {
        // create a new user, store its credentials into the database
        Commands::CreateUser { name, password } => {
            commands::create_user(&name, &password, &config)
                .await
                .expect("Failed to set user.");
        }
        // create a credential token
        Commands::CreateCredentialToken { name, password } => {
            commands::create_credential_token(&name, &password, &config)
                .await
                .expect("Failed to build credential token.");
        }
        // starts the web server
        Commands::RunServer => {
            commands::run_server(rocket, &config)
                .await
                .expect("Server failed.");
        }
    }
}
