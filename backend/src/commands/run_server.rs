use crate::auth::{login, KeyPair};
use crate::config::Config;
use crate::rest;
use crate::scheduling::SchedulerServer;
use rocket::{Build, Rocket};
use std::path::Path;

/// Start Scheduler hypervisor service
/// and Web server.
/// Both tied to `pool`.
pub async fn run_server(rocket: Rocket<Build>, config: &Config) -> Result<(), &'static str> {
    // launch process update listener loop
    let socket_path = Path::new(&config.hypervisor_socket_path).to_path_buf();

    let read_pool = config
        .database_read_pool()
        .await
        .or(Err("Failed to build read pool."))?;

    let write_pool = config
        .database_write_pool()
        .await
        .or(Err("Failed to build write pool."))?;

    let scheduler_server =
        SchedulerServer::new(socket_path, read_pool, write_pool, config.nb_of_workers);
    let scheduler_client = scheduler_server.client();
    scheduler_server.start();

    // Load RSA key pair (for auth)
    let key_pair = KeyPair::load_from(&config.public_key_path, &config.private_key_path)
        .await
        .or(Err("Could not read key pair"))?;

    let _ = rocket
        .manage(scheduler_client)
        .manage(key_pair)
        .mount(
            "/rest/scheduler/",
            routes![rest::job_status, rest::submit_job, rest::kill_job, login],
        )
        .launch()
        .await
        .or(Err("Server failed."))?;
    Ok(())
}
