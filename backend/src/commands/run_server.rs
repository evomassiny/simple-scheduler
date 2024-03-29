use crate::auth::{login, KeyPair};
use crate::config::Config;
use crate::rest;
use crate::scheduling::start_scheduler;
use rocket::{Build, Rocket};
use std::path::Path;

/// Start Scheduler hypervisor service
/// and Web server.
/// Both tied to `pool`.
pub async fn run_server(rocket: Rocket<Build>, config: &Config) -> Result<(), &'static str> {
    // launch process update listener loop
    let socket_path = Path::new(&config.hypervisor_socket_path).to_path_buf();

    let read_pool = config
        .database_pool()
        .await
        .or(Err("Failed to build read pool."))?;

    let scheduler_client = start_scheduler(read_pool, socket_path, config.nb_of_workers, 32);

    // re-launch unfinished jobs
    if let Err(e) = scheduler_client.relaunch().await {
        eprintln!("Relaunch job failed: {:?}", e);
    }

    // Load RSA key pair (for auth)
    let key_pair = KeyPair::load_from(&config.public_key_path, &config.private_key_path)
        .await
        .or(Err("Could not read key pair"))?;

    let _ = rocket
        .manage(scheduler_client)
        .manage(key_pair)
        .mount(
            "/rest/scheduler/",
            routes![
                // auth
                login,
                // jobs
                rest::job_status,
                rest::list_job_tasks,
                rest::submit_job,
                rest::kill_job,
                // tasks
                rest::task_outputs,
            ],
        )
        .launch()
        .await
        .or(Err("Server failed."))?;
    Ok(())
}
