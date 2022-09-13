//! This crate defines the hypervisor/scheduling part of the app,
//! it is implemented using the actor model,
//! each actor being a loop inside of a tokio task.
//!
//! The scheduling "server" is made of 5 actors:
//! * the `Executor` actor, which kills or spawn Jobs upon request from the `Queue` actor
//! * the `Queue` actor, it keeps tracks of which task must be spawned, and in which order.
//!   It listens for:
//!    * (running) task status updates from the "Status Cache" actor
//!    * scheduling orders (scheduling a new job, or kill another) from the scheduler client,
//!   Using those channels, it keeps un up-to-date queue of pending tasks, and ask the executor
//!   to spawn a new one everytime a computing slot becomes idle.
//! * the `status aggregator` actor: this one listen for status notification from the monitor
//!   processes (the processes that monitor the runners), 
//!   Everytime a status changes, it forward the message to the status cache,
//! * the `status cache actor`: this actor keeps a cached version of jobs and tasks statuses,
//!   the status are fed by:
//!   * the status aggregator actor, for tasks that were once started,
//!   * the queue actor, for tasks that were canceled.
//!   When a status changes, this actor notify the Database writer, so it can save the current
//!   state of the concerned task/job.
//! * the DataBase writer: it is the only part of the app that should write the database,
//!   this asserts that no concurrent writes happen.
//!   It can be queried by the status cache (when a task status changes) or by the client,
//!   when a new job is submitted to the scheduler. This is the actor that assign IDs to
//!   Tasks/Jobs (through sqlite).

use sqlx::sqlite::SqlitePool;
use std::path::PathBuf;

mod client;

mod cache_actor;
mod executor_actor;
mod queue_actor;
mod status_aggregator_actor;
mod db_writer_actor;

pub use crate::scheduling::client::SchedulerClient;

use crate::scheduling::executor_actor::spawn_executor_actor;

use crate::scheduling::status_aggregator_actor::{
    spawn_task_status_aggregator_actor, StatusUpdate,
};

use crate::scheduling::queue_actor::{
    spawn_queue_actor,
    QueuedTaskHandle,
    QueuedTaskStateClient,
    QueueSubmissionHandle,
    QueueSubmissionClient,
    QueueSubmission, 
    TaskEvent,
};

use crate::scheduling::cache_actor::{
    spawn_cache_actor, ReadRequest, WriteRequest,
    CacheWriteHandle, CacheWriter, CacheReader, CacheReadHandle,
};

use crate::scheduling::db_writer_actor::{
    spawn_db_writer_actor, DbWriterHandle,
};

use rocket::tokio::sync::mpsc::unbounded_channel;

///
/// This function spawn 5 actors, which collaborate to
/// handle the scheduling of tasks/jobs and the storage of their status.
/// Together, they are the "hypervisor" of the app.
///
/// The work is split into 5 concurrent actors, communicating
/// with each other using tokio channels.
/// 
/// This function returns a `SchedulerClient`, an handle
/// to the scheduling actors.
/// 
pub fn start_scheduler(
    pool: SqlitePool,
    hypervisor_socket: PathBuf,
    worker_pool_size: usize,
    cache_size: usize,
) -> SchedulerClient {
    // database writer actor
    let db_writer_handle: DbWriterHandle = spawn_db_writer_actor(pool.clone());

    // status aggregator actor
    let (status_tx, status_rx) = unbounded_channel::<StatusUpdate>();
    spawn_task_status_aggregator_actor(hypervisor_socket.clone(), status_tx);

    // executor actor
    let executor_handle = spawn_executor_actor(pool.clone(), hypervisor_socket);

    let (status_sender, status_receiver) = unbounded_channel::<TaskEvent>();
    let queued_tasks_handle = QueuedTaskStateClient(status_sender);

    let (submission_sender, submission_receiver) = unbounded_channel::<QueueSubmission>();
    let submission_handle = QueueSubmissionClient(submission_sender);

    let (to_cache, from_cache_handle) = unbounded_channel::<WriteRequest>();
    let cache_writer_handle = CacheWriter(to_cache);

    let (read_tx, read_rx) = unbounded_channel::<ReadRequest>();
    let cache_reader_handle = CacheReader(read_tx);

    // queue actor
    spawn_queue_actor(
        executor_handle,
        cache_writer_handle.clone(),
        status_receiver,
        submission_receiver,
        worker_pool_size,
    );
    
    // cache actor
    spawn_cache_actor(
        queued_tasks_handle.clone(),
        db_writer_handle.clone(),
        read_rx,
        from_cache_handle,
        status_rx,
        cache_size,
    );

    SchedulerClient {
        read_pool: pool,
        db_writer_handle: db_writer_handle,
        status_cache_writer: cache_writer_handle,
        status_cache_reader: cache_reader_handle,
        submission_handle,
    }
}
