//! 
//! This crate defines the hypervisor/scheduling part of the app,
//! it is implemented using the actor model,
//! each actor being a loop inside of a tokio task.
//!
//! The scheduling "server" is made of 5 actors:
//!              ┌────────────────┐   ┌───────────────┐        ┌───────────┐
//!              │                │   │               │        │           │
//!              │                │   │               ├───────►│  Status   │
//!              │   Executor     │◄──┤     Queue     │        │  Cache    ◄─┐
//!              │                │   │               │◄───────┤           │ │
//!              │                │   │               │        │           │ │
//!              └─────────┬──────┘   └─▲────────▲────┘        └────────┬──┘ │
//!                        │            │        │                      │    │
//!                        │            │        │                      │    │
//!                        │            │        │                      │    │
//!                        │            │        │              ┌───────▼──┐ │
//!                        │            │        │              │          │ │
//! ┌-----------┐    ─-----▼─----┐      │        │              │  DB      │ │
//! │ Running   │   │  Job       │      │        │              │  Writer  │ │
//! │ Job       │   │  Monitor   │      │        │              │          │ │
//! │           ├───►            │      │        │              └───▲┌─────┘ │
//! └-----------┘   └─-----------┘      │        │                  ││       │
//!                        │            │        │                  ││       │
//!                        │            │        │                  ││       │
//!                        │            │        │       ┌──────────┘▼───────┴─┐
//!                        │            │        │       │                     │
//!                        │            │        │       │                     │
//!                        │            │        │       │                     │
//!                        │            │        │       │  SCHEDULER CLIENT   │
//!               ┌────────▼───────┐    │        └───────┤                     │
//!               │                │    │                │                     │
//!               │    Status      │    │                │                     │
//!               │    Aggregator  ├────┘                │                     │
//!               │                │                     └─────────────────────┘
//!               └────────────────┘
//! 
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

//!
//! This function spawn 5 actors, which collaborate to
//! handle the scheduling of tasks/jobs and the storage of their status.
//! Together, they are the "hypervisor" of the app.
//!
//! The work is split into 5 concurrent actors, communicating
//! with each other using tokio channels.
//! 
//! This function returns a `SchedulerClient`, an handle
//! to the scheduling actors.
//! 
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
