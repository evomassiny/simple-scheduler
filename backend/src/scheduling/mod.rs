use sqlx::sqlite::SqlitePool;
use std::path::PathBuf;

mod client;
mod server;

mod cache_actor;
mod executor_actor;
mod queue_actor;
mod status_aggregator_actor;

pub use crate::scheduling::client::SchedulerClient;
pub use crate::scheduling::server::SchedulerServer;

use crate::scheduling::executor_actor::spawn_executor_actor;

use crate::scheduling::status_aggregator_actor::{
    spawn_task_status_aggregator_actor, StatusUpdate,
};

use crate::scheduling::queue_actor::{spawn_queue_actor, QueueActorHandle, QueueOrder, TaskEvent};

use crate::scheduling::cache_actor::{
    spawn_cache_actor, CacheActorHandle, ReadRequest, WriteRequest,
};

use rocket::tokio::sync::mpsc::unbounded_channel;

pub fn spawn_actors(
    pool: SqlitePool,
    hypervisor_socket: PathBuf,
    worker_pool_size: usize,
    cache_size: usize,
) {
    let (status_tx, status_rx) = unbounded_channel::<StatusUpdate>();
    spawn_task_status_aggregator_actor(hypervisor_socket.clone(), status_tx);

    let executor_handle = spawn_executor_actor(pool, hypervisor_socket);

    let (task_sender, task_receiver) = unbounded_channel::<TaskEvent>();
    let queue_handle = QueueActorHandle {
        queue_actor: task_sender,
    };

    let (to_cache, from_cache_handle) = unbounded_channel::<WriteRequest>();
    let cache_writer_handle = CacheActorHandle { to_cache };

    let (_order_sender, order_receiver) = unbounded_channel::<QueueOrder>();

    let (_read_tx, read_rx) = unbounded_channel::<ReadRequest>();

    spawn_queue_actor(
        executor_handle,
        cache_writer_handle,
        task_receiver,
        order_receiver,
        worker_pool_size,
    );
    spawn_cache_actor(
        queue_handle,
        read_rx,
        from_cache_handle,
        status_rx,
        cache_size,
    );
}
