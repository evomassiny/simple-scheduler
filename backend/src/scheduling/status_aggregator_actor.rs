//!
//! This actor collects all status updates coming from
//! tasks monitors, such as PENDING, RUNNING, FAILED, ...
//!
//! Its sole purpose is to cache those status in memory,
//! and forward the status message to the rest of the app
//! only when the status of a task __changes__.
//!
//! This way we reduce the among of messages that needs to be processed
//! by the rest of the app by a big factor.
//!
//! The actor is also responsible for flaging tasks which are not sending
//! status update as lost (eg FAILED). This occurs in the unlikely
//! situation where a monitor process crashes or is OOM-killed.
//!
use crate::config::GRACE_PERIOD_IN_SECONDS;
use crate::messaging::AsyncSendable;
use crate::messaging::ExecutorQuery;
use crate::messaging::MonitorMsg;
use crate::messaging::TaskStatus;
use crate::models::{Status, TaskId};
use crate::rocket::tokio::io::AsyncWriteExt;
use rocket::tokio::sync::mpsc::UnboundedSender;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::error::Error;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use rocket::tokio::{
    self,
    net::{UnixListener, UnixStream},
    time::interval,
};

type Version = usize;
type AccessDate = SystemTime;

/// A status notication,
/// that we send to the CacheActor
#[derive(Debug)]
pub struct StatusUpdate {
    pub task_id: TaskId,
    pub status: Status,
}

pub struct StatusEntry {
    task_id: TaskId,
    /// status version, maintained by the
    /// monitor process (using it allows us to handle unordered messages).
    version: Version,
    /// update age, maintained by the version cache,
    date: SystemTime,
    /// last recorded task status
    status: TaskStatus,
}

/// Status Cache of Tasks
/// (LRU cache)
pub struct TaskStatusCache {
    version_age_by_task: HashMap<TaskId, StatusEntry>,
    /// task from which we don't get an update since
    /// this period are considered lost
    grace_period: Duration,
    /// oldest entries are dropped,
    /// if the capacity is reached.
    capacity: usize,
}

impl TaskStatusCache {
    pub fn new() -> Self {
        Self {
            version_age_by_task: HashMap::new(),
            grace_period: Duration::from_secs(GRACE_PERIOD_IN_SECONDS),
            capacity: 2048, // arbitrary
        }
    }

    /// Cache a msg status, return either or not
    /// the task status changed since the last message.
    pub fn status_changed(&mut self, task: TaskId, version: Version, status: TaskStatus) -> bool {
        let now: SystemTime = SystemTime::now();
        let mut changed: bool = false;
        match self.version_age_by_task.entry(task) {
            Entry::Vacant(entry) => {
                let status_date = StatusEntry {
                    task_id: task,
                    version,
                    date: now,
                    status,
                };
                entry.insert(status_date);
                changed = true;
            }
            Entry::Occupied(mut entry) => {
                let status_date: &mut StatusEntry = entry.get_mut();
                if version > status_date.version {
                    changed = status != status_date.status;
                    status_date.status = status;
                    status_date.version = version;
                }
            }
        }
        changed
    }

    /// Collect old entries and entries that won't change
    fn list_done_tasks(&mut self) -> Vec<TaskId> {
        self.version_age_by_task
            .values()
            .filter(|entry| entry.status.is_terminated())
            .map(|entry| entry.task_id)
            .collect::<Vec<TaskId>>()
    }

    // collect the IDs of all tasks where the last
    // update message is older that the "grace period"
    fn list_lost_tasks(&self) -> Vec<TaskId> {
        let mut losts: Vec<TaskId> = Vec::new();
        let now = SystemTime::now();
        for entry in self.version_age_by_task.values() {
            // terminated task do not send updates
            if entry.status.is_terminated() {
                continue;
            }
            let lost = match now.duration_since(entry.date) {
                Ok(duration) => duration > self.grace_period,
                Err(e) => {
                    // unlikely case when the system clock
                    // acts weirdly. If this happens, mark the
                    // task as lost.
                    true
                }
            };
            if lost {
                losts.push(entry.task_id);
            }
        }
        losts
    }

    fn drop_entry(&mut self, task: TaskId) {
        let _ = self.version_age_by_task.remove(&task);
    }

    /// Shrink internal hashmap to its capacity
    fn shrink_to_capacity(&mut self) {
        self.version_age_by_task.shrink_to(self.capacity);
    }
}

/// reads a process monitor message:
/// * if its a status update, update the status cache,
///   warns the `job_cache` actor, if the task status changed.
/// * if its a termination notice, drop the cached status associated with the task
///   and warn the `job_cache` actor.
pub async fn process_monitor_message(
    stream: &mut UnixStream,
    version_cache: &mut TaskStatusCache,
    job_cache_handle: UnboundedSender<StatusUpdate>,
) -> Result<(), Box<dyn Error>> {
    let msg = MonitorMsg::async_read_from(stream).await?;
    match msg {
        // update task status from the task monitor
        MonitorMsg::StatusBroadcast {
            task_id,
            status,
            update_version,
        } => {
            // close connection with monitor process
            let _ = ExecutorQuery::Ok.async_send_to(&mut *stream).await;
            let _ = stream.shutdown().await;
            println!("{task_id}, {status:?}");

            // send new version to job status cache
            if version_cache.status_changed(task_id, update_version, status) {
                let status: Status = match status {
                    TaskStatus::Pending => Status::Pending,
                    TaskStatus::Stopped => Status::Stopped,
                    TaskStatus::Killed => Status::Killed,
                    TaskStatus::Failed => Status::Failed,
                    TaskStatus::Succeed => Status::Succeed,
                    TaskStatus::Running => Status::Running,
                };
                job_cache_handle.send(StatusUpdate { task_id, status })?;
            }
        }
        MonitorMsg::SuicideNote { task_id } => {
            // close connection with monitor process
            let _ = ExecutorQuery::Ok.async_send_to(&mut *stream).await;
            let _ = stream.shutdown().await;
            println!("status aggregator: stopping the monitoring of {task_id}");
            version_cache.drop_entry(task_id);
        }
        MonitorMsg::Ok => { /* nothing to do */ }
    }
    Ok(())
}

/// Identify lost tasks and mark them as 'Failed' (warns the job_cache about it)
/// Also drop entries that won't change their status.
pub async fn collect_garbage(
    version_cache: &mut TaskStatusCache,
    job_cache_handle: UnboundedSender<StatusUpdate>,
) -> Result<(), Box<dyn Error>> {
    // drop lost tasks and warn job_cache
    let lost_tasks = version_cache.list_lost_tasks();
    for task_id in lost_tasks {
        job_cache_handle.send(StatusUpdate {
            task_id,
            status: Status::Failed,
        })?;
        version_cache.drop_entry(task_id);
    }
    // drop terminated tasks
    let done_tasks = version_cache.list_done_tasks();
    for task_id in done_tasks {
        job_cache_handle.send(StatusUpdate {
            task_id,
            status: Status::Failed,
        })?;
        version_cache.drop_entry(task_id);
    }
    // avoid infinite growth of the cache.
    version_cache.shrink_to_capacity();
    Ok(())
}

/// Spawn an actor that:
/// * listens on any incoming connection from monitor processes,
/// * manages an up-to-date cache of Task statuses,
/// * sends status updates to the main cache actor anytime a task status changes.
pub fn spawn_task_status_aggregator_actor(
    hypervisor_socket: PathBuf,
    job_cache_handle: UnboundedSender<StatusUpdate>,
) {
    // remove the socket file
    let _ = std::fs::remove_file(&hypervisor_socket);
    let listener = UnixListener::bind(&hypervisor_socket).expect("Cant bind to hypervisor socket.");

    tokio::task::spawn(async move {
        let mut version_cache = TaskStatusCache::new();
        let mut gc_interval = interval(Duration::from_secs(GRACE_PERIOD_IN_SECONDS));
        loop {
            tokio::select! {
                // listen for messages from monitor processes
                // (cancelation safe according to tokio's doc)
                acceptation = listener.accept()  => {
                    match acceptation {
                        Ok((mut stream, _addr)) => {
                            if let Err(e) = process_monitor_message(
                                &mut stream,
                                &mut version_cache,
                                job_cache_handle.clone(),
                            )
                            .await
                            {
                                eprintln!("Error while processing update msg: {:?}", e);
                            }
                        }
                        Err(e) => {
                            eprintln!(
                                "Connection to hypervisor socket '{:?}' failed: {:?}",
                                &hypervisor_socket, e,
                            );
                        }
                    }
                }
                // Run garbage collection every `GRACE_PERIOD_IN_SECONDS` seconds,
                // In this we detect lost task, and drop terminates tasks
                // from the cache.
                _ = gc_interval.tick()  => {
                    let result = collect_garbage(
                        &mut version_cache,
                        job_cache_handle.clone(),
                    ).await;
                    if let Err(e) = result {
                        eprintln!("Task garbage collection failed: {:?}", e);
                    }
                }
            }
        }
    });
}
