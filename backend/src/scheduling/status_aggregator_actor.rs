use crate::messaging::MonitorMsg;
use rocket::tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::error::Error;
use std::path::PathBuf;

use rocket::tokio::{
    self,
    net::{UnixListener, UnixStream},
    time::Duration,
};

type Version = usize;
type AccessDate = usize;

/// A status notication,
/// that we send to the CacheActor
#[derive(Debug)]
pub struct StatusUpdate {
    task_id: TaskId,
    status: Status,
}

struct StatusDate {
    task_id: TaskId,
    /// status version, maintained by the
    /// monitor process (using it allows us to handle unordered messages).
    version: Version,
    /// update age, maintained by the version cache,
    date: AccessDate,
    /// last recorded task status
    status: TaskStatus,
}

/// Status Cache of Tasks
struct TaskStatusCache {
    pub version_age_by_task: HashMap<TaskId, StatusDate>,
    /// represent the "date" of a status
    time_stamp: AccessDate,
    /// cache size,
    /// oldest entries are dropped,
    /// if the capicity is reached.
    pub capacity: usize,
}

impl TaskStatusCache {
    /// Cache a msg status, return either or not
    /// the task status changed since the last message.
    pub fn status_changed(&mut self, task: TaskId, version: Version, status: TaskStatus) -> bool {
        let now: AccessDate = self.current_date();
        let mut changed: bool = false;
        match self.version_age_by_task.entry(task) {
            Entry::Vacant(entry) => {
                let status_date = StatusDate {
                    task_id: task,
                    version,
                    date: now,
                    status,
                };
                entry.insert(status_date);
            }
            Entry::Occupied(mut entry) => {
                let mut status_date = entry.get_mut();
                if version > status_date.version {
                    changed = status != status_date.status;
                    status_date.status = status;
                    status_date.version = version;
                }
            }
        }
        self.garbage_collect();
        changed
    }

    /// remove old entries and entries that won't change (eg: status of terminated tasks)
    fn garbage_collect(&mut self) {
        if self.version_age_by_task.len() >= self.capacity {
            // first pass remove task that can't change their status
            self.drop_state_in_final_state();
            // second pass remove oldest entries
            while self.version_age_by_task.len() >= self.capacity {
                self.drop_oldest_entry();
            }
        }
    }

    fn drop_state_in_final_state(&mut self) {
        let mut tasks: Vec<TaskId> = self
            .version_age_by_task
            .values()
            .filter_map(|status_date| {
                if status_date.status.is_terminated() {
                    Some(status_date.task_id)
                } else {
                    None
                }
            })
            .collect();
        for task in tasks {
            self.version_age_by_task.remove(task);
        }
    }

    fn drop_oldest_entry(&mut self) {
        let mut dates: Vec<(AccessDate, TaskId)> = self
            .version_age_by_task
            .values()
            .map(|status_date| (status_date.date, status_date.id))
            .collect();
        dates.sort_by_key(|&(date, _id)| date);

        if let Some((_date, task_id)) = dates.first() {
            self.version_age_by_task.remove(task_id);
        }
    }

    /// increment the time_stamp
    /// handle overflows by reseting all status' dates
    /// (while keeping the date order)
    fn current_date(&mut self) -> AccessDate {
        match self.access_counter {
            AccessDate::MAX => {
                let mut dates: Vec<(AccessDate, TaskId)> = self
                    .version_age_by_task
                    .values()
                    .map(|status_date| (status_date.date, status_date.id))
                    .collect();
                dates.sort_by_key(|&(date, _id)| date);
                for (index, (_date, task_id)) in dates.iter().enumerate() {
                    self.version_age_by_task
                        .get_mut(task_id)
                        .unwrap() // safe because key and values.id are garantied to be consisitent
                        .date = index;
                }
                self.time_stamp = dates.len();
            }
            _ => {
                self.time_stamp += 1;
            }
        }
        self.time_stamp
    }
}

pub fn process_monitor_message(
    stream: &mut UnixStream,
    version_cache: &mut TaskStatusCache,
    job_cache_handle: UnboundedSender<StatusUpdate>
) -> Result<(), Box<dyn Error>> {
    let msg = MonitorMsg::async_read_from(stream).await?;
    match msg {
        // update task status from the task monitor
        MonitorMsg::StatusUpdate {
            _task_handle,
            task_id,
            status,
            update_version,
        } => {
            // close connection with monitor process
            use crate::messaging::ExecutorQuery;
            let _ = ExecutorQuery::Ok.async_send_to(&mut *stream).await;
            let _ = stream.shutdown().await;

            // send new version to job status cache
            if version_cache.status_changed(task_id, update_version, status) {
                job_cache_handle.send(StatusUpdate { task_id, status })?;
            }
        }
        MonitorMsg::Ok => { /* nothing to do */ }
    }
    Ok(())
}

/// Spawn an actor that:
/// * listens on any incoming connection from monitor processes,
/// * manages an up-to-date cache of Task statuses,
/// * sends status updates to the main cache actor anytime a task status changes.
pub fn spawn_task_status_aggregator_actor(
    hypervisor_socket: PathBuf,
    job_cache_handle: UnboundedSender<StatusUpdate>) {
    // remove the socket file
    let _ = std::fs::remove_file(&hypervisor_socket);
    let listener = UnixListener::bind(&hypervisor_socket).expect("Cant bind to hypervisor socket.");

    tokio::task::spawn(async move {
        let mut version_cache = TaskStatusCache {
            version_age_by_task: HashMap::new(),
            time_stamp: 0,
            capacity: 2048, // arbitrary
        };

        loop {
            // also listen for messages, either from the web app or from a monitor process.
            match listener.accept() {
                Ok((mut stream, _addr)) => {
                    if let Err(e) =
                        process_monitor_message(&mut stream, version_cache, job_cache_handle)
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
    });
}
