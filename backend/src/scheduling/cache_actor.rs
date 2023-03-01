use crate::models::{JobId, Status, TaskId};
use rocket::{
    async_trait,
    tokio::{
        self,
        sync::{
            mpsc::{UnboundedReceiver, UnboundedSender},
            oneshot::{channel, Sender},
        },
    },
};
use std::collections::hash_map::Entry;
use std::collections::HashMap;

use super::db_writer_actor::DbWriterHandle;
use super::queue_actor::QueuedTaskHandle;
use super::status_aggregator_actor::StatusUpdate;

type AccessId = usize;

pub const ACCESS_COUNT_ORIGIN: usize = 0;

#[derive(Debug)]
pub enum CacheError {
    UnknownTask(TaskId),
    UnknownJob(JobId),
    SendFailed,
}

/// status of a Job, and its tasks
#[derive(PartialEq, Debug, Clone)]
pub struct JobStatusDetail {
    pub id: JobId,
    pub status: Status,
    pub task_statuses: Vec<(TaskId, Status)>,
}

#[derive(PartialEq, Debug)]
pub enum CacheResponse {
    /// either a cache miss, of a non-existent JobId
    UnknownJob,
    JobStatus(JobStatusDetail),
}

#[derive(Debug)]
pub enum CacheReadRequest {
    /// ask for the status of a Job
    GetJob {
        id: JobId,
        from: Sender<CacheResponse>,
    },
}

#[derive(Debug)]
pub enum CacheWriteRequest {
    /// Cancel a task (maybe its parent failed)
    CancelTask(TaskId),
    /// Cancel a job (children cancellation must
    /// be handled with `CancelTask` requests
    CancelJob(JobId),
    /// Add Cache entry from DB (after cache miss)
    AddExistingJob(JobStatusDetail),
}

#[derive(Debug)]
struct Job {
    id: JobId,
    status: Status,
    tasks: Vec<TaskId>,
    last_access: AccessId,
}

#[derive(Debug)]
struct Task {
    /// Optional because we might get a task status
    /// before receiving its associated "Add{New,Existing}Job"
    /// request.
    job: Option<JobId>,
    status: Status,
}

/// State associated with the cache actor,
/// basically an LRU cache for job and tasks statuses.
pub struct CacheActor<T> {
    queue_handle: T,
    tasks: HashMap<TaskId, Task>,
    jobs: HashMap<JobId, Job>,
    /// handle to the Database writer handle
    db_writer_handle: DbWriterHandle,
    /// counter, we use it to
    /// quantify the "age" of each cache entry
    access_counter: AccessId,
    /// maximum nb of job entries
    capacity: usize,
}

impl<T: QueuedTaskHandle> CacheActor<T> {
    /// increment the access counter,
    /// handle overflows by reseting all jobs last_access
    /// (while keeping the acces order)
    fn increment_access_counter(&mut self) {
        match self.access_counter {
            AccessId::MAX => {
                let mut accesses: Vec<(AccessId, JobId)> = self
                    .jobs
                    .values()
                    .map(|job| (job.last_access, job.id))
                    .collect();
                accesses.sort_by_key(|&(access, _id)| access);
                for (index, (_access, job_id)) in accesses.iter().enumerate() {
                    self.jobs
                        .get_mut(job_id)
                        .unwrap() // safe because key and values.id are garantied to be consisitent
                        .last_access = index;
                }
                self.access_counter = accesses.len();
            }
            _ => {
                self.access_counter += 1;
            }
        }
    }

    /// Iter all stored jobs, and remove
    /// the least recently accessed job
    fn drop_least_recently_accessed_job(&mut self) -> Result<(), CacheError> {
        let mut accesses: Vec<(AccessId, JobId)> = self
            .jobs
            .values()
            .map(|job| (job.last_access, job.id))
            .collect();
        accesses.sort_by_key(|&(access, _id)| access);
        if let Some((_access, id)) = accesses.first() {
            let job = self.jobs.remove(id).ok_or(CacheError::UnknownJob(*id))?;
            for task_id in job.tasks {
                let _task = self
                    .tasks
                    .remove(&task_id)
                    .ok_or(CacheError::UnknownTask(*id))?;
            }
        }
        Ok(())
    }

    pub fn handle_status_notication(&mut self, update: StatusUpdate) -> Result<(), CacheError> {
        let maybe_job;
        match self.tasks.entry(update.task_id) {
            Entry::Vacant(entry) => {
                let task = Task {
                    status: update.status,
                    job: None,
                };
                entry.insert(task);

                maybe_job = None;
            }
            Entry::Occupied(mut entry) => {
                let mut task = entry.get_mut();

                maybe_job = task.job;
                task.status = update.status;
            }
        }
        // sync db state
        self.db_writer_handle
            .set_task_status(update.task_id, update.status);

        // warn Queue Actor
        match update.status {
            Status::Failed | Status::Killed => self.queue_handle.set_task_failed(update.task_id),
            Status::Running => self.queue_handle.set_task_started(update.task_id),
            Status::Succeed => self.queue_handle.set_task_succeed(update.task_id),
            Status::Pending => {}
            _ => eprintln!("Bad status transtition."),
        }
        if let Some(job_id) = maybe_job {
            let _ = self.update_job_status(job_id)?;
        }
        Ok(())
    }

    /// Lookup state of all tasks composing the job, and update its status accordingly.
    ///
    /// * If all task failed => failure
    /// * If ONE task canceled => Canceled
    /// * If ONE task Stopped => Stopped
    /// * If ONE task Killed => killed
    /// * If mix failure/succed => succeed
    fn update_job_status(&mut self, job_id: JobId) -> Result<Status, CacheError> {
        let mut job = self
            .jobs
            .get_mut(&job_id)
            .ok_or(CacheError::UnknownJob(job_id))?;
        let mut statuses = Vec::new();
        for task_id in &job.tasks {
            let status = if let Some(task) = self.tasks.get(task_id) {
                task.status
            } else {
                Status::Pending
            };
            statuses.push(status);
        }
        let mut has_running = false;
        let mut finished = true;
        for status in &statuses {
            if !status.is_finished() {
                finished = false;
                if let Status::Running = status {
                    has_running = true;
                }
            }
        }
        if has_running {
            job.status = Status::Running;
            return Ok(Status::Running);
        }
        if !finished {
            job.status = Status::Pending;
            return Ok(Status::Pending);
        }

        let mut job_status = Status::Failed;
        for status in &statuses {
            match status {
                Status::Stopped => {
                    job_status = Status::Stopped;
                    break;
                }
                Status::Canceled => {
                    job_status = Status::Failed;
                    break;
                }
                Status::Killed => {
                    job_status = Status::Killed;
                    break;
                }
                Status::Succeed => {
                    job_status = Status::Succeed;
                }
                _ => {}
            }
        }
        job.status = job_status;
        self.db_writer_handle.set_job_status(job_id, job_status);
        Ok(job_status)
    }

    pub fn handle_read_request(&mut self, request: CacheReadRequest) -> Result<(), CacheError> {
        self.increment_access_counter();
        match request {
            // Request for the status of a Job,
            // Return `CacheResponse::JobStatus {..}` if we now about it,
            // `CacheResponse::UnknownJob` otherwise.
            CacheReadRequest::GetJob { id, from } => {
                match self.jobs.get_mut(&id) {
                    Some(mut job) => {
                        job.last_access = self.access_counter;
                        let mut task_statuses: Vec<(TaskId, Status)> =
                            Vec::with_capacity(job.tasks.len());
                        // collect all task statuses
                        for task_id in &job.tasks {
                            task_statuses.push((
                                *task_id,
                                self.tasks
                                    .get(task_id)
                                    .ok_or(CacheError::UnknownTask(*task_id))?
                                    .status,
                            ));
                        }
                        // send status
                        let _ = from
                            .send(CacheResponse::JobStatus(JobStatusDetail {
                                id,
                                status: job.status,
                                task_statuses,
                            }))
                            .map_err(|_| CacheError::SendFailed)?;
                    }
                    None => {
                        let _ = from
                            .send(CacheResponse::UnknownJob)
                            .map_err(|_| CacheError::SendFailed)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Add a job and its tasks to the cache.
    fn add_job(
        &mut self,
        id: JobId,
        status: Status,
        tasks: Vec<(TaskId, Status)>,
    ) -> Result<(), CacheError> {
        let mut task_ids = Vec::with_capacity(tasks.len());
        for (task_id, status) in tasks {
            // don't override existing status
            let _ = self.tasks.entry(task_id).or_insert(Task {
                job: Some(id),
                status,
            });
            task_ids.push(task_id);
        }
        self.jobs.insert(
            id,
            Job {
                last_access: self.access_counter,
                tasks: task_ids,
                status,
                id,
            },
        );
        if self.jobs.len() > self.capacity {
            let _ = self.drop_least_recently_accessed_job()?;
        }

        Ok(())
    }

    pub fn handle_write_request(&mut self, request: CacheWriteRequest) -> Result<(), CacheError> {
        self.increment_access_counter();
        match request {
            // Add an existing Job to the cache
            CacheWriteRequest::AddExistingJob(JobStatusDetail {
                id,
                status,
                task_statuses,
            }) => {
                let _ = self.add_job(id, status, task_statuses)?;
            }
            CacheWriteRequest::CancelTask(task_id) => {
                // sync db state
                self.db_writer_handle
                    .set_task_status(task_id, Status::Canceled);
                match self.tasks.entry(task_id) {
                    Entry::Vacant(entry) => {
                        entry.insert(Task {
                            status: Status::Canceled,
                            job: None,
                        });
                    }
                    Entry::Occupied(mut entry) => {
                        let mut task = entry.get_mut();
                        task.status = Status::Canceled;
                        if let Some(job_id) = task.job {
                            let _ = self.update_job_status(job_id)?;
                        }
                    }
                }
            }
            CacheWriteRequest::CancelJob(job_id) => {
                // sync db state
                self.db_writer_handle
                    .set_job_status(job_id, Status::Canceled);
                let mut job = self
                    .jobs
                    .get_mut(&job_id)
                    .ok_or(CacheError::UnknownJob(job_id))?;
                job.status = Status::Canceled;
                job.last_access = self.access_counter;
            }
        }
        Ok(())
    }
}

/// trait for interacting with the CacheActor
pub trait CacheWriteHandle {
    fn cancel_task(&self, task: TaskId);
    fn cancel_job(&self, job: JobId);
    fn add_job(&self, job_status: JobStatusDetail);
}

/// handle to CacheActor
#[derive(Clone)]
pub struct CacheWriter(pub UnboundedSender<CacheWriteRequest>);

impl CacheWriteHandle for CacheWriter {
    fn cancel_task(&self, task: TaskId) {
        let _ = self.0.send(CacheWriteRequest::CancelTask(task));
    }

    fn cancel_job(&self, job: JobId) {
        let _ = self.0.send(CacheWriteRequest::CancelJob(job));
    }

    fn add_job(&self, job_status: JobStatusDetail) {
        let _ = self.0.send(CacheWriteRequest::AddExistingJob(job_status));
    }
}

/// trait for interacting with the CacheActor
#[async_trait]
pub trait CacheReadHandle {
    async fn get_job_status(&self, job: JobId) -> Option<JobStatusDetail>;
}

/// handle to CacheActor
#[derive(Clone)]
pub struct CacheReader(pub UnboundedSender<CacheReadRequest>);

#[async_trait]
impl CacheReadHandle for CacheReader {
    async fn get_job_status(&self, job: JobId) -> Option<JobStatusDetail> {
        let (tx, rx) = channel::<CacheResponse>();
        let _ = self.0.send(CacheReadRequest::GetJob { id: job, from: tx });

        match rx.await {
            Ok(CacheResponse::JobStatus(job_status)) => Some(job_status),
            _ => None,
        }
    }
}

/// Spawn an actor responsible for keeping track of the
/// status of jobs and tasks.
///
/// This cache acts as a least recently used (LRU) cache,
/// when its capacity is reached, the least recently used
/// entry is dropped.
///
/// Every change in a Job/Task should be registered to this actor using the
/// UnboundedSender<CacheWriteRequest> paired with `write_requests`,
/// this is actor is reponsible for forwarding the "write order" to the `db_writer_actor`.
///
pub fn spawn_cache_actor<Q>(
    queue_handle: Q,
    db_writer_handle: DbWriterHandle,
    mut read_requests: UnboundedReceiver<CacheReadRequest>,
    mut write_requests: UnboundedReceiver<CacheWriteRequest>,
    mut status_notifications: UnboundedReceiver<StatusUpdate>,
    capacity: usize,
) where
    Q: QueuedTaskHandle + Send + 'static,
{
    // actor state
    let mut cache_actor = CacheActor {
        queue_handle,
        db_writer_handle,
        tasks: HashMap::new(),
        jobs: HashMap::new(),
        access_counter: ACCESS_COUNT_ORIGIN,
        capacity,
    };

    tokio::spawn(async move {
        loop {
            tokio::select! {
               // this asserts that futures are polled in order, eg
               // it introduce a priority 'writes from queue' > 'writes from monitors' > 'read request'
               biased;
               Some(write_request) = write_requests.recv() => {
                    match cache_actor.handle_write_request(write_request) {
                        Ok(_) => {},
                        Err(error) => eprintln!("Cache actor failed while processing write: {:?}", error)
                    }
               },
               Some(status_notification) = status_notifications.recv() => {
                    match cache_actor.handle_status_notication(status_notification) {
                        Ok(_) => {},
                        Err(error) => eprintln!("Cache actor failed while processing status notif: {:?}", error)
                    }
               },
               Some(read_request) = read_requests.recv() => {
                    match cache_actor.handle_read_request(read_request) {
                        Ok(_) => {},
                        Err(error) => eprintln!("Cache actor failed while processing read: {:?}", error)
                    }
               },
               else => break
            }
        }
    });
}

#[cfg(test)]
mod actor_tests {
    use crate::models::{Status, TaskId};
    use crate::scheduling::cache_actor::{
        spawn_cache_actor, CacheReadRequest, CacheResponse, CacheWriteRequest, JobStatusDetail,
    };
    use crate::scheduling::db_writer_actor::{DbWriteRequest, DbWriterHandle};
    use crate::scheduling::queue_actor::QueuedTaskHandle;
    use crate::scheduling::StatusUpdate;
    use rocket::tokio;
    use rocket::tokio::sync::{mpsc::unbounded_channel, oneshot};

    pub struct QueueMockUp {}
    impl QueuedTaskHandle for QueueMockUp {
        fn set_task_succeed(&self, _task: TaskId) {}
        fn set_task_failed(&self, _task: TaskId) {}
        fn set_task_started(&self, _task: TaskId) {}
    }

    #[tokio::test]
    async fn test_adding_jobs() {
        let queue = QueueMockUp {};

        let (db_req_tx, _db_req_rx) = unbounded_channel::<DbWriteRequest>();
        let db_writer_handle = DbWriterHandle(db_req_tx);

        let (read_tx, read_rx) = unbounded_channel::<CacheReadRequest>();
        let (write_tx, write_rx) = unbounded_channel::<CacheWriteRequest>();
        let (status_tx, status_rx) = unbounded_channel::<StatusUpdate>();

        spawn_cache_actor(queue, db_writer_handle, read_rx, write_rx, status_rx, 5);

        // Add a new Job
        let job_status = JobStatusDetail {
            id: 0,
            status: Status::Pending,
            task_statuses: vec![(0, Status::Pending), (1, Status::Pending)],
        };
        let write_request = CacheWriteRequest::AddExistingJob(job_status);
        let _ = write_tx
            .send(write_request)
            .expect("failed to write to cache");

        // start task 0
        let status_update = StatusUpdate {
            task_id: 0,
            status: Status::Running,
        };
        let _ = status_tx
            .send(status_update)
            .expect("failed to update task to cache");

        // request job status
        let (tx, rx) = oneshot::channel::<CacheResponse>();
        let read_request = CacheReadRequest::GetJob { id: 0, from: tx };
        let _ = read_tx
            .send(read_request)
            .expect("failed to send read request");
        let reponse: CacheResponse = rx.await.expect("response failed");

        assert_eq!(
            reponse,
            CacheResponse::JobStatus(JobStatusDetail {
                id: 0,
                status: Status::Running,
                task_statuses: vec![(0, Status::Running), (1, Status::Pending)],
            })
        );
    }
}
