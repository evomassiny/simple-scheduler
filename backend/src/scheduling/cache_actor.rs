use crate::models::{JobId, Status, TaskId};
use rocket::tokio::{
    self,
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        oneshot::Sender,
    },
};
use std::collections::hash_map::Entry;
use std::collections::HashMap;

use super::queue_actor::QueueHandle;
use super::status_aggregator_actor::StatusUpdate;

type StatusVersion = usize;
type AccessId = usize;

pub const ACCESS_COUNT_ORIGIN: usize = 0;

#[derive(Debug)]
pub enum CacheError {
    UnknownTask(TaskId),
    UnknownJob(JobId),
    SendFailed,
}

#[derive(PartialEq, Debug)]
pub enum CacheResponse {
    /// either a cache miss, of a non-existent JobId
    UnknownJob,
    JobStatus {
        id: JobId,
        status: Status,
        task_statuses: Vec<(TaskId, Status)>,
    },
}

#[derive(Debug)]
pub enum ReadRequest {
    /// ask for the status of a Job
    GetJob {
        id: JobId,
        from: Sender<CacheResponse>,
    },
}

#[derive(Debug)]
pub enum WriteRequest {
    /// Cancel a task (maybe its parent failed)
    CancelTask(TaskId),
    /// Cancel a job (children cancellation must
    /// be handled with `CancelTask` requests
    CancelJob(JobId),
    /// Add new Pending job
    AddNewJob { id: JobId, tasks: Vec<TaskId> },
    /// Add Cache entry from DB (after cache miss)
    AddExistingJob {
        id: JobId,
        status: Status,
        tasks: Vec<(TaskId, Status)>,
    },
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

pub struct CacheActor<T> {
    queue_handle: T,
    tasks: HashMap<TaskId, Task>,
    jobs: HashMap<JobId, Job>,
    /// counter
    access_counter: AccessId,
    /// maximum nb of job entries
    capacity: usize,
}

impl<T: QueueHandle> CacheActor<T> {
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
        // warn Queue Actor
        match update.status {
            Status::Failed | Status::Killed => self.queue_handle.set_task_failed(update.task_id),
            Status::Running => self.queue_handle.set_task_started(update.task_id),
            Status::Succeed => self.queue_handle.set_task_succeed(update.task_id),
            _ => eprintln!("Bad status transtition."),
        }

        if let Some(job_id) = maybe_job {
            let _ = self.update_job_status(job_id)?;
        }
        //self.sync_task(task_id);
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
            statuses.push(
                self.tasks
                    .get(task_id)
                    .ok_or(CacheError::UnknownTask(*task_id))?
                    .status,
            );
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
        Ok(job_status)
    }

    pub fn handle_read_request(&mut self, request: ReadRequest) -> Result<(), CacheError> {
        self.increment_access_counter();
        match request {
            // Request for the status of a Job,
            // Return `CacheResponse::JobStatus {..}` if we now about it,
            // `CacheResponse::UnknownJob` otherwise.
            ReadRequest::GetJob { id, from } => {
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
                            .send(CacheResponse::JobStatus {
                                id,
                                status: job.status,
                                task_statuses,
                            })
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

    pub fn handle_write_request(&mut self, request: WriteRequest) -> Result<(), CacheError> {
        self.increment_access_counter();
        match request {
            // Add a new (pending) Job to the cache
            WriteRequest::AddNewJob { id, tasks } => {
                let tasks = tasks
                    .iter()
                    .map(|task_id| (*task_id, Status::Pending))
                    .collect();
                let _ = self.add_job(id, Status::Pending, tasks)?;
                let _new_status = self.update_job_status(id)?;
                // TODO!
                //self.sync_job();
                //self.sync_task();
            }
            // Add an existing Job to the cache
            WriteRequest::AddExistingJob { id, status, tasks } => {
                let _ = self.add_job(id, status, tasks)?;
            }
            WriteRequest::CancelTask(task_id) => {
                self.tasks
                    .get_mut(&task_id)
                    .ok_or(CacheError::UnknownTask(task_id))?
                    .status = Status::Canceled;
                // TODO!
                //self.sync_task();
            }
            WriteRequest::CancelJob(job_id) => {
                let mut job = self
                    .jobs
                    .get_mut(&job_id)
                    .ok_or(CacheError::UnknownTask(job_id))?;
                job.status = Status::Canceled;
                job.last_access = self.access_counter;
                //self.sync_job();
            }
        }
        Ok(())
    }
}

/// trait for interacting with the CacheActor
pub trait CacheWriteHandle {
    fn cancel_task(&self, task: TaskId);
    fn cancel_job(&self, job: JobId);
    fn add_job(&self, job: JobId, job_status: Status, tasks: Vec<(TaskId, Status)>);
}

/// handle to CacheActor
pub struct CacheActorHandle {
    pub to_cache: UnboundedSender<WriteRequest>,
}
impl CacheWriteHandle for CacheActorHandle {
    fn cancel_task(&self, task: TaskId) {
        let _ = self.to_cache.send(WriteRequest::CancelTask(task));
    }

    fn cancel_job(&self, job: JobId) {
        let _ = self.to_cache.send(WriteRequest::CancelJob(job));
    }

    fn add_job(&self, job: JobId, job_status: Status, tasks: Vec<(TaskId, Status)>) {
        let _ = self.to_cache.send(WriteRequest::AddExistingJob {
            id: job,
            status: job_status,
            tasks,
        });
    }
}

async fn manage_cache<Q: QueueHandle>(
    mut cache_actor: CacheActor<Q>,
    mut read_requests: UnboundedReceiver<ReadRequest>,
    mut write_requests: UnboundedReceiver<WriteRequest>,
    mut task_notifs: UnboundedReceiver<StatusUpdate>,
) {
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
           Some(status_notification) = task_notifs.recv() => {
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
}

pub fn spawn_cache_actor<Q>(
    queue_handle: Q,
    read_requests: UnboundedReceiver<ReadRequest>,
    write_requests: UnboundedReceiver<WriteRequest>,
    task_notifs: UnboundedReceiver<StatusUpdate>,
    capacity: usize,
) where
    Q: QueueHandle + Send + 'static,
{
    let cache_actor = CacheActor {
        queue_handle,
        tasks: HashMap::new(),
        jobs: HashMap::new(),
        access_counter: ACCESS_COUNT_ORIGIN,
        capacity,
    };
    tokio::spawn(async move {
        manage_cache(cache_actor, read_requests, write_requests, task_notifs).await
    });
}

#[cfg(test)]
mod actor_tests {
    use crate::models::Status;
    use crate::scheduling::cache_actor::*;
    use crate::scheduling::queue_actor::QueueHandle;
    use tokio::sync::{mpsc::unbounded_channel, oneshot};

    pub struct QueueMockUp {}
    impl QueueHandle for QueueMockUp {
        fn set_task_succeed(&self, _task: TaskId) {}
        fn set_task_failed(&self, _task: TaskId) {}
        fn set_task_started(&self, _task: TaskId) {}
    }

    #[tokio::test]
    async fn test_adding_jobs() {
        let queue = QueueMockUp {};

        let (read_tx, read_rx) = unbounded_channel::<ReadRequest>();
        let (write_tx, write_rx) = unbounded_channel::<WriteRequest>();
        let (status_tx, status_rx) = unbounded_channel::<StatusUpdate>();

        spawn_cache_actor(queue, read_rx, write_rx, status_rx, 5);

        // Add a new Job
        let write_request = WriteRequest::AddNewJob {
            id: 0,
            tasks: vec![0, 1],
        };
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
        let read_request = ReadRequest::GetJob { id: 0, from: tx };
        let _ = read_tx
            .send(read_request)
            .expect("failed to send read request");
        let reponse: CacheResponse = rx.await.expect("response failed");
        assert_eq!(
            reponse,
            CacheResponse::JobStatus {
                id: 0,
                status: Status::Running,
                task_statuses: vec![(0, Status::Running), (1, Status::Pending)],
            }
        );
    }
}
