use crate::models::{JobId, TaskId, Status};
use rocket::tokio::{
    self,
    sync::{
        mpsc::{UnboundedReceiver},
        oneshot::Sender,
    },
};
use std::collections::HashMap;

use super::queue_actor::{
    QueueNotifier,
};

type StatusAge = usize;
type AccessId = usize;

pub const ACCESS_COUNT_ORIGIN: usize = 0;


#[derive(Debug)]
pub enum CacheError {
    UnknownTask(TaskId),
    UnknownJob(JobId),
}

pub enum TaskStatusNotif {
    HasStatus {
        task_id: TaskId,
        status: Status,
        age: StatusAge,
    },
}

pub enum CacheResponse {
    /// either a cache miss, of a non-existent JobId
    UnknownJob,
    JobStatus {
        id: JobId,
        status: Status,
        task_statuses: Vec<(TaskId, Status)>,
    }
}

pub enum ReadRequest {
    /// ask for the status of a Job
    GetJob {
        id: JobId,
        from: Sender<CacheResponse>
    },
}
pub enum WriteRequest {
    /// Cancel a task (maybe its parent failed)
    CancelTask(TaskId),
    /// Cancel a job (children cancellation must
    /// be handled with `CancelTask` requests
    CancelJob(JobId),
    /// Add new Pending job
    AddNewJob {
        id: JobId,
        tasks: Vec<TaskId>,
    },
    /// Add Cache entry from DB (after cache miss)
    AddExistingJob{
        id: JobId,
        status: Status,
        tasks: Vec<(TaskId, Status)>,
    },
}

pub struct Job {
    id: JobId,
    status: Status,
    tasks: Vec<TaskId>,
    last_access: AccessId,
}

pub struct Cache<T> {
    queue_handle: T,
    tasks: HashMap<TaskId, (StatusAge, Status)>,
    jobs: HashMap<JobId, Job>,
    /// counter
    access_counter: AccessId,
    /// maximum nb of job entries
    capacity: usize,
}

impl <T: QueueNotifier> Cache<T> {

    /// increment the access counter,
    /// handle overflows by reseting all jobs age
    /// (while keeping the acces order)
    fn increment_access_counter(&mut self) {
        match self.access_counter {
            AccessId::MAX => {
                let mut accesses: Vec<(AccessId, JobId)> = self.jobs.values().map(|job| (job.last_access, job.id)).collect();
                accesses.sort_by_key(|&(access, _id)| access);
                for (index, (_access, job_id)) in accesses.iter().enumerate() {
                    self.jobs.get_mut(job_id)
                        .unwrap() // safe because key and values.id are garantied to be consisitent
                        .last_access = index;
                }
                self.access_counter = accesses.len();
            },
            _ => {
                self.access_counter += 1;
            }
        }
    }

    /// Iter all stored jobs, and remove
    /// the least recently accessed job
    fn drop_least_recently_accessed_job(&mut self) -> Result<(), CacheError> {
        let mut accesses: Vec<(AccessId, JobId)> = self.jobs.values().map(|job| (job.last_access, job.id)).collect();
        accesses.sort_by_key(|&(access, _id)| access);
        if let Some((_access, id)) = accesses.first() {
            // unwrap is safe because the id wa
            let job = self.jobs.remove(id).ok_or(CacheError::UnknownJob(*id))?;
            for task_id in job.tasks {
                let _task = self.tasks.remove(&task_id).ok_or(CacheError::UnknownTask(*id))?;
            }
        }
        Ok(())
    }

    pub fn handle_status_notication(&mut self, notif: TaskStatusNotif) -> Result<(), CacheError> {
        match notif {
                TaskStatusNotif::HasStatus { task_id, status, age } => {
                    let must_update: bool = if let Some(status_tuple) = self.tasks.get(&task_id) {
                        age >= status_tuple.0
                    } else { true };
                    if must_update {
                        // warn Queue Actor
                        match status {
                            Status::Failed | Status::Killed => self.queue_handle.set_task_failed(task_id),
                            Status::Running => self.queue_handle.set_task_started(task_id),
                            Status::Succeed => self.queue_handle.set_task_succeed(task_id),
                            _ => eprintln!("Bad status transtition.")
                        }
                        self.tasks.insert(task_id, (age, status));
                        //self.sync_task(task_id);
                    }
            },
        }
        Ok(())
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
                            task_statuses.push(
                                (
                                    *task_id,
                                    self.tasks.get(task_id)
                                        .ok_or(CacheError::UnknownTask(*task_id))?
                                        .1
                                )
                            );
                        }
                        // send status
                        from.send(
                            CacheResponse::JobStatus {
                                id,
                                status: job.status,
                                task_statuses, 
                            }
                        );
                    },
                    None => {
                        from.send(CacheResponse::UnknownJob);
                    }
                }
            }
        }
        Ok(())
    }

    fn add_job(&mut self, id: JobId, status: Status, tasks: Vec<(TaskId, Status)>) -> Result<(), CacheError> {
        let mut task_ids = Vec::with_capacity(tasks.len());
        for (task_id, status) in tasks {
            // don't override existing status
            let _ = self.tasks.entry(task_id)
                .or_insert((ACCESS_COUNT_ORIGIN, status));
            task_ids.push(task_id);
        }
        self.jobs.insert(id, Job {
            last_access: self.access_counter,
            tasks: task_ids,
            status,
            id,
        });
        if self.jobs.len() > self.capacity {
            let _ = self.drop_least_recently_accessed_job()?;
        }

        Ok(())
    }

    pub fn handle_write_request(&mut self, request: WriteRequest) -> Result<(), CacheError> {
        self.increment_access_counter();
        match request {
            /// Add a new (pending) Job to the cache
            WriteRequest::AddNewJob { id, tasks } => {
                let tasks = tasks.iter().map(|task_id| (*task_id, Status::Pending)).collect();
                self.add_job(id, Status::Pending, tasks);
                // TODO!
                //self.update_job_status(id);
                //self.sync_job();
                //self.sync_task();
            },
            /// Add an existing Job to the cache
            WriteRequest::AddExistingJob { id, status, tasks } => {
                self.add_job(id, status, tasks);
            },
            WriteRequest::CancelTask(task_id) => {
                self.tasks.get_mut(&task_id)
                    .ok_or(CacheError::UnknownTask(task_id))?
                    .1 = Status::Canceled;
                    // TODO!
                //self.sync_task();
            }
            WriteRequest::CancelJob(job_id) => {
                let mut job = self.jobs.get_mut(&job_id)
                    .ok_or(CacheError::UnknownTask(job_id))?;
                job.status = Status::Canceled;
                job.last_access = self.access_counter;
                //self.sync_job();
            }
        }
        Ok(())
    }
}


/// 
pub trait CacheWriter {
    fn cancel_task(&self, task: TaskId);
    fn cancel_job(&self, job: JobId);
    fn add_job(&self, job: JobId, job_status: Status, tasks: Vec<(TaskId, Status)>);
}

async fn manage_cache<Q: QueueNotifier>(
    mut cache_actor: Cache<Q>,
    mut read_requests: UnboundedReceiver<ReadRequest>,
    mut write_requests: UnboundedReceiver<WriteRequest>,
    mut task_notifs: UnboundedReceiver<TaskStatusNotif>,
) {
    loop {
        tokio::select! {
           // this asserts that futures are polled in order, eg 
           // it introduce a priority task event > orders > claim request
           biased;
           Some(read_request) = read_requests.recv() => {
                match cache_actor.handle_read_request(read_request) {
                    Ok(_) => {},
                    Err(error) => eprintln!("Cache actor failed while processing read: {:?}", error)
                }
           },
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
           else => break
        }
    }
}

pub fn spawn_cache_actor<Q>(
        queue_handle: Q,
        read_requests: UnboundedReceiver<ReadRequest>,
        write_requests: UnboundedReceiver<WriteRequest>,
        task_notifs: UnboundedReceiver<TaskStatusNotif>,
        capacity: usize,
    )
    where Q: QueueNotifier + Send + 'static
{
    let cache_actor = Cache {
        queue_handle,
        tasks: HashMap::new(),
        jobs: HashMap::new(),
        access_counter: ACCESS_COUNT_ORIGIN,
        capacity,
    };
    tokio::spawn(async move {  manage_cache(cache_actor, read_requests, write_requests, task_notifs).await } );
}
