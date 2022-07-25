use super::cache_actor::CacheWriter;
use crate::models::{JobId, TaskId};
use rocket::tokio::{
    self,
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        oneshot::Sender,
    },
};
use std::collections::HashMap;

/// TODO: move into "killer actor"
pub trait KillerHandle {
    fn kill(&mut self, task: TaskId);
}

#[derive(Debug)]
pub enum QueueError {
    UnknownTask(TaskId),
    BadState(TaskId),
    BadTransition(TaskEvent),
    SendFailed,
}

#[derive(Debug)]
pub struct QueuedTask {
    id: TaskId,
    group: JobId,
    children: Option<Vec<TaskId>>,
    /// low value means high priority
    priority: usize,
    state: QueuedState,
}

#[derive(Debug)]
pub enum QueuedState {
    AwaitingParents(Vec<TaskId>),
    AwaitingSubmission,
    AwaitingMurder,
    Claimed,
    Running,
}

#[derive(Debug)]
pub enum QueueOrder {
    SubmitTask {
        id: TaskId,
        group: JobId,
        number_of_task_in_group: usize,
        children: Option<Vec<TaskId>>,
        parents: Option<Vec<TaskId>>,
    },
    CancelGroup(JobId),
}

#[derive(Debug)]
pub enum TaskEvent {
    TaskSucceed(TaskId),
    TaskFailed(TaskId),
    TaskStarted(TaskId),
}

#[derive(Debug)]
pub struct ClaimRequest {
    claimer: Sender<Option<TaskId>>,
}

struct QueueActor<K, S> {
    queue: HashMap<TaskId, QueuedTask>,
    killer: K,
    store: S,
}

impl<K: KillerHandle, S: CacheWriter> QueueActor<K, S> {
    pub fn new(killer: K, store: S) -> Self {
        Self {
            queue: HashMap::new(),
            killer,
            store,
        }
    }

    pub fn handle_claim_request(&mut self, req: ClaimRequest) -> Result<(), QueueError> {
        let mut readys: Vec<(TaskId, usize)> = Vec::new();
        for task in self.queue.values() {
            if let QueuedState::AwaitingSubmission = task.state {
                readys.push((task.id, task.priority));
            }
        }
        readys.sort_by_key(|(_id, priority)| *priority);
        let response = match readys.get(0) {
            Some((task_id, _priority)) => {
                self.queue
                    .get_mut(task_id)
                    .ok_or(QueueError::UnknownTask(*task_id))?
                    .state = QueuedState::Claimed;
                Some(*task_id)
            }
            None => None,
        };
        let _ = req
            .claimer
            .send(response)
            .map_err(|_| QueueError::SendFailed)?;
        Ok(())
    }

    pub fn handle_order(&mut self, order: QueueOrder) -> Result<(), QueueError> {
        match order {
            // insert a new item in the queue
            QueueOrder::SubmitTask {
                id,
                group,
                children,
                parents,
                number_of_task_in_group,
            } => {
                let state = match parents {
                    Some(parents) => QueuedState::AwaitingParents(parents),
                    None => QueuedState::AwaitingSubmission,
                };
                let task = QueuedTask {
                    id,
                    group,
                    priority: number_of_task_in_group,
                    state,
                    children,
                };
                self.queue.insert(id, task);
            }
            // remove Awaiting item in the queue, and all its children.
            // if some task is running, order its murder, but leave it as running
            QueueOrder::CancelGroup(group_id) => {
                let mut to_remove = Vec::new();
                // mark the running one as "AwaitingMurder",
                // keep track of the awaiting ones
                for task in self.queue.values_mut() {
                    if task.group != group_id {
                        continue;
                    }
                    match task.state {
                        QueuedState::Claimed => {
                            task.state = QueuedState::AwaitingMurder;
                        }
                        QueuedState::Running => {
                            // kill the runnning ones
                            self.killer.kill(task.id);
                            task.state = QueuedState::AwaitingMurder;
                        }
                        QueuedState::AwaitingMurder => {}
                        _ => to_remove.push(task.id),
                    }
                }
                // remove all the awaiting ones
                for id in to_remove {
                    let _ = self.queue.remove(&id);
                    self.store.cancel_task(id);
                }
                self.store.cancel_job(group_id);
            }
        }
        Ok(())
    }

    pub fn handle_event(&mut self, event: TaskEvent) -> Result<(), QueueError> {
        match event {
            // remove task from queue,
            // and remove task_id from children dependency list
            TaskEvent::TaskSucceed(task_id) => {
                // update children
                let task = self
                    .queue
                    .remove(&task_id)
                    .ok_or(QueueError::UnknownTask(task_id))?;
                if let Some(children) = task.children {
                    for child_id in children {
                        let child_task = self
                            .queue
                            .get_mut(&child_id)
                            .ok_or(QueueError::UnknownTask(child_id))?;
                        match &child_task.state {
                            QueuedState::AwaitingParents(parents) => {
                                let parents: Vec<TaskId> = parents
                                    .iter()
                                    .copied()
                                    .filter(|id| *id != task_id)
                                    .collect();
                                if parents.is_empty() {
                                    child_task.state = QueuedState::AwaitingSubmission;
                                } else {
                                    child_task.state = QueuedState::AwaitingParents(parents);
                                }
                            }
                            _ => continue,
                        }
                    }
                }
            }
            // remove task and its children from queue,
            TaskEvent::TaskFailed(task_id) => {
                let faulty = self
                    .queue
                    .remove(&task_id)
                    .ok_or(QueueError::UnknownTask(task_id))?;
                // recursively remove children,
                // and cancel them
                if let Some(mut to_remove) = faulty.children {
                    while let Some(child_id) = to_remove.pop() {
                        let child = self
                            .queue
                            .remove(&child_id)
                            .ok_or(QueueError::UnknownTask(child_id))?;
                        if let Some(grand_children) = child.children {
                            to_remove.extend(grand_children);
                        }
                        // warn store that task was canceled
                        self.store.cancel_task(child_id);
                    }
                }
            }
            // mark the task as running, unless
            // it was marked as 'AwaitingMurder'
            // in this case kill it.
            TaskEvent::TaskStarted(task_id) => {
                let mut task = self
                    .queue
                    .get_mut(&task_id)
                    .ok_or(QueueError::UnknownTask(task_id))?;
                match &task.state {
                    // mark task as runnning
                    QueuedState::Claimed => {
                        task.state = QueuedState::Running;
                    }
                    // kill task then remove it
                    QueuedState::AwaitingMurder => {
                        self.killer.kill(task_id);
                        drop(task);
                        let _ = self.queue.remove(&task_id);
                    }
                    // cannot reason about it.
                    _state => {
                        return Err(QueueError::BadTransition(TaskEvent::TaskStarted(task_id)));
                    }
                }
            }
        }
        Ok(())
    }
}

async fn manage_queue<K: KillerHandle, S: CacheWriter>(
    mut queue_actor: QueueActor<K, S>,
    mut events: UnboundedReceiver<TaskEvent>,
    mut orders: UnboundedReceiver<QueueOrder>,
    mut claims: UnboundedReceiver<ClaimRequest>,
) {
    loop {
        tokio::select! {
           // this asserts that futures are polled in order, eg
           // it introduce a priority task event > orders > claim request
           biased;
           Some(event) = events.recv() => {
                match queue_actor.handle_event(event) {
                    Ok(_) => {},
                    Err(error) => eprintln!("queue actor: {:?}", error)
                }
           },
           Some(order) = orders.recv() => {
                match queue_actor.handle_order(order) {
                    Ok(_) => {},
                    Err(error) => eprintln!("queue actor: {:?}", error)
                }
           },
           Some(claim) = claims.recv() => {
                match queue_actor.handle_claim_request(claim) {
                    Ok(_) => {},
                    Err(error) => eprintln!("queue actor: {:?}", error)
                }
           },
           else => break
        }
    }
}

pub fn spawn_queue_actor<K, S>(
    killer: K,
    store: S,
    events: UnboundedReceiver<TaskEvent>,
    orders: UnboundedReceiver<QueueOrder>,
    claims: UnboundedReceiver<ClaimRequest>,
) where
    K: KillerHandle + Send + 'static,
    S: CacheWriter + Send + 'static,
{
    let queue_actor = QueueActor::new(killer, store);
    tokio::spawn(async move { manage_queue(queue_actor, events, orders, claims).await });
}

/// Handle to the QueueActor,
/// for the CacheActor
pub trait QueueNotifier {
    fn set_task_succeed(&self, task: TaskId);
    fn set_task_failed(&self, task: TaskId);
    fn set_task_started(&self, task: TaskId);
}

pub struct QueueActorHandle<Msg> {
    queue_actor: UnboundedSender<Msg>,
}
impl QueueNotifier for QueueActorHandle<TaskEvent> {
    fn set_task_succeed(&self, task: TaskId) {
        self.queue_actor.send(TaskEvent::TaskSucceed(task));
    }

    fn set_task_failed(&self, task: TaskId) {
        self.queue_actor.send(TaskEvent::TaskFailed(task));
    }

    fn set_task_started(&self, task: TaskId) {
        self.queue_actor.send(TaskEvent::TaskStarted(task));
    }
}

#[cfg(test)]
mod actor_tests {
    use crate::models::Status;
    use crate::scheduling::queue_actor::*;

    pub struct KillerMockUp {}
    impl KillerHandle for KillerMockUp {
        fn kill(&mut self, _task: TaskId) {}
    }

    pub struct CacheMockUp {}
    impl CacheWriter for CacheMockUp {
        fn cancel_task(&self, _task: TaskId) {}
        fn cancel_job(&self, _job: JobId) {}
        fn add_job(&self, _job: JobId, _job_status: Status, _tasks: Vec<(TaskId, Status)>) {}
    }

    #[tokio::test]
    async fn test_filling_queue_with_one_task() {
        let killer = KillerMockUp {};
        let store = CacheMockUp {};
        use tokio::sync::{mpsc::unbounded_channel, oneshot};

        let (_task_sender, task_receiver) = unbounded_channel::<TaskEvent>();
        let (order_sender, order_receiver) = unbounded_channel::<QueueOrder>();
        let (claims_sender, claims_receiver) = unbounded_channel::<ClaimRequest>();

        spawn_queue_actor(
            killer,
            store,
            task_receiver,
            order_receiver,
            claims_receiver,
        );

        let order = QueueOrder::SubmitTask {
            id: 42,
            group: 0,
            number_of_task_in_group: 1,
            children: None,
            parents: None,
        };
        let _ = order_sender.send(order).expect("failed to send task");

        // Request one task
        let (sender, receiver) = oneshot::channel::<Option<TaskId>>();
        let request = ClaimRequest { claimer: sender };
        let _ = claims_sender.send(request).expect("request failed");
        let task: Option<TaskId> = receiver.await.expect("response failed");
        assert_eq!(task, Some(42));

        // Request another task
        let (sender, receiver) = oneshot::channel::<Option<TaskId>>();
        let request = ClaimRequest { claimer: sender };
        let _ = claims_sender.send(request).expect("request failed");
        let task: Option<TaskId> = receiver.await.expect("response failed");
        assert_eq!(task, None);
    }

    #[tokio::test]
    async fn test_canceling_tasks() {
        let killer = KillerMockUp {};
        let store = CacheMockUp {};
        use tokio::sync::{mpsc::unbounded_channel, oneshot};

        let (task_sender, task_receiver) = unbounded_channel::<TaskEvent>();
        let (order_sender, order_receiver) = unbounded_channel::<QueueOrder>();
        let (claims_sender, claims_receiver) = unbounded_channel::<ClaimRequest>();

        spawn_queue_actor(
            killer,
            store,
            task_receiver,
            order_receiver,
            claims_receiver,
        );

        // Spawn 2 tasks
        let order = QueueOrder::SubmitTask {
            id: 1,
            group: 0,
            number_of_task_in_group: 2,
            children: Some(vec![2]),
            parents: None,
        };
        let _ = order_sender.send(order).expect("failed to send task");
        let order = QueueOrder::SubmitTask {
            id: 2,
            group: 0,
            number_of_task_in_group: 2,
            children: None,
            parents: Some(vec![1]),
        };
        let _ = order_sender.send(order).expect("failed to send task");

        // run the firt one: Claim it
        let (sender, receiver) = oneshot::channel::<Option<TaskId>>();
        let request = ClaimRequest { claimer: sender };
        let _ = claims_sender.send(request).expect("request failed");
        let task: Option<TaskId> = receiver.await.expect("response failed");
        assert_eq!(task, Some(1));

        // run the firt one: declare it as started
        let task_event = TaskEvent::TaskStarted(1);
        let _ = task_sender.send(task_event).expect("status update failed");

        // Cancel the job
        let order = QueueOrder::CancelGroup(0);
        let _ = order_sender.send(order).expect("failed to send task");

        // Request another task, assert that there is none
        let (sender, receiver) = oneshot::channel::<Option<TaskId>>();
        let request = ClaimRequest { claimer: sender };
        let _ = claims_sender.send(request).expect("request failed");
        let task: Option<TaskId> = receiver.await.expect("response failed");
        assert_eq!(task, None);
    }

    #[tokio::test]
    async fn test_task_dependency() {
        let killer = KillerMockUp {};
        let store = CacheMockUp {};
        use tokio::sync::{mpsc::unbounded_channel, oneshot};

        let (task_sender, task_receiver) = unbounded_channel::<TaskEvent>();
        let (order_sender, order_receiver) = unbounded_channel::<QueueOrder>();
        let (claims_sender, claims_receiver) = unbounded_channel::<ClaimRequest>();

        spawn_queue_actor(
            killer,
            store,
            task_receiver,
            order_receiver,
            claims_receiver,
        );

        // Spawn 3 tasks, with dependencies:
        //     1
        //   /  \
        //  2    3
        let order = QueueOrder::SubmitTask {
            id: 1,
            group: 0,
            number_of_task_in_group: 3,
            children: Some(vec![2, 3]),
            parents: None,
        };
        let _ = order_sender.send(order).expect("failed to send task");
        let order = QueueOrder::SubmitTask {
            id: 2,
            group: 0,
            number_of_task_in_group: 3,
            children: None,
            parents: Some(vec![1]),
        };
        let _ = order_sender.send(order).expect("failed to send task");
        let order = QueueOrder::SubmitTask {
            id: 3,
            group: 0,
            number_of_task_in_group: 3,
            children: None,
            parents: Some(vec![1]),
        };
        let _ = order_sender.send(order).expect("failed to send task");

        // run the firt one: Claim it
        let (sender, receiver) = oneshot::channel::<Option<TaskId>>();
        let request = ClaimRequest { claimer: sender };
        let _ = claims_sender.send(request).expect("request failed");
        let task: Option<TaskId> = receiver.await.expect("response failed");
        assert_eq!(task, Some(1));

        // run the firt one: declare it as started
        let task_event = TaskEvent::TaskStarted(1);
        let _ = task_sender.send(task_event).expect("status update failed");

        // Request another task, assert that there is none, because the dependency
        // of the 2 remaining ones are not fulfilled
        let (sender, receiver) = oneshot::channel::<Option<TaskId>>();
        let request = ClaimRequest { claimer: sender };
        let _ = claims_sender.send(request).expect("request failed");
        let task: Option<TaskId> = receiver.await.expect("response failed");
        assert_eq!(task, None);

        // finish the first one
        let task_event = TaskEvent::TaskSucceed(1);
        let _ = task_sender.send(task_event).expect("status update failed");

        // assert that the 2 remainings can be claimed
        let (sender, receiver) = oneshot::channel::<Option<TaskId>>();
        let request = ClaimRequest { claimer: sender };
        let _ = claims_sender.send(request).expect("request failed");
        let task: Option<TaskId> = receiver.await.expect("response failed");
        assert!(task == Some(2) || task == Some(3));

        let (sender, receiver) = oneshot::channel::<Option<TaskId>>();
        let request = ClaimRequest { claimer: sender };
        let _ = claims_sender.send(request).expect("request failed");
        let task: Option<TaskId> = receiver.await.expect("response failed");
        assert!(task == Some(2) || task == Some(3));
    }

    async fn test_failure_propagation() {
        let killer = KillerMockUp {};
        let store = CacheMockUp {};
        use tokio::sync::{mpsc::unbounded_channel, oneshot};

        let (task_sender, task_receiver) = unbounded_channel::<TaskEvent>();
        let (order_sender, order_receiver) = unbounded_channel::<QueueOrder>();
        let (claims_sender, claims_receiver) = unbounded_channel::<ClaimRequest>();

        spawn_queue_actor(
            killer,
            store,
            task_receiver,
            order_receiver,
            claims_receiver,
        );

        // Spawn 3 tasks, with dependencies:
        //     1
        //     |
        //     2
        //     |
        //     3
        let order = QueueOrder::SubmitTask {
            id: 1,
            group: 0,
            number_of_task_in_group: 3,
            children: Some(vec![2]),
            parents: None,
        };
        let _ = order_sender.send(order).expect("failed to send task");
        let order = QueueOrder::SubmitTask {
            id: 2,
            group: 0,
            number_of_task_in_group: 3,
            children: Some(vec![3]),
            parents: Some(vec![1]),
        };
        let _ = order_sender.send(order).expect("failed to send task");
        let order = QueueOrder::SubmitTask {
            id: 3,
            group: 0,
            number_of_task_in_group: 3,
            children: None,
            parents: Some(vec![2]),
        };
        let _ = order_sender.send(order).expect("failed to send task");

        // run the firt one: Claim it
        let (sender, receiver) = oneshot::channel::<Option<TaskId>>();
        let request = ClaimRequest { claimer: sender };
        let _ = claims_sender.send(request).expect("request failed");
        let task: Option<TaskId> = receiver.await.expect("response failed");
        assert_eq!(task, Some(1));

        // run the firt one: declare it as started
        let task_event = TaskEvent::TaskStarted(1);
        let _ = task_sender.send(task_event).expect("status update failed");

        // finish the first one
        let task_event = TaskEvent::TaskSucceed(1);
        let _ = task_sender.send(task_event).expect("status update failed");

        // claim the second one
        let (sender, receiver) = oneshot::channel::<Option<TaskId>>();
        let request = ClaimRequest { claimer: sender };
        let _ = claims_sender.send(request).expect("request failed");
        let task: Option<TaskId> = receiver.await.expect("response failed");
        assert_eq!(task, Some(2));

        // start it
        let task_event = TaskEvent::TaskStarted(2);
        let _ = task_sender.send(task_event).expect("status update failed");

        // mark it as failed
        let task_event = TaskEvent::TaskFailed(2);
        let _ = task_sender.send(task_event).expect("status update failed");

        // assert that the remaining one cannot be claimed,
        // because its dependency (task 2) failed.
        let (sender, receiver) = oneshot::channel::<Option<TaskId>>();
        let request = ClaimRequest { claimer: sender };
        let _ = claims_sender.send(request).expect("request failed");
        let task: Option<TaskId> = receiver.await.expect("response failed");
        assert_eq!(task, None);
    }
}
