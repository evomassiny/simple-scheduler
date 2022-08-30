use super::cache_actor::CacheWriter;
use super::executor_actor::ExecutorHandle;
use crate::models::{JobId, TaskId};
use rocket::tokio::{
    self,
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        oneshot::Sender,
    },
};
use std::collections::HashMap;

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
    /// task must wait for its parent
    AwaitingParents(Vec<TaskId>),
    /// task can be spawned
    AwaitingSpawning,
    /// we requested to cancel a task right
    /// after spawning it, before it even ran.
    AwaitingMurder,
    /// the task as been spawned,
    /// but we dont know (yet) if it's running.
    Spawned,
    /// task is running
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
    executor_handle: K,
    store: S,
    worker_pool_size: usize,
    busy_workers: usize,
}

impl<K: ExecutorHandle, S: CacheWriter> QueueActor<K, S> {
    pub fn new(executor_handle: K, store: S, worker_pool_size: usize) -> Self {
        Self {
            queue: HashMap::new(),
            executor_handle,
            store,
            worker_pool_size,
            busy_workers: 0,
        }
    }

    pub fn has_idle_workers(&self) -> bool {
        self.busy_workers < self.worker_pool_size
    }

    /// try to spawn a task, return true if any task has been spawned
    pub fn spawn_task_to_executor(&mut self) -> Result<bool, QueueError> {
        // select tasks that are ready to be launched
        let mut readys: Vec<(TaskId, usize)> = Vec::new();
        for task in self.queue.values() {
            if let QueuedState::AwaitingSpawning = task.state {
                readys.push((task.id, task.priority));
            }
        }
        // pick the highest priority one
        readys.sort_by_key(|(_id, priority)| *priority);
        let has_spawned = match readys.get(0) {
            Some((task_id, _priority)) => {
                // set task status as "Spawned"
                self.queue
                    .get_mut(task_id)
                    .ok_or(QueueError::UnknownTask(*task_id))?
                    .state = QueuedState::Spawned;
                // consider that the task has taken a slot
                self.busy_workers += 1;
                // spawn it
                self.executor_handle.spawn(*task_id);
                true
            }
            None => false,
        };
        Ok(has_spawned)
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
                    None => QueuedState::AwaitingSpawning,
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
                        QueuedState::Spawned => {
                            task.state = QueuedState::AwaitingMurder;
                        }
                        QueuedState::Running => {
                            // kill the runnning ones
                            self.executor_handle.kill(task.id);
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
                                    child_task.state = QueuedState::AwaitingSpawning;
                                } else {
                                    child_task.state = QueuedState::AwaitingParents(parents);
                                }
                            }
                            _ => continue,
                        }
                    }
                }
                // update running task count
                self.busy_workers = match self.busy_workers {
                    0 => 0,
                    nb => nb - 1,
                };
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
                // update running task count
                self.busy_workers = match self.busy_workers {
                    0 => 0,
                    nb => nb - 1,
                };
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
                    QueuedState::Spawned => {
                        task.state = QueuedState::Running;
                    }
                    // kill task then remove it
                    QueuedState::AwaitingMurder => {
                        self.executor_handle.kill(task_id);
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

async fn manage_queue<K: ExecutorHandle, S: CacheWriter>(
    mut queue_actor: QueueActor<K, S>,
    mut events: UnboundedReceiver<TaskEvent>,
    mut orders: UnboundedReceiver<QueueOrder>,
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
           else => break
        }
        // spawn tasks if we can
        'spawn_loop: while queue_actor.has_idle_workers() {
            //eprintln!("Spawning");
            match queue_actor.spawn_task_to_executor() {
                Ok(has_spawned_a_task) => {
                    if !has_spawned_a_task {
                        // no task could be spawned, we can 
                        // stop 
                        break 'spawn_loop;
                    }
                },
                Err(error) => eprintln!("queue actor: {:?}", error)
            }
        }

    }
}

pub fn spawn_queue_actor<K, S>(
    executor_handle: K,
    store: S,
    events: UnboundedReceiver<TaskEvent>,
    orders: UnboundedReceiver<QueueOrder>,
    worker_pool_size: usize,
) where
    K: ExecutorHandle + Send + 'static,
    S: CacheWriter + Send + 'static,
{
    let queue_actor = QueueActor::new(executor_handle, store, worker_pool_size);
    tokio::spawn(async move { manage_queue(queue_actor, events, orders).await });
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
mod queue_actor_tests {
    use crate::models::Status;
    use crate::scheduling::queue_actor::*;

    pub struct ExecutorMockUp {
        pub spawn_tx: UnboundedSender<TaskId>,
        pub kill_tx: UnboundedSender<TaskId>,
    }
    impl ExecutorHandle for ExecutorMockUp {
        fn kill(&mut self, task: TaskId) { 
            if let Err(e) = self.kill_tx.send(task) {
                eprintln!("kill send: {}", e);
            }
        }
        fn spawn(&mut self, task: TaskId) {
            if let Err(e) = self.spawn_tx.send(task) {
                eprintln!("spawn send: {}", e);
            }
        }
    }

    pub struct CacheMockUp {}
    impl CacheWriter for CacheMockUp {
        fn cancel_task(&self, _task: TaskId) {}
        fn cancel_job(&self, _job: JobId) {}
        fn add_job(&self, _job: JobId, _job_status: Status, _tasks: Vec<(TaskId, Status)>) {}
    }

    #[tokio::test]
    async fn test_filling_queue_with_one_task() {
        use tokio::sync::{mpsc::unbounded_channel, oneshot};
        // build executor mock-up
        let (mut spawn_tx, mut spawn_rx) = unbounded_channel::<TaskId>();
        let (mut kill_tx, mut kill_rx) = unbounded_channel::<TaskId>();

        let mut executor_handle = ExecutorMockUp {spawn_tx, kill_tx };

        let store = CacheMockUp {};

        let (_task_sender, task_receiver) = unbounded_channel::<TaskEvent>();
        let (order_sender, order_receiver) = unbounded_channel::<QueueOrder>();

        spawn_queue_actor(
            executor_handle,
            store,
            task_receiver,
            order_receiver,
            10,
        );

        let order = QueueOrder::SubmitTask {
            id: 42,
            group: 0,
            number_of_task_in_group: 1,
            children: None,
            parents: None,
        };
        let _ = order_sender.send(order).expect("failed to send task");
        
        // assert that the first one has been spawned
        assert_eq!(spawn_rx.recv().await, Some(42));
    }

    #[tokio::test]
    async fn test_canceling_tasks() {
        use tokio::sync::{mpsc::unbounded_channel, oneshot};
        use tokio::sync::mpsc::error::TryRecvError;
        // build executor mock-up
        let (spawn_tx, mut spawn_rx) = unbounded_channel::<TaskId>();
        let (kill_tx, mut kill_rx) = unbounded_channel::<TaskId>();
        let mut executor_handle = ExecutorMockUp {spawn_tx, kill_tx };

        let store = CacheMockUp {};

        let (task_sender, task_receiver) = unbounded_channel::<TaskEvent>();
        let (order_sender, order_receiver) = unbounded_channel::<QueueOrder>();

        spawn_queue_actor(
            executor_handle,
            store,
            task_receiver,
            order_receiver,
            10,
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

        // assert that the first one has been spawned
        assert_eq!(spawn_rx.recv().await, Some(1));

        // run the firt one: declare it as started
        let task_event = TaskEvent::TaskStarted(1);
        let _ = task_sender.send(task_event).expect("status update failed");

        // Cancel the job
        let order = QueueOrder::CancelGroup(0);
        let _ = order_sender.send(order).expect("failed to send task");
        
        // assert that the second one has been killed
        assert_eq!(kill_rx.recv().await, Some(1));
        
        // declare the first one as failed (killed)
        let task_event = TaskEvent::TaskFailed(1);
        let _ = task_sender.send(task_event).expect("status update failed");
        assert_eq!(spawn_rx.try_recv(), Err(TryRecvError::Empty));
    }

    #[tokio::test]
    async fn test_task_dependency() {
        use tokio::sync::mpsc::error::TryRecvError;
        // build executor mock-up
        let (spawn_tx, mut spawn_rx) = unbounded_channel::<TaskId>();
        let (kill_tx, mut kill_rx) = unbounded_channel::<TaskId>();
        let mut executor_handle = ExecutorMockUp {spawn_tx, kill_tx };
    
        let store = CacheMockUp {};
        use tokio::sync::{mpsc::unbounded_channel, oneshot};

        let (task_sender, task_receiver) = unbounded_channel::<TaskEvent>();
        let (order_sender, order_receiver) = unbounded_channel::<QueueOrder>();

        spawn_queue_actor(
            executor_handle,
            store,
            task_receiver,
            order_receiver,
            10,
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

        // assert that the first one has been spawned
        assert_eq!(spawn_rx.recv().await, Some(1));

        // run the firt one: declare it as started
        let task_event = TaskEvent::TaskStarted(1);
        let _ = task_sender.send(task_event).expect("status update failed");

        // assert that the first has _NOT_ been spawned
        // because the dependency
        // of the 2 remaining ones are not fulfilled
        assert_eq!(spawn_rx.try_recv(), Err(TryRecvError::Empty));

        // finish the first one
        let task_event = TaskEvent::TaskSucceed(1);
        let _ = task_sender.send(task_event).expect("status update failed");

        // assert that the 2 remainings have been spawned
        assert!(spawn_rx.recv().await.is_some());
        assert!(spawn_rx.recv().await.is_some());
    }

    #[tokio::test]
    async fn test_failure_propagation() {
        use tokio::sync::mpsc::error::TryRecvError;
        let store = CacheMockUp {};
        use tokio::sync::{mpsc::unbounded_channel, oneshot};

        // build executor mock-up
        let (spawn_tx, mut spawn_rx) = unbounded_channel::<TaskId>();
        let (kill_tx, mut kill_rx) = unbounded_channel::<TaskId>();
        let mut executor_handle = ExecutorMockUp {spawn_tx, kill_tx };
    
        let (task_sender, task_receiver) = unbounded_channel::<TaskEvent>();
        let (order_sender, order_receiver) = unbounded_channel::<QueueOrder>();

        spawn_queue_actor(
            executor_handle,
            store,
            task_receiver,
            order_receiver,
            10,
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
        
        // assert that the first one has been spawned
        assert_eq!(spawn_rx.recv().await, Some(1));

        // run the firt one: declare it as started
        let task_event = TaskEvent::TaskStarted(1);
        let _ = task_sender.send(task_event).expect("status update failed");

        // finish the first one
        let task_event = TaskEvent::TaskSucceed(1);
        let _ = task_sender.send(task_event).expect("status update failed");

        // assert that the second one has been spawned
        assert_eq!(spawn_rx.recv().await, Some(2));

        // start the second one
        let task_event = TaskEvent::TaskStarted(2);
        let _ = task_sender.send(task_event).expect("status update failed");

        // mark it as failed
        let task_event = TaskEvent::TaskFailed(2);
        let _ = task_sender.send(task_event).expect("status update failed");

        // assert that the remaining one cannot be claimed,
        // because its dependency (task 2) failed.
        assert_eq!(spawn_rx.try_recv(), Err(TryRecvError::Empty));
    }
}