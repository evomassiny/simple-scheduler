use crate::models::{
    Existing, Job, Model, ModelError, Status, Task, TaskCommandArgs, TaskDependency, User,
};
use crate::workflows::WorkFlowGraph;
use sqlx::sqlite::SqliteConnection;
use std::collections::HashMap;

/// A `Batch` is a graph of task to be executed.
/// It is a composition of 3 kinds of models:
/// * one `crate::models::Job`: metadata about the whole Batch
/// * a collections of `crate::model::Task`s, (commands to be executed)
/// * a collections of `crate::model::TaskDependency`s, which define the ordering
/// constraints of the whole batch execution.
#[derive(Debug)]
pub struct Batch {
    pub job: Job,
    /// One task == one bash command to execute == one node in the execution graph
    pub tasks: Vec<Task>,
    /// execution graph edge
    pub dependencies: Vec<TaskDependency>,
}

impl Batch {
    pub async fn from_graph(
        workflow: &WorkFlowGraph,
        user: &User<Existing>,
        conn: &mut SqliteConnection,
    ) -> Result<Self, ModelError> {
        // validate input
        if !workflow.are_task_names_unique() {
            return Err(ModelError::InvalidTaskName);
        }
        if !workflow.is_cycle_free() {
            return Err(ModelError::DependencyCycle);
        }

        // Create and save Job
        let mut job = Job::new(&workflow.name, user);
        let _ = job.save(conn).await?;
        let job_id: i64 = job.id.ok_or(ModelError::ModelNotFound)?;

        // create and save all tasks
        let mut tasks: Vec<Task> = Vec::new();
        for graph_task in &workflow.tasks {
            let mut task = Task {
                id: None,
                name: graph_task.name.clone(),
                status: Status::Pending,
                last_update_version: None,
                handle: "".to_string(),
                command_args: TaskCommandArgs::from_strings(graph_task.commands()),
                job: job_id,
                stdout: None,
                stderr: None,
            };
            let _ = task.save(conn).await?;
            tasks.push(task);
        }

        // create and save TaskDependency
        let mut dependencies: Vec<TaskDependency> = Vec::new();
        for (task_idx, deps_ids) in workflow.dependency_indices.iter().enumerate() {
            let child: i64 = tasks[task_idx].id().ok_or(ModelError::ModelNotFound)?;
            for dep_idx in deps_ids {
                let parent: i64 = tasks[*dep_idx].id().ok_or(ModelError::ModelNotFound)?;
                let mut dependency = TaskDependency {
                    id: None,
                    child,
                    parent,
                    job: job_id,
                };
                let _ = dependency.save(conn).await?;
                dependencies.push(dependency);
            }
        }
        Ok(Self {
            job,
            tasks,
            dependencies,
        })
    }

    pub async fn from_job(job: Job, conn: &mut SqliteConnection) -> Result<Self, ModelError> {
        // select tasks by job id
        let tasks = Task::select_by_job(job.id.ok_or(ModelError::ModelNotFound)?, conn).await?;
        let dependencies =
            TaskDependency::select_by_job(job.id.ok_or(ModelError::ModelNotFound)?, conn).await?;
        Ok(Self {
            job,
            tasks,
            dependencies,
        })
    }

    pub async fn next_ready_task<'a, 'b>(&'a mut self) -> Result<Option<&'b mut Task>, ModelError>
    where
        'a: 'b,
    {
        // build task index
        let mut task_index: HashMap<i64, usize> = HashMap::new();
        for (idx, task) in self.tasks.iter().enumerate() {
            task_index.insert(task.id.ok_or(ModelError::ModelNotFound)?, idx);
        }
        // build task dependencies index
        let mut dependencies_index: HashMap<i64, Vec<i64>> = HashMap::new();
        for dependency in self.dependencies.iter() {
            match dependencies_index.get_mut(&dependency.child) {
                // append to existing vec
                Some(deps) => deps.push(dependency.parent),
                // insert new vec
                None => {
                    let _ = dependencies_index.insert(dependency.child, vec![dependency.parent]);
                }
            }
        }
        let mut ready_idx: Option<usize> = None;
        // find task with fulfilled dependencies
        'ready_task_lookup: for (idx, task) in self.tasks.iter().enumerate() {
            // ignore running, of finished tasks
            if !task.status.is_pending() {
                continue 'ready_task_lookup;
            }
            let task_id = task.id.ok_or(ModelError::InvalidTaskId)?;
            // iter dependencies
            if let Some(parent_ids) = dependencies_index.get(&task_id) {
                for parent_id in parent_ids {
                    let parent_idx = task_index.get(parent_id).ok_or(ModelError::InvalidTaskId)?;
                    let parent = &self.tasks[*parent_idx];
                    /* TODO handle failed states */
                    if !parent.status.is_finished() {
                        continue 'ready_task_lookup;
                    }
                }
            }
            ready_idx = Some(idx);
            break;
        }
        match ready_idx {
            Some(idx) => Ok(self.tasks.get_mut(idx)),
            None => Ok(None),
        }
    }
}
