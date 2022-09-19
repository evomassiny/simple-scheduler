use crate::models::{
    Job, JobId, ModelError, NewJob, NewTask, NewTaskDep, Status, Task, TaskCommandArgs, TaskDepId,
    TaskDependency, TaskId, UserId,
};
use crate::tasks::TaskHandle;
use crate::workflows::WorkFlowGraph;
use sqlx::sqlite::SqliteConnection;
use sqlx::Connection;


/// A `Batch` is a graph of task to be executed.
/// It is a composition of 3 kinds of models:
/// * one `crate::models::Job`: metadata about the whole Batch
/// * a collections of `crate::model::Task`s, (commands to be executed)
/// * a collections of `crate::model::TaskDependency`s, which define the ordering
/// constraints of the whole batch execution.
#[derive(Debug)]
pub struct Batch {
    pub job: Job<JobId>,
    /// One task == one bash command to execute == one node in the execution graph
    pub tasks: Vec<Task<TaskId>>,
    /// execution graph edge
    pub dependencies: Vec<TaskDependency<TaskDepId>>,
}

impl Batch {
    /// Create a graph (job + tasks + dependencies) in one DB transaction.
    pub async fn from_graph(
        workflow: &WorkFlowGraph,
        user: UserId,
        conn: &mut SqliteConnection,
    ) -> Result<Self, ModelError> {
        // validate input
        if !workflow.are_task_names_unique() {
            return Err(ModelError::InvalidTaskName);
        }
        if !workflow.is_cycle_free() {
            return Err(ModelError::DependencyCycle);
        }

        let mut transaction = conn
            .begin()
            .await
            .map_err(|e| ModelError::DbError(e.to_string()))?;
        // Create and save Job
        let job = Job::<NewJob>::new(&workflow.name, user)
            .save(&mut transaction)
            .await?;

        // create and save all tasks
        let mut tasks: Vec<Task<TaskId>> = Vec::new();
        for graph_task in &workflow.tasks {
            let mut task = Task {
                id: NewTask,
                name: graph_task.name.clone(),
                status: Status::Pending,
                handle: "".to_string(),
                job: job.id,
                stdout: None,
                stderr: None,
            }
            .save(&mut transaction)
            .await?;

            // the handle string depends of the task ID,
            // so we must set it in a second step.
            let handle = TaskHandle::from_task_id(task.id).handle_string();
            task.handle = handle;
            task.save(&mut transaction).await?;

            // save each of their individual command line args
            let command_args = TaskCommandArgs::from_strings(graph_task.commands(), task.id);
            for cmd_arg in command_args {
                cmd_arg.save(&mut transaction).await?;
            }
            tasks.push(task);
        }

        // create and save TaskDependency
        let mut dependencies: Vec<TaskDependency<TaskDepId>> = Vec::new();
        for (task_idx, deps_ids) in workflow.dependency_indices.iter().enumerate() {
            let child: TaskId = tasks[task_idx].id;
            for dep_idx in deps_ids {
                let parent: TaskId = tasks[*dep_idx].id;
                let dependency = TaskDependency {
                    id: NewTaskDep,
                    child,
                    parent,
                    job: job.id,
                }
                .save(&mut transaction)
                .await?;
                dependencies.push(dependency);
            }
        }
        let _ = transaction
            .commit()
            .await
            .map_err(|e| ModelError::DbError(e.to_string()))?;
        Ok(Self {
            job,
            tasks,
            dependencies,
        })
    }

    pub async fn from_job(
        job: Job<JobId>,
        conn: &mut SqliteConnection,
    ) -> Result<Self, ModelError> {
        // select tasks by job id
        let tasks = Task::select_by_job(job.id, conn).await?;
        let dependencies = TaskDependency::select_by_job(job.id, conn).await?;
        Ok(Self {
            job,
            tasks,
            dependencies,
        })
    }
}
