mod batches;
mod common;
mod jobs;
mod status;
mod tasks;
mod users;

pub use crate::models::batches::Batch;
pub use crate::models::common::ModelError;
pub use crate::models::jobs::{Job, JobId, NewJob};
pub use crate::models::status::Status;
pub use crate::models::tasks::{
    NewTask, NewTaskDep, Task, TaskCommandArgs, TaskDepId, TaskDependency, TaskId,
};
pub use crate::models::users::{create_or_update_user, NewUser, User, UserId};

#[cfg(test)]
mod test {
    use crate::models::{create_or_update_user, Batch};
    use crate::workflows::{WorkFlowGraph, WorkFlowTask};
    use rocket::tokio;
    use sqlx::{Connection, Executor, SqliteConnection};
    use std::collections::HashMap;

    /// create an in-memory database, and execute the initial migration.
    async fn setup_in_memory_database() -> Result<SqliteConnection, Box<dyn std::error::Error>> {
        let mut conn = SqliteConnection::connect("sqlite::memory:").await?;
        conn.execute(include_str!("../../migrations/20210402155322_creation.sql"))
            .await?;
        Ok(conn)
    }

    /// build a test graph With 2 tasks A and B, with B depending on A
    fn build_test_workflow_graph() -> WorkFlowGraph {
        WorkFlowGraph {
            name: "test-job".to_string(),
            tasks: vec![
                WorkFlowTask {
                    name: "A".to_string(),
                    cluster_name: None,
                    node_count: 1,
                    executable: "touch".to_string(),
                    executable_arguments: vec!["/tmp/tmp-empty-file".to_string()],
                },
                WorkFlowTask {
                    name: "B".to_string(),
                    cluster_name: None,
                    node_count: 1,
                    executable: "rm".to_string(),
                    executable_arguments: vec!["/tmp/tmp-empty-file".to_string()],
                },
            ],
            dependency_indices: vec![vec![], vec![0]],
            name_to_idx: {
                let mut name_to_idx: HashMap<String, usize> = HashMap::new();
                name_to_idx.insert("A".to_string(), 0);
                name_to_idx.insert("B".to_string(), 1);
                name_to_idx
            },
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_graph_persistence() {
        // build DB
        let mut conn = setup_in_memory_database().await.unwrap();
        let user = create_or_update_user("user-name", "covfefe", &mut conn)
            .await
            .unwrap();
        // build graph
        let graph = build_test_workflow_graph();
        assert!(graph.is_cycle_free());
        assert!(graph.are_task_names_unique());

        // test batch creation
        let batch = Batch::from_graph(&graph, user.id, &mut conn)
            .await
            .expect("Failed to build Batch");
        assert_eq!(&batch.job.name, "test-job");

        // test (loosely) the model creation in the database
        let (job_name,): (String,) = sqlx::query_as("SELECT name FROM jobs")
            .fetch_one(&mut conn)
            .await
            .expect("failed to count tasks");
        assert_eq!(&job_name, &batch.job.name);

        let (task_count,): (i64,) = sqlx::query_as("SELECT count(id) FROM tasks")
            .fetch_one(&mut conn)
            .await
            .expect("failed to count tasks");
        assert_eq!(task_count, 2);

        let (dep_count,): (i64,) = sqlx::query_as("SELECT count(id) FROM task_dependencies")
            .fetch_one(&mut conn)
            .await
            .expect("failed to count tasks");
        assert_eq!(dep_count, 1);
    }
}
