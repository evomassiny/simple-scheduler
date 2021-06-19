use std::collections::HashMap;

#[derive(Debug)]
pub enum WorkflowError {
    TaskDoesNotExist,
    DependencyCycle,
}

#[derive(Debug)]
pub struct WorkFlowTask {
    pub name: String,
    /// executor name
    pub cluster_name: Option<String>,
    /// CPU count
    pub node_count: usize,
    pub executable: String,
    pub executable_arguments: Vec<String>,
}

#[derive(Debug)]
pub struct WorkFlowGraph {
    pub name: String,
    pub tasks: Vec<WorkFlowTask>,
    /// indices of Task instances in `tasks`
    pub (crate) dependency_indices: Vec<Vec<usize>>,
    /// index of each task (referenced by names) in `self.tasks`
    pub (crate) name_to_idx: HashMap<String, usize>,
}

impl WorkFlowGraph {

    /// returns references to the dependencies of `self`
    pub fn get_task_dependencies<'a>(&'a self, task_name: &str) -> Result<Vec<&'a WorkFlowTask>, WorkflowError> {
        let task_idx: usize = *self.name_to_idx
            .get(task_name)
            .ok_or(WorkflowError::TaskDoesNotExist)?;
        let dependency_indices = self.dependency_indices
            .get(task_idx)
            .ok_or(WorkflowError::TaskDoesNotExist)?;
        let mut tasks: Vec<&WorkFlowTask> = Vec::new();
        for dep_idx in dependency_indices {
            tasks.push(&self.tasks[*dep_idx]);
        }
        Ok(tasks)
    }

}
