pub struct WorkFlowTask {
    pub name: String,
    /// executor name
    pub cluster_name: String,
    /// CPU count
    pub node_count: usize,
    pub executable: String,
    pub executable_arguments: Vec<String>,
    /// indices of Task instances in `WorkflowGraph.tasks`
    dependency_indices: Vec<usize>,
}

pub struct WorkFlowGraph {
    pub name: String,
    tasks: Vec<WorkFlowTask>,
}
