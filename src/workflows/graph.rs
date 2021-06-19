use std::collections::{HashSet,HashMap};

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

    fn is_task_dependancy_cycle_free(&self, task_idx: usize) -> Result<(), WorkflowError> {
        if task_idx >= self.tasks.len() {
            return Err(WorkflowError::TaskDoesNotExist);
        }

        // one Vec per branch depth in digraph
        let mut queues: Vec<Vec<usize>> = vec![vec![task_idx]];
        // keeps track of every node traversed, 
        // if we traverse a node twice, we have have a cycle.
        let mut cursor_in_queues: Vec<usize> = vec![0];

        // tarverse the dependency 
        while !queues.is_empty() {
            // fetch working node
            let mut level = queues.len() - 1;
            let node_idx_in_level: usize = cursor_in_queues[level];
            let node = queues[level][node_idx_in_level];

            // if we traverse a node twice, it means there is a cycle
            for level_idx in 0..level {
                let traversed_node_idx_in_queue = cursor_in_queues[level_idx];
                let traversed_node = queues[level_idx][traversed_node_idx_in_queue];
                if node == traversed_node {
                    return Err(WorkflowError::DependencyCycle);
                }
            }

            // add dependencies, if any
            let node_deps = &self.dependency_indices[node];
            if !node_deps.is_empty() {
                queues.push(node_deps.clone());
                cursor_in_queues.push(0);
                continue;
            } 
            
            // If no dependencies; set current node as "parsed" (eg update cursors),
            // backtrack if current node was the last one of level.
            'cursor_update: while !queues.is_empty() {
                level = queues.len() - 1;
                cursor_in_queues[level] += 1;
                let neighbour_count = queues[level].len(); // nb of node at the same level as `node`
                // if there is still nodes to parse in level
                if cursor_in_queues[level] != neighbour_count {
                    break 'cursor_update;
                }
                let _ = queues.pop();
                let _ = cursor_in_queues.pop();
            }
        }
        Ok(())
    }

    /// Check for cycle in task dependencies
    pub fn is_cycle_free(&self) -> bool {
        // NOTE: this is suboptimal: we might check several time the cycle starting
        // from a task, if this task is in the dependency tree of several other tasks.
        for task_idx in 0..self.tasks.len() {
            if let Err(WorkflowError::DependencyCycle) = self.is_task_dependancy_cycle_free(task_idx) {
                return false;
            }
        }
        true
    }

}

#[test]
fn test_cycle_detection() {
    let di_graph = WorkFlowGraph::from_str(
        include_str!("../../test-data/workflow.xml")
    ).unwrap();
    assert_eq!(di_graph.is_cycle_free(), true);

    let cycle_graph = WorkFlowGraph::from_str(
        include_str!("../../test-data/workflow_with_cycle.xml")
    ).unwrap();
    assert_eq!(cycle_graph.is_cycle_free(), false);

}
