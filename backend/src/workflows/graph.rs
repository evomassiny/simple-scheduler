use std::collections::{HashMap, HashSet};

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

impl WorkFlowTask {
    /// Format executable and executable argument into a list
    /// of shell command arguments.
    pub fn commands(&self) -> Vec<String> {
        let mut cmds = vec![self.executable.clone()];
        for arg in &self.executable_arguments {
            cmds.push(arg.clone())
        }
        cmds
    }
}

/// Represents a graph of `WorkFlowTask`,
/// can be any kind of graph,
/// but only cycle-free digraph are considered
/// valid for the rest of the application.
///
/// This struct can validate such graphs.
///
/// NOTE: graph nodes (i.e tasks) are stored in a flat array,
/// edges are stored in an other array.
#[derive(Debug)]
pub struct WorkFlowGraph {
    pub name: String,
    pub tasks: Vec<WorkFlowTask>,
    /// Edges in the task graph:
    ///  * each elements is a list of edge starting from `self.tasks[element_index]`.
    ///  * each elements in that list is the task index (in self.tasks) where this edge is going
    ///  to.
    pub(crate) dependency_indices: Vec<Vec<usize>>,
    /// index of each task (referenced by names) in `self.tasks`
    pub(crate) name_to_idx: HashMap<String, usize>,
}

impl WorkFlowGraph {
    /// returns references to the dependencies of `self`
    #[allow(dead_code)]
    pub fn get_task_dependencies<'a>(
        &'a self,
        task_name: &str,
    ) -> Result<Vec<&'a WorkFlowTask>, WorkflowError> {
        let task_idx: usize = *self
            .name_to_idx
            .get(task_name)
            .ok_or(WorkflowError::TaskDoesNotExist)?;
        let dependency_indices = self
            .dependency_indices
            .get(task_idx)
            .ok_or(WorkflowError::TaskDoesNotExist)?;
        let mut tasks: Vec<&WorkFlowTask> = Vec::new();
        for dep_idx in dependency_indices {
            tasks.push(&self.tasks[*dep_idx]);
        }
        Ok(tasks)
    }

    /// Check for cycle in the dependency graph,
    /// of the `self.tasks[task_idx]` task.
    fn is_task_dependency_cycle_free(&self, task_idx: usize) -> Result<(), WorkflowError> {
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
            if let Err(WorkflowError::DependencyCycle) =
                self.is_task_dependency_cycle_free(task_idx)
            {
                return false;
            }
        }
        true
    }

    /// Check for name duplicates in task
    pub fn are_task_names_unique(&self) -> bool {
        let mut names = HashSet::new();
        for task in &self.tasks {
            if names.contains(&task.name) {
                return false;
            }
            names.insert(task.name.clone());
        }
        true
    }
}

#[test]
fn test_cycle_detection() {
    use std::str::FromStr;
    let di_graph = WorkFlowGraph::from_str(include_str!("../../test-data/workflow.xml")).unwrap();
    assert_eq!(di_graph.is_cycle_free(), true);

    let cycle_graph =
        WorkFlowGraph::from_str(include_str!("../../test-data/workflow_with_cycle.xml")).unwrap();
    assert_eq!(cycle_graph.is_cycle_free(), false);
}

#[test]
fn test_name_uniqueness() {
    use std::str::FromStr;
    let di_graph = WorkFlowGraph::from_str(include_str!("../../test-data/workflow.xml")).unwrap();
    assert_eq!(di_graph.are_task_names_unique(), true);

    let invalid_graph = WorkFlowGraph::from_str(include_str!(
        "../../test-data/workflow_with_duplicated_task_names.xml"
    ))
    .unwrap();
    assert_eq!(invalid_graph.are_task_names_unique(), false);
}
