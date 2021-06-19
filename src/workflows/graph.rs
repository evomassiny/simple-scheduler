use crate::workflows::parser::{
    Info,
    GenericInformation,
    Parallel,
    Argument,
    Arguments,
    StaticCommand,
    NativeExecutable,
    TaskRef,
    Dependencies,
    Task,
    TaskFlow,
    Job,
    CLUSTER_ATTRIBUTE_NAME,
};

use std::collections::HashMap;

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
    tasks: Vec<WorkFlowTask>,
    /// indices of Task instances in `tasks`
    dependency_indices: Vec<Vec<usize>>,
}

impl WorkFlowGraph {


    pub fn from_str(workflow: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let job: Job = Job::from_str(workflow)?;

        let mut dependency_indices: Vec<Vec<usize>> = Vec::new();
        let mut tasks: Vec<WorkFlowTask> = Vec::new();

        // first pass to collect the task names
        let mut name_to_idx: HashMap<String, usize> = HashMap::new();
        for (i, task) in job.task_flow.tasks.iter().enumerate() {
            name_to_idx.insert(task.name.clone(), i);
        }

        // Creates tasks
        for (i, task) in job.task_flow.tasks.iter().enumerate() {
            // collect dependencies
            let mut dependencies: Vec<usize> = Vec::new();
            if let Some(deps) = &task.dependencies {
                for dep in &deps.tasks {
                    if let Some(task_idx) = name_to_idx.get(&dep.task_ref) {
                        dependencies.push(*task_idx);
                    }
                }
            }
            dependency_indices.push(dependencies);
            
            // collect node_count
            let mut node_count = 0;
            if let Some(parallel_info) = &task.parallel {
                node_count = parallel_info.numberOfNodes;
            }
            // collect cluster_name
            let mut cluster_name: Option<String> = None;
            for info in &task.genericInformation.infos {
                if info.name == CLUSTER_ATTRIBUTE_NAME {
                    cluster_name = Some(info.value.clone());
                }
            }
            // collect executable
            let executable = task.executable.command.value.clone();
            let arguments = task.executable
                .command
                .arguments
                .arguments
                .iter().map(|a| a.value.clone())
                .collect::<Vec<String>>();
            
            tasks.push(
                WorkFlowTask {
                    name: task.name.clone(),
                    cluster_name: cluster_name,
                    node_count: node_count,
                    executable: executable,
                    executable_arguments: arguments,
                }
            )
        }

        Ok(Self {
            name: job.name,
            tasks: tasks,
            dependency_indices: dependency_indices,
        })
    }
}

#[test]
fn test_deserialization() {
    const JOB_STR: &str = r#"<?xml version="1.0"?>
<job name="job-name">
  <taskFlow>
    <task name="A" runAsMe="true">
      <genericInformation>
        <info name="NODE_ACCESS_TOKEN" value="cluster_name_1"/>
      </genericInformation>
      <parallel numberOfNodes="20">
        <topology>
          <bestProximity/>
        </topology>
      </parallel>
      <nativeExecutable>
        <staticCommand value="executable_name">
          <arguments>
            <argument value="argument_1"/>
            <argument value="argument_2"/>
          </arguments>
        </staticCommand>
      </nativeExecutable>
    </task>
  </taskFlow>
</job>"#;

    let taskflow = WorkFlowGraph::from_str(JOB_STR).unwrap();
    assert_eq!(taskflow.name, String::from("job-name"));
    assert_eq!(taskflow.tasks[0].name, String::from("A"));
    assert_eq!(taskflow.tasks[0].cluster_name, Some("cluster_name_1".into()));
    assert_eq!(taskflow.tasks[0].node_count, 20);
    assert_eq!(taskflow.tasks[0].executable, String::from("executable_name"));
    assert_eq!(taskflow.tasks[0].executable_arguments[0], String::from("argument_1"));
    assert_eq!(taskflow.tasks[0].executable_arguments[1], String::from("argument_2"));
}
