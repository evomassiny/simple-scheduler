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
};

use std::collections::HashMap;

#[derive(Debug)]
pub struct WorkFlowTask {
    pub name: String,
    /// executor name
    pub cluster_name: String,
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

        let mut name_to_idx: HashMap<String, usize> = HashMap::new();
        let mut dependency_indices: Vec<Vec<usize>> = Vec::new();
        let mut tasks: Vec<WorkFlowTask> = Vec::new();

        for task in job.task_flow.tasks {
            unimplemented!();
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
    //assert_eq!(job.task_flow.tasks[0].name, String::from("A"));
    eprintln!("{:#?}", taskflow);
}
