#![allow(non_snake_case)]
use serde::Deserialize;
use quick_xml::de::from_reader;
use crate::workflows::graph::{
    WorkFlowGraph,WorkFlowTask
};
use std::collections::HashMap;

pub const CLUSTER_ATTRIBUTE_NAME: &str = "NODE_ACCESS_TOKEN";

/**
 * Those structs are deserialized by `quick_xml`,
 * this is why they closely match the format/structure of a Workflow XML.
 *
 * Those types should only be used to create `crate::workflows::graph::WorkFlowGraph`s
 */


#[derive(Debug, Deserialize, PartialEq)]
pub struct Info {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct GenericInformation {
    #[serde(rename = "info")]
    pub infos: Vec<Info>,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct Parallel {
    pub numberOfNodes: usize,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct Argument {
    pub value: String,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct Arguments {
    #[serde(rename = "$value")]
    pub arguments: Vec<Argument>,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct StaticCommand {
    pub value: String,
    pub arguments: Arguments,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct NativeExecutable {
    #[serde(rename = "staticCommand")]
    pub command: StaticCommand,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct TaskRef {
    #[serde(rename = "ref")]
    pub task_ref: String,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct Dependencies {
    #[serde(rename = "task")]
    pub tasks: Vec<TaskRef>,
}


#[derive(Debug, Deserialize, PartialEq)]
pub struct Task {
    pub name: String,
    pub runAsMe: bool,
    pub genericInformation: GenericInformation,
    pub depends: Option<Dependencies>,
    pub parallel: Option<Parallel>,
    #[serde(rename = "nativeExecutable")]
    pub executable: NativeExecutable,

}

#[derive(Debug, Deserialize, PartialEq)]
pub struct TaskFlow {
    #[serde(rename = "task")]
    pub tasks: Vec<Task>,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct Job {
    pub name: String,
    #[serde(rename = "taskFlow")]
    pub task_flow: TaskFlow,
}

impl Job {
    pub fn from_str(job_str: &str) -> Result<Self, String>  {
        from_reader(job_str.as_bytes()).map_err(|e| format!("Error while parsing job: {:?}", e))
    }
}


impl WorkFlowGraph {


    /// Build a WorkFlowTask from an XML string representation.
    /// NOTE: 
    /// does not check for dependency cycles.
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
        for task in job.task_flow.tasks.iter() {
            // collect dependencies
            let mut dependencies: Vec<usize> = Vec::new();
            if let Some(deps) = &task.depends {
                for dep in &deps.tasks {
                    match name_to_idx.get(&dep.task_ref) {
                        Some(task_idx) => dependencies.push(*task_idx),
                        None => return Err(format!("Bad task_ref: '{}'", &dep.task_ref).into()),
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
            name_to_idx: name_to_idx,
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

    let job: Job = Job::from_str(JOB_STR).unwrap();
    assert_eq!(job.name, String::from("job-name"));
    assert_eq!(job.task_flow.tasks[0].name, String::from("A"));
}

#[test]
fn test_single_task_parsing() {
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

#[test]
fn test_several_tasks_parsing() {
    const JOB_STR: &str = include_str!("../../test-data/workflow.xml");
    let taskflow = WorkFlowGraph::from_str(JOB_STR).unwrap();
    let C_deps: Vec<&WorkFlowTask> = taskflow.get_task_dependencies("C").unwrap();
    // check dependencies
    assert_eq!(C_deps[0].name, String::from("A"));
}

