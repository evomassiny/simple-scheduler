use serde::Deserialize;
use quick_xml::de::{from_str, from_reader, DeError};

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
    pub dependencies: Option<Dependencies>,
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
    eprintln!("{:#?}", job);
}
