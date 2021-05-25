use serde::Deserialize;
use quick_xml::de::{from_str, from_reader, DeError};

#[derive(Debug, Deserialize, PartialEq)]
struct Info {
    name: String,
    value: String,
}

#[derive(Debug, Deserialize, PartialEq)]
struct GenericInformation {
    #[serde(rename = "info")]
    infos: Vec<Info>,
}

#[derive(Debug, Deserialize, PartialEq)]
struct Parallel {
    numberOfNodes: usize,
}

#[derive(Debug, Deserialize, PartialEq)]
struct Argument {
    value: String,
}

#[derive(Debug, Deserialize, PartialEq)]
struct Arguments {
    #[serde(rename = "$value")]
    arguments: Vec<Argument>,
}

#[derive(Debug, Deserialize, PartialEq)]
struct StaticCommand {
    value: String,
    arguments: Arguments,
}

#[derive(Debug, Deserialize, PartialEq)]
struct NativeExecutable {
    #[serde(rename = "staticCommand")]
    command: StaticCommand,
}

#[derive(Debug, Deserialize, PartialEq)]
struct TaskRef {
    #[serde(rename = "ref")]
    task_ref: String,
}

#[derive(Debug, Deserialize, PartialEq)]
struct Dependencies {
    #[serde(rename = "task")]
    tasks: Vec<TaskRef>,
}


#[derive(Debug, Deserialize, PartialEq)]
struct Task {
    name: String,
    runAsMe: bool,
    genericInformation: GenericInformation,
    dependencies: Option<Dependencies>,
    parallel: Option<Parallel>,
    #[serde(rename = "nativeExecutable")]
    executable: NativeExecutable,

}

#[derive(Debug, Deserialize, PartialEq)]
struct TaskFlow {
    #[serde(rename = "task")]
    tasks: Vec<Task>,
}

#[derive(Debug, Deserialize, PartialEq)]
struct Job {
    name: String,
    #[serde(rename = "taskFlow")]
    task_flow: TaskFlow,
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

    let job: Job = from_reader(JOB_STR.as_bytes()).unwrap();
    assert_eq!(job.name, String::from("job-name"));
    assert_eq!(job.task_flow.tasks[0].name, String::from("A"));
    eprintln!("{:#?}", job);
}
