mod graph;
mod parser;

mod parser_test {
    const JOB: &str = r#"<?xml version="1.0"?>
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
    <task name="B" runAsMe="true">
      <genericInformation>
        <info name="NODE_ACCESS_TOKEN" value="cluster_name_2"/>
      </genericInformation>
      <nativeExecutable>
        <staticCommand value="executable_name">
          <arguments>
            <argument value="argument_1"/>
            <argument value="argument_2"/>
          </arguments>
        </staticCommand>
      </nativeExecutable>
    </task>
    <task name="C" runAsMe="true">
      <genericInformation>
        <info name="NODE_ACCESS_TOKEN" value="cluster_name_2"/>
      </genericInformation>
      <depends>
        <task ref="A"/>
      </depends>
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

}
