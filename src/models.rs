use chrono::NaiveDateTime;

#[derive(Queryable)]
pub struct Job {
    pub id: i32,
    pub name: String,
    pub submit_time: NaiveDateTime,
    pub status: u8,
}

#[derive(Queryable)]
pub struct Process {
    pub id: i32,
    pub pid: i32,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Queryable)]
pub struct Task {
    pub id: i32,
    pub name: String,
    pub status: u8,
    pub command: String,
    pub process: i32,
    pub job: i32,
}

#[derive(Queryable)]
pub struct TaskDependency {
    pub id: i32,
    pub child: i32,
    pub parent: i32,
}
