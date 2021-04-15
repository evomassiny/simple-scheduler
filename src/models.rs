use chrono::NaiveDateTime;

#[derive(Debug)]
pub struct Job {
    pub id: i32,
    pub name: String,
    pub submit_time: NaiveDateTime,
    pub status: u8,
}

#[derive(Debug)]
pub struct Task {
    pub id: i32,
    pub name: String,
    pub status: u8,
    pub command: String,
    pub job: i32,
}

#[derive(Debug)]
pub struct TaskDependency {
    pub id: i32,
    pub child: i32,
    pub parent: i32,
}
