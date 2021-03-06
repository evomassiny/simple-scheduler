#![feature(proc_macro_hygiene, decl_macro)]
#[macro_use] extern crate rocket;
#[macro_use] extern crate rocket_contrib;
#[macro_use] extern crate diesel;
extern crate chrono;
extern crate tempfile;
extern crate nix;
pub mod schema;
pub mod models;
pub mod executor;

#[database("tasks_db")]
struct TaskDbConn(rocket_contrib::databases::diesel::SqliteConnection);


#[get("/")]
fn index() -> &'static str {
    "Hello, world!"
}


fn main() {
    let e = models::Process::spawn("sleep 60 && echo yes");
    dbg!(&e);
    rocket::ignite()
        //.attach(TaskDbConn::fairing())
        .mount("/", routes![index])
        .launch();
}
