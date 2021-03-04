#![feature(proc_macro_hygiene, decl_macro)]
#[macro_use] extern crate rocket;
#[macro_use] extern crate rocket_contrib;
#[macro_use] extern crate diesel;
extern crate chrono;
pub mod schema;
pub mod models;

#[database("tasks_db")]
struct TaskDbConn(rocket_contrib::databases::diesel::SqliteConnection);


#[get("/")]
fn index() -> &'static str {
    "Hello, world!"
}


fn main() {
    rocket::ignite()
        .attach(TaskDbConn::fairing())
        .mount("/", routes![index])
        .launch();
}
