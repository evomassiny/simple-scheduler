
table! {
    jobs (id) {
        id -> Integer,
        name -> Text,
        submit_time -> Timestamp,
        status -> Integer,
    }
}

table! {
    processes (id) {
        id -> Integer,
        pid -> Nullable<Integer>,
        stdout -> Nullable<Text>,
        stderr -> Nullable<Text>,
    }
}

table! {
    task_dependencies (id) {
        id -> Integer,
        child -> Nullable<Integer>,
        parent -> Nullable<Integer>,
    }
}

table! {
    tasks (id) {
        id -> Integer,
        name -> Text,
        status -> Integer,
        command -> Text,
        process -> Nullable<Integer>,
        job -> Nullable<Integer>,
    }
}

joinable!(tasks -> jobs (job));
joinable!(tasks -> processes (process));

allow_tables_to_appear_in_same_query!(
    jobs,
    processes,
    task_dependencies,
    tasks,
);
