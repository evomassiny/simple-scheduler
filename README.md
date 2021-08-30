# About

This is a job hypervisor,
mimicking a small subset of the "proactive-scheduler".


# Install
```
cargo install sqlx-cli
sqlx database create
sqlx migrate run 

cargo build release
```

# Internals
[executor](/doc/executor.md)
