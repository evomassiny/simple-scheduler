# About

This is a job hypervisor,
mimicking a really small subset of the ["proactive-scheduler"](https://www.activeeon.com/products/workflows-scheduling/).

This app is an hypervisor, able to spawn jobs according to 
an user-provided execution plans.

Concretly it's a web app providing a REST API to spawn a bunch of bash command on the web-server.

The app acts as an hypervisor, and is able to schedule the jobs' execution order according to a dependency
graph provided by the user (in XML).

The app also keeps track of the status of each job, and is able to retreive their stdout/stderr output.


# Install
```
cargo install sqlx-cli
cd backend
sqlx database create
sqlx migrate run 

cargo build release
```

# Internals
* this app only support Linux,
* the main web server is an async rust rocket server,
* the app also spawn a dedicated hypervisor service (communication is done through named unix streams),
* every job is run under its own process, and is monitored through a dedicated monitor process (see [executor doc](/doc/executor.md))

## use of "unsafe"
The executor part of this app use a lot of libc function:
* fork process
* handle signals/sigaction
* dup stderr/stdout to files
* create inter processus semaphores
* write into argv[0]

When available, most of this is done through the nix abstractions, otherwise directly through the libc. Both use unsafe APIs.
