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
0. assert that sqlite is installed:
```shell
sudo apt install libsqlite3-dev sqlite3
```

1. create empty database
```
cargo install sqlx-cli
cd backend
sqlx database create --database-url sqlite:database.sqlite
sqlx migrate run --database-url sqlite:database.sqlite
```

2. create a temporary job directory (for temporary artifacts)
```
mkdir /opt/simple-scheduler/process-dirs
```

3. generate key pairs
```
utils/gen_key_pair.sh
mv keys /opt/simple-scheduler/simple-scheduler/keys
```

4. create the app configuration file (use `openssl rand -base64 32` to generate your own secret key):
```bash
cat << EOF > /opt/simple-scheduler/simple-scheduler/backend/Rocket.toml
[default]
address = "0.0.0.0"
limits = { forms = "64 kB", json = "100 MiB" }
port = 8000

[release]
secret_key = "SECRET_KEY"
database_url = "sqlite:database-prod.sqlite" 
process_directory = "/opt/simple-scheduler/process-dirs"
hypervisor_socket_path = "/tmp/simple-scheduler.sock"
public_key_path = "/opt/simple-scheduler/simple-scheduler/keys/pub.key"
private_key_path = "/opt/simple-scheduler/simple-scheduler/keys/priv.key"
nb_of_workers = 10
EOF
```

5. Add a user with login: "debug-user", pass: "debug-password"
```
cargo build --release
target/release/simple-scheduler create-user debug-user debug-password
```

6. Create a systemd unit file to run the scheduler
```
sudo su
cat << EOF > /usr/lib/systemd/system/simple-scheduler.service
[Unit]
Description=Simple Scheduler
After=network.target

[Service]
User=${USER}
Environment=ROCKET_ENV=release

WorkingDirectory=/opt/simple-scheduler/simple-scheduler/backend/
ExecStart=/opt/simple-scheduler/simple-scheduler/backend/target/release/simple-scheduler run-server
Type=simple
PIDFile=/opt/simple-scheduler/simple-scheduler.pid

[Install]
WantedBy=default.target
EOF
```

7. enable and start it:
```bash
sudo systemctl enable simple-scheduler.service
sudo systemctl start simple-scheduler.service
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
