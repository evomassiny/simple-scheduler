# About

This is a job hypervisor,
mimicking a really small subset of the ["proactive-scheduler"](https://www.activeeon.com/products/workflows-scheduling/).

This app is an hypervisor, able to spawn jobs according to 
an user-provided execution plan.

Concretly it's a web app providing a REST API to spawn a bunch of bash commands on a web-server.

The app acts as an hypervisor, and is able to schedule jobs' execution order according to a dependency
graph provided by the user (in XML).

The app also keeps track of the status of each job, and is able to retreive their stdout/stderr output.


# Install
0. assert that sqlite and rustup are installed:
```shell
# sqlite
sudo apt install libsqlite3-dev sqlite3
# rustup
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

1. clone the repo
```
mkdir -p /opt/simple-scheduler
cd /opt/simple-scheduler
git clone https://github.com/evomassiny/simple-scheduler.git
```

2. create empty database
```
cargo install sqlx-cli
cd simple-scheduler/backend
sqlx database create --database-url sqlite:database.sqlite
sqlx migrate run --database-url sqlite:database.sqlite
```

3. create a temporary job directory (for temporary artifacts)
```
mkdir /opt/simple-scheduler/process-dirs
```

4. generate key pairs
```
utils/gen_key_pair.sh
mv keys /opt/simple-scheduler/simple-scheduler/keys
```

5. create the app configuration file (use `openssl rand -base64 32` to generate your own secret key):
```bash
cat << EOF > /opt/simple-scheduler/simple-scheduler/backend/Rocket.toml
[default]
address = "0.0.0.0"
limits = { forms = "64 kB", json = "100 MiB" }
port = 8000

[release]
secret_key = "SECRET_KEY"
database_url = "sqlite:database.sqlite" 
process_directory = "/opt/simple-scheduler/process-dirs"
hypervisor_socket_path = "/tmp/simple-scheduler.sock"
public_key_path = "/opt/simple-scheduler/simple-scheduler/keys/pub.key"
private_key_path = "/opt/simple-scheduler/simple-scheduler/keys/priv.key"
nb_of_workers = 10
EOF
```

6. Add a user with login: "debug-user", pass: "debug-password"
```
cargo build --release
target/release/simple-scheduler create-user debug-user debug-password
```

7. Create a systemd unit file to run the scheduler
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

8. enable and start it:
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
* to fork process
* to handle signals/sigaction
* to dup stderr/stdout to files
* to create inter processus semaphores
* to write into argv[0]

When available, most of this is done through the nix abstractions, otherwise directly through the libc. Both use unsafe APIs.
