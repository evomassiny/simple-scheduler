FROM docker.io/library/rust:1-slim-bookworm AS builder

# add `musl` target, to build a fully static binary
RUN rustup target add x86_64-unknown-linux-musl
RUN apt-get update --quiet && apt-get install git musl-tools libsqlite3-dev sqlite3 -y
COPY ./backend /simple-scheduler/backend
WORKDIR /simple-scheduler/backend

# compile binary
RUN cargo build --release --target=x86_64-unknown-linux-musl

# build database
RUN sqlite3 /tmp/database.sqlite < ./migrations/20210402155322_creation.sql
