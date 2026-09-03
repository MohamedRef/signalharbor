FROM rust:1.92-slim-bookworm

RUN rustup component add --toolchain 1.92.0 clippy rustfmt

WORKDIR /app
