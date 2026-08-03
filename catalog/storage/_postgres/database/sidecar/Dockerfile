FROM rust:1.93-alpine AS chef

RUN apk add --no-cache musl-dev pkgconfig openssl-dev openssl-libs-static
RUN cargo install cargo-chef --locked

WORKDIR /app

FROM chef AS planner

COPY sidecars/postgres-database ./postgres-database

WORKDIR /app/postgres-database
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder

WORKDIR /app/postgres-database

COPY --from=planner /app/postgres-database/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

COPY sidecars/postgres-database .

RUN cargo build --release

FROM alpine:3.21

RUN apk add --no-cache ca-certificates

COPY --from=builder /app/postgres-database/target/release/sidecar-postgres-database /usr/local/bin/sidecar-postgres-database

ENV PORT=8090
EXPOSE 8090

CMD ["sidecar-postgres-database"]