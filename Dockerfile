# syntax=docker/dockerfile:1.7

# cargo-chef stage: installs the cargo-chef binary used by the planner and
# builder stages. Reused as the base for both, so the toolchain (rustc,
# cargo, build-base, binutils, protobuf-dev) is laid down once.
FROM rust:1.95-alpine3.23 AS chef
RUN apk add --no-cache build-base binutils openssl-dev openssl-libs-static protobuf-dev
RUN cargo install cargo-chef --locked
WORKDIR /build

# Planner stage: emits a recipe.json that lists every Cargo dependency
# without compiling them. The recipe is small and changes only when
# Cargo.toml / Cargo.lock change, so it forms a stable cache key.
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Builder stage: cook the dependencies from the recipe (this is the
# expensive step that cargo-chef caches), then copy the source and build
# the actual binary. The source-only layer invalidates on every change,
# but the dep layer is reused.
FROM chef AS builder
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

COPY Cargo.toml Cargo.lock* ./
COPY build.rs ./
COPY proto/ ./proto/
COPY src/ ./src/
RUN cargo build --release && \
    strip target/release/sentinella-hub-k8s-agent

FROM gcr.io/distroless/cc-debian12:nonroot AS agent-runtime
COPY --from=builder /build/target/release/sentinella-hub-k8s-agent /usr/local/bin/sentinella-hub-k8s-agent
USER nonroot:nonroot
EXPOSE 9090
ENTRYPOINT ["/usr/local/bin/sentinella-hub-k8s-agent"]
