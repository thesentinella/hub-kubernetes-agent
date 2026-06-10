# syntax=docker/dockerfile:1.7
FROM rust:1.95-alpine3.23 AS rust-builder
WORKDIR /build

RUN apk add --no-cache build-base binutils

# Cache deps
COPY Cargo.toml Cargo.lock* ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src target/release/deps/sentinella_hub_k8s_agent*

COPY src/ ./src/
RUN cargo build --release && \
    strip target/release/sentinella-hub-k8s-agent

FROM golang:1.26-bookworm AS go-builder
WORKDIR /build/sidecar

COPY sidecar/ ./
RUN CGO_ENABLED=0 go build -o /out/tetragon-sidecar .

FROM gcr.io/distroless/cc-debian12:nonroot AS agent-runtime
COPY --from=rust-builder /build/target/release/sentinella-hub-k8s-agent /usr/local/bin/sentinella-hub-k8s-agent
USER nonroot:nonroot
EXPOSE 9090
ENTRYPOINT ["/usr/local/bin/sentinella-hub-k8s-agent"]

FROM gcr.io/distroless/static-debian12:nonroot AS sidecar-runtime
COPY --from=go-builder /out/tetragon-sidecar /usr/local/bin/tetragon-sidecar
USER nonroot:nonroot
EXPOSE 9801
ENTRYPOINT ["/usr/local/bin/tetragon-sidecar"]
