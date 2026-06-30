//! Minimal /healthz, /readyz, /metrics endpoints on :9090.

use anyhow::Result;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use once_cell::sync::Lazy;
use prometheus::{Encoder, IntCounter, IntCounterVec, IntGauge, Registry, TextEncoder};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::net::TcpListener;
use tracing::{info, warn};

pub static REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);

pub static SNAPSHOTS_SENT: Lazy<IntCounterVec> = Lazy::new(|| {
    let c = IntCounterVec::new(
        prometheus::Opts::new("agent_snapshots_total", "Snapshots sent by outcome"),
        &["outcome"],
    )
    .expect("failed to create agent_snapshots_total metric");
    REGISTRY
        .register(Box::new(c.clone()))
        .expect("failed to register agent_snapshots_total metric");
    c
});

pub static COMMANDS_RECEIVED: Lazy<IntCounter> = Lazy::new(|| {
    let c = IntCounter::new(
        "agent_commands_received_total",
        "Commands received from Hub",
    )
    .expect("failed to create agent_commands_received_total metric");
    REGISTRY
        .register(Box::new(c.clone()))
        .expect("failed to register agent_commands_received_total metric");
    c
});

pub static COMMANDS_EXECUTED: Lazy<IntCounterVec> = Lazy::new(|| {
    let c = IntCounterVec::new(
        prometheus::Opts::new("agent_commands_executed_total", "Commands by status"),
        &["status"],
    )
    .expect("failed to create agent_commands_executed_total metric");
    REGISTRY
        .register(Box::new(c.clone()))
        .expect("failed to register agent_commands_executed_total metric");
    c
});

pub static IS_LEADER: Lazy<IntGauge> = Lazy::new(|| {
    let g = IntGauge::new("agent_is_leader", "1 if this pod currently holds the lease")
        .expect("failed to create agent_is_leader metric");
    REGISTRY
        .register(Box::new(g.clone()))
        .expect("failed to register agent_is_leader metric");
    g
});

pub static TETRAGON_CONNECTED: Lazy<IntGauge> = Lazy::new(|| {
    let g = IntGauge::new(
        "agent_tetragon_connected",
        "1 if the agent currently has a live Tetragon gRPC stream",
    )
    .expect("failed to create agent_tetragon_connected metric");
    REGISTRY
        .register(Box::new(g.clone()))
        .expect("failed to register agent_tetragon_connected metric");
    g
});

pub static TETRAGON_RECONNECTS: Lazy<IntCounter> = Lazy::new(|| {
    let c = IntCounter::new(
        "agent_tetragon_reconnects_total",
        "Tetragon stream reconnects after a previously ready stream ended",
    )
    .expect("failed to create agent_tetragon_reconnects_total metric");
    REGISTRY
        .register(Box::new(c.clone()))
        .expect("failed to register agent_tetragon_reconnects_total metric");
    c
});

pub static TETRAGON_CONNECTION_FAILURES: Lazy<IntCounter> = Lazy::new(|| {
    let c = IntCounter::new(
        "agent_tetragon_connection_failures_total",
        "Tetragon connection or stream startup failures before the stream became ready",
    )
    .expect("failed to create agent_tetragon_connection_failures_total metric");
    REGISTRY
        .register(Box::new(c.clone()))
        .expect("failed to register agent_tetragon_connection_failures_total metric");
    c
});

pub static TETRAGON_POLICY_APPLY_FAILURES: Lazy<IntCounter> = Lazy::new(|| {
    let c = IntCounter::new(
        "agent_tetragon_policy_apply_failures_total",
        "Tetragon tracing policy apply failures",
    )
    .expect("failed to create agent_tetragon_policy_apply_failures_total metric");
    REGISTRY
        .register(Box::new(c.clone()))
        .expect("failed to register agent_tetragon_policy_apply_failures_total metric");
    c
});

pub static DEPENDENCY_PARSE_FAILURES: Lazy<IntCounter> = Lazy::new(|| {
    let c = IntCounter::new(
        "agent_dependency_parse_failures_total",
        "Dependency event lines that could not be parsed or normalized",
    )
    .expect("failed to create agent_dependency_parse_failures_total metric");
    REGISTRY
        .register(Box::new(c.clone()))
        .expect("failed to register agent_dependency_parse_failures_total metric");
    c
});

pub static DEPENDENCY_EVENTS_DROPPED: Lazy<IntCounter> = Lazy::new(|| {
    let c = IntCounter::new(
        "agent_dependency_events_dropped_total",
        "Dependency events dropped because snapshot caps were exceeded",
    )
    .expect("failed to create agent_dependency_events_dropped_total metric");
    REGISTRY
        .register(Box::new(c.clone()))
        .expect("failed to register agent_dependency_events_dropped_total metric");
    c
});

pub static DEPENDENCY_EVENTS_SKIPPED: Lazy<IntCounter> = Lazy::new(|| {
    let c = IntCounter::new(
        "agent_dependency_events_skipped_total",
        "Dependency events skipped because per-source fanout caps were exceeded",
    )
    .expect("failed to create agent_dependency_events_skipped_total metric");
    REGISTRY
        .register(Box::new(c.clone()))
        .expect("failed to register agent_dependency_events_skipped_total metric");
    c
});

pub static DEPENDENCY_SNAPSHOTS_TRUNCATED: Lazy<IntCounter> = Lazy::new(|| {
    let c = IntCounter::new(
        "agent_dependency_snapshots_truncated_total",
        "Dependency snapshots truncated by internal caps",
    )
    .expect("failed to create agent_dependency_snapshots_truncated_total metric");
    REGISTRY
        .register(Box::new(c.clone()))
        .expect("failed to register agent_dependency_snapshots_truncated_total metric");
    c
});

pub static SNAPSHOT_SEND_SUCCESS: Lazy<IntCounter> = Lazy::new(|| {
    let c = IntCounter::new(
        "agent_snapshot_send_success_total",
        "Successful snapshot send operations",
    )
    .expect("failed to create agent_snapshot_send_success_total metric");
    REGISTRY
        .register(Box::new(c.clone()))
        .expect("failed to register agent_snapshot_send_success_total metric");
    c
});

pub static SNAPSHOT_SEND_FAILURE: Lazy<IntCounter> = Lazy::new(|| {
    let c = IntCounter::new(
        "agent_snapshot_send_failure_total",
        "Failed snapshot send operations",
    )
    .expect("failed to create agent_snapshot_send_failure_total metric");
    REGISTRY
        .register(Box::new(c.clone()))
        .expect("failed to register agent_snapshot_send_failure_total metric");
    c
});

pub static COMMAND_POLL_SUCCESS: Lazy<IntCounter> = Lazy::new(|| {
    let c = IntCounter::new(
        "agent_command_poll_success_total",
        "Successful command polls that returned work",
    )
    .expect("failed to create agent_command_poll_success_total metric");
    REGISTRY
        .register(Box::new(c.clone()))
        .expect("failed to register agent_command_poll_success_total metric");
    c
});

pub static COMMAND_POLL_EMPTY: Lazy<IntCounter> = Lazy::new(|| {
    let c = IntCounter::new(
        "agent_command_poll_empty_total",
        "Successful command polls that returned no work",
    )
    .expect("failed to create agent_command_poll_empty_total metric");
    REGISTRY
        .register(Box::new(c.clone()))
        .expect("failed to register agent_command_poll_empty_total metric");
    c
});

pub static COMMAND_POLL_FAILURE: Lazy<IntCounter> = Lazy::new(|| {
    let c = IntCounter::new("agent_command_poll_failure_total", "Failed command polls")
        .expect("failed to create agent_command_poll_failure_total metric");
    REGISTRY
        .register(Box::new(c.clone()))
        .expect("failed to register agent_command_poll_failure_total metric");
    c
});

pub static COMMAND_ACK_SUCCESS: Lazy<IntCounter> = Lazy::new(|| {
    let c = IntCounter::new(
        "agent_command_ack_success_total",
        "Successful command acknowledgement sends",
    )
    .expect("failed to create agent_command_ack_success_total metric");
    REGISTRY
        .register(Box::new(c.clone()))
        .expect("failed to register agent_command_ack_success_total metric");
    c
});

pub static COMMAND_ACK_FAILURE: Lazy<IntCounter> = Lazy::new(|| {
    let c = IntCounter::new(
        "agent_command_ack_failure_total",
        "Failed command acknowledgement sends",
    )
    .expect("failed to create agent_command_ack_failure_total metric");
    REGISTRY
        .register(Box::new(c.clone()))
        .expect("failed to register agent_command_ack_failure_total metric");
    c
});

static DEPENDENCY_REQUIRED: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(false));
static DEPENDENCY_READY: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(true));

pub fn set_dependency_required(required: bool) {
    DEPENDENCY_REQUIRED.store(required, Ordering::Relaxed);
}

pub fn set_dependency_ready(ready: bool) {
    DEPENDENCY_READY.store(ready, Ordering::Relaxed);
}

pub async fn run() -> Result<()> {
    // Force lazy init so /metrics shows the families even before first use.
    let _ = &*SNAPSHOTS_SENT;
    let _ = &*COMMANDS_RECEIVED;
    let _ = &*COMMANDS_EXECUTED;
    let _ = &*IS_LEADER;
    let _ = &*TETRAGON_CONNECTED;
    let _ = &*TETRAGON_RECONNECTS;
    let _ = &*TETRAGON_CONNECTION_FAILURES;
    let _ = &*TETRAGON_POLICY_APPLY_FAILURES;
    let _ = &*DEPENDENCY_PARSE_FAILURES;
    let _ = &*DEPENDENCY_EVENTS_DROPPED;
    let _ = &*DEPENDENCY_EVENTS_SKIPPED;
    let _ = &*DEPENDENCY_SNAPSHOTS_TRUNCATED;
    let _ = &*SNAPSHOT_SEND_SUCCESS;
    let _ = &*SNAPSHOT_SEND_FAILURE;
    let _ = &*COMMAND_POLL_SUCCESS;
    let _ = &*COMMAND_POLL_EMPTY;
    let _ = &*COMMAND_POLL_FAILURE;
    let _ = &*COMMAND_ACK_SUCCESS;
    let _ = &*COMMAND_ACK_FAILURE;

    let addr: std::net::SocketAddr = ([0, 0, 0, 0], 9090).into();
    let listener = TcpListener::bind(addr).await?;
    info!("health/metrics server listening on {}", addr);

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        tokio::spawn(async move {
            let svc = service_fn(handle);
            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, svc)
                .await
            {
                warn!("health connection error: {}", e);
            }
        });
    }
}

async fn handle(
    req: Request<hyper::body::Incoming>,
) -> Result<Response<Full<Bytes>>, std::convert::Infallible> {
    let resp = match req.uri().path() {
        "/healthz" | "/livez" => Response::new(Full::new(Bytes::from("ok"))),
        "/readyz" => {
            if DEPENDENCY_REQUIRED.load(Ordering::Relaxed)
                && !DEPENDENCY_READY.load(Ordering::Relaxed)
            {
                Response::builder()
                    .status(503)
                    .body(Full::new(Bytes::from("dependency stream not ready")))
                    .expect("failed to build readiness response")
            } else {
                Response::new(Full::new(Bytes::from("ok")))
            }
        }
        "/metrics" => {
            let mut buf = Vec::new();
            let encoder = TextEncoder::new();
            let _ = encoder.encode(&REGISTRY.gather(), &mut buf);
            Response::builder()
                .header("content-type", encoder.format_type())
                .body(Full::new(Bytes::from(buf)))
                .expect("failed to build metrics response")
        }
        _ => Response::builder()
            .status(404)
            .body(Full::new(Bytes::from("not found")))
            .expect("failed to build 404 response"),
    };
    Ok(resp)
}
