//! Minimal /healthz, /readyz, /metrics endpoints on :9090.

use anyhow::Result;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use once_cell::sync::Lazy;
use prometheus::{Encoder, IntCounter, IntCounterVec, IntGauge, Registry, TextEncoder};
use tokio::net::TcpListener;
use tracing::{info, warn};

pub static REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);

pub static SNAPSHOTS_SENT: Lazy<IntCounterVec> = Lazy::new(|| {
    let c = IntCounterVec::new(
        prometheus::Opts::new("agent_snapshots_total", "Snapshots sent by outcome"),
        &["outcome"],
    )
    .unwrap();
    REGISTRY.register(Box::new(c.clone())).unwrap();
    c
});

pub static COMMANDS_RECEIVED: Lazy<IntCounter> = Lazy::new(|| {
    let c = IntCounter::new(
        "agent_commands_received_total",
        "Commands received from Hub",
    )
    .unwrap();
    REGISTRY.register(Box::new(c.clone())).unwrap();
    c
});

pub static COMMANDS_EXECUTED: Lazy<IntCounterVec> = Lazy::new(|| {
    let c = IntCounterVec::new(
        prometheus::Opts::new("agent_commands_executed_total", "Commands by status"),
        &["status"],
    )
    .unwrap();
    REGISTRY.register(Box::new(c.clone())).unwrap();
    c
});

pub static IS_LEADER: Lazy<IntGauge> = Lazy::new(|| {
    let g = IntGauge::new("agent_is_leader", "1 if this pod currently holds the lease").unwrap();
    REGISTRY.register(Box::new(g.clone())).unwrap();
    g
});

pub async fn run() -> Result<()> {
    // Force lazy init so /metrics shows the families even before first use.
    let _ = &*SNAPSHOTS_SENT;
    let _ = &*COMMANDS_RECEIVED;
    let _ = &*COMMANDS_EXECUTED;
    let _ = &*IS_LEADER;

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
        "/healthz" | "/livez" | "/readyz" => Response::new(Full::new(Bytes::from("ok"))),
        "/metrics" => {
            let mut buf = Vec::new();
            let encoder = TextEncoder::new();
            let _ = encoder.encode(&REGISTRY.gather(), &mut buf);
            Response::builder()
                .header("content-type", encoder.format_type())
                .body(Full::new(Bytes::from(buf)))
                .unwrap()
        }
        _ => Response::builder()
            .status(404)
            .body(Full::new(Bytes::from("not found")))
            .unwrap(),
    };
    Ok(resp)
}
