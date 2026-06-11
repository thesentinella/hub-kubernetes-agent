use crate::health;
use once_cell::sync::{Lazy, OnceCell};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Status};
use tracing::{info, warn};

pub const DEFAULT_GRPC_ADDRESS: &str = "tetragon-grpc.tetragon.svc.cluster.local:54321";
const DEP_WINDOW_SECONDS: u64 = 60;
const MAX_BUFFERED_EVENTS: usize = 20_000;
const POLICY_NAME: &str = "sentinella-tcp-connect";
const POLICY_YAML: &str = r#"apiVersion: cilium.io/v1alpha1
kind: TracingPolicy
metadata:
  name: "sentinella-tcp-connect"
spec:
  kprobes:
  - call: "tcp_close"
    syscall: false
    args:
    - index: 0
      type: "sock"
  - call: "tcp_sendmsg"
    syscall: false
    args:
    - index: 0
      type: "sock"
    - index: 2
      type: int
"#;

pub mod proto {
    tonic::include_proto!("tetragon");
}

#[derive(Clone)]
struct BufferedLine {
    line: String,
    recorded_at_ms: u128,
}

struct State {
    lines: Mutex<VecDeque<BufferedLine>>,
}

static STATE: OnceCell<Arc<State>> = OnceCell::new();
static STARTED: Lazy<Mutex<bool>> = Lazy::new(|| Mutex::new(false));

pub fn init(enabled: bool, address: String) {
    health::set_dependency_required(enabled);
    if !enabled {
        health::set_dependency_ready(true);
        return;
    }

    let mut started = STARTED.lock().expect("tetragon start mutex poisoned");
    if *started {
        return;
    }
    *started = true;

    let state = Arc::new(State {
        lines: Mutex::new(VecDeque::new()),
    });
    let _ = STATE.set(state.clone());
    health::set_dependency_ready(false);
    tokio::spawn(run_loop(address, state));
}

pub fn snapshot_ndjson() -> String {
    let Some(state) = STATE.get() else {
        return String::new();
    };
    let mut lines = state.lines.lock().expect("tetragon line mutex poisoned");
    prune_locked(&mut lines);
    let mut body = String::new();
    for line in lines.iter() {
        body.push_str(&line.line);
        body.push('\n');
    }
    body
}

async fn run_loop(address: String, state: Arc<State>) {
    loop {
        match connect_and_stream(&address, state.clone()).await {
            Ok(()) => {
                health::set_dependency_ready(false);
                warn!(address = %address, "tetragon stream ended; reconnecting");
            }
            Err(error) => {
                health::set_dependency_ready(false);
                warn!(address = %address, error = %error, "tetragon stream unavailable");
            }
        }
        sleep(Duration::from_secs(5)).await;
    }
}

async fn connect_and_stream(address: &str, state: Arc<State>) -> anyhow::Result<()> {
    let channel = grpc_channel(address).await?;
    let mut client = proto::fine_guidance_sensors_client::FineGuidanceSensorsClient::new(channel);

    ensure_policy(&mut client).await?;
    let request = proto::GetEventsRequest {
        allow_list: vec![proto::Filter {
            event_set: vec![proto::EventType::ProcessKprobe as i32],
            policy_names: vec![POLICY_NAME.to_string()],
        }],
    };
    let mut stream = client.get_events(request).await?.into_inner();
    health::set_dependency_ready(true);
    info!(address = %address, "connected to tetragon gRPC");

    while let Some(response) = stream.message().await? {
        if let Some(line) = event_line(&response) {
            let mut lines = state.lines.lock().expect("tetragon line mutex poisoned");
            prune_locked(&mut lines);
            if lines.len() >= MAX_BUFFERED_EVENTS {
                lines.pop_front();
            }
            lines.push_back(BufferedLine {
                line,
                recorded_at_ms: now_unix_ms(),
            });
        }
    }

    Ok(())
}

async fn grpc_channel(address: &str) -> anyhow::Result<Channel> {
    let endpoint = if address.contains("://") {
        Endpoint::from_shared(address.to_string())?
    } else {
        Endpoint::from_shared(format!("http://{address}"))?
    };
    Ok(endpoint.connect().await?)
}

async fn ensure_policy(
    client: &mut proto::fine_guidance_sensors_client::FineGuidanceSensorsClient<Channel>,
) -> anyhow::Result<()> {
    let request = proto::AddTracingPolicyRequest {
        yaml: POLICY_YAML.to_string(),
        domain: String::new(),
    };
    match client.add_tracing_policy(request).await {
        Ok(_) => Ok(()),
        Err(status) if status.code() == Code::AlreadyExists || already_exists_like(&status) => {
            Ok(())
        }
        Err(status) => Err(status.into()),
    }
}

fn already_exists_like(status: &Status) -> bool {
    status.message().to_ascii_lowercase().contains("exists")
}

fn event_line(response: &proto::GetEventsResponse) -> Option<String> {
    let proto::get_events_response::Event::ProcessKprobe(kprobe) = response.event.as_ref()?;
    let function_name = kprobe.function_name.as_str();
    if function_name != "tcp_sendmsg" && function_name != "tcp_close" {
        return None;
    }

    let sock = kprobe.args.iter().find_map(sock_arg)?;
    let bytes = if function_name == "tcp_sendmsg" {
        kprobe.args.get(1).map(int_arg).unwrap_or(0)
    } else {
        0
    };
    let timestamp_unix_ms = response
        .time
        .as_ref()
        .and_then(timestamp_ms)
        .unwrap_or_else(now_unix_ms);

    let line = serde_json::json!({
        "process_kprobe": {
            "function_name": function_name,
            "timestamp_unix_ms": timestamp_unix_ms,
            "args": if function_name == "tcp_sendmsg" {
                vec![
                    serde_json::json!({ "sock_arg": sock }),
                    serde_json::json!({ "int_arg": bytes }),
                ]
            } else {
                vec![serde_json::json!({ "sock_arg": sock })]
            }
        }
    });
    serde_json::to_string(&line).ok()
}

fn sock_arg(argument: &proto::KprobeArgument) -> Option<serde_json::Value> {
    let proto::kprobe_argument::Arg::SockArg(sock) = argument.arg.as_ref()? else {
        return None;
    };
    Some(serde_json::json!({
        "saddr": sock.saddr,
        "daddr": sock.daddr,
        "protocol": sock.protocol,
        "dport": sock.dport,
    }))
}

fn int_arg(argument: &proto::KprobeArgument) -> u64 {
    match argument.arg.as_ref() {
        Some(proto::kprobe_argument::Arg::IntArg(value)) => (*value).max(0) as u64,
        _ => 0,
    }
}

fn timestamp_ms(timestamp: &prost_types::Timestamp) -> Option<u128> {
    if timestamp.seconds < 0 {
        return None;
    }
    Some((timestamp.seconds as u128 * 1000) + (timestamp.nanos.max(0) as u128 / 1_000_000))
}

fn prune_locked(lines: &mut VecDeque<BufferedLine>) {
    let cutoff = now_unix_ms().saturating_sub(DEP_WINDOW_SECONDS as u128 * 1000);
    while lines
        .front()
        .map(|line| line.recorded_at_ms < cutoff)
        .unwrap_or(false)
    {
        lines.pop_front();
    }
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}
