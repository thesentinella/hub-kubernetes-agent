use crate::health;
use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::api::ListParams;
use kube::{Api, Client};
use once_cell::sync::{Lazy, OnceCell};
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;
use tokio::time::interval;
use tokio::time::sleep;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Status};
use tracing::{info, warn};

pub const DEFAULT_GRPC_ADDRESS: &str = "tetragon-grpc.tetragon.svc.cluster.local:54321";
const DEP_WINDOW_SECONDS: u64 = 60;
const MAX_BUFFERED_EVENTS: usize = 20_000;
const MAX_DISCOVERED_ENDPOINTS: usize = 16;
const DISCOVERY_INTERVAL_SECS: u64 = 30;
const STREAM_RECONNECT_DELAY_SECS: u64 = 5;
const FALLBACK_STREAM_KEY: &str = "__fallback__";
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StreamSource {
    Discovered,
    Fallback,
}

struct StreamEntry {
    source: StreamSource,
    connected: bool,
    active: Arc<AtomicBool>,
    handle: JoinHandle<()>,
}

struct ManagerState {
    streams: HashMap<String, StreamEntry>,
}

struct State {
    lines: Mutex<VecDeque<BufferedLine>>,
    manager: Mutex<ManagerState>,
    dependency_required: bool,
}

struct TetragonRuntime {
    client: Client,
    discovery_enabled: bool,
    discovery_namespace: String,
    discovery_service_name: String,
    grpc_port: u16,
    fallback_address: String,
    state: Arc<State>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct CoverageSnapshot {
    pub observed_endpoints: usize,
    pub connected_endpoints: usize,
    pub unavailable_endpoints: usize,
}

static STATE: OnceCell<Arc<State>> = OnceCell::new();
static STARTED: Lazy<Mutex<bool>> = Lazy::new(|| Mutex::new(false));

impl State {
    fn new(dependency_required: bool) -> Self {
        Self {
            lines: Mutex::new(VecDeque::new()),
            manager: Mutex::new(ManagerState {
                streams: HashMap::new(),
            }),
            dependency_required,
        }
    }

    fn coverage_snapshot(&self) -> CoverageSnapshot {
        let manager = self
            .manager
            .lock()
            .expect("tetragon manager mutex poisoned");
        let observed_endpoints = manager.streams.len();
        let connected_endpoints = manager
            .streams
            .values()
            .filter(|entry| entry.connected)
            .count();
        CoverageSnapshot {
            observed_endpoints,
            connected_endpoints,
            unavailable_endpoints: observed_endpoints.saturating_sub(connected_endpoints),
        }
    }

    fn refresh_health(&self) {
        let connected = self.coverage_snapshot().connected_endpoints;
        health::TETRAGON_CONNECTED.set(connected as i64);
        health::set_dependency_ready(!self.dependency_required || connected > 0);
    }

    fn stream_exists(&self, key: &str) -> bool {
        let manager = self
            .manager
            .lock()
            .expect("tetragon manager mutex poisoned");
        manager.streams.contains_key(key)
    }

    fn register_stream(&self, key: String, entry: StreamEntry) {
        let mut manager = self
            .manager
            .lock()
            .expect("tetragon manager mutex poisoned");
        manager.streams.insert(key, entry);
        drop(manager);
        self.refresh_health();
    }

    fn set_stream_connected(&self, key: &str, connected: bool) -> bool {
        let mut manager = self
            .manager
            .lock()
            .expect("tetragon manager mutex poisoned");
        let Some(entry) = manager.streams.get_mut(key) else {
            return false;
        };
        let was_connected = entry.connected;
        entry.connected = connected;
        drop(manager);
        self.refresh_health();
        was_connected
    }

    fn remove_stream(&self, key: &str) -> Option<StreamEntry> {
        let mut manager = self
            .manager
            .lock()
            .expect("tetragon manager mutex poisoned");
        let entry = manager.streams.remove(key);
        drop(manager);
        if entry.is_some() {
            self.refresh_health();
        }
        entry
    }

    fn discovered_keys(&self) -> BTreeSet<String> {
        let manager = self
            .manager
            .lock()
            .expect("tetragon manager mutex poisoned");
        manager
            .streams
            .iter()
            .filter(|(_, entry)| matches!(entry.source, StreamSource::Discovered))
            .map(|(key, _)| key.clone())
            .collect()
    }

    fn has_fallback_stream(&self) -> bool {
        let manager = self
            .manager
            .lock()
            .expect("tetragon manager mutex poisoned");
        manager
            .streams
            .get(FALLBACK_STREAM_KEY)
            .map(|entry| matches!(entry.source, StreamSource::Fallback))
            .unwrap_or(false)
    }

    fn no_streams(&self) -> bool {
        let manager = self
            .manager
            .lock()
            .expect("tetragon manager mutex poisoned");
        manager.streams.is_empty()
    }
}

pub fn init(client: Client, cfg: &crate::config::Config) {
    let dependency_required =
        cfg.collect_dependencies_tetragon && cfg.tetragon_required_for_readiness;
    health::set_dependency_required(dependency_required);
    health::TETRAGON_CONNECTED.set(0);
    if !cfg.collect_dependencies_tetragon {
        health::set_dependency_ready(true);
        return;
    }

    health::set_dependency_ready(!dependency_required);

    let mut started = STARTED.lock().expect("tetragon start mutex poisoned");
    if *started {
        return;
    }
    *started = true;

    let state = Arc::new(State::new(dependency_required));
    let _ = STATE.set(state.clone());

    let runtime = Arc::new(TetragonRuntime {
        client,
        discovery_enabled: cfg.tetragon_endpoint_discovery_enabled,
        discovery_namespace: cfg.tetragon_service_namespace.clone(),
        discovery_service_name: cfg.tetragon_service_name.clone(),
        grpc_port: cfg.tetragon_grpc_port,
        fallback_address: cfg.tetragon_grpc_address.clone(),
        state,
    });
    tokio::spawn(run_manager(runtime));
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

pub fn coverage_snapshot() -> CoverageSnapshot {
    let Some(state) = STATE.get() else {
        return CoverageSnapshot::default();
    };
    state.coverage_snapshot()
}

async fn run_manager(runtime: Arc<TetragonRuntime>) {
    if !runtime.discovery_enabled {
        ensure_fallback_stream(runtime.clone()).await;
        return;
    }

    if let Err(error) = discover_and_reconcile(&runtime).await {
        warn!(error = %error, "tetragon endpoint discovery failed; falling back to configured address");
        ensure_fallback_stream(runtime.clone()).await;
    }

    let mut ticker = interval(Duration::from_secs(DISCOVERY_INTERVAL_SECS));
    loop {
        ticker.tick().await;
        if let Err(error) = discover_and_reconcile(&runtime).await {
            warn!(error = %error, "tetragon endpoint discovery failed; continuing with existing streams");
            if runtime.state.no_streams() {
                ensure_fallback_stream(runtime.clone()).await;
            }
        }
    }
}

async fn discover_and_reconcile(runtime: &Arc<TetragonRuntime>) -> anyhow::Result<()> {
    let endpoints = discover_tetragon_endpoints(runtime).await?;
    if endpoints.is_empty() {
        if runtime.state.no_streams() {
            ensure_fallback_stream(runtime.clone()).await;
        }
        return Ok(());
    }

    let discovered_keys = runtime.state.discovered_keys();
    for key in discovered_keys
        .into_iter()
        .filter(|key| !endpoints.contains(key))
        .collect::<Vec<_>>()
    {
        if let Some(entry) = runtime.state.remove_stream(&key) {
            entry.active.store(false, Ordering::Relaxed);
            entry.handle.abort();
        }
    }

    if runtime.state.has_fallback_stream() {
        if let Some(entry) = runtime.state.remove_stream(FALLBACK_STREAM_KEY) {
            entry.active.store(false, Ordering::Relaxed);
            entry.handle.abort();
        }
    }

    for address in endpoints {
        if runtime.state.stream_exists(&address) {
            continue;
        }
        spawn_stream(
            runtime.clone(),
            address.clone(),
            address,
            StreamSource::Discovered,
        );
    }

    Ok(())
}

async fn ensure_fallback_stream(runtime: Arc<TetragonRuntime>) {
    if runtime.state.stream_exists(FALLBACK_STREAM_KEY) {
        return;
    }
    let address = runtime.fallback_address.clone();
    spawn_stream(
        runtime,
        FALLBACK_STREAM_KEY.to_string(),
        address,
        StreamSource::Fallback,
    );
}

fn spawn_stream(runtime: Arc<TetragonRuntime>, key: String, address: String, source: StreamSource) {
    if runtime.state.stream_exists(&key) {
        return;
    }

    let active = Arc::new(AtomicBool::new(true));
    let task_active = active.clone();
    let state = runtime.state.clone();
    let task_key = key.clone();
    let task_address = address.clone();
    let task_source = source;
    let handle = tokio::spawn(async move {
        run_stream(task_key, task_address, task_source, state, task_active).await;
    });

    runtime.state.register_stream(
        key,
        StreamEntry {
            source,
            connected: false,
            active,
            handle,
        },
    );
}

async fn run_stream(
    key: String,
    address: String,
    source: StreamSource,
    state: Arc<State>,
    active: Arc<AtomicBool>,
) {
    loop {
        if !active.load(Ordering::Relaxed) {
            break;
        }

        match connect_and_stream(&address, state.clone(), &key).await {
            Ok(()) => {
                let was_connected = state.set_stream_connected(&key, false);
                if !active.load(Ordering::Relaxed) {
                    break;
                }
                if was_connected {
                    health::TETRAGON_RECONNECTS.inc();
                    warn!(address = %address, source = ?source, "tetragon stream ended; reconnecting");
                } else {
                    health::TETRAGON_CONNECTION_FAILURES.inc();
                    warn!(address = %address, source = ?source, "tetragon stream unavailable");
                }
            }
            Err(error) => {
                let was_connected = state.set_stream_connected(&key, false);
                if !active.load(Ordering::Relaxed) {
                    break;
                }
                if was_connected {
                    health::TETRAGON_RECONNECTS.inc();
                    warn!(
                        address = %address,
                        source = ?source,
                        error = %error,
                        "tetragon stream lost; reconnecting"
                    );
                } else {
                    health::TETRAGON_CONNECTION_FAILURES.inc();
                    warn!(
                        address = %address,
                        source = ?source,
                        error = %error,
                        "tetragon stream unavailable"
                    );
                }
            }
        }

        sleep(Duration::from_secs(STREAM_RECONNECT_DELAY_SECS)).await;
    }
}

async fn connect_and_stream(address: &str, state: Arc<State>, key: &str) -> anyhow::Result<()> {
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
    let _ = state.set_stream_connected(key, true);
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

async fn discover_tetragon_endpoints(
    runtime: &TetragonRuntime,
) -> anyhow::Result<BTreeSet<String>> {
    let api: Api<EndpointSlice> =
        Api::namespaced(runtime.client.clone(), &runtime.discovery_namespace);
    let lp = ListParams::default().labels(&format!(
        "kubernetes.io/service-name={}",
        runtime.discovery_service_name
    ));
    let slices = api.list(&lp).await?;
    let mut addresses = BTreeSet::new();

    for slice in slices {
        let port = slice
            .ports
            .as_ref()
            .and_then(|ports| ports.iter().find_map(|port| port.port))
            .map(|port| port as u16)
            .unwrap_or(runtime.grpc_port);

        for endpoint in slice.endpoints {
            if endpoint
                .conditions
                .as_ref()
                .and_then(|conditions| conditions.ready)
                == Some(false)
            {
                continue;
            }

            if let Some(target_ref) = endpoint.target_ref.as_ref() {
                if target_ref.kind.as_deref() != Some("Pod") {
                    continue;
                }
            }

            for address in endpoint.addresses {
                if addresses.len() >= MAX_DISCOVERED_ENDPOINTS {
                    warn!(
                        max_endpoints = MAX_DISCOVERED_ENDPOINTS,
                        service_namespace = %runtime.discovery_namespace,
                        service_name = %runtime.discovery_service_name,
                        "tetragon endpoint discovery hit stream cap; skipping remaining endpoints"
                    );
                    return Ok(addresses);
                }

                let Some(address) = normalize_discovered_address(&address) else {
                    warn!(
                        address = %address,
                        service_namespace = %runtime.discovery_namespace,
                        service_name = %runtime.discovery_service_name,
                        "skipping non-IP tetragon endpoint address"
                    );
                    continue;
                };

                addresses.insert(format!("{address}:{port}"));
            }
        }
    }

    Ok(addresses)
}

fn normalize_discovered_address(address: &str) -> Option<String> {
    address.parse::<IpAddr>().ok().map(|ip| ip.to_string())
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
        Err(status) => {
            health::TETRAGON_POLICY_APPLY_FAILURES.inc();
            Err(anyhow::anyhow!(
                "failed to ensure tracing policy {}: {}",
                POLICY_NAME,
                status
            ))
        }
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

#[cfg(test)]
mod tests {
    use super::normalize_discovered_address;

    #[test]
    fn normalize_discovered_address_accepts_ip_literals() {
        assert_eq!(
            normalize_discovered_address("10.1.2.3"),
            Some("10.1.2.3".to_string())
        );
    }

    #[test]
    fn normalize_discovered_address_rejects_dns_names() {
        assert_eq!(normalize_discovered_address("tetragon-0"), None);
    }
}
