//! Leader election via a Kubernetes `Lease` in the agent's own namespace.
//!
//! Only the elected leader runs the inventory collector. All pods still poll
//! for commands so a node-targeted command can be picked up by the right pod
//! once action execution is enabled.
//!
//! The implementation uses the `kube-leader-election` crate, which renews the
//! Lease on each `try_acquire_or_renew()` call. We renew at lease_ttl/3 to
//! comfortably stay ahead of expiry under transient API server slowness.
//!
//! Note: Kubernetes Lease-based election is non-fencing — under heavy clock
//! skew or network partitions, two pods could briefly both believe they are
//! the leader. For inventory collection this only causes a duplicate snapshot,
//! which is harmless. When action execution is enabled, the executor must
//! still tolerate this (idempotent operations, server-side dedup by command
//! id). See README "Leader election" section.

use anyhow::Result;
use kube::Client;
use kube_leader_election::{LeaseLock, LeaseLockParams, LeaseLockResult};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn};

#[derive(Clone)]
pub struct LeaderState {
    is_leader: Arc<AtomicBool>,
}

impl LeaderState {
    pub fn new() -> Self {
        Self {
            is_leader: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::Acquire)
    }

    fn set(&self, value: bool) {
        let prev = self.is_leader.swap(value, Ordering::AcqRel);
        if prev != value {
            crate::health::IS_LEADER.set(if value { 1 } else { 0 });
            if value {
                info!("became leader");
            } else {
                info!("lost leadership");
            }
        }
    }
}

/// Spawn a background task that continuously tries to acquire/renew the lease.
/// `lease_ttl` is the validity window written into the Lease; we renew every
/// `lease_ttl / 3`.
pub async fn run_leader_loop(
    client: Client,
    namespace: String,
    lease_name: String,
    holder_id: String,
    lease_ttl: Duration,
    state: LeaderState,
) -> Result<()> {
    let lock = LeaseLock::new(
        client,
        &namespace,
        LeaseLockParams {
            holder_id: holder_id.clone(),
            lease_name: lease_name.clone(),
            lease_ttl,
        },
    );

    let renew_interval = lease_ttl / 3;
    info!(
        lease = %lease_name,
        namespace = %namespace,
        holder = %holder_id,
        ttl_secs = lease_ttl.as_secs(),
        renew_secs = renew_interval.as_secs(),
        "starting leader election"
    );

    loop {
        match lock.try_acquire_or_renew().await {
            Ok(result) => {
                state.set(matches!(result, LeaseLockResult::Acquired(_)));
            }
            Err(e) => {
                warn!("lease acquire/renew failed: {:#}", e);
                // On error, do not flip leader state immediately; the existing
                // lease (if held) is still valid until ttl expires server-side.
                // Just retry sooner.
                sleep(Duration::from_secs(2)).await;
                continue;
            }
        }
        sleep(renew_interval).await;
    }
}
