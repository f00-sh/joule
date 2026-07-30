//! Distributed compute cluster membership, live capacity, and placement.
//!
//! Internet-wide volunteer pool. Nodes join the control plane over any path.
//! Scheduling of free/loaded compute lives in [`scheduler`].

mod scheduler;

pub use scheduler::{
    compute_state, free_slots, free_stream_slots, max_slots, max_streams, pool_max_streams,
    ComputeState, NodeSchedule, SchedulerSnapshot, DEFAULT_MODEL_LAYERS, STREAM_BUDGET_MIB,
};

use joule_proto::{
    ClusterCapacity, ClusterPlan, DeviceClass, LogicalDevice, NodeCaps, NodeId, CLUSTER_MODEL,
    CLUSTER_MODEL_LABEL,
};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClusterError {
    #[error("no eligible nodes for model {0}")]
    NoEligibleNodes(String),
    #[error("unknown node {0}")]
    UnknownNode(NodeId),
}

#[derive(Debug, Clone, Default)]
pub struct Reputation {
    pub ok: u64,
    pub fail: u64,
    pub banned_until: Option<Instant>,
}

impl Reputation {
    pub fn score(&self) -> f64 {
        let total = (self.ok + self.fail).max(1) as f64;
        self.ok as f64 / total
    }

    pub fn is_banned(&self, now: Instant) -> bool {
        self.banned_until.is_some_and(|t| now < t)
    }

    pub fn record_ok(&mut self) {
        self.ok = self.ok.saturating_add(1);
    }

    pub fn record_fail(&mut self, ban_after_fails: u64, ban_for: Duration) {
        self.fail = self.fail.saturating_add(1);
        // Ban if more fails than oks and at least ban_after_fails failures.
        if self.fail >= ban_after_fails && self.fail > self.ok {
            self.banned_until = Some(Instant::now() + ban_for);
        }
    }
}

#[derive(Debug, Clone)]
pub struct Node {
    pub id: NodeId,
    pub account: String,
    pub caps: NodeCaps,
    pub healthy: bool,
    pub load: f32,
    pub last_seen: Instant,
    /// In-flight inference jobs assigned by control (for load balancing).
    pub inflight: u32,
    pub reputation: Reputation,
    /// Round-robin salt updated when selected.
    pub rr_seq: u64,
}

/// In-memory cluster registry fed by agent hellos/heartbeats.
#[derive(Debug)]
pub struct Cluster {
    nodes: HashMap<NodeId, Node>,
    stale_after: Duration,
    rr_counter: u64,
}

impl Default for Cluster {
    fn default() -> Self {
        Self::new(Duration::from_secs(30))
    }
}

impl Cluster {
    pub fn new(stale_after: Duration) -> Self {
        Self {
            nodes: HashMap::new(),
            stale_after,
            rr_counter: 0,
        }
    }

    pub fn upsert_node(&mut self, id: NodeId, account: impl Into<String>, caps: NodeCaps) {
        let account = account.into();
        if let Some(existing) = self.nodes.get_mut(&id) {
            existing.account = account;
            existing.caps = caps;
            existing.healthy = true;
            existing.last_seen = Instant::now();
            return;
        }
        self.nodes.insert(
            id.clone(),
            Node {
                id,
                account,
                caps,
                healthy: true,
                load: 0.0,
                last_seen: Instant::now(),
                inflight: 0,
                reputation: Reputation::default(),
                rr_seq: 0,
            },
        );
    }

    pub fn set_health(
        &mut self,
        id: &NodeId,
        healthy: bool,
        load: f32,
    ) -> Result<(), ClusterError> {
        let node = self
            .nodes
            .get_mut(id)
            .ok_or_else(|| ClusterError::UnknownNode(id.clone()))?;
        node.healthy = healthy;
        node.load = load;
        node.last_seen = Instant::now();
        Ok(())
    }

    pub fn remove_node(&mut self, id: &NodeId) {
        self.nodes.remove(id);
    }

    pub fn apply_staleness(&mut self) {
        let now = Instant::now();
        let stale = self.stale_after;
        for n in self.nodes.values_mut() {
            if now.duration_since(n.last_seen) > stale {
                n.healthy = false;
            }
        }
    }

    pub fn reap_dead(&mut self, dead_after: Duration) {
        let now = Instant::now();
        self.nodes
            .retain(|_, n| now.duration_since(n.last_seen) <= dead_after);
    }

    pub fn nodes(&self) -> impl Iterator<Item = &Node> {
        self.nodes.values()
    }

    pub fn get(&self, id: &NodeId) -> Option<&Node> {
        self.nodes.get(id)
    }

    pub fn get_mut(&mut self, id: &NodeId) -> Option<&mut Node> {
        self.nodes.get_mut(id)
    }

    pub fn account_is_donating(&self, account: &str) -> bool {
        let now = Instant::now();
        self.nodes
            .values()
            .any(|n| n.account == account && n.healthy && !n.reputation.is_banned(now))
    }

    /// Healthy, non-banned donors (pool membership).
    pub(crate) fn eligible(&self) -> Vec<&Node> {
        let now = Instant::now();
        self.nodes
            .values()
            .filter(|p| p.healthy && !p.reputation.is_banned(now))
            .collect()
    }

    /// Rank schedulable donors (have free slots). Prefer free over loaded.
    pub fn rank_workers(&self, _model: &str) -> Vec<NodeId> {
        self.rank_schedulable()
    }

    /// How many healthy donors are in the pool (not necessarily free).
    pub fn pool_size(&self) -> usize {
        self.eligible().len()
    }

    /// Acquire one free/loaded slot (not full). Prefer free nodes.
    pub fn acquire_worker(&mut self, _model: &str) -> Option<NodeId> {
        self.try_acquire_slot()
    }

    /// Acquire up to `n` slots on distinct free/loaded nodes.
    pub fn acquire_workers(&mut self, _model: &str, n: usize) -> Vec<NodeId> {
        self.try_acquire_slots(n)
    }

    pub fn preferred_pipeline_stages(&self) -> usize {
        let n = self.pool_size();
        if n >= 4 {
            4
        } else if n >= 2 {
            n
        } else {
            1
        }
    }

    pub fn release_worker(&mut self, id: &NodeId) {
        if let Some(n) = self.nodes.get_mut(id) {
            n.inflight = n.inflight.saturating_sub(1);
        }
    }

    /// Random healthy non-banned node for spot challenges (any model).
    pub fn pick_challenge_target(&self) -> Option<&Node> {
        let now = Instant::now();
        let mut eligible: Vec<&Node> = self
            .nodes
            .values()
            .filter(|n| n.healthy && !n.reputation.is_banned(now))
            .collect();
        if eligible.is_empty() {
            return None;
        }
        // Prefer nodes with fewer recent challenges (use fail+ok as proxy) and lower inflight.
        eligible.sort_by(|a, b| {
            (a.inflight, a.reputation.ok + a.reputation.fail)
                .cmp(&(b.inflight, b.reputation.ok + b.reputation.fail))
        });
        Some(eligible[0])
    }

    pub fn record_challenge_ok(&mut self, id: &NodeId) {
        if let Some(n) = self.nodes.get_mut(id) {
            n.reputation.record_ok();
        }
    }

    pub fn record_challenge_fail(&mut self, id: &NodeId) {
        if let Some(n) = self.nodes.get_mut(id) {
            n.reputation.record_fail(3, Duration::from_secs(120));
        }
    }

    pub fn capacity(&self) -> ClusterCapacity {
        let mut nodes_total = 0u32;
        let mut nodes_healthy = 0u32;
        let mut nodes_gpu = 0u32;
        let mut nodes_metal = 0u32;
        let mut nodes_cpu = 0u32;
        let mut mem_mib_total = 0u64;
        let mut mem_mib_healthy = 0u64;
        let mut throughput_class_sum = 0u64;
        let now = Instant::now();

        for n in self.nodes.values() {
            nodes_total += 1;
            mem_mib_total += u64::from(n.caps.mem_mib);
            match n.caps.device {
                DeviceClass::Gpu => nodes_gpu += 1,
                DeviceClass::Metal => nodes_metal += 1,
                DeviceClass::Cpu => nodes_cpu += 1,
            }
            if n.healthy && !n.reputation.is_banned(now) {
                nodes_healthy += 1;
                mem_mib_healthy += u64::from(n.caps.mem_mib);
                throughput_class_sum += u64::from(n.caps.throughput_class);
            }
        }

        let models_available = if nodes_healthy > 0 {
            vec![CLUSTER_MODEL.to_string()]
        } else {
            vec![]
        };

        let stream_slots_total = scheduler::pool_max_streams(mem_mib_healthy);
        let stream_slots_used = self
            .eligible()
            .iter()
            .map(|n| n.inflight)
            .max()
            .unwrap_or(0)
            .min(stream_slots_total);

        // Public product view: one accelerator whose VRAM is the sum of donors.
        // model_ready filled by control once manifest gates are applied.
        let logical_device = Some(LogicalDevice {
            id: "joule-pool".into(),
            name: format!("joule supercomputer ({CLUSTER_MODEL_LABEL})"),
            kind: "aggregate_gpu".into(),
            vram_mib: mem_mib_healthy,
            vram_gib: mem_mib_healthy / 1024,
            backends: nodes_healthy,
            model: CLUSTER_MODEL.to_string(),
            ready: nodes_healthy > 0,
            model_ready: false,
            model_progress_pct: 0,
            inference_mode: String::new(),
            readiness_message: String::new(),
        });

        ClusterCapacity {
            nodes_total,
            nodes_healthy,
            nodes_gpu,
            nodes_metal,
            nodes_cpu,
            mem_mib_total,
            mem_mib_healthy,
            throughput_class_sum,
            models_available,
            stream_slots_total,
            stream_slots_used,
            logical_device,
        }
    }

    /// Placement for the single cluster model: always VRAM-shard across the healthy pool.
    /// `prefer_pipeline` / `pipeline_stages` are ignored (whole pool is the plan).
    pub fn plan_for(
        &self,
        _model: &str,
        _prefer_pipeline: bool,
        _pipeline_stages: usize,
    ) -> Result<ClusterPlan, ClusterError> {
        self.plan_sharded_pool()
    }

    /// Full-pool VRAM-sharded plan (alias).
    pub fn plan_full_pool(&self) -> Result<ClusterPlan, ClusterError> {
        self.plan_sharded_pool()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use joule_proto::DeviceClass;

    fn node(mem: u32, device: DeviceClass) -> (NodeId, NodeCaps) {
        (NodeId::new(), NodeCaps::for_cluster(device, mem, 10))
    }

    #[test]
    fn stream_uses_whole_pool_not_one_gpu() {
        let mut c = Cluster::default();
        let (a, ca) = node(8192, DeviceClass::Gpu);
        let (b, cb) = node(16384, DeviceClass::Gpu);
        c.upsert_node(a.clone(), "alice", ca);
        c.upsert_node(b.clone(), "bob", cb);
        let plan = c.try_acquire_stream().unwrap();
        assert_eq!(plan.shards.len(), 2);
        assert_eq!(plan.pool_mem_mib, 8192 + 16384);
        // both nodes participate (inflight on each)
        assert_eq!(c.get(&a).unwrap().inflight, 1);
        assert_eq!(c.get(&b).unwrap().inflight, 1);
        c.release_stream(&plan);
        assert_eq!(c.get(&a).unwrap().inflight, 0);
    }

    #[test]
    fn banned_node_skipped() {
        let mut c = Cluster::default();
        let (a, ca) = node(8192, DeviceClass::Gpu);
        let (b, cb) = node(16384, DeviceClass::Gpu);
        c.upsert_node(a.clone(), "alice", ca);
        c.upsert_node(b.clone(), "bob", cb);
        for _ in 0..5 {
            c.record_challenge_fail(&a);
        }
        let pick = c.acquire_worker(CLUSTER_MODEL).unwrap();
        assert_eq!(pick, b);
    }

    #[test]
    fn capacity_single_model_uses_full_pool() {
        let mut c = Cluster::default();
        let (a, ca) = node(8192, DeviceClass::Gpu);
        let (b, cb) = node(16384, DeviceClass::Gpu);
        c.upsert_node(a.clone(), "alice", ca);
        c.upsert_node(b, "bob", cb);
        c.set_health(&a, false, 1.0).unwrap();
        assert!(c.account_is_donating("bob"));
        assert!(!c.account_is_donating("alice"));
        let cap = c.capacity();
        assert_eq!(cap.models_available, vec![CLUSTER_MODEL.to_string()]);
        assert_eq!(cap.nodes_healthy, 1);
    }

    #[test]
    fn full_pool_pipeline_uses_all_healthy() {
        let mut c = Cluster::default();
        for _ in 0..3 {
            let (id, caps) = node(8192, DeviceClass::Gpu);
            c.upsert_node(id, "donor", caps);
        }
        let plan = c.plan_full_pool().unwrap();
        assert_eq!(plan.model, CLUSTER_MODEL);
        assert_eq!(plan.shards.len(), 3);
        assert_eq!(plan.pool_mem_mib, 8192 * 3);
    }
}
