//! Distributed compute cluster membership, live capacity, and placement.
//!
//! Internet-wide volunteer pool. Nodes join the control plane over any path.
//! Load balancing uses inflight work + advertised load + reputation.

use joule_proto::{
    ClusterCapacity, ClusterPlan, DeviceClass, NodeCaps, NodeId, ShardAssignment, ShardRole,
};
use std::collections::{BTreeSet, HashMap};
use std::time::{Duration, Instant};
use thiserror::Error;
use uuid::Uuid;

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

    /// Scheduling score: lower is better.
    fn schedule_score(n: &Node) -> (i64, i64, i64, u64) {
        // inflight heavily weighted; load * 100; prefer high reputation (invert); then rr
        let inflight = i64::from(n.inflight) * 1000;
        let load = (n.load * 100.0) as i64;
        let rep = -((n.reputation.score() * 1000.0) as i64);
        (
            inflight + load + rep,
            -(i64::from(n.caps.mem_mib)),
            -(i64::from(n.caps.throughput_class)),
            n.rr_seq,
        )
    }

    fn eligible<'a>(&'a self, model: &str) -> Vec<&'a Node> {
        let now = Instant::now();
        self.nodes
            .values()
            .filter(|p| {
                p.healthy
                    && !p.reputation.is_banned(now)
                    && p.caps.models.iter().any(|m| m == model)
            })
            .collect()
    }

    /// Rank eligible workers (best first) without mutating.
    pub fn rank_workers(&self, model: &str) -> Vec<NodeId> {
        let mut eligible = self.eligible(model);
        eligible.sort_by(|a, b| Self::schedule_score(a).cmp(&Self::schedule_score(b)));
        eligible.into_iter().map(|n| n.id.clone()).collect()
    }

    /// Pick best worker and bump inflight + rr (call release_worker when done).
    pub fn acquire_worker(&mut self, model: &str) -> Option<NodeId> {
        let ranked = self.rank_workers(model);
        let id = ranked.into_iter().next()?;
        self.rr_counter = self.rr_counter.wrapping_add(1);
        if let Some(n) = self.nodes.get_mut(&id) {
            n.inflight = n.inflight.saturating_add(1);
            n.rr_seq = self.rr_counter;
        }
        Some(id)
    }

    /// Pick up to `n` distinct workers for multi-donor paths (primary + challenge).
    pub fn acquire_workers(&mut self, model: &str, n: usize) -> Vec<NodeId> {
        let ranked = self.rank_workers(model);
        let mut out = Vec::new();
        for id in ranked.into_iter().take(n) {
            self.rr_counter = self.rr_counter.wrapping_add(1);
            if let Some(node) = self.nodes.get_mut(&id) {
                node.inflight = node.inflight.saturating_add(1);
                node.rr_seq = self.rr_counter;
            }
            out.push(id);
        }
        out
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
        let mut models = BTreeSet::new();
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
                for m in &n.caps.models {
                    models.insert(m.clone());
                }
            }
        }

        ClusterCapacity {
            nodes_total,
            nodes_healthy,
            nodes_gpu,
            nodes_metal,
            nodes_cpu,
            mem_mib_total,
            mem_mib_healthy,
            throughput_class_sum,
            models_available: models.into_iter().collect(),
        }
    }

    pub fn plan_for(
        &self,
        model: &str,
        prefer_pipeline: bool,
        pipeline_stages: usize,
    ) -> Result<ClusterPlan, ClusterError> {
        let mut eligible: Vec<&Node> = self.eligible(model);
        if eligible.is_empty() {
            return Err(ClusterError::NoEligibleNodes(model.to_string()));
        }
        eligible.sort_by(|a, b| Self::schedule_score(a).cmp(&Self::schedule_score(b)));

        let stages = pipeline_stages.max(1);
        if prefer_pipeline && eligible.len() >= stages && stages > 1 {
            let shards = eligible
                .iter()
                .take(stages)
                .enumerate()
                .map(|(i, p)| {
                    let start = (i as u32) * 8;
                    ShardAssignment {
                        node: p.id.clone(),
                        role: ShardRole::Pipeline,
                        layer_start: Some(start),
                        layer_end: Some(start + 7),
                        tp_rank: None,
                        tp_world: None,
                    }
                })
                .collect();
            return Ok(ClusterPlan {
                plan_id: Uuid::new_v4(),
                model: model.to_string(),
                shards,
            });
        }

        let best = eligible[0];
        Ok(ClusterPlan {
            plan_id: Uuid::new_v4(),
            model: model.to_string(),
            shards: vec![ShardAssignment {
                node: best.id.clone(),
                role: ShardRole::Replica,
                layer_start: None,
                layer_end: None,
                tp_rank: None,
                tp_world: None,
            }],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use joule_proto::DeviceClass;

    fn node(mem: u32, model: &str, device: DeviceClass) -> (NodeId, NodeCaps) {
        (
            NodeId::new(),
            NodeCaps {
                device,
                mem_mib: mem,
                throughput_class: 10,
                models: vec![model.into()],
            },
        )
    }

    #[test]
    fn load_balance_prefers_less_inflight() {
        let mut c = Cluster::default();
        let (a, ca) = node(8192, "kimi-open-q4", DeviceClass::Gpu);
        let (b, cb) = node(8192, "kimi-open-q4", DeviceClass::Gpu);
        c.upsert_node(a.clone(), "alice", ca);
        c.upsert_node(b.clone(), "bob", cb);
        let first = c.acquire_worker("kimi-open-q4").unwrap();
        let second = c.acquire_worker("kimi-open-q4").unwrap();
        assert_ne!(first, second, "second pick should prefer the free donor");
        c.release_worker(&first);
        c.release_worker(&second);
    }

    #[test]
    fn banned_node_skipped() {
        let mut c = Cluster::default();
        let (a, ca) = node(8192, "kimi-open-q4", DeviceClass::Gpu);
        let (b, cb) = node(16384, "kimi-open-q4", DeviceClass::Gpu);
        c.upsert_node(a.clone(), "alice", ca);
        c.upsert_node(b.clone(), "bob", cb);
        for _ in 0..5 {
            c.record_challenge_fail(&a);
        }
        let pick = c.acquire_worker("kimi-open-q4").unwrap();
        assert_eq!(pick, b);
    }

    #[test]
    fn capacity_and_donating() {
        let mut c = Cluster::default();
        let (a, ca) = node(8192, "kimi-open-q4", DeviceClass::Gpu);
        let (b, cb) = node(16384, "kimi-open-q4", DeviceClass::Gpu);
        c.upsert_node(a.clone(), "alice", ca);
        c.upsert_node(b, "bob", cb);
        c.set_health(&a, false, 1.0).unwrap();
        assert!(c.account_is_donating("bob"));
        assert!(!c.account_is_donating("alice"));
    }
}
