//! Distributed compute cluster membership, live capacity, and placement.
//!
//! Internet-wide volunteer pool. Nodes join the control plane over any path.
//! Only healthy capacity and model fit matter for placement and the dashboard.

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

#[derive(Debug, Clone)]
pub struct Node {
    pub id: NodeId,
    pub account: String,
    pub caps: NodeCaps,
    pub healthy: bool,
    pub load: f32,
    pub last_seen: Instant,
}

/// In-memory cluster registry fed by agent hellos/heartbeats.
#[derive(Debug)]
pub struct Cluster {
    nodes: HashMap<NodeId, Node>,
    /// Nodes without a heartbeat newer than this are marked unhealthy.
    stale_after: Duration,
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
        }
    }

    pub fn upsert_node(&mut self, id: NodeId, account: impl Into<String>, caps: NodeCaps) {
        let account = account.into();
        self.nodes.insert(
            id.clone(),
            Node {
                id,
                account,
                caps,
                healthy: true,
                load: 0.0,
                last_seen: Instant::now(),
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

    pub fn touch(&mut self, id: &NodeId) -> Result<(), ClusterError> {
        let node = self
            .nodes
            .get_mut(id)
            .ok_or_else(|| ClusterError::UnknownNode(id.clone()))?;
        node.last_seen = Instant::now();
        Ok(())
    }

    pub fn remove_node(&mut self, id: &NodeId) {
        self.nodes.remove(id);
    }

    /// Mark nodes with stale heartbeats unhealthy (still counted in totals).
    pub fn apply_staleness(&mut self) {
        let now = Instant::now();
        let stale = self.stale_after;
        for n in self.nodes.values_mut() {
            if now.duration_since(n.last_seen) > stale {
                n.healthy = false;
            }
        }
    }

    /// Drop nodes that have been stale far longer than the heartbeat window.
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

    /// True if this account has at least one healthy node right now.
    pub fn account_is_donating(&self, account: &str) -> bool {
        self.nodes
            .values()
            .any(|n| n.account == account && n.healthy)
    }

    /// Pick a healthy node that can serve `model` (lowest load, then most mem).
    pub fn pick_worker(&self, model: &str) -> Option<&Node> {
        let mut eligible: Vec<&Node> = self
            .nodes
            .values()
            .filter(|p| p.healthy && p.caps.models.iter().any(|m| m == model))
            .collect();
        if eligible.is_empty() {
            return None;
        }
        eligible.sort_by(|a, b| {
            a.load
                .partial_cmp(&b.load)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| b.caps.mem_mib.cmp(&a.caps.mem_mib))
        });
        Some(eligible[0])
    }

    /// Live aggregate for the dashboard and `GET /v1/cluster/capacity`.
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

        for n in self.nodes.values() {
            nodes_total += 1;
            mem_mib_total += u64::from(n.caps.mem_mib);
            match n.caps.device {
                DeviceClass::Gpu => nodes_gpu += 1,
                DeviceClass::Metal => nodes_metal += 1,
                DeviceClass::Cpu => nodes_cpu += 1,
            }
            if n.healthy {
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

    /// Build a serving plan across the distributed cluster.
    pub fn plan_for(
        &self,
        model: &str,
        prefer_pipeline: bool,
        pipeline_stages: usize,
    ) -> Result<ClusterPlan, ClusterError> {
        let mut eligible: Vec<&Node> = self
            .nodes
            .values()
            .filter(|p| {
                p.healthy
                    && p.caps.models.iter().any(|m| m == model)
                    && matches!(p.caps.device, DeviceClass::Gpu | DeviceClass::Metal)
            })
            .collect();

        if eligible.is_empty() {
            eligible = self
                .nodes
                .values()
                .filter(|p| p.healthy && p.caps.models.iter().any(|m| m == model))
                .collect();
        }

        if eligible.is_empty() {
            return Err(ClusterError::NoEligibleNodes(model.to_string()));
        }

        eligible.sort_by(|a, b| {
            b.caps.mem_mib.cmp(&a.caps.mem_mib).then_with(|| {
                a.load
                    .partial_cmp(&b.load)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
        });

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
    fn single_replica_when_one_node() {
        let mut c = Cluster::default();
        let (id, caps) = node(8192, "kimi-open-q4", DeviceClass::Gpu);
        c.upsert_node(id, "alice", caps);
        let plan = c.plan_for("kimi-open-q4", true, 2).unwrap();
        assert_eq!(plan.shards.len(), 1);
        assert_eq!(plan.shards[0].role, ShardRole::Replica);
    }

    #[test]
    fn pipeline_when_enough_nodes() {
        let mut c = Cluster::default();
        for mem in [8192, 12288, 16384] {
            let (id, caps) = node(mem, "kimi-open-q4", DeviceClass::Gpu);
            c.upsert_node(id, "alice", caps);
        }
        let plan = c.plan_for("kimi-open-q4", true, 2).unwrap();
        assert_eq!(plan.shards.len(), 2);
        assert!(plan.shards.iter().all(|s| s.role == ShardRole::Pipeline));
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
        let cap = c.capacity();
        assert_eq!(cap.nodes_healthy, 1);
        assert_eq!(cap.mem_mib_healthy, 16384);
    }
}
