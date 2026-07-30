//! Distributed cluster membership, live capacity, and placement.
//!
//! The cluster is internet-wide. Nodes join over whatever path reaches the
//! control plane. We do not classify or prefer LAN vs WAN vs carrier pigeon —
//! only healthy capacity and model fit matter for placement and the dashboard.

use joule_proto::{
    ClusterCapacity, ClusterPlan, DeviceClass, NodeCaps, NodeId, ShardAssignment, ShardRole,
};
use std::collections::{BTreeSet, HashMap};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum ClusterError {
    #[error("no eligible nodes for model {0}")]
    NoEligibleNodes(String),
    #[error("unknown node {0:?}")]
    UnknownNode(NodeId),
}

#[derive(Debug, Clone)]
pub struct Node {
    pub id: NodeId,
    pub caps: NodeCaps,
    pub healthy: bool,
    pub load: f32,
}

/// In-memory cluster registry. Network transport is a later PR.
#[derive(Debug, Default)]
pub struct Cluster {
    nodes: HashMap<NodeId, Node>,
}

impl Cluster {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn upsert_node(&mut self, id: NodeId, caps: NodeCaps) {
        self.nodes.insert(
            id.clone(),
            Node {
                id,
                caps,
                healthy: true,
                load: 0.0,
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
        Ok(())
    }

    pub fn nodes(&self) -> impl Iterator<Item = &Node> {
        self.nodes.values()
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
    /// Prefer multi-node pipeline when enough healthy peers exist; else replica.
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
            // CPU fallback for lab / CI.
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
        let mut c = Cluster::new();
        let (id, caps) = node(8192, "kimi-open-q4", DeviceClass::Gpu);
        c.upsert_node(id, caps);
        let plan = c.plan_for("kimi-open-q4", true, 2).unwrap();
        assert_eq!(plan.shards.len(), 1);
        assert_eq!(plan.shards[0].role, ShardRole::Replica);
    }

    #[test]
    fn pipeline_when_enough_nodes() {
        let mut c = Cluster::new();
        for mem in [8192, 12288, 16384] {
            let (id, caps) = node(mem, "kimi-open-q4", DeviceClass::Gpu);
            c.upsert_node(id, caps);
        }
        let plan = c.plan_for("kimi-open-q4", true, 2).unwrap();
        assert_eq!(plan.shards.len(), 2);
        assert!(plan.shards.iter().all(|s| s.role == ShardRole::Pipeline));
    }

    #[test]
    fn capacity_aggregates_healthy_only_for_throughput() {
        let mut c = Cluster::new();
        let (a, ca) = node(8192, "kimi-open-q4", DeviceClass::Gpu);
        let (b, cb) = node(16384, "kimi-open-q4", DeviceClass::Gpu);
        c.upsert_node(a.clone(), ca);
        c.upsert_node(b, cb);
        c.set_health(&a, false, 1.0).unwrap();
        let cap = c.capacity();
        assert_eq!(cap.nodes_total, 2);
        assert_eq!(cap.nodes_healthy, 1);
        assert_eq!(cap.mem_mib_total, 8192 + 16384);
        assert_eq!(cap.mem_mib_healthy, 16384);
        assert_eq!(cap.throughput_class_sum, 10);
        assert_eq!(cap.models_available, vec!["kimi-open-q4".to_string()]);
    }
}
