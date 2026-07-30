//! Local mesh membership and placement (in-process for early research).
//!
//! Research mesh-first: prefer multi-node plans when enough peers exist;
//! fall back to single-node replica plans so development stays unblocked.

use joule_proto::{DeviceClass, MeshPlan, NodeCaps, NodeId, ShardAssignment, ShardRole};
use std::collections::HashMap;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum MeshError {
    #[error("no eligible nodes for model {0}")]
    NoEligibleNodes(String),
    #[error("unknown node {0:?}")]
    UnknownNode(NodeId),
}

#[derive(Debug, Clone)]
pub struct Peer {
    pub id: NodeId,
    pub caps: NodeCaps,
    pub healthy: bool,
    pub load: f32,
}

/// In-memory peer table. Network transport is a later PR.
#[derive(Debug, Default)]
pub struct Mesh {
    peers: HashMap<NodeId, Peer>,
}

impl Mesh {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn upsert_peer(&mut self, id: NodeId, caps: NodeCaps) {
        self.peers.insert(
            id.clone(),
            Peer {
                id,
                caps,
                healthy: true,
                load: 0.0,
            },
        );
    }

    pub fn set_health(&mut self, id: &NodeId, healthy: bool, load: f32) -> Result<(), MeshError> {
        let peer = self
            .peers
            .get_mut(id)
            .ok_or_else(|| MeshError::UnknownNode(id.clone()))?;
        peer.healthy = healthy;
        peer.load = load;
        Ok(())
    }

    pub fn peers(&self) -> impl Iterator<Item = &Peer> {
        self.peers.values()
    }

    /// Build a serving plan. Mesh-first policy:
    /// 1. Prefer pipeline shards across healthy GPU/Metal peers when ≥2 and stages > 1.
    /// 2. Else single best replica.
    pub fn plan_for(
        &self,
        model: &str,
        prefer_pipeline: bool,
        pipeline_stages: usize,
    ) -> Result<MeshPlan, MeshError> {
        let mut eligible: Vec<&Peer> = self
            .peers
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
                .peers
                .values()
                .filter(|p| p.healthy && p.caps.models.iter().any(|m| m == model))
                .collect();
        }

        if eligible.is_empty() {
            return Err(MeshError::NoEligibleNodes(model.to_string()));
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
            return Ok(MeshPlan {
                plan_id: Uuid::new_v4(),
                model: model.to_string(),
                shards,
            });
        }

        let best = eligible[0];
        Ok(MeshPlan {
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

    fn peer(mem: u32, model: &str) -> (NodeId, NodeCaps) {
        (
            NodeId::new(),
            NodeCaps {
                device: DeviceClass::Gpu,
                mem_mib: mem,
                throughput_class: 10,
                models: vec![model.into()],
            },
        )
    }

    #[test]
    fn single_replica_when_one_node() {
        let mut m = Mesh::new();
        let (id, caps) = peer(8192, "kimi-open-q4");
        m.upsert_peer(id, caps);
        let plan = m.plan_for("kimi-open-q4", true, 2).unwrap();
        assert_eq!(plan.shards.len(), 1);
        assert_eq!(plan.shards[0].role, ShardRole::Replica);
    }

    #[test]
    fn pipeline_when_enough_nodes() {
        let mut m = Mesh::new();
        for mem in [8192, 12288, 16384] {
            let (id, caps) = peer(mem, "kimi-open-q4");
            m.upsert_peer(id, caps);
        }
        let plan = m.plan_for("kimi-open-q4", true, 2).unwrap();
        assert_eq!(plan.shards.len(), 2);
        assert!(plan.shards.iter().all(|s| s.role == ShardRole::Pipeline));
    }
}
