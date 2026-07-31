//! Mesh peer directory (Phase A): who is alive and how to dial them.
//!
//! Control still relays today, but multiaddrs enable **direct** peer blob paths.
//! See docs/design/decentral-discovery-v0.md.

use joule_proto::NodeId;
use serde::Serialize;
use std::collections::HashMap;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Serialize)]
pub struct MeshPeer {
    pub node: NodeId,
    pub multiaddrs: Vec<String>,
    pub load: f32,
    pub healthy: bool,
    pub blob_count: u32,
    /// Self-reported claim (UI only — never used for plan/mint).
    #[serde(default)]
    pub mem_mib: u32,
    /// Protocol-verified capacity from cluster (sole plan/mint input).
    #[serde(default)]
    pub verified_mem_mib: u32,
    #[serde(default)]
    pub throughput_class: u16,
    #[serde(skip)]
    pub last_seen: Instant,
}

#[derive(Debug, Default)]
pub struct MeshDirectory {
    peers: HashMap<NodeId, MeshPeer>,
}

impl MeshDirectory {
    pub fn new() -> Self {
        Self::default()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn upsert(
        &mut self,
        node: NodeId,
        multiaddrs: Vec<String>,
        load: f32,
        healthy: bool,
        blob_count: u32,
        mem_mib: u32,
        verified_mem_mib: u32,
        throughput_class: u16,
    ) {
        self.peers.insert(
            node.clone(),
            MeshPeer {
                node,
                multiaddrs,
                load,
                healthy,
                blob_count,
                mem_mib,
                verified_mem_mib,
                throughput_class,
                last_seen: Instant::now(),
            },
        );
    }

    /// (node, **verified** mem) for healthy peers — Phase D PlanOffer input.
    /// Claim-only peers (`verified_mem_mib == 0`) are excluded.
    pub fn plan_donors(&self) -> Vec<(NodeId, u32)> {
        let mut v: Vec<_> = self
            .peers
            .values()
            .filter(|p| p.healthy && joule_cluster::placement_mem_mib(p.verified_mem_mib) > 0)
            .map(|p| {
                (
                    p.node.clone(),
                    joule_cluster::economic_mem_mib(p.verified_mem_mib),
                )
            })
            .collect();
        v.sort_by(|a, b| {
            b.1.cmp(&a.1)
                .then_with(|| a.0.to_string().cmp(&b.0.to_string()))
        });
        v
    }

    pub fn remove(&mut self, node: &NodeId) {
        self.peers.remove(node);
    }

    pub fn multiaddrs_for(&self, node: &NodeId) -> Vec<String> {
        self.peers
            .get(node)
            .map(|p| p.multiaddrs.clone())
            .unwrap_or_default()
    }

    pub fn prune_stale(&mut self, max_age: Duration) {
        let now = Instant::now();
        self.peers
            .retain(|_, p| now.duration_since(p.last_seen) < max_age);
    }

    pub fn healthy_count(&self) -> u32 {
        self.peers.values().filter(|p| p.healthy).count() as u32
    }

    pub fn list(&self) -> Vec<MeshPeer> {
        let mut v: Vec<_> = self.peers.values().cloned().collect();
        v.sort_by_key(|a| a.node.to_string());
        v
    }

    pub fn snapshot(&self) -> MeshSnapshot {
        MeshSnapshot {
            peers: self.list(),
            healthy: self.healthy_count(),
            total: self.peers.len() as u32,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MeshSnapshot {
    pub peers: Vec<MeshPeer>,
    pub healthy: u32,
    pub total: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_and_multiaddrs() {
        let mut m = MeshDirectory::new();
        let id = NodeId::new();
        m.upsert(
            id.clone(),
            vec!["tcp://127.0.0.1:7702".into()],
            0.1,
            true,
            3,
            8192, // claim
            0,    // verified — claim-only
            40,
        );
        assert_eq!(m.healthy_count(), 1);
        assert_eq!(
            m.multiaddrs_for(&id),
            vec!["tcp://127.0.0.1:7702".to_string()]
        );
        // Claim alone does not enter plan_donors
        assert!(m.plan_donors().is_empty());
        m.upsert(
            id.clone(),
            vec!["tcp://127.0.0.1:7702".into()],
            0.1,
            true,
            3,
            8192,
            4096, // verified
            40,
        );
        assert_eq!(m.plan_donors().len(), 1);
        assert_eq!(m.plan_donors()[0].1, 4096);
        m.remove(&id);
        assert_eq!(m.healthy_count(), 0);
    }
}
