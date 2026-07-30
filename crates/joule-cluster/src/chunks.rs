//! Redundant, content-addressed model chunk placement.
//!
//! **No client needs the full model.** Each node stores a subset of chunks.
//! **Redundancy:** each chunk is assigned to `replica_factor` distinct nodes
//! (primary + overlapping replicas) so dropouts do not lose the model.
//!
//! See docs/design/broadcast-v0.md § model download / chunks.

use joule_proto::NodeId;
use serde::{Deserialize, Serialize};

/// Default: every chunk on 2 nodes (survive one dropout).
pub const DEFAULT_REPLICA_FACTOR: u32 = 2;

/// One content-addressed piece of a model (file / layer range).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelChunk {
    pub index: u32,
    pub path: String,
    pub sha256: String,
    pub size: u64,
    pub layer_start: u32,
    pub layer_end: u32,
}

/// Why this node holds a chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChunkRole {
    /// Primary holder for inference placement.
    Primary,
    /// Overlapping replica for survival when primary drops.
    Replica,
}

/// One chunk assigned to one node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkHold {
    pub chunk_index: u32,
    pub sha256: String,
    pub path: String,
    pub size: u64,
    pub layer_start: u32,
    pub layer_end: u32,
    pub role: ChunkRole,
}

/// Full assignment map for the pool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedundantChunkPlan {
    pub replica_factor: u32,
    pub chunk_count: u32,
    pub node_count: u32,
    /// Per-chunk list of holder node ids (primary first).
    pub holders: Vec<Vec<NodeId>>,
    /// Per-node list of chunks they must fetch/store.
    pub by_node: Vec<NodeChunkPlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeChunkPlan {
    pub node: NodeId,
    pub verified_mem_mib: u32,
    pub holds: Vec<ChunkHold>,
    /// Sum of sizes this node should store.
    pub total_bytes: u64,
}

/// Assign chunks to nodes with ring-style overlapping replicas.
///
/// For chunk `i` and replica factor `R` over `N` nodes (N ≥ 1):
/// holders = `node[(i + k) % N]` for `k = 0..min(R,N)`
/// with primary = k=0.
///
/// Nodes are ordered **stable** by descending verified mem, then node id —
/// so larger cards get more primary weight when chunk count is high
/// (they also appear more often if we later weight the ring; v0 is uniform ring
/// for simplicity + guaranteed overlap).
///
/// **Invariant:** every chunk has `min(R, N)` holders. Losing fewer than
/// `min(R,N)` nodes cannot erase any chunk (as long as remaining holders stay up).
pub fn plan_redundant_chunks(
    nodes: &[(NodeId, u32)],
    chunks: &[ModelChunk],
    replica_factor: u32,
) -> Result<RedundantChunkPlan, String> {
    if chunks.is_empty() {
        return Err("no chunks".into());
    }
    if nodes.is_empty() {
        return Err("no nodes".into());
    }
    let r = replica_factor.max(1) as usize;
    let n = nodes.len();
    let r_eff = r.min(n);

    // Stable order: largest verified mem first.
    let mut ordered: Vec<(NodeId, u32)> = nodes.to_vec();
    ordered.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0 .0.cmp(&b.0 .0)));

    let mut holders: Vec<Vec<NodeId>> = Vec::with_capacity(chunks.len());
    let mut by_node_holds: Vec<Vec<ChunkHold>> = vec![Vec::new(); n];

    for (ci, ch) in chunks.iter().enumerate() {
        let mut hlist = Vec::with_capacity(r_eff);
        for k in 0..r_eff {
            let ni = (ci + k) % n;
            let role = if k == 0 {
                ChunkRole::Primary
            } else {
                ChunkRole::Replica
            };
            hlist.push(ordered[ni].0.clone());
            by_node_holds[ni].push(ChunkHold {
                chunk_index: ch.index,
                sha256: ch.sha256.clone(),
                path: ch.path.clone(),
                size: ch.size,
                layer_start: ch.layer_start,
                layer_end: ch.layer_end,
                role,
            });
        }
        holders.push(hlist);
    }

    let by_node: Vec<NodeChunkPlan> = ordered
        .iter()
        .enumerate()
        .map(|(i, (id, mem))| {
            let holds = by_node_holds[i].clone();
            let total_bytes = holds.iter().map(|h| h.size).sum();
            NodeChunkPlan {
                node: id.clone(),
                verified_mem_mib: *mem,
                holds,
                total_bytes,
            }
        })
        .collect();

    Ok(RedundantChunkPlan {
        replica_factor: r_eff as u32,
        chunk_count: chunks.len() as u32,
        node_count: n as u32,
        holders,
        by_node,
    })
}

/// Digests one node must obtain (unique sha256 list).
pub fn required_digests_for_node(plan: &RedundantChunkPlan, node: &NodeId) -> Vec<String> {
    plan.by_node
        .iter()
        .find(|p| &p.node == node)
        .map(|p| {
            let mut v: Vec<String> = p.holds.iter().map(|h| h.sha256.clone()).collect();
            v.sort();
            v.dedup();
            v
        })
        .unwrap_or_default()
}

/// True if every chunk still has at least one holder in `alive`.
pub fn plan_survives(plan: &RedundantChunkPlan, alive: &[NodeId]) -> bool {
    let set: std::collections::HashSet<&NodeId> = alive.iter().collect();
    plan.holders
        .iter()
        .all(|h| h.iter().any(|n| set.contains(n)))
}

/// How many live holders each chunk has (for rebalance triggers).
pub fn live_replica_counts(plan: &RedundantChunkPlan, alive: &[NodeId]) -> Vec<u32> {
    let set: std::collections::HashSet<&NodeId> = alive.iter().collect();
    plan.holders
        .iter()
        .map(|h| h.iter().filter(|n| set.contains(n)).count() as u32)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn nid() -> NodeId {
        NodeId(Uuid::new_v4())
    }

    fn chunks(n: u32) -> Vec<ModelChunk> {
        (0..n)
            .map(|i| ModelChunk {
                index: i,
                path: format!("c{i}.st"),
                sha256: format!("{:064x}", i + 1),
                size: 1000 * (i as u64 + 1),
                layer_start: i * 10,
                layer_end: i * 10 + 9,
            })
            .collect()
    }

    #[test]
    fn each_chunk_has_r_holders() {
        let nodes: Vec<_> = (0..5).map(|_| (nid(), 8192u32)).collect();
        let ch = chunks(8);
        let plan = plan_redundant_chunks(&nodes, &ch, 2).unwrap();
        assert_eq!(plan.replica_factor, 2);
        for h in &plan.holders {
            assert_eq!(h.len(), 2);
            assert_ne!(h[0], h[1]);
        }
    }

    #[test]
    fn no_node_holds_everything_when_many_chunks() {
        let nodes: Vec<_> = (0..4).map(|_| (nid(), 8192u32)).collect();
        let ch = chunks(12);
        let plan = plan_redundant_chunks(&nodes, &ch, 2).unwrap();
        for np in &plan.by_node {
            assert!(
                np.holds.len() < ch.len(),
                "node should not store all {} chunks, has {}",
                ch.len(),
                np.holds.len()
            );
        }
    }

    #[test]
    fn survives_one_dropout_with_r2() {
        let nodes: Vec<_> = (0..4).map(|_| (nid(), 8192u32)).collect();
        let ch = chunks(6);
        let plan = plan_redundant_chunks(&nodes, &ch, 2).unwrap();
        // Drop first node
        let alive: Vec<_> = nodes.iter().skip(1).map(|(id, _)| id.clone()).collect();
        assert!(plan_survives(&plan, &alive));
        let counts = live_replica_counts(&plan, &alive);
        assert!(counts.iter().all(|&c| c >= 1));
    }

    #[test]
    fn required_digests_unique() {
        let a = nid();
        let b = nid();
        let nodes = vec![(a.clone(), 8192), (b, 8192)];
        let ch = chunks(4);
        let plan = plan_redundant_chunks(&nodes, &ch, 2).unwrap();
        let d = required_digests_for_node(&plan, &a);
        let mut sorted = d.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(d.len(), sorted.len());
    }
}
