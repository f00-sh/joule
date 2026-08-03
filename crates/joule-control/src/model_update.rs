//! Apply signed model_update bodies: plan redundant chunks, tell each agent what to fetch.

use crate::app::App;
use joule_cluster::{
    order_digests_for_layer_fetch, plan_redundant_chunks, preferred_weight_files,
    required_digests_for_node, ModelChunk,
};
use joule_proto::{Envelope, Message, NodeId, OperatorKind, SignedEnvelope};
use serde::Deserialize;
use tracing::{info, warn};

#[derive(Debug, Deserialize)]
pub struct ModelUpdateBody {
    #[serde(default)]
    pub model_id: String,
    #[serde(default = "default_r")]
    pub replica_factor: u32,
    #[serde(default)]
    pub chunks: Vec<ChunkSpec>,
    /// Alternate layout: quants[0].files
    #[serde(default)]
    pub quants: Vec<QuantSpecBody>,
}

#[derive(Debug, Deserialize)]
pub struct QuantSpecBody {
    #[serde(default)]
    pub files: Vec<ChunkSpec>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChunkSpec {
    #[serde(default)]
    pub index: u32,
    #[serde(default)]
    pub path: String,
    pub sha256: String,
    #[serde(default)]
    pub size: u64,
    #[serde(default)]
    pub layer_start: u32,
    #[serde(default)]
    pub layer_end: u32,
}

fn default_r() -> u32 {
    2
}

pub fn parse_model_chunks(body_json: &str) -> Result<(Vec<ModelChunk>, u32), String> {
    let body: ModelUpdateBody =
        serde_json::from_str(body_json).map_err(|e| format!("model_update json: {e}"))?;
    let mut specs = body.chunks;
    if specs.is_empty() {
        if let Some(q) = body.quants.first() {
            specs = q.files.clone();
        }
    }
    if specs.is_empty() {
        return Err("model_update has no chunks/files".into());
    }
    let chunks: Vec<ModelChunk> = specs
        .into_iter()
        .enumerate()
        .map(|(i, c)| ModelChunk {
            index: if c.index != 0 { c.index } else { i as u32 },
            path: if c.path.is_empty() {
                format!("chunk-{i:03}")
            } else {
                c.path
            },
            sha256: c.sha256.to_lowercase(),
            size: c.size,
            layer_start: c.layer_start,
            layer_end: c.layer_end,
        })
        .collect();
    Ok((chunks, body.replica_factor.max(1)))
}

/// After a verified model_update broadcast: store plan + send FetchDigests per node.
pub async fn apply_model_update(app: &App, envelope: &SignedEnvelope) {
    if envelope.kind != OperatorKind::ModelUpdate {
        return;
    }
    let (chunks, r) = match parse_model_chunks(&envelope.body_json) {
        Ok(v) => v,
        Err(e) => {
            warn!(error = %e, "model_update parse failed");
            return;
        }
    };

    let nodes: Vec<(NodeId, u32)> = {
        let g = app.state.read().await;
        g.cluster
            .nodes()
            .filter(|n| n.healthy && joule_cluster::placement_mem_mib(n.verified_mem_mib) > 0)
            .map(|n| {
                (
                    n.id.clone(),
                    joule_cluster::placement_mem_mib(n.verified_mem_mib),
                )
            })
            .collect()
    };
    if nodes.is_empty() {
        warn!("model_update: no healthy nodes with verified capacity");
        return;
    }

    let plan = match plan_redundant_chunks(&nodes, &chunks, r) {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "chunk plan failed");
            return;
        }
    };

    {
        let mut g = app.state.write().await;
        g.active_chunks = chunks;
        g.active_replica_factor = plan.replica_factor;
    }

    let model_hint = serde_json::from_str::<ModelUpdateBody>(&envelope.body_json)
        .ok()
        .map(|b| b.model_id)
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "model".into());
    info!(
        model = %model_hint,
        chunks = plan.chunk_count,
        nodes = plan.node_count,
        replica_factor = plan.replica_factor,
        "model_update: assigning digests (no full-model download)"
    );

    let routes = app.routes.lock().await;
    for np in &plan.by_node {
        let digests = required_digests_for_node(&plan, &np.node);
        if digests.is_empty() {
            continue;
        }
        // Layer-aware fetch preference: donor's assigned chunk layer union →
        // preferred K3 weight files first (file↔layer map call site).
        let digests = order_node_digests_by_layer_preference(np, &digests);
        if let Some(tx) = routes.get(&np.node) {
            let msg = Message::FetchDigests {
                digests,
                reason: format!("model_update:{}", envelope.id),
                replica_factor: plan.replica_factor,
            };
            let _ = tx.send(Envelope::new(np.node.clone(), msg));
        }
    }
}

/// Reorder FetchDigests so weight files intersecting the node's hold layer band come first.
fn order_node_digests_by_layer_preference(
    np: &joule_cluster::NodeChunkPlan,
    digests: &[String],
) -> Vec<String> {
    if np.holds.is_empty() {
        return digests.to_vec();
    }
    let ls = np.holds.iter().map(|h| h.layer_start).min().unwrap_or(0);
    let le = np
        .holds
        .iter()
        .map(|h| h.layer_end)
        .max()
        .unwrap_or(ls)
        .min(joule_cluster::K3_MODEL_LAYERS.saturating_sub(1));
    let pairs: Vec<(String, String)> = np
        .holds
        .iter()
        .map(|h| (h.path.clone(), h.sha256.clone()))
        .collect();
    match order_digests_for_layer_fetch(ls, le, &pairs) {
        Ok(ordered) if !ordered.is_empty() => {
            // Keep any digests not in holds at the end.
            let mut out = ordered;
            for d in digests {
                if !out.iter().any(|x| x == d) {
                    out.push(d.clone());
                }
            }
            let _ = preferred_weight_files(ls, le); // exercised call site for map
            out
        }
        _ => digests.to_vec(),
    }
}

/// Periodic: for under-replicated digests, ask non-seeders to fetch.
pub async fn rebalance_replicas(app: &App) {
    let (under, r, all_nodes) = {
        let g = app.state.read().await;
        if g.active_chunks.is_empty() {
            return;
        }
        let r = g.active_replica_factor.max(1);
        let under: Vec<(String, u32)> = g
            .active_chunks
            .iter()
            .map(|c| {
                let n = g.blobs.seeder_count(&c.sha256);
                (c.sha256.clone(), n)
            })
            .filter(|(_, n)| *n < r)
            .collect();
        let all: Vec<NodeId> = g
            .cluster
            .nodes()
            .filter(|n| n.healthy)
            .map(|n| n.id.clone())
            .collect();
        (under, r, all)
    };
    if under.is_empty() {
        return;
    }

    let routes = app.routes.lock().await;
    for (hash, have) in under {
        let g = app.state.read().await;
        let candidates = g.blobs.non_seeders(&hash, &all_nodes);
        drop(g);
        // Ask up to (r - have) candidates to pull this digest.
        let need = r.saturating_sub(have) as usize;
        for node in candidates.into_iter().take(need) {
            if let Some(tx) = routes.get(&node) {
                let msg = Message::FetchDigests {
                    digests: vec![hash.clone()],
                    reason: format!("rebalance:need>={r}"),
                    replica_factor: r,
                };
                let _ = tx.send(Envelope::new(node.clone(), msg));
                info!(%node, %hash, "rebalance: asked node to fetch replica");
            }
        }
    }
}
