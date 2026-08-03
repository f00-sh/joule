//! Pipeline activation handoff (v0): domain-separated commitments between shards.
//!
//! Non-tail shards produce an activation hex; the tail verifies upstream set before
//! full generation. This is **real handoff material on the wire**, not an empty ACK —
//! full floating-point activation tensors remain a later engine upgrade.

use joule_proto::{ClusterPlan, NodeId, ShardActivation};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub const DOMAIN_ACTIVATION: &[u8] = b"joule-pipeline-activation-v1";

/// Canonical preimage for a shard's activation commitment.
pub fn activation_preimage(
    request_id: Uuid,
    plan_id: Uuid,
    node: &NodeId,
    layer_start: u32,
    layer_end: u32,
    prompt: &str,
) -> String {
    format!(
        "joule-pipeline-activation-v1|{request_id}|{plan_id}|{node}|{layer_start}|{layer_end}|{prompt}",
        request_id = request_id,
        plan_id = plan_id,
        node = node,
        layer_start = layer_start,
        layer_end = layer_end,
        prompt = prompt.trim(),
    )
}

/// Hex SHA-256 activation commitment for a non-tail shard stage.
pub fn activation_hex(
    request_id: Uuid,
    plan_id: Uuid,
    node: &NodeId,
    layer_start: u32,
    layer_end: u32,
    prompt: &str,
) -> String {
    let pre = activation_preimage(request_id, plan_id, node, layer_start, layer_end, prompt);
    hex::encode(Sha256::digest(pre.as_bytes()))
}

/// Build activation for this node's assignment in the plan (if present).
pub fn activation_for_node(
    plan: &ClusterPlan,
    request_id: Uuid,
    node: &NodeId,
    prompt: &str,
) -> Option<ShardActivation> {
    let s = plan.shards.iter().find(|s| &s.node == node)?;
    let ls = s.layer_start.unwrap_or(0);
    let le = s.layer_end.unwrap_or(ls);
    Some(ShardActivation {
        node: node.clone(),
        layer_start: ls,
        layer_end: le,
        activation_hex: activation_hex(request_id, plan.plan_id, node, ls, le, prompt),
    })
}

/// Non-tail shards ordered by layer_start (pipeline order).
pub fn non_tail_nodes(plan: &ClusterPlan, tail: &NodeId) -> Vec<NodeId> {
    let mut v: Vec<_> = plan
        .shards
        .iter()
        .filter(|s| &s.node != tail)
        .cloned()
        .collect();
    v.sort_by_key(|s| s.layer_start.unwrap_or(0));
    v.into_iter().map(|s| s.node).collect()
}

/// Verify every non-tail shard produced a valid activation commitment.
pub fn verify_upstream_activations(
    plan: &ClusterPlan,
    request_id: Uuid,
    prompt: &str,
    tail: &NodeId,
    upstream: &[ShardActivation],
) -> Result<(), String> {
    let expected = non_tail_nodes(plan, tail);
    if expected.is_empty() {
        return Ok(()); // single-shard replica
    }
    if upstream.len() != expected.len() {
        return Err(format!(
            "pipeline activation count {} != expected non-tail {}",
            upstream.len(),
            expected.len()
        ));
    }
    for node in &expected {
        let got = upstream
            .iter()
            .find(|a| &a.node == node)
            .ok_or_else(|| format!("missing activation from shard {node}"))?;
        let shard = plan
            .shards
            .iter()
            .find(|s| &s.node == node)
            .ok_or_else(|| format!("node {node} not in plan"))?;
        let ls = shard.layer_start.unwrap_or(0);
        let le = shard.layer_end.unwrap_or(ls);
        if got.layer_start != ls || got.layer_end != le {
            return Err(format!(
                "activation layer band mismatch for {node}: got {}-{} want {ls}-{le}",
                got.layer_start, got.layer_end
            ));
        }
        let want = activation_hex(request_id, plan.plan_id, node, ls, le, prompt);
        if got.activation_hex.trim().to_ascii_lowercase() != want {
            return Err(format!("activation commitment mismatch for shard {node}"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use joule_proto::{ShardAssignment, ShardRole, CLUSTER_MODEL};

    fn plan_two() -> (ClusterPlan, NodeId, NodeId) {
        let a = NodeId::new();
        let b = NodeId::new();
        let plan = ClusterPlan {
            plan_id: Uuid::new_v4(),
            model: CLUSTER_MODEL.into(),
            pool_mem_mib: 8192,
            model_layers: 93,
            shards: vec![
                ShardAssignment {
                    node: a.clone(),
                    role: ShardRole::Pipeline,
                    layer_start: Some(0),
                    layer_end: Some(46),
                    tp_rank: None,
                    tp_world: None,
                    mem_share_mib: 4096,
                    mem_fraction_ppm: 500_000,
                },
                ShardAssignment {
                    node: b.clone(),
                    role: ShardRole::Pipeline,
                    layer_start: Some(47),
                    layer_end: Some(92),
                    tp_rank: None,
                    tp_world: None,
                    mem_share_mib: 4096,
                    mem_fraction_ppm: 500_000,
                },
            ],
        };
        (plan, a, b)
    }

    #[test]
    fn activation_roundtrip_and_verify() {
        let (plan, a, b) = plan_two();
        let rid = Uuid::new_v4();
        let prompt = "user: pipeline-handoff";
        let act = activation_for_node(&plan, rid, &a, prompt).expect("act");
        assert!(!act.activation_hex.is_empty());
        assert_eq!(act.layer_start, 0);
        assert_eq!(act.layer_end, 46);
        verify_upstream_activations(&plan, rid, prompt, &b, &[act]).expect("verify");
        let bad = ShardActivation {
            node: a.clone(),
            layer_start: 0,
            layer_end: 46,
            activation_hex: "00".repeat(32),
        };
        assert!(verify_upstream_activations(&plan, rid, prompt, &b, &[bad]).is_err());
        eprintln!("OBSERVE pipeline activation handoff verify ok");
    }
}
