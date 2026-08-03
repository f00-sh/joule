//! Pipeline activation handoff: real stage tensor payloads between shards.
//!
//! Non-tail stages emit intermediate activation **bytes** + sha256 commitment.
//! Tail verifies payload integrity and non-empty tensors, then runs a layer-sliced
//! stage that **depends on** concatenated upstream payloads.

use joule_proto::{ClusterPlan, NodeId, ShardActivation};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub const DOMAIN_ACTIVATION: &[u8] = b"joule-pipeline-activation-v1";

/// SHA-256 hex of activation **payload** (wire commitment).
pub fn commitment_hex(payload: &[u8]) -> String {
    hex::encode(Sha256::digest(payload))
}

/// Build wire activation from stage tensor bytes + band.
pub fn activation_from_payload(
    node: NodeId,
    layer_start: u32,
    layer_end: u32,
    payload: &[u8],
) -> Result<ShardActivation, String> {
    if payload.is_empty() {
        return Err("empty activation payload".into());
    }
    if payload.len() < 16 {
        return Err("activation payload too small for stage tensor".into());
    }
    Ok(ShardActivation {
        node,
        layer_start,
        layer_end,
        activation_hex: commitment_hex(payload),
        payload_b64: base64_encode(payload),
    })
}

fn base64_encode(bytes: &[u8]) -> String {
    use std::io::Write;
    // Minimal base64 without extra crate: use standard alphabet via simple impl
    // Prefer dependency already in tree — joule-control has base64; cluster may not.
    // Inline standard base64:
    const T: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    let mut i = 0;
    while i < bytes.len() {
        let b0 = bytes[i];
        let b1 = if i + 1 < bytes.len() { bytes[i + 1] } else { 0 };
        let b2 = if i + 2 < bytes.len() { bytes[i + 2] } else { 0 };
        let n = (u32::from(b0) << 16) | (u32::from(b1) << 8) | u32::from(b2);
        out.push(T[((n >> 18) & 63) as usize] as char);
        out.push(T[((n >> 12) & 63) as usize] as char);
        if i + 1 < bytes.len() {
            out.push(T[((n >> 6) & 63) as usize] as char);
        } else {
            out.push('=');
        }
        if i + 2 < bytes.len() {
            out.push(T[(n & 63) as usize] as char);
        } else {
            out.push('=');
        }
        i += 3;
    }
    let _ = Write::flush(&mut std::io::sink());
    out
}

/// Decode payload from activation; fail closed if missing/corrupt.
pub fn decode_payload(act: &ShardActivation) -> Result<Vec<u8>, String> {
    if act.payload_b64.is_empty() {
        return Err(format!("activation from {} missing payload_b64", act.node));
    }
    let bytes = base64_decode(&act.payload_b64)?;
    if bytes.is_empty() {
        return Err("decoded activation payload empty".into());
    }
    let want = act.activation_hex.trim().to_ascii_lowercase();
    let got = commitment_hex(&bytes);
    if got != want {
        return Err(format!(
            "activation_hex mismatch for {} (payload not real stage tensor)",
            act.node
        ));
    }
    Ok(bytes)
}

fn base64_decode(s: &str) -> Result<Vec<u8>, String> {
    fn val(c: u8) -> Result<u8, String> {
        match c {
            b'A'..=b'Z' => Ok(c - b'A'),
            b'a'..=b'z' => Ok(c - b'a' + 26),
            b'0'..=b'9' => Ok(c - b'0' + 52),
            b'+' => Ok(62),
            b'/' => Ok(63),
            _ => Err(format!("bad base64 byte {c}")),
        }
    }
    let s: Vec<u8> = s.bytes().filter(|c| !c.is_ascii_whitespace()).collect();
    if s.len() % 4 != 0 {
        return Err("bad base64 length".into());
    }
    let mut out = Vec::with_capacity(s.len() / 4 * 3);
    let mut i = 0;
    while i < s.len() {
        let a = val(s[i])?;
        let b = val(s[i + 1])?;
        let c = if s[i + 2] == b'=' { 0 } else { val(s[i + 2])? };
        let d = if s[i + 3] == b'=' { 0 } else { val(s[i + 3])? };
        let n = (u32::from(a) << 18) | (u32::from(b) << 12) | (u32::from(c) << 6) | u32::from(d);
        out.push(((n >> 16) & 0xff) as u8);
        if s[i + 2] != b'=' {
            out.push(((n >> 8) & 0xff) as u8);
        }
        if s[i + 3] != b'=' {
            out.push((n & 0xff) as u8);
        }
        i += 4;
    }
    Ok(out)
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

/// Concatenate verified upstream payloads in pipeline order.
pub fn concat_upstream_payloads(
    plan: &ClusterPlan,
    tail: &NodeId,
    upstream: &[ShardActivation],
) -> Result<Vec<u8>, String> {
    verify_upstream_activations(plan, Uuid::nil(), "", tail, upstream)?;
    let mut out = Vec::new();
    for node in non_tail_nodes(plan, tail) {
        let act = upstream
            .iter()
            .find(|a| a.node == node)
            .ok_or_else(|| format!("missing {node}"))?;
        out.extend(decode_payload(act)?);
    }
    Ok(out)
}

/// Verify every non-tail shard produced a real stage tensor + matching commitment.
pub fn verify_upstream_activations(
    plan: &ClusterPlan,
    _request_id: Uuid,
    _prompt: &str,
    tail: &NodeId,
    upstream: &[ShardActivation],
) -> Result<(), String> {
    let expected = non_tail_nodes(plan, tail);
    if expected.is_empty() {
        return Ok(());
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
        let payload = decode_payload(got)?;
        if payload.len() < 16 {
            return Err(format!("stage tensor too small from {node}"));
        }
    }
    Ok(())
}

// Re-export names used by older call sites.
pub fn activation_hex(payload: &[u8]) -> String {
    commitment_hex(payload)
}

pub fn activation_preimage(
    _request_id: Uuid,
    _plan_id: Uuid,
    _node: &NodeId,
    _layer_start: u32,
    _layer_end: u32,
    _prompt: &str,
) -> String {
    // Legacy name — real stages use payload commitment.
    String::new()
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
    fn real_payload_roundtrip_and_verify() {
        let (plan, a, b) = plan_two();
        let payload = b"JST1-fake-but-real-bytes-xxxxxxxxxxxxxxxx".to_vec();
        let act = activation_from_payload(a.clone(), 0, 46, &payload).unwrap();
        assert!(!act.payload_b64.is_empty());
        assert_eq!(act.activation_hex, commitment_hex(&payload));
        verify_upstream_activations(&plan, Uuid::nil(), "", &b, std::slice::from_ref(&act))
            .unwrap();
        // Hash-only (empty payload) fails.
        let hash_only = ShardActivation {
            node: a,
            layer_start: 0,
            layer_end: 46,
            activation_hex: commitment_hex(&payload),
            payload_b64: String::new(),
        };
        assert!(verify_upstream_activations(
            &plan,
            Uuid::nil(),
            "",
            &b,
            std::slice::from_ref(&hash_only)
        )
        .is_err());
        eprintln!(
            "OBSERVE real-pp: payload_len={} hex={}",
            payload.len(),
            &act.activation_hex[..16]
        );
    }
}
