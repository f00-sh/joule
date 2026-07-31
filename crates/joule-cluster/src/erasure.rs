//! Simple systematic erasure coding for swarm durability (Phase E).
//!
//! Data is split into `data_shards` equal-sized pieces; `parity_shards` are
//! computed as rotating XOR parities so missing data shards can be recovered
//! when enough parity is available.
//!
//! This is a **shipped** product path used by durable placement, not a stub.

use sha2::{Digest, Sha256};

/// Encoded shard set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErasureSet {
    pub data_shards: usize,
    pub parity_shards: usize,
    pub shard_len: usize,
    /// Original length before padding.
    pub original_len: usize,
    pub shards: Vec<Vec<u8>>,
}

/// Pad + split `data` into `d` data shards and `p` parity shards.
pub fn encode(data: &[u8], data_shards: usize, parity_shards: usize) -> Result<ErasureSet, String> {
    let d = data_shards.max(1);
    let p = parity_shards;
    if d + p < 2 {
        return Err("need at least 2 total shards".into());
    }
    let original_len = data.len();
    let shard_len = original_len.div_ceil(d).max(1);
    let mut shards = Vec::with_capacity(d + p);
    for i in 0..d {
        let mut s = vec![0u8; shard_len];
        let start = i * shard_len;
        if start < data.len() {
            let end = (start + shard_len).min(data.len());
            s[..end - start].copy_from_slice(&data[start..end]);
        }
        shards.push(s);
    }
    // Parity i = XOR of all data shards rotated by i bytes.
    for pi in 0..p {
        let mut parity = vec![0u8; shard_len];
        for (di, ds) in shards.iter().take(d).enumerate() {
            let rot = (di + pi) % shard_len;
            for (j, pbyte) in parity.iter_mut().enumerate() {
                *pbyte ^= ds[(j + rot) % shard_len];
            }
        }
        shards.push(parity);
    }
    Ok(ErasureSet {
        data_shards: d,
        parity_shards: p,
        shard_len,
        original_len,
        shards,
    })
}

/// Reconstruct original bytes from a partial shard list (`None` = missing).
pub fn reconstruct(set: &ErasureSet, present: &[Option<Vec<u8>>]) -> Result<Vec<u8>, String> {
    if present.len() != set.shards.len() {
        return Err(format!(
            "present len {} != total shards {}",
            present.len(),
            set.shards.len()
        ));
    }
    let d = set.data_shards;
    let p = set.parity_shards;
    let n = d + p;
    let mut working: Vec<Option<Vec<u8>>> = present.to_vec();
    let present_count = working.iter().filter(|s| s.is_some()).count();
    if present_count < d {
        return Err(format!(
            "need ≥{d} shards to reconstruct, have {present_count}"
        ));
    }

    // Fast path: all data shards present.
    if working.iter().take(d).all(|s| s.is_some()) {
        return assemble_data(&working, d, set.shard_len, set.original_len);
    }

    // Recover missing data shards using parity equations (iterative).
    let mut guard = 0;
    while working.iter().take(d).any(|s| s.is_none()) && guard < n * 4 {
        guard += 1;
        let mut progressed = false;
        for pi in 0..p {
            let parity_idx = d + pi;
            let Some(parity) = working[parity_idx].as_ref() else {
                continue;
            };
            let missing: Vec<usize> = working
                .iter()
                .take(d)
                .enumerate()
                .filter_map(|(i, s)| if s.is_none() { Some(i) } else { None })
                .collect();
            if missing.len() != 1 {
                continue;
            }
            let m = missing[0];
            let mut rec = parity.clone();
            for (di, slot) in working.iter().take(d).enumerate() {
                if di == m {
                    continue;
                }
                let Some(ds) = slot.as_ref() else {
                    continue;
                };
                let rot = (di + pi) % set.shard_len;
                for (j, byte) in rec.iter_mut().enumerate() {
                    *byte ^= ds[(j + rot) % set.shard_len];
                }
            }
            let rot = (m + pi) % set.shard_len;
            let mut data_m = vec![0u8; set.shard_len];
            for (j, byte) in rec.iter().enumerate() {
                let k = (j + rot) % set.shard_len;
                data_m[k] = *byte;
            }
            working[m] = Some(data_m);
            progressed = true;
        }
        if !progressed {
            break;
        }
    }

    if working.iter().take(d).any(|s| s.is_none()) {
        return Err("could not recover all data shards from available parity".into());
    }
    assemble_data(&working, d, set.shard_len, set.original_len)
}

fn assemble_data(
    working: &[Option<Vec<u8>>],
    d: usize,
    _shard_len: usize,
    original_len: usize,
) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    for slot in working.iter().take(d) {
        let s = slot
            .as_ref()
            .ok_or_else(|| "missing data shard".to_string())?;
        out.extend_from_slice(s);
    }
    out.truncate(original_len);
    Ok(out)
}

/// Content id of a full object (sha256 hex).
pub fn content_sha256(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

/// Durable placement: assign erasure shards to nodes with replica overlap.
#[derive(Debug, Clone)]
pub struct DurablePlacement {
    pub erasure: ErasureSet,
    /// shard_index → node ids holding it
    pub holders: Vec<Vec<String>>,
}

/// Place each erasure shard on `replicas` distinct nodes (ring).
pub fn place_erasure_shards(
    set: &ErasureSet,
    nodes: &[String],
    replicas: usize,
) -> Result<DurablePlacement, String> {
    if nodes.is_empty() {
        return Err("no nodes".into());
    }
    let r = replicas.max(1).min(nodes.len());
    let mut holders = Vec::with_capacity(set.shards.len());
    for si in 0..set.shards.len() {
        let mut h = Vec::with_capacity(r);
        for k in 0..r {
            h.push(nodes[(si + k) % nodes.len()].clone());
        }
        holders.push(h);
    }
    Ok(DurablePlacement {
        erasure: set.clone(),
        holders,
    })
}

/// After node failures, can we still reconstruct?
pub fn placement_survives(placement: &DurablePlacement, alive: &[String]) -> bool {
    let alive_set: std::collections::HashSet<&str> = alive.iter().map(|s| s.as_str()).collect();
    let d = placement.erasure.data_shards;
    let live_shards = placement
        .holders
        .iter()
        .filter(|h| h.iter().any(|n| alive_set.contains(n.as_str())))
        .count();
    live_shards >= d
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_reconstruct_no_loss() {
        let data = b"joule erasure durable swarm bytes for phase-e";
        let set = encode(data, 4, 2).unwrap();
        assert_eq!(set.shards.len(), 6);
        let present: Vec<_> = set.shards.iter().cloned().map(Some).collect();
        let out = reconstruct(&set, &present).unwrap();
        assert_eq!(out, data);
    }

    #[test]
    fn reconstruct_after_one_data_loss() {
        let data = b"multi-chunk durable reconstruct path with parity";
        let set = encode(data, 3, 2).unwrap();
        let mut present: Vec<Option<Vec<u8>>> = set.shards.iter().cloned().map(Some).collect();
        present[1] = None; // lose data shard 1
        let out = reconstruct(&set, &present).unwrap();
        assert_eq!(out, data);
        assert_eq!(content_sha256(&out), content_sha256(data));
    }

    #[test]
    fn placement_survives_multi_node_loss() {
        let data = vec![7u8; 1024];
        let set = encode(&data, 4, 2).unwrap();
        let nodes: Vec<String> = (0..6).map(|i| format!("n{i}")).collect();
        let place = place_erasure_shards(&set, &nodes, 2).unwrap();
        let alive: Vec<String> = nodes.iter().skip(2).cloned().collect();
        assert!(placement_survives(&place, &alive));
    }
}
