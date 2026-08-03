//! Pure-Rust layer-band matmul stage (toy transformer blocks).
//!
//! - **Band-scoped select:** only tensors from preferred weight files for `[Ls,Le]`
//!   participate when source metadata is available.
//! - **Multi-layer stack:** applies `N = min(span, MAX_STACK)` matmul blocks so
//!   longer bands do more real f32 compute.
//!
//! Magic `JST3` on the wire.

use crate::stage::{activation_commitment_hex, StageOutput, StageRequest};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};

/// Hidden width for the pure-Rust band matmul toy kernel.
///
/// Sized so lab-tiny / tiny safetensors fixtures (e.g. 4×4 f32) can participate
/// without multi-hundred-GB K3 weights.
pub const MATMUL_DIM: usize = 4;

/// Cap stack depth so pathological layer spans stay bounded in CI.
pub const MAX_STACK_BLOCKS: u32 = 32;

/// Layer-band stage via pure-Rust matmul over f32 weight tensors (all tensors).
pub fn stage_activation_matmul(
    req: &StageRequest,
    tensors: &HashMap<String, Vec<u8>>,
) -> Result<StageOutput, String> {
    stage_activation_matmul_scoped(req, tensors, None, &[])
}

/// Band-scoped multi-layer matmul.
///
/// `tensor_sources`: tensor name → weight file basename.
/// `preferred_files`: basenames for this layer band (from file↔layer map).
/// When both are non-empty, only tensors whose source is preferred are used.
/// If that filter yields nothing but tensors exist (lab single-file), fall back
/// to all tensors so prepare_and_install still works.
pub fn stage_activation_matmul_scoped(
    req: &StageRequest,
    tensors: &HashMap<String, Vec<u8>>,
    tensor_sources: Option<&HashMap<String, String>>,
    preferred_files: &[String],
) -> Result<StageOutput, String> {
    if req.layer_end < req.layer_start {
        return Err("layer_end < layer_start".into());
    }
    if req.require_upstream && req.upstream.is_empty() {
        return Err("matmul stage requires non-empty upstream when require_upstream".into());
    }

    let selected = select_band_tensors(tensors, tensor_sources, preferred_files);
    if selected.is_empty() {
        return Err("matmul stage: no tensors after band-scoped select".into());
    }

    let matrices = collect_matrices(&selected);
    if matrices.is_empty() {
        return Err("matmul stage: no f32 weight matrix in selected tensors".into());
    }

    let span = req
        .layer_end
        .saturating_sub(req.layer_start)
        .saturating_add(1);
    let stack = span.clamp(1, MAX_STACK_BLOCKS);

    let mut state = initial_state(req);
    let mut applied = 0u32;

    for block in 0..stack {
        let (matrix, bias) = &matrices[(block as usize) % matrices.len()];
        // Layer-index affine so deeper stack positions compute differently
        // even when reusing the same weight matrix.
        let layer_ix = req.layer_start.saturating_add(block);
        state = matvec_add(matrix, &state, bias.as_deref());
        for v in &mut state {
            *v = v.clamp(-8.0, 8.0);
            if *v < 0.0 {
                *v *= 0.1;
            }
            // Per-block scale from absolute layer index.
            *v *= 1.0 + (layer_ix as f32) * 0.001 + (block as f32) * 0.01;
        }
        applied = applied.saturating_add(1);
    }

    pack_jst3(req, &state, applied, selected.len() as u32, &selected)
}

/// Filter tensors to those whose source file is in `preferred_files`.
pub fn select_band_tensors(
    tensors: &HashMap<String, Vec<u8>>,
    tensor_sources: Option<&HashMap<String, String>>,
    preferred_files: &[String],
) -> HashMap<String, Vec<u8>> {
    let prefer: HashSet<&str> = preferred_files.iter().map(|s| s.as_str()).collect();
    let mut out = HashMap::new();
    if prefer.is_empty() || tensor_sources.is_none() {
        for (k, v) in tensors {
            if k != "__joule_armed__" {
                out.insert(k.clone(), v.clone());
            }
        }
        return out;
    }
    let sources = tensor_sources.unwrap();
    for (k, v) in tensors {
        if k == "__joule_armed__" {
            continue;
        }
        if let Some(src) = sources.get(k) {
            if prefer.contains(src.as_str())
                || prefer
                    .iter()
                    .any(|p| src.ends_with(p) || p.ends_with(src.as_str()))
            {
                out.insert(k.clone(), v.clone());
            }
        }
    }
    // Lab / single-file: preferred K3 names don't match model.safetensors → use all.
    if out.is_empty() {
        for (k, v) in tensors {
            if k != "__joule_armed__" {
                out.insert(k.clone(), v.clone());
            }
        }
    }
    out
}

fn collect_matrices(tensors: &HashMap<String, Vec<u8>>) -> Vec<(Vec<f32>, Option<Vec<f32>>)> {
    let mut names: Vec<&String> = tensors.keys().collect();
    names.sort();
    let need = MATMUL_DIM * MATMUL_DIM;
    let mut out = Vec::new();
    for name in names {
        let Some(w) = as_f32s(&tensors[name]) else {
            continue;
        };
        if w.is_empty() {
            continue;
        }
        let matrix: Vec<f32> = if w.len() >= need {
            w[..need].to_vec()
        } else {
            (0..need).map(|i| w[i % w.len()]).collect()
        };
        let bias = if w.len() >= need + MATMUL_DIM {
            Some(w[need..need + MATMUL_DIM].to_vec())
        } else {
            None
        };
        out.push((matrix, bias));
    }
    out
}

fn initial_state(req: &StageRequest) -> Vec<f32> {
    let mut state = vec![0.0f32; MATMUL_DIM];
    let mut seed = Sha256::digest(req.prompt.trim().as_bytes()).to_vec();
    if !req.upstream.is_empty() {
        let mut h = Sha256::new();
        h.update(&seed);
        h.update(&req.upstream);
        seed = h.finalize().to_vec();
    }
    seed.extend_from_slice(&req.layer_start.to_le_bytes());
    seed.extend_from_slice(&req.layer_end.to_le_bytes());
    let seed = Sha256::digest(&seed);
    for i in 0..MATMUL_DIM {
        let b0 = seed[i % 32];
        let b1 = seed[(i + 7) % 32];
        let u = u16::from_le_bytes([b0, b1]);
        state[i] = (u as f32 / 65535.0) * 2.0 - 1.0;
    }
    if req.upstream.len() >= 4 {
        let n = (req.upstream.len() / 4).min(MATMUL_DIM);
        for (i, slot) in state.iter_mut().enumerate().take(n) {
            let mut buf = [0u8; 4];
            buf.copy_from_slice(&req.upstream[i * 4..i * 4 + 4]);
            let raw = f32::from_le_bytes(buf);
            let v = if raw.is_finite() {
                raw.clamp(-2.0, 2.0) * 0.25
            } else {
                0.0
            };
            *slot += v;
        }
    }
    state
}

fn matvec_add(w: &[f32], x: &[f32], bias: Option<&[f32]>) -> Vec<f32> {
    let mut y = vec![0.0f32; MATMUL_DIM];
    for row in 0..MATMUL_DIM {
        let mut acc = 0.0f32;
        let base = row * MATMUL_DIM;
        for col in 0..MATMUL_DIM {
            acc += w[base + col] * x[col];
        }
        if let Some(b) = bias {
            acc += b[row];
        }
        y[row] = acc;
    }
    y
}

fn as_f32s(bytes: &[u8]) -> Option<Vec<f32>> {
    if bytes.len() < 4 || bytes.len() % 4 != 0 {
        return None;
    }
    let n = bytes.len() / 4;
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let mut b = [0u8; 4];
        b.copy_from_slice(&bytes[i * 4..i * 4 + 4]);
        let v = f32::from_le_bytes(b);
        if !v.is_finite() {
            return None;
        }
        out.push(v);
    }
    Some(out)
}

fn pack_jst3(
    req: &StageRequest,
    state: &[f32],
    layers_applied: u32,
    tensors_used: u32,
    tensors: &HashMap<String, Vec<u8>>,
) -> Result<StageOutput, String> {
    let mut out = Vec::with_capacity(64 + state.len() * 4);
    out.extend_from_slice(b"JST3");
    out.extend_from_slice(&req.layer_start.to_le_bytes());
    out.extend_from_slice(&req.layer_end.to_le_bytes());
    out.extend_from_slice(&(MATMUL_DIM as u32).to_le_bytes());
    out.extend_from_slice(&layers_applied.to_le_bytes());
    out.extend_from_slice(&tensors_used.to_le_bytes());
    out.extend_from_slice(&(req.upstream.len() as u32).to_le_bytes());
    for v in state {
        out.extend_from_slice(&v.to_le_bytes());
    }
    let tag = Sha256::digest(b"joule-stage-matmul-v2-stack");
    out.extend_from_slice(&tag[..8]);
    if out.len() < 48 {
        return Err("matmul activation too small".into());
    }
    let text = if req.is_tail {
        // Prefer real decode from activation state + embeddings when present.
        if let Some(decoded) = crate::decode::generate_from_activation_state(
            &req.model,
            "band",
            tensors,
            None,
            state,
            &req.prompt,
            24,
        ) {
            let digest = activation_commitment_hex(&out);
            // Annotate with stage meta but body is activation+embedding tokens.
            Some(format!(
                "{decoded} [L{}-{}:upstream_bytes={}:act={}:matmul:stack={}]",
                req.layer_start,
                req.layer_end,
                req.upstream.len(),
                &digest[..16],
                layers_applied,
            ))
        } else {
            let digest = activation_commitment_hex(&out);
            // No embedding matrix: still surface matmul state commitment (not prompt-only).
            Some(format!(
                "[joule-decode-act:{}/matmul stack={} act={}] {}",
                req.model,
                layers_applied,
                &digest[..16],
                req.prompt.trim()
            ))
        }
    } else {
        None
    };
    let completion_tokens = text
        .as_ref()
        .map(|t| t.split_whitespace().count() as u32)
        .unwrap_or(0);
    Ok(StageOutput {
        activation: out,
        text,
        prompt_tokens: req.prompt.split_whitespace().count() as u32,
        completion_tokens,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f32_bytes(vals: &[f32]) -> Vec<u8> {
        let mut out = Vec::with_capacity(vals.len() * 4);
        for v in vals {
            out.extend_from_slice(&v.to_le_bytes());
        }
        out
    }

    fn square_w(scale: f32) -> Vec<u8> {
        let mut w = vec![0.0f32; MATMUL_DIM * MATMUL_DIM + MATMUL_DIM];
        for i in 0..MATMUL_DIM {
            w[i * MATMUL_DIM + i] = scale;
            w[MATMUL_DIM * MATMUL_DIM + i] = 0.01 * (i as f32);
        }
        f32_bytes(&w)
    }

    #[test]
    fn matmul_stage_depends_on_weight_bits_and_upstream() {
        let req = StageRequest::lab("kimi-open", "matmul-hi", 0, 5, vec![], false, false);
        let mut tensors = HashMap::new();
        tensors.insert("blk.0.weight".into(), square_w(1.0));
        let a = stage_activation_matmul(&req, &tensors).unwrap();
        assert!(a.activation.starts_with(b"JST3"));
        tensors.insert("blk.0.weight".into(), square_w(1.5));
        let b = stage_activation_matmul(&req, &tensors).unwrap();
        assert_ne!(a.activation, b.activation);
        let mut req_up = req.clone();
        req_up.upstream = vec![1, 2, 3, 4, 5, 6, 7, 8];
        tensors.insert("blk.0.weight".into(), square_w(1.0));
        let c = stage_activation_matmul(&req_up, &tensors).unwrap();
        assert_ne!(a.activation, c.activation);
        let mut empty = HashMap::new();
        empty.insert("tiny".into(), vec![1u8, 2, 3]);
        assert!(stage_activation_matmul(&req, &empty).is_err());
        eprintln!(
            "OBSERVE matmul-stage: jst3_a={} jst3_b={} jst3_up={} ok",
            a.activation.len(),
            b.activation.len(),
            c.activation.len()
        );
    }

    #[test]
    fn band_scoped_select_and_stack_depth() {
        let mut tensors = HashMap::new();
        tensors.insert("a.w".into(), square_w(1.0));
        tensors.insert("b.w".into(), square_w(2.0));
        let mut sources = HashMap::new();
        sources.insert("a.w".into(), "model-00001-of-00016.safetensors".into());
        sources.insert("b.w".into(), "model-00008-of-00016.safetensors".into());

        // Prefer only file 1 → only a.w
        let pref = vec!["model-00001-of-00016.safetensors".into()];
        let sel = select_band_tensors(&tensors, Some(&sources), &pref);
        assert_eq!(sel.len(), 1);
        assert!(sel.contains_key("a.w"));
        assert!(!sel.contains_key("b.w"));

        // Narrow band: 1 layer → stack 1; wide band → more blocks.
        let narrow = StageRequest::lab("kimi-open", "stack", 0, 0, vec![], false, false);
        let wide = StageRequest::lab("kimi-open", "stack", 0, 11, vec![], false, false);
        let only_a: HashMap<_, _> = tensors
            .iter()
            .filter(|(k, _)| *k == "a.w")
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let n = stage_activation_matmul_scoped(&narrow, &only_a, Some(&sources), &pref).unwrap();
        let w = stage_activation_matmul_scoped(&wide, &only_a, Some(&sources), &pref).unwrap();
        assert_ne!(
            n.activation, w.activation,
            "longer band must apply deeper matmul stack"
        );
        // layers_applied is at offset 4+4+4+4 = 16 after magic (4) + ls + le + dim
        let n_applied = u32::from_le_bytes(n.activation[16..20].try_into().unwrap());
        let w_applied = u32::from_le_bytes(w.activation[16..20].try_into().unwrap());
        assert_eq!(n_applied, 1);
        assert_eq!(w_applied, 12);
        eprintln!(
            "OBSERVE band-stack: select_n={} narrow_blocks={} wide_blocks={}",
            sel.len(),
            n_applied,
            w_applied
        );
    }
}
