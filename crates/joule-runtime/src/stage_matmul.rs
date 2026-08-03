//! Pure-Rust layer-band matmul stage (toy transformer block).
//!
//! Not full Kimi kernels — real f32 matmul over loaded safetensors so activation
//! is a **compute** of weights × state, not hash theater (JST2).
//!
//! Magic `JST3` on the wire.

use crate::stage::{activation_commitment_hex, StageOutput, StageRequest};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// Hidden width for the pure-Rust band matmul toy kernel.
///
/// Sized so lab-tiny / tiny safetensors fixtures (e.g. 4×4 f32) can participate
/// without multi-hundred-GB K3 weights.
pub const MATMUL_DIM: usize = 4;

/// Layer-band stage via pure-Rust matmul over f32 weight tensors.
///
/// - Builds a state vector from prompt + upstream + layer band.
/// - For each tensor (sorted by name), interprets leading f32s as `DIM×DIM` W
///   and optional bias, applies `state = W·state + bias` then a light nonlinearity.
/// - Packs result as `JST3` activation bytes.
///
/// Fail closed if no usable f32 weight matrix can be formed.
pub fn stage_activation_matmul(
    req: &StageRequest,
    tensors: &HashMap<String, Vec<u8>>,
) -> Result<StageOutput, String> {
    if req.layer_end < req.layer_start {
        return Err("layer_end < layer_start".into());
    }
    if req.require_upstream && req.upstream.is_empty() {
        return Err("matmul stage requires non-empty upstream when require_upstream".into());
    }

    let mut state = initial_state(req);
    let mut applied = 0u32;

    let mut names: Vec<&String> = tensors
        .keys()
        .filter(|k| k.as_str() != "__joule_armed__")
        .collect();
    names.sort();

    for name in names {
        let bytes = &tensors[name];
        let Some(w) = as_f32s(bytes) else {
            continue;
        };
        let need = MATMUL_DIM * MATMUL_DIM;
        // Tile/repeat f32 payload into a full DIM×DIM matrix when shorter
        // (still real matmul — not hash theater).
        let matrix_owned: Vec<f32> = if w.len() >= need {
            w[..need].to_vec()
        } else if w.is_empty() {
            continue;
        } else {
            (0..need).map(|i| w[i % w.len()]).collect()
        };
        let bias: Option<Vec<f32>> = if w.len() >= need + MATMUL_DIM {
            Some(w[need..need + MATMUL_DIM].to_vec())
        } else {
            None
        };
        state = matvec_add(&matrix_owned, &state, bias.as_deref());
        // Affine residual + ReLU-ish clamp so layers compose nonlinearly.
        for v in &mut state {
            *v = v.clamp(-8.0, 8.0);
            if *v < 0.0 {
                *v *= 0.1;
            }
        }
        // Band-dependent scale so layer range affects the kernel, not only the header.
        let span = req
            .layer_end
            .saturating_sub(req.layer_start)
            .saturating_add(1) as f32;
        let scale = 1.0 + (span * 0.01) + (req.layer_start as f32 * 0.001);
        for v in &mut state {
            *v *= scale;
        }
        applied = applied.saturating_add(1);
    }

    if applied == 0 {
        return Err("matmul stage: no f32 weight matrix of size DIM×DIM in loaded tensors".into());
    }

    pack_jst3(req, &state, applied)
}

fn initial_state(req: &StageRequest) -> Vec<f32> {
    let mut state = vec![0.0f32; MATMUL_DIM];
    // Prompt → floats via repeated sha blocks.
    let mut seed = Sha256::digest(req.prompt.trim().as_bytes()).to_vec();
    if !req.upstream.is_empty() {
        let mut h = Sha256::new();
        h.update(&seed);
        h.update(&req.upstream);
        seed = h.finalize().to_vec();
    }
    // Fold layer band into seed.
    seed.extend_from_slice(&req.layer_start.to_le_bytes());
    seed.extend_from_slice(&req.layer_end.to_le_bytes());
    let seed = Sha256::digest(&seed);
    for i in 0..MATMUL_DIM {
        let b0 = seed[i % 32];
        let b1 = seed[(i + 7) % 32];
        let u = u16::from_le_bytes([b0, b1]);
        state[i] = (u as f32 / 65535.0) * 2.0 - 1.0;
    }
    // Mix upstream bytes as additive f32 when present (true PP handoff).
    if req.upstream.len() >= 4 {
        let n = (req.upstream.len() / 4).min(MATMUL_DIM);
        for (i, slot) in state.iter_mut().enumerate().take(n) {
            let mut buf = [0u8; 4];
            buf.copy_from_slice(&req.upstream[i * 4..i * 4 + 4]);
            // Map raw bytes to small floats (not NaN/Inf).
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

/// `y = W·x + b` with row-major W of shape [DIM, DIM].
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
) -> Result<StageOutput, String> {
    let mut out = Vec::with_capacity(64 + state.len() * 4);
    out.extend_from_slice(b"JST3");
    out.extend_from_slice(&req.layer_start.to_le_bytes());
    out.extend_from_slice(&req.layer_end.to_le_bytes());
    out.extend_from_slice(&(MATMUL_DIM as u32).to_le_bytes());
    out.extend_from_slice(&layers_applied.to_le_bytes());
    out.extend_from_slice(&(req.upstream.len() as u32).to_le_bytes());
    for v in state {
        out.extend_from_slice(&v.to_le_bytes());
    }
    // Domain tag so protocol commitments bind to matmul path.
    let tag = Sha256::digest(b"joule-stage-matmul-v1");
    out.extend_from_slice(&tag[..8]);
    if out.len() < 48 {
        return Err("matmul activation too small".into());
    }
    let text = if req.is_tail {
        let digest = activation_commitment_hex(&out);
        Some(format!(
            "[joule-pipeline-stage:{}:L{}-{}:upstream_bytes={}:act={}:matmul] {}",
            req.model,
            req.layer_start,
            req.layer_end,
            req.upstream.len(),
            &digest[..16],
            req.prompt.trim()
        ))
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
            w[i * MATMUL_DIM + i] = scale; // diagonal
            w[MATMUL_DIM * MATMUL_DIM + i] = 0.01 * (i as f32); // bias
        }
        f32_bytes(&w)
    }

    #[test]
    fn matmul_stage_depends_on_weight_bits_and_upstream() {
        let req = StageRequest::lab("kimi-open", "matmul-hi", 0, 5, vec![], false, false);
        let mut tensors = HashMap::new();
        tensors.insert("blk.0.weight".into(), square_w(1.0));
        let a = stage_activation_matmul(&req, &tensors).unwrap();
        assert!(
            a.activation.starts_with(b"JST3"),
            "magic={}",
            a.activation[0]
        );
        // Flip one weight coefficient.
        let w2 = square_w(1.0);
        // Change W[0,0] from 1.0 to 1.5
        let alt = square_w(1.5);
        assert_ne!(w2, alt);
        tensors.insert("blk.0.weight".into(), alt);
        let b = stage_activation_matmul(&req, &tensors).unwrap();
        assert_ne!(
            a.activation, b.activation,
            "weight bit/scale change must change matmul activation"
        );
        // Upstream changes output.
        let mut req_up = req.clone();
        req_up.upstream = vec![1, 2, 3, 4, 5, 6, 7, 8];
        tensors.insert("blk.0.weight".into(), square_w(1.0));
        let c = stage_activation_matmul(&req_up, &tensors).unwrap();
        assert_ne!(
            a.activation, c.activation,
            "upstream must affect matmul state"
        );
        // No usable matrix fails closed.
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
}
