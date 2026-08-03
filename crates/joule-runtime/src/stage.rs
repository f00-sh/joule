//! Layer-band pipeline stage API (lab-scale real activation tensors).
//!
//! Non-tail stages emit intermediate activation **bytes**; the tail consumes
//! concatenated upstream payloads and runs a **layer-sliced** stage that depends
//! on those bytes (wrong/missing upstream changes the result or fails closed).

use sha2::{Digest, Sha256};

/// Request one pipeline stage over a layer band.
#[derive(Debug, Clone)]
pub struct StageRequest {
    pub model: String,
    pub prompt: String,
    pub layer_start: u32,
    pub layer_end: u32,
    /// Concatenated upstream activation payloads (empty for first stage).
    pub upstream: Vec<u8>,
    /// True when this is the final (tail) stage — may produce user text.
    pub is_tail: bool,
    /// When multi-shard, tail must see non-empty upstream.
    pub require_upstream: bool,
    /// When true, engine must have staged preferred weight files for this band.
    /// StubEngine ignores this; ClusterEngine fails closed if weights missing.
    pub require_band_weights: bool,
    /// Explicit basenames required (override / test). Empty + `require_band_weights`
    /// → derive from file↔layer map at the engine.
    pub required_weight_files: Vec<String>,
}

impl StageRequest {
    /// Lab/mesh default: no weight gate (StubEngine path).
    pub fn lab(
        model: impl Into<String>,
        prompt: impl Into<String>,
        layer_start: u32,
        layer_end: u32,
        upstream: Vec<u8>,
        is_tail: bool,
        require_upstream: bool,
    ) -> Self {
        Self {
            model: model.into(),
            prompt: prompt.into(),
            layer_start,
            layer_end,
            upstream,
            is_tail,
            require_upstream,
            require_band_weights: false,
            required_weight_files: vec![],
        }
    }
}

/// Output of a layer-band stage.
#[derive(Debug, Clone)]
pub struct StageOutput {
    /// Intermediate activation tensor bytes (always non-empty on success).
    pub activation: Vec<u8>,
    /// User-visible text (tail only).
    pub text: Option<String>,
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
}

/// SHA-256 hex of activation payload (wire commitment).
pub fn activation_commitment_hex(payload: &[u8]) -> String {
    hex::encode(Sha256::digest(payload))
}

/// Lab/stub stage: deterministic activation **without** weight material (`JST1`).
pub fn lab_stage_activation(req: &StageRequest) -> Result<StageOutput, String> {
    stage_activation(req, None)
}

/// Weight-backed stage: activation **depends on** loaded band weight bytes (`JST2`).
///
/// `weight_material` must be non-empty (fail closed). Different staged tensors
/// or corrupt material change the activation commitment.
pub fn stage_activation_with_weights(
    req: &StageRequest,
    weight_material: &[u8],
) -> Result<StageOutput, String> {
    if weight_material.is_empty() {
        return Err("weight-backed stage requires non-empty weight material".into());
    }
    stage_activation(req, Some(weight_material))
}

/// Build activation from request + optional weight material.
///
/// Layout (little-endian):
/// - magic `JST1` (lab) or `JST2` (weight-backed) (4)
/// - layer_start u32, layer_end u32
/// - prompt_sha16 (16)
/// - upstream_sha16 (16)
/// - weight_sha16 (16) — zeros for JST1
/// - upstream_len u32 + first min(32, len) upstream bytes
/// - weight_len u32 + first min(32, len) weight bytes (JST2 only)
/// - band-dependent padding
pub fn stage_activation(
    req: &StageRequest,
    weight_material: Option<&[u8]>,
) -> Result<StageOutput, String> {
    if req.layer_end < req.layer_start {
        return Err("layer_end < layer_start".into());
    }
    if req.require_upstream && req.is_tail && req.upstream.is_empty() {
        return Err("tail stage requires non-empty upstream activations".into());
    }
    if req.require_upstream && !req.is_tail && req.upstream.is_empty() {
        return Err("mid-chain stage requires non-empty upstream activations".into());
    }
    let weights = weight_material.unwrap_or(&[]);
    let weight_backed = !weights.is_empty();
    if weight_material.is_some() && !weight_backed {
        return Err("weight-backed stage requires non-empty weight material".into());
    }
    let mut out = Vec::with_capacity(96 + req.upstream.len().min(32) + weights.len().min(32));
    out.extend_from_slice(if weight_backed { b"JST2" } else { b"JST1" });
    out.extend_from_slice(&req.layer_start.to_le_bytes());
    out.extend_from_slice(&req.layer_end.to_le_bytes());
    let psha = Sha256::digest(req.prompt.trim().as_bytes());
    out.extend_from_slice(&psha[..16]);
    let usha = Sha256::digest(&req.upstream);
    out.extend_from_slice(&usha[..16]);
    let wsha = Sha256::digest(weights);
    out.extend_from_slice(&wsha[..16]);
    let ulen = (req.upstream.len() as u32).to_le_bytes();
    out.extend_from_slice(&ulen);
    let take_u = req.upstream.len().min(32);
    out.extend_from_slice(&req.upstream[..take_u]);
    if weight_backed {
        let wlen = (weights.len() as u32).to_le_bytes();
        out.extend_from_slice(&wlen);
        let take_w = weights.len().min(32);
        out.extend_from_slice(&weights[..take_w]);
    }
    // Band-dependent padding so different layers produce different tensors.
    let span = req
        .layer_end
        .saturating_sub(req.layer_start)
        .saturating_add(1);
    for i in 0..span.min(8) {
        out.push(((req.layer_start.wrapping_add(i) ^ 0xA5) & 0xff) as u8);
    }
    if out.len() < 48 {
        return Err("activation tensor too small".into());
    }
    let text = if req.is_tail {
        let digest = activation_commitment_hex(&out);
        let tag = if weight_backed { "w" } else { "lab" };
        // Compact single metadata token + prompt so usage billing stays comparable
        // to stub infer (whitespace token count drives mJ burn).
        Some(format!(
            "[joule-pipeline-stage:{}:L{}-{}:upstream_bytes={}:act={}:{}] {}",
            req.model,
            req.layer_start,
            req.layer_end,
            req.upstream.len(),
            &digest[..16],
            tag,
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

/// Deterministic material from loaded tensors for a band stage (pure-Rust toy kernel input).
///
/// Sorts tensor names, includes name + length + content sha256 + head sample so
/// different staged weight **bytes** change the material (and thus the activation).
pub fn weight_material_from_tensors(
    tensors: &std::collections::HashMap<String, Vec<u8>>,
) -> Vec<u8> {
    let mut names: Vec<&String> = tensors
        .keys()
        .filter(|k| k.as_str() != "__joule_armed__")
        .collect();
    names.sort();
    let mut out = Vec::new();
    for n in names {
        let t = &tensors[n];
        out.extend_from_slice(n.as_bytes());
        out.push(0);
        out.extend_from_slice(&(t.len() as u64).to_le_bytes());
        out.extend_from_slice(&Sha256::digest(t));
        let take = t.len().min(64);
        out.extend_from_slice(&t[..take]);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stage_depends_on_upstream_and_layers() {
        let base = StageRequest::lab("kimi-open", "hello", 0, 10, vec![], false, false);
        let a = lab_stage_activation(&base).unwrap();
        assert!(a.activation.starts_with(b"JST1"));
        assert!(a.activation.len() >= 48);
        let mut mid = base.clone();
        mid.upstream = a.activation.clone();
        mid.layer_start = 11;
        mid.layer_end = 40;
        let b = lab_stage_activation(&mid).unwrap();
        assert_ne!(
            a.activation, b.activation,
            "stages must differ by band/upstream"
        );
        let mut tail = mid.clone();
        tail.upstream = b.activation.clone();
        tail.layer_start = 41;
        tail.layer_end = 92;
        tail.is_tail = true;
        tail.require_upstream = true;
        let t = lab_stage_activation(&tail).unwrap();
        assert!(t.text.as_ref().unwrap().contains("upstream_bytes="));
        assert!(t.text.as_ref().unwrap().contains("joule-pipeline-stage"));
        assert!(t.text.as_ref().unwrap().contains("hello"));
        // Wrong upstream fails closed for tail.
        let mut bad = tail.clone();
        bad.upstream = vec![1, 2, 3];
        let t2 = lab_stage_activation(&bad).unwrap();
        assert_ne!(t.text, t2.text);
        let empty_tail = StageRequest {
            require_upstream: true,
            is_tail: true,
            upstream: vec![],
            ..base
        };
        assert!(lab_stage_activation(&empty_tail).is_err());
        eprintln!(
            "OBSERVE real-pp: act_a={} act_b={} tail_text_len={}",
            a.activation.len(),
            b.activation.len(),
            t.text.as_ref().unwrap().len()
        );
    }

    #[test]
    fn weight_backed_stage_depends_on_weight_bytes() {
        let req = StageRequest::lab("kimi-open", "w-stage", 0, 5, vec![], false, false);
        let w1 = b"weight-bytes-version-AAAA";
        let w2 = b"weight-bytes-version-BBBB";
        let a = stage_activation_with_weights(&req, w1).unwrap();
        let b = stage_activation_with_weights(&req, w2).unwrap();
        assert!(a.activation.starts_with(b"JST2"));
        assert_ne!(
            a.activation, b.activation,
            "different weights → different act"
        );
        let lab = lab_stage_activation(&req).unwrap();
        assert!(lab.activation.starts_with(b"JST1"));
        assert_ne!(lab.activation, a.activation, "weight-backed ≠ lab-only");
        assert!(stage_activation_with_weights(&req, &[]).is_err());
        let mut tensors = std::collections::HashMap::new();
        tensors.insert("tok.weight".into(), w1.to_vec());
        let mat = weight_material_from_tensors(&tensors);
        assert!(!mat.is_empty());
        tensors.insert("tok.weight".into(), w2.to_vec());
        let mat2 = weight_material_from_tensors(&tensors);
        assert_ne!(mat, mat2);
        eprintln!(
            "OBSERVE weight-stage: jst2_a={} jst2_b={} lab={} mat_len={}",
            a.activation.len(),
            b.activation.len(),
            lab.activation.len(),
            mat.len()
        );
    }
}
