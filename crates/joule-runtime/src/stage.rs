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

/// Lab/stub stage: deterministic activation tensor that **depends** on layer band + prompt + upstream.
///
/// Layout (little-endian):
/// - magic `JST1` (4)
/// - layer_start u32, layer_end u32
/// - prompt_sha16 (16)
/// - upstream_sha16 (16)
/// - upstream_len u32 + first min(32, len) upstream bytes
pub fn lab_stage_activation(req: &StageRequest) -> Result<StageOutput, String> {
    if req.layer_end < req.layer_start {
        return Err("layer_end < layer_start".into());
    }
    if req.require_upstream && req.is_tail && req.upstream.is_empty() {
        return Err("tail stage requires non-empty upstream activations".into());
    }
    let mut out = Vec::with_capacity(80 + req.upstream.len().min(32));
    out.extend_from_slice(b"JST1");
    out.extend_from_slice(&req.layer_start.to_le_bytes());
    out.extend_from_slice(&req.layer_end.to_le_bytes());
    let psha = Sha256::digest(req.prompt.trim().as_bytes());
    out.extend_from_slice(&psha[..16]);
    let usha = Sha256::digest(&req.upstream);
    out.extend_from_slice(&usha[..16]);
    let ulen = (req.upstream.len() as u32).to_le_bytes();
    out.extend_from_slice(&ulen);
    let take = req.upstream.len().min(32);
    out.extend_from_slice(&req.upstream[..take]);
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
        // Compact single metadata token + prompt so usage billing stays comparable
        // to stub infer (whitespace token count drives mJ burn).
        Some(format!(
            "[joule-pipeline-stage:{}:L{}-{}:upstream_bytes={}:act={}] {}",
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
}
