//! Generate text from resident tensors (pure Rust, no FFI).
//!
//! Full Kimi transformer decode is not here yet. This path **uses real loaded
//! weights** when an embedding matrix is present (e.g. lab-tiny
//! `tok_embeddings.weight` F32 [V, D]):
//! - tokenize by simple byte/hash ids into vocab
//! - lookup embedding rows
//! - accumulate + project to next-token scores via a fold over embedding dims
//! - emit printable tokens
//!
//! Tail multi-shard path: [`generate_from_activation_state`] seeds the hidden
//! state from upstream pipeline activations (e.g. JST3 f32 tail) so token text
//! depends on both activations and embedding material — not prompt-echo alone.
//!
//! That is enough to prove the load→infer path is tensor-backed and deterministic.

use crate::load::LoadedModel;
use crate::stage::StageRequest;
use std::collections::HashMap;

/// Run a short generation using resident tensors when possible.
pub fn generate(loaded: &LoadedModel, prompt: &str, max_tokens: u32) -> String {
    if let Some(emb) = find_embedding(loaded) {
        return generate_from_embedding(loaded, &emb, prompt, max_tokens);
    }
    // Config/meta only (e.g. kimi-k3-meta) or unknown layout.
    let names: Vec<&str> = loaded.tensors.keys().take(8).map(|s| s.as_str()).collect();
    format!(
        "[joule-loaded:{}/{} bytes={} tensors={} keys={:?}] {}",
        loaded.model_id,
        loaded.quant,
        loaded.bytes_resident,
        loaded.tensors.len(),
        names,
        prompt
    )
}

/// Tail decode: token text from **activation state** + embedding rows in `tensors`.
///
/// `state` is the post-stage f32 hidden vector (from matmul / JST3). Changing
/// either the activation seed or embedding bytes changes the returned text.
/// Returns `None` when no usable embedding matrix is present.
pub fn generate_from_activation_state(
    model_id: &str,
    quant_hint: &str,
    tensors: &HashMap<String, Vec<u8>>,
    tensor_info: Option<&[crate::load::TensorInfo]>,
    state: &[f32],
    prompt: &str,
    max_tokens: u32,
) -> Option<String> {
    let emb = find_embedding_in_tensors(tensors, tensor_info)?;
    Some(generate_from_activation_emb(
        model_id, quant_hint, &emb, state, prompt, max_tokens,
    ))
}

/// Convenience: tail decode for a stage request using resident LoadedModel + state.
pub fn generate_tail_from_stage(
    loaded: &LoadedModel,
    req: &StageRequest,
    state: &[f32],
    max_tokens: u32,
) -> Option<String> {
    generate_from_activation_state(
        &loaded.model_id,
        &loaded.quant,
        &loaded.tensors,
        Some(&loaded.tensor_info),
        state,
        &req.prompt,
        max_tokens,
    )
}

struct EmbView {
    name: String,
    vocab: usize,
    dim: usize,
    /// Row-major f32 data
    data: Vec<f32>,
}

fn find_embedding(loaded: &LoadedModel) -> Option<EmbView> {
    find_embedding_in_tensors(&loaded.tensors, Some(&loaded.tensor_info))
}

fn find_embedding_in_tensors(
    tensors: &HashMap<String, Vec<u8>>,
    tensor_info: Option<&[crate::load::TensorInfo]>,
) -> Option<EmbView> {
    // Prefer common names, else first F32 2D matrix with both dims > 8.
    let prefer = [
        "tok_embeddings.weight",
        "model.embed_tokens.weight",
        "embed_tokens.weight",
        "transformer.wte.weight",
    ];
    for name in prefer {
        if let Some(v) = tensor_f32_2d_raw(tensors, tensor_info, name) {
            return Some(v);
        }
    }
    if let Some(infos) = tensor_info {
        for info in infos {
            if info.dtype.eq_ignore_ascii_case("F32") && info.shape.len() == 2 {
                if let Some(v) = tensor_f32_2d_raw(tensors, tensor_info, &info.name) {
                    if v.vocab >= 16 && v.dim >= 8 {
                        return Some(v);
                    }
                }
            }
        }
    }
    // Heuristic without tensor_info: largest multiple-of-4 buffer as [V, D] with D in 8..=256.
    let mut best: Option<EmbView> = None;
    for (name, bytes) in tensors {
        if name == "__joule_armed__" || bytes.len() < 16 * 8 * 4 {
            continue;
        }
        let n = bytes.len() / 4;
        for dim in [8usize, 16, 32, 64, 128, 256] {
            if n % dim != 0 {
                continue;
            }
            let vocab = n / dim;
            if !(16..=8192).contains(&vocab) {
                continue;
            }
            let mut data = Vec::with_capacity(n);
            for chunk in bytes.chunks_exact(4).take(n) {
                data.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
            }
            let cand = EmbView {
                name: name.clone(),
                vocab,
                dim,
                data,
            };
            let better = best
                .as_ref()
                .map(|b| cand.vocab * cand.dim > b.vocab * b.dim)
                .unwrap_or(true);
            if better {
                best = Some(cand);
            }
        }
    }
    best
}

fn tensor_f32_2d_raw(
    tensors: &HashMap<String, Vec<u8>>,
    tensor_info: Option<&[crate::load::TensorInfo]>,
    name: &str,
) -> Option<EmbView> {
    let bytes = tensors.get(name)?;
    let (vocab, dim) = if let Some(infos) = tensor_info {
        let info = infos.iter().find(|t| t.name == name)?;
        if info.shape.len() != 2 {
            return None;
        }
        (info.shape[0], info.shape[1])
    } else {
        // Infer [V, D] for known emb names: prefer dim=64/128 when divides.
        let n = bytes.len() / 4;
        let dim = [64usize, 128, 32, 16, 8]
            .into_iter()
            .find(|d| n % d == 0 && n / d >= 16)?;
        (n / dim, dim)
    };
    if bytes.len() < vocab * dim * 4 {
        return None;
    }
    let mut data = Vec::with_capacity(vocab * dim);
    for chunk in bytes.chunks_exact(4).take(vocab * dim) {
        data.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
    }
    Some(EmbView {
        name: name.into(),
        vocab,
        dim,
        data,
    })
}

fn generate_from_embedding(
    loaded: &LoadedModel,
    emb: &EmbView,
    prompt: &str,
    max_tokens: u32,
) -> String {
    let ids = tokenize(prompt, emb.vocab);
    let mut state = vec![0.0f32; emb.dim];
    for &id in &ids {
        add_row(&mut state, emb, id);
    }
    let norm = state.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-6);
    for x in &mut state {
        *x /= norm;
    }
    generate_loop(
        &loaded.model_id,
        &loaded.quant,
        emb,
        &state,
        prompt,
        max_tokens,
        "joule-tensor",
    )
}

fn generate_from_activation_emb(
    model_id: &str,
    quant_hint: &str,
    emb: &EmbView,
    act_state: &[f32],
    prompt: &str,
    max_tokens: u32,
) -> String {
    // Project / pad activation into embedding dim, mix with a light prompt seed
    // so both activation bits and embeddings drive tokens.
    let mut state = vec![0.0f32; emb.dim];
    if !act_state.is_empty() {
        for (i, s) in state.iter_mut().enumerate() {
            *s = act_state[i % act_state.len()];
        }
    }
    let ids = tokenize(prompt, emb.vocab);
    for &id in ids.iter().take(8) {
        let off = id.min(emb.vocab.saturating_sub(1)) * emb.dim;
        for (i, s) in state.iter_mut().enumerate().take(emb.dim) {
            *s = 0.85 * *s + 0.15 * emb.data[off + i];
        }
    }
    let norm = state.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-6);
    for x in &mut state {
        *x /= norm;
    }
    generate_loop(
        model_id,
        quant_hint,
        emb,
        &state,
        prompt,
        max_tokens,
        "joule-decode",
    )
}

fn generate_loop(
    model_id: &str,
    quant: &str,
    emb: &EmbView,
    state_in: &[f32],
    prompt: &str,
    max_tokens: u32,
    tag: &str,
) -> String {
    let max_tokens = max_tokens.clamp(1, 128) as usize;
    let mut state = state_in.to_vec();
    if state.len() != emb.dim {
        state.resize(emb.dim, 0.0);
    }

    let mut out = String::new();
    out.push_str(&format!(
        "[{tag}:{model_id}/{quant} emb={} V={} D={}] ",
        emb.name, emb.vocab, emb.dim
    ));
    // Short prompt context (not the sole content — tokens follow from state).
    let echo: String = prompt.chars().take(40).collect();
    if !echo.is_empty() {
        out.push_str(&echo);
        out.push(' ');
    }

    let ids = tokenize(prompt, emb.vocab);
    let mut prev = ids.last().copied().unwrap_or(1);
    for step in 0..max_tokens {
        let scores = score_vocab(emb, &state, prev, step);
        let next = argmax(&scores);
        let ch = detokenize(next);
        out.push(ch);
        let off = next * emb.dim;
        for (i, x) in state.iter_mut().enumerate().take(emb.dim) {
            *x = 0.92 * *x + 0.08 * emb.data[off + i];
        }
        prev = next;
        if ch == '\n' && step > 8 {
            break;
        }
    }
    out
}

fn tokenize(prompt: &str, vocab: usize) -> Vec<usize> {
    let mut ids = Vec::new();
    if prompt.is_empty() {
        return vec![0];
    }
    for (i, b) in prompt.bytes().enumerate() {
        let id = (usize::from(b)
            .wrapping_mul(131)
            .wrapping_add(i.wrapping_mul(17)))
            % vocab.max(1);
        ids.push(id);
    }
    ids
}

fn detokenize(id: usize) -> char {
    // Map to printable ASCII
    let table = b"abcdefghijklmnopqrstuvwxyz 0123456789.,!?-'\n";
    char::from(table[id % table.len()])
}

fn add_row(state: &mut [f32], emb: &EmbView, id: usize) {
    let row = id.min(emb.vocab.saturating_sub(1));
    let off = row * emb.dim;
    for (i, s) in state.iter_mut().enumerate().take(emb.dim) {
        *s += emb.data[off + i];
    }
}

fn score_vocab(emb: &EmbView, state: &[f32], prev: usize, step: usize) -> Vec<f32> {
    // Cheap scoring: don't full matmul V*D — sample a subset of vocab + local neighborhood
    let mut scores = HashMap::new();
    let sample = emb.vocab.clamp(32, 256);
    for k in 0..sample {
        let id = (prev
            .wrapping_mul(31)
            .wrapping_add(k.wrapping_mul(97))
            .wrapping_add(step.wrapping_mul(13)))
            % emb.vocab;
        let off = id * emb.dim;
        let mut s = 0.0f32;
        for (i, st) in state.iter().enumerate().take(emb.dim) {
            s += *st * emb.data[off + i];
        }
        scores.insert(id, s);
    }
    // Always include prev neighbors
    for d in 0..16 {
        let id = (prev + d) % emb.vocab;
        let off = id * emb.dim;
        let mut s = 0.0f32;
        for (i, st) in state.iter().enumerate().take(emb.dim) {
            s += *st * emb.data[off + i];
        }
        scores.insert(id, s + 0.01 * d as f32);
    }
    let mut out = vec![f32::NEG_INFINITY; emb.vocab];
    for (id, s) in scores {
        out[id] = s;
    }
    out
}

fn argmax(scores: &[f32]) -> usize {
    let mut best_i = 0usize;
    let mut best = f32::NEG_INFINITY;
    for (i, &s) in scores.iter().enumerate() {
        if s > best {
            best = s;
            best_i = i;
        }
    }
    best_i
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::load::load_model;
    use crate::manifest::ManifestFile;
    use crate::weights::WeightsStore;
    use std::fs;

    #[test]
    fn lab_tiny_generates_with_tensors() {
        let dir = std::env::temp_dir().join(format!("joule-dec-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let store = WeightsStore::new(&dir);
        let m = ManifestFile::load_default().unwrap();
        let spec = m.model("kimi-open").unwrap();
        let quant = spec
            .weights
            .quants
            .iter()
            .find(|q| q.id == "lab-tiny")
            .unwrap();
        store.prepare(spec, quant).unwrap();
        let loaded = load_model(&store, spec, quant).unwrap();
        assert!(!loaded.tensors.is_empty());
        let out = generate(&loaded, "hello joule", 24);
        assert!(out.contains("joule-tensor"), "out={out}");
        assert!(out.contains("hello"), "out={out}");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn tail_decode_depends_on_activation_and_embeddings() {
        let dir = std::env::temp_dir().join(format!("joule-dec-act-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let store = WeightsStore::new(&dir);
        let m = ManifestFile::load_default().unwrap();
        let spec = m.model("kimi-open").unwrap();
        let quant = spec
            .weights
            .quants
            .iter()
            .find(|q| q.id == "lab-tiny")
            .unwrap();
        store.prepare(spec, quant).unwrap();
        let loaded = load_model(&store, spec, quant).unwrap();
        let a = vec![0.1f32, 0.2, 0.3, 0.4];
        let b = vec![9.0f32, -2.0, 0.5, 1.0];
        let ta = generate_from_activation_state(
            &loaded.model_id,
            &loaded.quant,
            &loaded.tensors,
            Some(&loaded.tensor_info),
            &a,
            "tail-decode-probe",
            24,
        )
        .expect("decode a");
        let tb = generate_from_activation_state(
            &loaded.model_id,
            &loaded.quant,
            &loaded.tensors,
            Some(&loaded.tensor_info),
            &b,
            "tail-decode-probe",
            24,
        )
        .expect("decode b");
        assert!(ta.contains("joule-decode"), "ta={ta}");
        assert!(
            !ta.starts_with("[joule-pipeline-stage:"),
            "must not be stage-tag only: {ta}"
        );
        assert_ne!(
            ta, tb,
            "different activation seeds must change tail decode text"
        );
        // Flip embedding material → different text for same activation.
        let mut tensors2 = loaded.tensors.clone();
        if let Some(emb) = tensors2.get_mut("tok_embeddings.weight") {
            for (i, byte) in emb.iter_mut().enumerate().take(64) {
                *byte ^= 0x5Au8.wrapping_add(i as u8);
            }
        }
        let tc = generate_from_activation_state(
            &loaded.model_id,
            &loaded.quant,
            &tensors2,
            Some(&loaded.tensor_info),
            &a,
            "tail-decode-probe",
            24,
        )
        .expect("decode c");
        assert_ne!(
            ta, tc,
            "embedding flip must change tail decode for same activation"
        );
        eprintln!(
            "OBSERVE tail-decode: ta_len={} tb_len={} tc_len={} act_diff=true emb_diff=true",
            ta.len(),
            tb.len(),
            tc.len()
        );
        let _ = fs::remove_dir_all(&dir);
    }
}
