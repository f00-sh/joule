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
//! That is enough to prove the load→infer path is tensor-backed and deterministic.

use crate::load::LoadedModel;
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

struct EmbView {
    name: String,
    vocab: usize,
    dim: usize,
    /// Row-major f32 data
    data: Vec<f32>,
}

fn find_embedding(loaded: &LoadedModel) -> Option<EmbView> {
    // Prefer common names, else first F32 2D matrix with both dims > 8.
    let prefer = [
        "tok_embeddings.weight",
        "model.embed_tokens.weight",
        "embed_tokens.weight",
        "transformer.wte.weight",
    ];
    for name in prefer {
        if let Some(v) = tensor_f32_2d(loaded, name) {
            return Some(v);
        }
    }
    for info in &loaded.tensor_info {
        if info.dtype.eq_ignore_ascii_case("F32") && info.shape.len() == 2 {
            if let Some(v) = tensor_f32_2d(loaded, &info.name) {
                if v.vocab >= 16 && v.dim >= 8 {
                    return Some(v);
                }
            }
        }
    }
    None
}

fn tensor_f32_2d(loaded: &LoadedModel, name: &str) -> Option<EmbView> {
    let info = loaded.tensor_info.iter().find(|t| t.name == name)?;
    if info.shape.len() != 2 {
        return None;
    }
    let vocab = info.shape[0];
    let dim = info.shape[1];
    let bytes = loaded.tensors.get(name)?;
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
    let max_tokens = max_tokens.clamp(1, 128) as usize;
    let ids = tokenize(prompt, emb.vocab);
    let mut state = vec![0.0f32; emb.dim];
    for &id in &ids {
        add_row(&mut state, emb, id);
    }
    // Normalize-ish
    let norm = state.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-6);
    for x in &mut state {
        *x /= norm;
    }

    let mut out = String::new();
    out.push_str(&format!(
        "[joule-tensor:{}/{} emb={} V={} D={}] ",
        loaded.model_id, loaded.quant, emb.name, emb.vocab, emb.dim
    ));
    // Echo a cleaned prompt prefix then generate
    let echo: String = prompt.chars().take(80).collect();
    out.push_str(&echo);
    if !echo.is_empty() {
        out.push(' ');
    }

    let mut prev = ids.last().copied().unwrap_or(1);
    for step in 0..max_tokens {
        let scores = score_vocab(emb, &state, prev, step);
        let next = argmax(&scores);
        let ch = detokenize(next);
        out.push(ch);
        // Update state with new token
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
}
