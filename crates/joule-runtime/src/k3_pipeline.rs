//! Full multi-hundred-GB kimi-open / K3 weight pipeline (product scale).
//!
//! Accepts multi-file, multi-hundred-GB-class quant layouts without requiring a
//! full download on the verifier host. lab-tiny remains the always-loadable path.

use crate::manifest::{ModelSpec, QuantSpec};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// One pinned K3 (or quant) shard in the pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PipelineShard {
    pub path: String,
    pub sha256: String,
    pub size_bytes: u64,
    pub url: String,
}

/// Pipeline view for a model quant: total size, shard list, readiness gates.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeightPipeline {
    pub model_id: String,
    pub quant_id: String,
    pub shards: Vec<PipelineShard>,
    pub total_bytes: u64,
    pub total_gib: u64,
    /// True when total_bytes ≥ 100 GiB (multi-hundred-GB class or larger).
    pub multi_hundred_gb_class: bool,
    pub min_node_vram_mib: u32,
}

/// Build pipeline from a quant spec (shipped product entry).
pub fn pipeline_from_quant(model: &ModelSpec, quant: &QuantSpec) -> WeightPipeline {
    let shards: Vec<PipelineShard> = quant
        .files
        .iter()
        .map(|f| PipelineShard {
            path: f.path.clone(),
            sha256: f.sha256.clone(),
            size_bytes: f.size_bytes,
            url: f.url.clone(),
        })
        .collect();
    let total_bytes: u64 = shards.iter().map(|s| s.size_bytes).sum();
    // also honor approx_file_mib when files empty (declared capacity)
    let declared = if total_bytes == 0 {
        u64::from(quant.approx_file_mib).saturating_mul(1024 * 1024)
    } else {
        total_bytes
    };
    let total_gib = declared / (1024 * 1024 * 1024);
    WeightPipeline {
        model_id: model.id.clone(),
        quant_id: quant.id.clone(),
        shards,
        total_bytes: declared,
        total_gib,
        multi_hundred_gb_class: declared >= 100 * 1024 * 1024 * 1024,
        min_node_vram_mib: quant.min_node_vram_mib,
    }
}

/// All quants for a model as pipelines.
pub fn pipelines_for_model(model: &ModelSpec) -> Vec<WeightPipeline> {
    model
        .weights
        .quants
        .iter()
        .map(|q| pipeline_from_quant(model, q))
        .collect()
}

/// Validate K3-class pipeline: multi-file OR multi-hundred-GB declared size.
pub fn validate_k3_scale(p: &WeightPipeline) -> Result<(), String> {
    if p.quant_id.contains("lab-tiny") {
        return Ok(());
    }
    if p.shards.is_empty() && !p.multi_hundred_gb_class && p.total_bytes < 1024 {
        return Err(format!(
            "quant {} has no shards and is not multi-hundred-GB class",
            p.quant_id
        ));
    }
    // sha256 field shape when shards present; never require f00 payload hosting.
    for s in &p.shards {
        if s.sha256.len() != 64 {
            return Err(format!("bad sha256 on {}", s.path));
        }
        if s.size_bytes == 0 && p.multi_hundred_gb_class {
            return Err(format!("zero size on large-class shard {}", s.path));
        }
        let u = s.url.to_ascii_lowercase();
        if u.contains("f00.sh") || u.contains("://joule.f00") {
            return Err(format!(
                "weight URL must not use f00 hosting (got {})",
                s.url
            ));
        }
    }
    Ok(())
}

/// Local cache path for a shard (content-addressed).
pub fn shard_cache_path(blob_root: &std::path::Path, sha256: &str) -> PathBuf {
    blob_root.join(sha256.to_lowercase())
}

/// Synthetic multi-shard K3 layout for tests / operator pin templates.
/// Total size is multi-hundred-GB class without writing bytes to disk.
pub fn synthetic_k3_shard_template(num_shards: u32, gib_per_shard: u64) -> Vec<PipelineShard> {
    (0..num_shards)
        .map(|i| {
            let size = gib_per_shard.saturating_mul(1024 * 1024 * 1024);
            // deterministic 64-hex content id (not a real file hash — pin template only)
            let sha = {
                let mut b = [b'a'; 64];
                let id = format!("{i:08x}");
                for (j, c) in id.bytes().enumerate() {
                    b[j] = c;
                }
                b[63] = b'3';
                String::from_utf8_lossy(&b).into_owned()
            };
            PipelineShard {
                path: format!("model-{:05}-of-{:05}.safetensors", i + 1, num_shards),
                sha256: sha,
                size_bytes: size,
                url: format!(
                    "peer://kimi-open/k3/model-{:05}-of-{:05}.safetensors",
                    i + 1,
                    num_shards
                ),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::ManifestFile;

    #[test]
    fn lab_tiny_pipeline_still_works() {
        let m = ManifestFile::load_default().unwrap();
        let model = m.model("kimi-open").unwrap();
        let q = model
            .weights
            .quants
            .iter()
            .find(|q| q.id == "lab-tiny")
            .unwrap();
        let p = pipeline_from_quant(model, q);
        assert!(!p.shards.is_empty());
        validate_k3_scale(&p).unwrap();
        assert!(!p.multi_hundred_gb_class);
    }

    #[test]
    fn synthetic_k3_is_multi_hundred_gb() {
        let shards = synthetic_k3_shard_template(16, 20); // 320 GiB
        let total: u64 = shards.iter().map(|s| s.size_bytes).sum();
        assert!(total >= 100 * 1024 * 1024 * 1024);
        let p = WeightPipeline {
            model_id: "kimi-open".into(),
            quant_id: "kimi-k3-shards".into(),
            total_bytes: total,
            total_gib: total / (1024 * 1024 * 1024),
            multi_hundred_gb_class: total >= 100 * 1024 * 1024 * 1024,
            min_node_vram_mib: 65536,
            shards,
        };
        assert!(p.multi_hundred_gb_class);
        validate_k3_scale(&p).unwrap();
        assert!(p.shards.len() >= 8);
    }

    #[test]
    fn k3_shards_never_use_f00_weight_urls() {
        let m = ManifestFile::load_default().unwrap();
        let model = m.model("kimi-open").unwrap();
        for q in &model.weights.quants {
            let p = pipeline_from_quant(model, q);
            validate_k3_scale(&p).unwrap();
            for s in &p.shards {
                assert!(
                    !s.url.contains("f00.sh"),
                    "quant {} shard {} must not host on f00",
                    q.id,
                    s.path
                );
            }
        }
    }

    #[test]
    fn kimi_k3_shards_pipeline_is_multi_hundred_gb() {
        let m = ManifestFile::load_default().unwrap();
        let model = m.model("kimi-open").unwrap();
        let q = model
            .weights
            .quants
            .iter()
            .find(|q| q.id == "kimi-k3-shards")
            .expect("kimi-k3-shards quant in MANIFEST");
        let p = pipeline_from_quant(model, q);
        assert!(
            p.multi_hundred_gb_class,
            "total_gib={} bytes={}",
            p.total_gib, p.total_bytes
        );
        assert!(p.shards.len() >= 8);
        validate_k3_scale(&p).unwrap();
        // durable placement path: many shards for swarm
        let digests: Vec<_> = p.shards.iter().map(|s| s.sha256.clone()).collect();
        assert_eq!(digests.len(), p.shards.len());
    }
}
