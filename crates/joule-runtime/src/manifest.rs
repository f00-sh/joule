//! Model manifest: what joule will run, and how large the pool must be first.

use serde::{Deserialize, Serialize};
use std::path::Path;

/// Embedded default manifest (also checked into `models/MANIFEST.json`).
pub const EMBEDDED_MANIFEST: &str = include_str!("../../../models/MANIFEST.json");

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestFile {
    pub version: u32,
    pub models: Vec<ModelSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelSpec {
    pub id: String,
    pub label: String,
    #[serde(default)]
    pub description: String,
    /// Aggregate healthy donor VRAM required before we treat the pool as ready for this model.
    pub min_pool_vram_mib: u64,
    /// Minimum healthy backends (physical machines).
    pub min_backends: u32,
    #[serde(default = "default_layers")]
    pub model_layers: u32,
    pub weights: WeightsSpec,
}

fn default_layers() -> u32 {
    80
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeightsSpec {
    /// When false, agents only *arm* the cache; no multi‑GB download.
    pub published: bool,
    #[serde(default)]
    pub note: String,
    pub quants: Vec<QuantSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantSpec {
    pub id: String,
    pub min_node_vram_mib: u32,
    #[serde(default)]
    pub approx_file_mib: u32,
    #[serde(default)]
    pub files: Vec<WeightFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeightFile {
    pub path: String,
    pub sha256: String,
    pub url: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelReadiness {
    pub model: String,
    pub label: String,
    pub pool_vram_mib: u64,
    pub pool_vram_gib: u64,
    pub required_vram_mib: u64,
    pub required_vram_gib: u64,
    pub backends: u32,
    pub required_backends: u32,
    /// Aggregate VRAM + backend count gates passed.
    pub pool_ready: bool,
    /// Manifest says weight URLs/hashes are published.
    pub weights_published: bool,
    /// 0–100 progress toward pool size gate.
    pub pool_progress_pct: u8,
    /// What inference does today.
    pub inference_mode: InferenceMode,
    pub message: String,
    pub recommended_quant: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InferenceMode {
    /// Pool too small — stub only; do not fetch multi‑GB weights.
    StubAwaitingPool,
    /// Pool large enough, weights not published yet — cache armed, still stub.
    StubPoolReady,
    /// Weights published and prepared on disk (future real engine).
    WeightsReady,
}

impl ManifestFile {
    pub fn load_default() -> Result<Self, String> {
        serde_json::from_str(EMBEDDED_MANIFEST).map_err(|e| e.to_string())
    }

    pub fn load_path(path: &Path) -> Result<Self, String> {
        let raw = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
        serde_json::from_str(&raw).map_err(|e| e.to_string())
    }

    pub fn model(&self, id: &str) -> Option<&ModelSpec> {
        self.models.iter().find(|m| m.id == id)
    }

    pub fn primary(&self) -> Option<&ModelSpec> {
        self.models.first()
    }
}

impl ModelSpec {
    pub fn pick_quant(&self, node_vram_mib: u32) -> Option<&QuantSpec> {
        // Largest quant that still fits the node.
        let mut best: Option<&QuantSpec> = None;
        for q in &self.weights.quants {
            if node_vram_mib >= q.min_node_vram_mib {
                best = Some(q);
            }
        }
        best.or_else(|| self.weights.quants.first())
    }

    pub fn readiness(&self, pool_vram_mib: u64, backends: u32) -> ModelReadiness {
        let pool_ready = pool_vram_mib >= self.min_pool_vram_mib && backends >= self.min_backends;
        let vram_pct = pool_vram_mib
            .saturating_mul(100)
            .checked_div(self.min_pool_vram_mib)
            .unwrap_or(100)
            .min(100) as u8;
        let be_pct = (u64::from(backends) * 100)
            .checked_div(u64::from(self.min_backends))
            .unwrap_or(100)
            .min(100) as u8;
        let pool_progress_pct = vram_pct.min(be_pct);

        let (inference_mode, message) = if !pool_ready {
            (
                InferenceMode::StubAwaitingPool,
                format!(
                    "pool not ready for {}: need ≥{} GiB aggregate VRAM and ≥{} backends (have {} GiB, {} backends). Inference remains stub.",
                    self.id,
                    self.min_pool_vram_mib / 1024,
                    self.min_backends,
                    pool_vram_mib / 1024,
                    backends
                ),
            )
        } else if !self.weights.published {
            (
                InferenceMode::StubPoolReady,
                format!(
                    "pool ready for {} ({} GiB, {} backends). Weights not published yet — agents arm cache; inference still stub. {}",
                    self.id,
                    pool_vram_mib / 1024,
                    backends,
                    self.weights.note
                ),
            )
        } else {
            (
                InferenceMode::WeightsReady,
                format!(
                    "pool ready and weights published for {}. Agents should prepare quant artifacts.",
                    self.id
                ),
            )
        };

        ModelReadiness {
            model: self.id.clone(),
            label: self.label.clone(),
            pool_vram_mib,
            pool_vram_gib: pool_vram_mib / 1024,
            required_vram_mib: self.min_pool_vram_mib,
            required_vram_gib: self.min_pool_vram_mib / 1024,
            backends,
            required_backends: self.min_backends,
            pool_ready,
            weights_published: self.weights.published,
            pool_progress_pct,
            inference_mode,
            message,
            recommended_quant: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_manifest_loads() {
        let m = ManifestFile::load_default().unwrap();
        let kimi = m.model("kimi-open").unwrap();
        assert!(kimi.min_pool_vram_mib >= 64 * 1024);
        assert!(!kimi.weights.published);
        let r = kimi.readiness(8 * 1024, 1);
        assert!(!r.pool_ready);
        assert_eq!(r.inference_mode, InferenceMode::StubAwaitingPool);
        let r2 = kimi.readiness(72 * 1024, 5);
        assert!(r2.pool_ready);
        assert_eq!(r2.inference_mode, InferenceMode::StubPoolReady);
    }

    #[test]
    fn pick_quant_by_vram() {
        let m = ManifestFile::load_default().unwrap();
        let kimi = m.model("kimi-open").unwrap();
        assert_eq!(kimi.pick_quant(8192).unwrap().id, "q4_k_m");
        assert_eq!(kimi.pick_quant(24576).unwrap().id, "q8_0");
    }
}
