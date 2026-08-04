//! Model manifest: milestones, pool gates, and when we may load Kimi.

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
    pub min_pool_vram_mib: u64,
    pub min_backends: u32,
    #[serde(default = "default_layers")]
    pub model_layers: u32,
    #[serde(default)]
    pub milestones: Vec<MilestoneSpec>,
    pub weights: WeightsSpec,
}

fn default_layers() -> u32 {
    // Aligned with verified Kimi-K3 text_config.num_hidden_layers (offline meta pin).
    93
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MilestoneSpec {
    pub id: String,
    pub title: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub min_pool_vram_mib: u64,
    #[serde(default)]
    pub min_backends: u32,
    #[serde(default)]
    pub requires_weights_published: bool,
    #[serde(default)]
    pub requires_model_loaded: bool,
    #[serde(default)]
    pub requires_service_live: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeightsSpec {
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
pub struct MilestoneStatus {
    pub id: String,
    pub title: String,
    pub description: String,
    pub reached: bool,
    pub progress_pct: u8,
    pub min_pool_vram_mib: u64,
    pub min_backends: u32,
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
    pub pool_ready: bool,
    pub weights_published: bool,
    pub model_loaded: bool,
    pub service_live: bool,
    pub pool_progress_pct: u8,
    pub inference_mode: InferenceMode,
    pub message: String,
    pub recommended_quant: Option<String>,
    /// Ordered campaign milestones toward live Kimi service.
    pub milestones: Vec<MilestoneStatus>,
    /// First unreached milestone (the “countdown” target).
    pub next_milestone: Option<MilestoneStatus>,
    /// Estimated seconds to next milestone from recent VRAM growth (None if unknown).
    pub countdown_secs: Option<u64>,
    /// Human countdown string, e.g. "about 3 hours" or "awaiting more donors".
    pub countdown_label: String,
    /// When we may begin loading weights into the logical device.
    pub can_load_model: bool,
    /// When public service should flip to real completions.
    pub can_begin_service: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InferenceMode {
    StubAwaitingPool,
    StubPoolReady,
    LoadingWeights,
    ModelLoaded,
    ServiceLive,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RuntimeFlags {
    pub model_loaded: bool,
    pub service_live: bool,
    /// Required MANIFEST digests staged and sha256-verified (content-addressed).
    /// Without this, `service_live` must stay false and can_begin_service is false.
    pub digests_verified: bool,
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
    /// Prefer the largest quant that fits **and has files listed** (by total `size_bytes`).
    /// Among equal sizes, prefer more files. Falls back to any fitting quant, then first quant.
    pub fn pick_quant(&self, node_vram_mib: u32) -> Option<&QuantSpec> {
        let mut best_with_files: Option<&QuantSpec> = None;
        let mut best_files_score: (u64, usize) = (0, 0);
        let mut best_any: Option<&QuantSpec> = None;
        for q in &self.weights.quants {
            if node_vram_mib < q.min_node_vram_mib {
                continue;
            }
            best_any = Some(q);
            if q.files.is_empty() {
                continue;
            }
            let bytes: u64 = q.files.iter().map(|f| f.size_bytes).sum();
            let score = (bytes, q.files.len());
            if best_with_files.is_none() || score > best_files_score {
                // Skip multi-hundred-GB peer-only pins for auto-pick when smaller
                // loadable fixtures exist (lab-tiny / lab-mid). Full K3 is explicit.
                let peer_only = q.files.iter().all(|f| f.url.starts_with("peer://"));
                if peer_only && best_with_files.is_some() {
                    continue;
                }
                best_files_score = score;
                best_with_files = Some(q);
            }
        }
        best_with_files
            .or(best_any)
            .or_else(|| self.weights.quants.first())
    }

    fn milestone_progress(&self, m: &MilestoneSpec, pool_vram_mib: u64, backends: u32) -> u8 {
        let vram_pct = pool_vram_mib
            .saturating_mul(100)
            .checked_div(m.min_pool_vram_mib.max(1))
            .unwrap_or(100)
            .min(100) as u8;
        let be_pct = (u64::from(backends) * 100)
            .checked_div(u64::from(m.min_backends.max(1)))
            .unwrap_or(100)
            .min(100) as u8;
        let mut p = vram_pct.min(be_pct);
        if m.requires_weights_published && !self.weights.published {
            p = p.min(90);
        }
        p
    }

    fn milestone_reached(
        &self,
        m: &MilestoneSpec,
        pool_vram_mib: u64,
        backends: u32,
        flags: RuntimeFlags,
    ) -> bool {
        if pool_vram_mib < m.min_pool_vram_mib || backends < m.min_backends {
            return false;
        }
        if m.requires_weights_published && !self.weights.published {
            return false;
        }
        if m.requires_model_loaded && !flags.model_loaded {
            return false;
        }
        if m.requires_service_live && !flags.service_live {
            return false;
        }
        true
    }

    pub fn readiness(
        &self,
        pool_vram_mib: u64,
        backends: u32,
        flags: RuntimeFlags,
        vram_growth_mib_per_sec: Option<f64>,
    ) -> ModelReadiness {
        let pool_ready = pool_vram_mib >= self.min_pool_vram_mib && backends >= self.min_backends;
        let vram_pct = pool_vram_mib
            .saturating_mul(100)
            .checked_div(self.min_pool_vram_mib.max(1))
            .unwrap_or(100)
            .min(100) as u8;
        let be_pct = (u64::from(backends) * 100)
            .checked_div(u64::from(self.min_backends.max(1)))
            .unwrap_or(100)
            .min(100) as u8;
        let pool_progress_pct = vram_pct.min(be_pct);

        let milestones: Vec<MilestoneStatus> = self
            .milestones
            .iter()
            .map(|m| MilestoneStatus {
                id: m.id.clone(),
                title: m.title.clone(),
                description: m.description.clone(),
                reached: self.milestone_reached(m, pool_vram_mib, backends, flags),
                progress_pct: self.milestone_progress(m, pool_vram_mib, backends),
                min_pool_vram_mib: m.min_pool_vram_mib,
                min_backends: m.min_backends,
            })
            .collect();

        let next_milestone = milestones.iter().find(|m| !m.reached).cloned();

        let countdown_secs = next_milestone.as_ref().and_then(|nm| {
            let need_vram = nm.min_pool_vram_mib.saturating_sub(pool_vram_mib);
            if need_vram == 0 {
                return None;
            }
            let rate = vram_growth_mib_per_sec.filter(|r| *r > 0.01)?;
            Some((need_vram as f64 / rate).ceil() as u64)
        });

        let countdown_label = match (&next_milestone, countdown_secs) {
            (None, _) => "all milestones reached".into(),
            (Some(m), Some(secs)) => format!("next: {} — about {}", m.title, format_duration(secs)),
            (Some(m), None) => format!(
                "next: {} — need more donors ({} GiB / {} backends now)",
                m.title,
                pool_vram_mib / 1024,
                backends
            ),
        };

        let can_load_model = pool_ready && self.weights.published;
        // Fleet SoT: full K3 production service needs multi-backend + high VRAM
        // (same thresholds as MANIFEST min_* / full_k3_service_fleet_ok).
        let fleet_ok = crate::full_k3_service_fleet_ok(pool_vram_mib, backends);
        // Live path requires verified content digests — not model_loaded flag alone.
        let can_begin_service =
            can_load_model && flags.model_loaded && flags.digests_verified && fleet_ok;
        // Honor digests even if operator flips service_live early; never live under fleet.
        let service_live_honest =
            flags.service_live && flags.digests_verified && flags.model_loaded && fleet_ok;

        let (inference_mode, message) = if service_live_honest {
            (
                InferenceMode::ServiceLive,
                format!("{} is live on the joule logical device.", self.id),
            )
        } else if flags.model_loaded && !flags.digests_verified {
            (
                InferenceMode::LoadingWeights,
                format!(
                    "{}: model flag set but required digests not sha256-verified — not service_live.",
                    self.id
                ),
            )
        } else if flags.model_loaded {
            (
                InferenceMode::ModelLoaded,
                format!(
                    "{} weights are loaded on the mesh; service not marked live yet.",
                    self.id
                ),
            )
        } else if can_load_model {
            (
                InferenceMode::LoadingWeights,
                format!(
                    "pool ready and weights published — load {} into the logical device.",
                    self.id
                ),
            )
        } else if pool_ready {
            (
                InferenceMode::StubPoolReady,
                format!(
                    "pool ready for {} ({} GiB, {} backends). Weights not published yet. {}",
                    self.id,
                    pool_vram_mib / 1024,
                    backends,
                    self.weights.note
                ),
            )
        } else {
            (
                InferenceMode::StubAwaitingPool,
                format!(
                    "growing the logical device for {}: need ≥{} GiB and ≥{} backends (have {} GiB, {}). {}",
                    self.id,
                    self.min_pool_vram_mib / 1024,
                    self.min_backends,
                    pool_vram_mib / 1024,
                    backends,
                    countdown_label
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
            model_loaded: flags.model_loaded,
            service_live: service_live_honest,
            pool_progress_pct,
            inference_mode,
            message,
            recommended_quant: None,
            milestones,
            next_milestone,
            countdown_secs,
            countdown_label,
            can_load_model,
            can_begin_service,
        }
    }
}

fn format_duration(secs: u64) -> String {
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("~{} min", secs.div_ceil(60))
    } else if secs < 86400 {
        format!("~{:.1} hours", secs as f64 / 3600.0)
    } else {
        format!("~{:.1} days", secs as f64 / 86400.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn milestones_and_countdown() {
        let m = ManifestFile::load_default().unwrap();
        let kimi = m.model("kimi-open").unwrap();
        let r = kimi.readiness(8 * 1024, 1, RuntimeFlags::default(), Some(100.0));
        assert!(!r.pool_ready);
        assert!(r.next_milestone.is_some());
        assert!(r.countdown_secs.is_some());
        assert!(r.milestones.iter().any(|x| x.id == "spark" && x.reached));
        let r2 = kimi.readiness(72 * 1024, 5, RuntimeFlags::default(), None);
        assert!(r2.pool_ready);
        assert!(r2
            .milestones
            .iter()
            .any(|x| x.id == "kimi-eligible" && x.reached));
        assert!(r2.weights_published);
        assert!(r2.can_load_model); // weights published + pool ready
        assert!(
            !r2.can_begin_service,
            "needs digests_verified + model_loaded"
        );

        // Missing digests: service_live flag alone cannot claim live.
        let mut flags = RuntimeFlags {
            model_loaded: true,
            service_live: true,
            digests_verified: false,
        };
        let r3 = kimi.readiness(72 * 1024, 5, flags, None);
        assert!(!r3.service_live);
        assert!(!r3.can_begin_service);

        flags.digests_verified = true;
        let r4 = kimi.readiness(72 * 1024, 5, flags, None);
        assert!(r4.can_begin_service);
        assert!(r4.service_live);
    }
}
