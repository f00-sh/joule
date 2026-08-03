//! Inference backends + model manifest + weight cache + **model loading**.
//!
//! Kimi is not loaded until the logical device is large enough. When the pool
//! hits the load milestone (and weights exist), [`load_model`] maps tensors into
//! RAM. Service-live is a separate control flag after the mesh has loaded.

mod decode;
mod k3_pipeline;
mod load;
mod manifest;
mod software;
mod weights;

pub use decode::generate as generate_from_loaded;
pub use k3_pipeline::{
    pipeline_from_quant, pipelines_for_model, shard_cache_path, synthetic_k3_shard_template,
    validate_k3_scale, PipelineShard, WeightPipeline,
};
pub use load::{load_model, LoadError, LoadReport, LoadedModel, TensorInfo};
pub use manifest::{
    InferenceMode, ManifestFile, MilestoneStatus, ModelReadiness, ModelSpec, QuantSpec,
    RuntimeFlags, EMBEDDED_MANIFEST,
};
// prepare_and_install is defined below next to ClusterEngine (crate root).
pub use software::{
    apply_staged, current_arch, current_os, match_target, parse_software_update, read_stage,
    stage_blob, SoftwareTarget, SoftwareUpdateBody, StageStatus,
};
pub use weights::{BlobAnnounce, PrepareStatus, WeightsStore};

/// Gate: digests verified for the primary quant of the default manifest model.
/// Used by control `service_live` honesty and readiness flags.
pub fn digests_verified_for_primary_lab(store: &WeightsStore) -> Result<bool, String> {
    let m = ManifestFile::load_default()?;
    let spec = m.primary().ok_or_else(|| "no primary model".to_string())?;
    let quant = spec
        .pick_quant(8192)
        .or_else(|| spec.weights.quants.first())
        .ok_or_else(|| "no quant".to_string())?;
    Ok(store.digests_verified(&spec.id, quant))
}

use async_trait::async_trait;
use joule_proto::ClusterPlan;
use std::sync::{Arc, Mutex};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("model not loaded: {0}")]
    NotLoaded(String),
    #[error("unsupported plan: {0}")]
    UnsupportedPlan(String),
    #[error("inference failed: {0}")]
    Infer(String),
    #[error("pool not ready: {0}")]
    PoolNotReady(String),
    #[error("load: {0}")]
    Load(String),
}

#[derive(Debug, Clone)]
pub struct InferRequest {
    pub model: String,
    pub prompt: String,
    pub max_tokens: u32,
}

#[derive(Debug, Clone)]
pub struct InferResponse {
    pub text: String,
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
}

#[async_trait]
pub trait Engine: Send + Sync {
    async fn load_plan(&self, plan: &ClusterPlan) -> Result<(), RuntimeError>;
    async fn infer(&self, req: InferRequest) -> Result<InferResponse, RuntimeError>;
}

pub struct StubEngine {
    loaded: Mutex<Option<String>>,
}

impl StubEngine {
    pub fn new() -> Self {
        Self {
            loaded: Mutex::new(None),
        }
    }

    pub fn expected_text(model: &str, prompt: &str) -> String {
        format!("[joule-stub:{model}] {prompt}")
    }

    pub fn expected_text_mode(mode: &str, model: &str, prompt: &str) -> String {
        format!("[joule-{mode}:{model}] {prompt}")
    }
}

impl Default for StubEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Engine for StubEngine {
    async fn load_plan(&self, plan: &ClusterPlan) -> Result<(), RuntimeError> {
        if plan.shards.is_empty() {
            return Err(RuntimeError::UnsupportedPlan("empty shards".into()));
        }
        *self.loaded.lock().expect("lock") = Some(plan.model.clone());
        Ok(())
    }

    async fn infer(&self, req: InferRequest) -> Result<InferResponse, RuntimeError> {
        let loaded = self.loaded.lock().expect("lock").clone();
        let Some(model) = loaded else {
            return Err(RuntimeError::NotLoaded(req.model));
        };
        if model != req.model {
            return Err(RuntimeError::NotLoaded(req.model));
        }
        let reply = Self::expected_text(&model, &req.prompt);
        Ok(InferResponse {
            text: reply.clone(),
            prompt_tokens: req.prompt.split_whitespace().count() as u32,
            completion_tokens: reply.split_whitespace().count() as u32,
        })
    }
}

/// Engine that can hold a real [`LoadedModel`] in RAM and answer accordingly.
pub struct ClusterEngine {
    plan_model: Mutex<Option<String>>,
    readiness: Mutex<Option<ModelReadiness>>,
    loaded: Mutex<Option<Arc<LoadedModel>>>,
}

impl ClusterEngine {
    pub fn new() -> Self {
        Self {
            plan_model: Mutex::new(None),
            readiness: Mutex::new(None),
            loaded: Mutex::new(None),
        }
    }

    pub fn update_readiness(&self, r: ModelReadiness) {
        *self.readiness.lock().expect("lock") = Some(r);
    }

    pub fn install_loaded(&self, model: LoadedModel) {
        *self.loaded.lock().expect("lock") = Some(Arc::new(model));
    }

    pub fn clear_loaded(&self) {
        *self.loaded.lock().expect("lock") = None;
    }

    pub fn loaded_report(&self) -> Option<LoadReport> {
        self.loaded
            .lock()
            .expect("lock")
            .as_ref()
            .map(|m| m.report())
    }

    pub fn is_model_loaded(&self) -> bool {
        self.loaded
            .lock()
            .expect("lock")
            .as_ref()
            .is_some_and(|m| !m.tensors.contains_key("__joule_armed__") || m.tensors.len() > 1)
            || self.loaded.lock().expect("lock").as_ref().is_some_and(|m| {
                m.bytes_resident > 32 && !m.tensors.contains_key("__joule_armed__")
            })
    }

    /// True if any LoadedModel is installed (including armed marker load).
    pub fn has_resident_weights(&self) -> bool {
        self.loaded.lock().expect("lock").is_some()
    }
}

/// Shipped agent path: prepare a quant into the weight store, load tensors, install on engine.
/// Used by `joule agent` after pool-ready / peer seed — not a test-only helper.
pub fn prepare_and_install(
    store: &WeightsStore,
    engine: &ClusterEngine,
    spec: &ModelSpec,
    quant: &QuantSpec,
) -> Result<LoadReport, String> {
    let st = store.prepare(spec, quant)?;
    if !st.files_complete {
        return Err(format!(
            "prepare incomplete for {}/{}: {}",
            spec.id, quant.id, st.message
        ));
    }
    let lm = load_model(store, spec, quant).map_err(|e| e.to_string())?;
    let report = lm.report();
    engine.install_loaded(lm);
    Ok(report)
}

impl Default for ClusterEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Engine for ClusterEngine {
    async fn load_plan(&self, plan: &ClusterPlan) -> Result<(), RuntimeError> {
        if plan.shards.is_empty() {
            return Err(RuntimeError::UnsupportedPlan("empty shards".into()));
        }
        *self.plan_model.lock().expect("lock") = Some(plan.model.clone());
        Ok(())
    }

    async fn infer(&self, req: InferRequest) -> Result<InferResponse, RuntimeError> {
        let plan_model = self.plan_model.lock().expect("lock").clone();
        let Some(model) = plan_model else {
            return Err(RuntimeError::NotLoaded(req.model));
        };
        if model != req.model {
            return Err(RuntimeError::NotLoaded(req.model));
        }

        let loaded = self.loaded.lock().expect("lock").clone();
        if let Some(lm) = loaded {
            // Tensor-backed path (lab-tiny embeddings or future full Kimi kernels).
            let reply = decode::generate(&lm, &req.prompt, req.max_tokens.max(16));
            return Ok(InferResponse {
                text: reply.clone(),
                prompt_tokens: req.prompt.split_whitespace().count() as u32,
                completion_tokens: reply.split_whitespace().count() as u32,
            });
        }

        let mode = self
            .readiness
            .lock()
            .expect("lock")
            .as_ref()
            .map(|r| match r.inference_mode {
                InferenceMode::StubAwaitingPool => "stub-awaiting-pool",
                InferenceMode::StubPoolReady => "stub-pool-ready",
                InferenceMode::LoadingWeights => "loading-weights",
                InferenceMode::ModelLoaded => "model-loaded",
                InferenceMode::ServiceLive => "service-live",
            })
            .unwrap_or("stub");
        let reply = StubEngine::expected_text_mode(mode, &model, &req.prompt);
        Ok(InferResponse {
            text: reply.clone(),
            prompt_tokens: req.prompt.split_whitespace().count() as u32,
            completion_tokens: reply.split_whitespace().count() as u32,
        })
    }
}

pub fn readiness_for_pool(pool_vram_mib: u64, backends: u32) -> Result<ModelReadiness, String> {
    readiness_for_pool_ex(pool_vram_mib, backends, RuntimeFlags::default(), None)
}

pub fn readiness_for_pool_ex(
    pool_vram_mib: u64,
    backends: u32,
    flags: RuntimeFlags,
    vram_growth_mib_per_sec: Option<f64>,
) -> Result<ModelReadiness, String> {
    let m = ManifestFile::load_default()?;
    let spec = m
        .primary()
        .ok_or_else(|| "manifest has no models".to_string())?;
    Ok(spec.readiness(pool_vram_mib, backends, flags, vram_growth_mib_per_sec))
}

#[cfg(test)]
mod tests {
    use super::*;
    use joule_proto::{NodeId, ShardAssignment, ShardRole, CLUSTER_MODEL};
    use uuid::Uuid;

    #[tokio::test]
    async fn stub_roundtrip() {
        let eng = StubEngine::new();
        let plan = ClusterPlan {
            plan_id: Uuid::new_v4(),
            model: CLUSTER_MODEL.into(),
            shards: vec![ShardAssignment {
                node: NodeId::new(),
                role: ShardRole::Replica,
                layer_start: Some(0),
                layer_end: Some(0),
                tp_rank: None,
                tp_world: None,
                mem_share_mib: 8192,
                mem_fraction_ppm: 1_000_000,
            }],
            pool_mem_mib: 8192,
            model_layers: 1,
        };
        eng.load_plan(&plan).await.unwrap();
        let out = eng
            .infer(InferRequest {
                model: CLUSTER_MODEL.into(),
                prompt: "hello".into(),
                max_tokens: 8,
            })
            .await
            .unwrap();
        assert!(out.text.contains("hello"));
    }

    #[test]
    fn readiness_gates_kimi() {
        let r = readiness_for_pool(10 * 1024, 2).unwrap();
        assert!(!r.pool_ready);
        assert!(r.next_milestone.is_some());
        let r = readiness_for_pool(72 * 1024, 5).unwrap();
        assert!(r.pool_ready);
        assert!(r.weights_published);
        assert!(r.can_load_model);
    }

    fn demo_plan() -> ClusterPlan {
        ClusterPlan {
            plan_id: Uuid::new_v4(),
            model: CLUSTER_MODEL.into(),
            shards: vec![ShardAssignment {
                node: NodeId::new(),
                role: ShardRole::Replica,
                layer_start: Some(0),
                layer_end: Some(0),
                tp_rank: None,
                tp_world: None,
                mem_share_mib: 1024,
                mem_fraction_ppm: 1_000_000,
            }],
            pool_mem_mib: 1024,
            model_layers: 1,
        }
    }

    #[test]
    fn digests_gate_service_live_honesty() {
        let dir = std::env::temp_dir().join(format!("joule-digests-{}", Uuid::new_v4()));
        let blob = std::env::temp_dir().join(format!("joule-digests-blobs-{}", Uuid::new_v4()));
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&blob);
        // Isolate blob root so parallel tests do not race shared HOME store.
        std::env::set_var("JOULE_BLOBS_DIR", &blob);
        let store = WeightsStore::new(&dir);
        assert!(!store.digests_verified(
            "kimi-open",
            ManifestFile::load_default()
                .unwrap()
                .primary()
                .unwrap()
                .pick_quant(8192)
                .unwrap()
        ));
        let m = ManifestFile::load_default().unwrap();
        let spec = m.primary().unwrap();
        let quant = spec.pick_quant(8192).unwrap();
        let eng = ClusterEngine::new();
        let report = prepare_and_install(&store, &eng, spec, quant).expect("install");
        assert!(report.tensors > 0 || report.bytes_resident > 0);
        assert!(
            store.digests_verified(&spec.id, quant),
            "sha256-verified digests required"
        );
        // Non-stub infer path when tensors installed.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            eng.load_plan(&demo_plan()).await.unwrap();
            let out = eng
                .infer(InferRequest {
                    model: CLUSTER_MODEL.into(),
                    prompt: "digest-live".into(),
                    max_tokens: 8,
                })
                .await
                .unwrap();
            assert!(
                !out.text.contains("[joule-stub:"),
                "tensor path must not be stub echo: {}",
                out.text
            );
        });
        std::env::remove_var("JOULE_BLOBS_DIR");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&blob);
    }

    /// Shipped path: prepare lab-tiny → load_model → ClusterEngine::install_loaded → infer.
    /// Must be tensor-backed (`joule-tensor`), not stub echo.
    #[tokio::test]
    async fn cluster_engine_lab_tiny_infer_is_tensor_backed() {
        use crate::manifest::ManifestFile;
        use crate::weights::WeightsStore;
        use std::fs;

        let dir = std::env::temp_dir().join(format!(
            "joule-cluster-eng-{}-{}",
            std::process::id(),
            Uuid::new_v4()
        ));
        let _ = fs::remove_dir_all(&dir);
        let store = WeightsStore::new(&dir);
        let m = ManifestFile::load_default().expect("manifest");
        let spec = m.model("kimi-open").expect("kimi-open");
        let quant = spec
            .weights
            .quants
            .iter()
            .find(|q| q.id == "lab-tiny")
            .expect("lab-tiny quant");
        let eng = ClusterEngine::new();
        eng.load_plan(&demo_plan()).await.expect("load_plan");
        let report = prepare_and_install(&store, &eng, spec, quant).expect("prepare_and_install");
        assert!(report.tensors >= 1, "lab-tiny tensors={}", report.tensors);
        assert!(eng.has_resident_weights());
        assert!(eng.is_model_loaded());

        let out = eng
            .infer(InferRequest {
                model: CLUSTER_MODEL.into(),
                prompt: "hello joule tensor path".into(),
                max_tokens: 24,
            })
            .await
            .expect("infer");
        assert!(
            out.text.contains("joule-tensor"),
            "must use tensor decode path, got {}",
            out.text
        );
        assert!(
            !out.text.contains("joule-stub"),
            "must not fall back to stub, got {}",
            out.text
        );
        assert!(
            out.text.contains("hello"),
            "prompt should influence tensor output, got {}",
            out.text
        );

        // Without tensors: plan-only ClusterEngine is stub-mode, proving the branch.
        let eng2 = ClusterEngine::new();
        eng2.load_plan(&demo_plan()).await.unwrap();
        let stub_out = eng2
            .infer(InferRequest {
                model: CLUSTER_MODEL.into(),
                prompt: "no weights".into(),
                max_tokens: 8,
            })
            .await
            .unwrap();
        assert!(
            stub_out.text.contains("joule-stub") || stub_out.text.contains("stub"),
            "unloaded engine must use stub path, got {}",
            stub_out.text
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// Next quant past lab-tiny: multi-file lab-mid is larger, multi-tensor, tensor-backed.
    #[tokio::test]
    async fn cluster_engine_lab_mid_infer_is_tensor_backed() {
        use crate::manifest::ManifestFile;
        use crate::weights::WeightsStore;
        use std::fs;

        let dir = std::env::temp_dir().join(format!(
            "joule-lab-mid-{}-{}",
            std::process::id(),
            Uuid::new_v4()
        ));
        let _ = fs::remove_dir_all(&dir);
        let store = WeightsStore::new(&dir);
        let m = ManifestFile::load_default().expect("manifest");
        let spec = m.model("kimi-open").expect("kimi-open");
        let tiny = spec
            .weights
            .quants
            .iter()
            .find(|q| q.id == "lab-tiny")
            .expect("lab-tiny");
        let mid = spec
            .weights
            .quants
            .iter()
            .find(|q| q.id == "lab-mid")
            .expect("lab-mid quant in MANIFEST");
        let tiny_bytes: u64 = tiny.files.iter().map(|f| f.size_bytes).sum();
        let mid_bytes: u64 = mid.files.iter().map(|f| f.size_bytes).sum();
        assert!(
            mid_bytes > tiny_bytes,
            "lab-mid ({mid_bytes}) must be larger than lab-tiny ({tiny_bytes})"
        );
        assert!(
            mid.files.len() > tiny.files.len(),
            "lab-mid should list more files than lab-tiny"
        );
        assert!(!mid.files.is_empty());
        assert!(mid
            .files
            .iter()
            .all(|f| !f.sha256.is_empty() && f.size_bytes > 0));

        // Mid-class (512–2047): lab-mid; large VRAM prefers lab-large.
        assert_eq!(
            spec.pick_quant(1024).expect("pick").id,
            "lab-mid",
            "pick_quant(1024) should prefer lab-mid"
        );
        // Tiny donors still get lab-tiny.
        assert_eq!(spec.pick_quant(256).unwrap().id, "lab-tiny");

        let eng = ClusterEngine::new();
        eng.load_plan(&demo_plan()).await.unwrap();
        let report = prepare_and_install(&store, &eng, spec, mid).expect("lab-mid install");
        assert!(
            report.tensors >= 3,
            "lab-mid must load ≥3 tensors, got {}",
            report.tensors
        );
        assert!(
            report.bytes_resident > tiny_bytes,
            "resident bytes {} should exceed lab-tiny {}",
            report.bytes_resident,
            tiny_bytes
        );

        let out = eng
            .infer(InferRequest {
                model: CLUSTER_MODEL.into(),
                prompt: "lab-mid pool infer".into(),
                max_tokens: 24,
            })
            .await
            .expect("infer");
        assert!(
            out.text.contains("joule-tensor"),
            "lab-mid must be tensor-backed, got {}",
            out.text
        );
        assert!(out.text.contains("lab-mid") || out.text.contains("kimi-open"));
        assert!(!out.text.contains("joule-stub"), "got {}", out.text);
        assert!(out.text.contains("lab-mid") || out.text.contains("pool"));

        let _ = fs::remove_dir_all(&dir);
    }

    /// lab-large is multi-MiB multi-layer, strictly above lab-mid.
    #[tokio::test]
    async fn cluster_engine_lab_large_infer_is_tensor_backed() {
        use crate::manifest::ManifestFile;
        use crate::weights::WeightsStore;
        use std::fs;

        let dir = std::env::temp_dir().join(format!(
            "joule-lab-large-{}-{}",
            std::process::id(),
            Uuid::new_v4()
        ));
        let _ = fs::remove_dir_all(&dir);
        let store = WeightsStore::new(&dir);
        let m = ManifestFile::load_default().expect("manifest");
        let spec = m.model("kimi-open").expect("kimi-open");
        let mid = spec
            .weights
            .quants
            .iter()
            .find(|q| q.id == "lab-mid")
            .expect("lab-mid");
        let large = spec
            .weights
            .quants
            .iter()
            .find(|q| q.id == "lab-large")
            .expect("lab-large in MANIFEST");
        let mid_b: u64 = mid.files.iter().map(|f| f.size_bytes).sum();
        let large_b: u64 = large.files.iter().map(|f| f.size_bytes).sum();
        assert!(
            large_b > mid_b,
            "lab-large {large_b} must exceed lab-mid {mid_b}"
        );
        assert!(
            large_b > 2 * 1024 * 1024,
            "lab-large must be multi-MiB, got {large_b}"
        );
        assert_eq!(spec.pick_quant(8192).unwrap().id, "lab-large");
        assert_eq!(spec.pick_quant(512).unwrap().id, "lab-mid");
        assert_eq!(spec.pick_quant(256).unwrap().id, "lab-tiny");

        let eng = ClusterEngine::new();
        eng.load_plan(&demo_plan()).await.unwrap();
        let report = prepare_and_install(&store, &eng, spec, large).expect("lab-large install");
        assert!(
            report.tensors >= 5,
            "lab-large multi-layer tensors, got {}",
            report.tensors
        );
        let out = eng
            .infer(InferRequest {
                model: CLUSTER_MODEL.into(),
                prompt: "lab-large multi-MiB".into(),
                max_tokens: 16,
            })
            .await
            .expect("infer");
        assert!(
            out.text.contains("joule-tensor"),
            "lab-large must be tensor-backed: {}",
            out.text
        );
        assert!(!out.text.contains("joule-stub"));
        let _ = fs::remove_dir_all(&dir);
    }
}
