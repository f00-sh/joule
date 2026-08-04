//! Inference backends + model manifest + weight cache + **model loading**.
//!
//! Kimi is not loaded until the logical device is large enough. When the pool
//! hits the load milestone (and weights exist), [`load_model`] maps tensors into
//! RAM. Service-live is a separate control flag after the mesh has loaded.

mod cuda_matvec;
mod decode;
mod gpu_engine;
mod k3_meta;
mod k3_pipeline;
mod load;
mod manifest;
mod software;
mod stage;
mod stage_matmul;
mod weights;

pub use cuda_matvec::{cuda_matvec4_f32, host_matvec_f32, production_matvec4};
pub use decode::{
    generate as generate_from_loaded, generate_from_activation_state, generate_tail_from_stage,
};
pub use gpu_engine::{
    full_k3_service_fleet_ok, probe_cuda_devices, production_digests_ok, CudaProbe,
    ProductionEngine,
};
pub use k3_meta::{
    config_sha256_hex, manifest_k3_config_digest, num_hidden_layers_from_config_json,
    placement_model_layers, verified_k3_model_layers, verified_k3_model_layers_from,
    EMBEDDED_K3_CONFIG_JSON,
};
pub use k3_pipeline::{
    pipeline_from_quant, pipelines_for_model, shard_cache_path, synthetic_k3_shard_template,
    validate_k3_scale, PipelineShard, WeightPipeline,
};
pub use load::{load_model, load_model_for_band, LoadError, LoadReport, LoadedModel, TensorInfo};
pub use manifest::{
    InferenceMode, ManifestFile, MilestoneStatus, ModelReadiness, ModelSpec, QuantSpec,
    RuntimeFlags, EMBEDDED_MANIFEST,
};
// prepare_and_install is defined below next to ClusterEngine (crate root).
pub use software::{
    apply_staged, current_arch, current_os, match_target, parse_software_update, read_stage,
    stage_blob, SoftwareTarget, SoftwareUpdateBody, StageStatus,
};
pub use stage::{
    activation_commitment_hex, lab_stage_activation, stage_activation,
    stage_activation_with_weights, weight_material_from_tensors, StageOutput, StageRequest,
};
pub use stage_matmul::{
    pack_jst3, select_band_tensors, stage_activation_matmul, stage_activation_matmul_scoped,
    MATMUL_DIM, MAX_STACK_BLOCKS,
};
pub use weights::{
    digests_verified_for_quant, is_lab_fixture_quant, is_synthetic_placeholder_digest,
    quant_can_unlock_service_digests, BlobAnnounce, PrepareStatus, WeightsStore,
};
// band helpers live on WeightsStore::required_weight_files_for_band / band_files_ready

/// Gate: digests verified for a **lab fixture** quant of the default manifest model.
/// Used for lab CI / protocol arming only — **never** for service_live / full-Kimi claims.
/// Prefer [`production_digests_ok`] / [`digests_verified_for_service_claim`] on control.
pub fn digests_verified_for_primary_lab(store: &WeightsStore) -> Result<bool, String> {
    let m = ManifestFile::load_default()?;
    let spec = m.primary().ok_or_else(|| "no primary model".to_string())?;
    let quant = spec
        .pick_quant(8192)
        .or_else(|| spec.weights.quants.first())
        .ok_or_else(|| "no quant".to_string())?;
    Ok(digests_verified_for_quant(store, &spec.id, quant))
}

/// Service / full-Kimi digest SoT: production `kimi-k3-shards` only.
pub fn digests_verified_for_service_claim(store: &WeightsStore) -> bool {
    production_digests_ok(store)
}

/// Quant recommendation for PoolStatus fanout.
///
/// - Fleet-ready (≥64 GiB verified VRAM and ≥3 backends) and production pins are
///   non-placeholder → **`kimi-k3-shards`** so donors prepare the full-Kimi path.
/// - Otherwise lab quant by memory class (protocol / first-light).
pub fn recommend_quant_for_pool(
    spec: &manifest::ModelSpec,
    pool_vram_mib: u64,
    backends: u32,
) -> Option<&manifest::QuantSpec> {
    if full_k3_service_fleet_ok(pool_vram_mib, backends) {
        if let Some(k3) = production_k3_quant(spec) {
            return Some(k3);
        }
    }
    spec.pick_quant(8192)
        .or_else(|| spec.weights.quants.first())
}

/// Production quant `kimi-k3-shards` when pins are real (non-placeholder).
pub fn production_k3_quant(spec: &manifest::ModelSpec) -> Option<&manifest::QuantSpec> {
    let k3 = spec
        .weights
        .quants
        .iter()
        .find(|q| q.id == "kimi-k3-shards")?;
    if quant_can_unlock_service_digests(k3) {
        Some(k3)
    } else {
        None
    }
}

/// PoolStatus prepare quant: honor control recommend, else lab by node VRAM.
///
/// Callers re-prepare when the returned id changes (lab→kimi-k3-shards).
pub fn resolve_agent_quant<'a>(
    spec: &'a manifest::ModelSpec,
    recommend_quant: Option<&str>,
    node_vram_mib: u32,
) -> Option<&'a manifest::QuantSpec> {
    if let Some(id) = recommend_quant {
        if let Some(q) = spec.weights.quants.iter().find(|q| q.id == id) {
            return Some(q);
        }
    }
    spec.pick_quant(node_vram_mib.max(256))
}

/// PlanAccept band quant: control recommend first; else production K3 when pins
/// are real (8 GiB donors still load K3 bands for multi-donor full-Kimi PP);
/// else lab pick_quant.
pub fn resolve_plan_band_quant<'a>(
    spec: &'a manifest::ModelSpec,
    recommend_quant: Option<&str>,
    node_vram_mib: u32,
) -> Option<&'a manifest::QuantSpec> {
    if let Some(id) = recommend_quant {
        if let Some(q) = spec.weights.quants.iter().find(|q| q.id == id) {
            return Some(q);
        }
    }
    if let Some(k3) = production_k3_quant(spec) {
        return Some(k3);
    }
    spec.pick_quant(node_vram_mib.max(256))
}

/// True when agent should re-prepare after PoolStatus (quant upgrade lab→K3).
pub fn agent_should_reprepare(last_armed_quant: Option<&str>, selected_quant_id: &str) -> bool {
    match last_armed_quant {
        None => true,
        Some(prev) => prev != selected_quant_id,
    }
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
    /// Layer-band pipeline stage: emits real activation tensor bytes.
    async fn stage_layers(&self, req: StageRequest) -> Result<StageOutput, RuntimeError> {
        // Default / StubEngine: ignore weight gate (mesh tests, lab PP without files).
        lab_stage_activation(&req).map_err(RuntimeError::Infer)
    }
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

    /// Snapshot of resident weights for production CUDA stage/infer (ADR 0003).
    pub fn loaded_model_snapshot(&self) -> Option<std::sync::Arc<LoadedModel>> {
        self.loaded.lock().expect("lock").clone()
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

/// Per-shard donor path: stage + install **only** preferred weight files for
/// layer band `[layer_start, layer_end]` (file↔layer map). Does not require
/// quant files outside the preferred set.
///
/// Fail closed if preferred files are missing. Resident basenames ⊆ preferred.
pub fn prepare_and_install_for_band(
    store: &WeightsStore,
    engine: &ClusterEngine,
    spec: &ModelSpec,
    quant: &QuantSpec,
    layer_start: u32,
    layer_end: u32,
) -> Result<LoadReport, String> {
    let preferred = WeightsStore::required_weight_files_for_band(quant, layer_start, layer_end)?;
    let st = store.prepare_for_band(spec, quant, layer_start, layer_end)?;
    if !st.files_complete {
        return Err(format!(
            "band prepare incomplete for {}/{} L{}-{}: {}",
            spec.id, quant.id, layer_start, layer_end, st.message
        ));
    }
    let lm = load_model_for_band(store, spec, quant, layer_start, layer_end)
        .map_err(|e| e.to_string())?;
    // Invariant: only preferred (required) basenames are resident.
    for b in &lm.loaded_file_basenames {
        if !preferred.iter().any(|p| p == b) {
            return Err(format!(
                "band install leaked non-preferred file {b} (preferred={preferred:?})"
            ));
        }
    }
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

    async fn stage_layers(&self, req: StageRequest) -> Result<StageOutput, RuntimeError> {
        // Weight gate + weight-backed activation when LoadedModel is resident.
        let loaded = self.loaded.lock().expect("lock").clone();
        if req.require_band_weights {
            let need = if !req.required_weight_files.is_empty() {
                req.required_weight_files.clone()
            } else {
                joule_cluster::preferred_weight_files(req.layer_start, req.layer_end)
                    .map_err(RuntimeError::Infer)?
            };
            let Some(lm) = loaded.as_ref() else {
                return Err(RuntimeError::Infer(format!(
                    "missing band weights: model not loaded (need layers {}-{} files {need:?})",
                    req.layer_start, req.layer_end
                )));
            };
            let match_one = |f: &str| {
                lm.loaded_file_basenames
                    .iter()
                    .any(|b| b == f || b.ends_with(f))
            };
            let all_need = !need.is_empty() && need.iter().all(|f| match_one(f));
            let any_need = need.iter().any(|f| match_one(f));
            if all_need {
                // K3/band-exact load satisfied.
            } else if any_need {
                let missing: Vec<_> = need.iter().filter(|f| !match_one(f)).cloned().collect();
                return Err(RuntimeError::Infer(format!(
                    "missing band weights: incomplete preferred set for layers {}-{} missing={missing:?} have={:?}",
                    req.layer_start, req.layer_end, lm.loaded_file_basenames
                )));
            } else {
                let real = lm
                    .loaded_file_basenames
                    .iter()
                    .any(|b| b != "__joule_armed__" && !b.is_empty());
                if !real {
                    return Err(RuntimeError::Infer(format!(
                        "missing band weights: no staged files for layers {}-{} (preferred {need:?})",
                        req.layer_start, req.layer_end
                    )));
                }
            }
        }
        // Band-scoped multi-layer pure-Rust matmul (JST3). Prefer preferred-file
        // tensors when source metadata exists; stack depth scales with layer span.
        if let Some(lm) = loaded {
            let preferred = if !req.required_weight_files.is_empty() {
                req.required_weight_files.clone()
            } else {
                joule_cluster::preferred_weight_files(req.layer_start, req.layer_end)
                    .unwrap_or_default()
            };
            match stage_activation_matmul_scoped(
                &req,
                &lm.tensors,
                Some(&lm.tensor_sources),
                &preferred,
            ) {
                Ok(out) => return Ok(out),
                Err(e) => {
                    let material = weight_material_from_tensors(&lm.tensors);
                    if material.is_empty() {
                        if req.require_band_weights {
                            return Err(RuntimeError::Infer(format!("missing band weights: {e}")));
                        }
                        return lab_stage_activation(&req).map_err(RuntimeError::Infer);
                    }
                    return stage_activation_with_weights(&req, &material)
                        .map_err(RuntimeError::Infer);
                }
            }
        }
        if req.require_band_weights {
            return Err(RuntimeError::Infer(
                "missing band weights: model not loaded".into(),
            ));
        }
        lab_stage_activation(&req).map_err(RuntimeError::Infer)
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
    fn k3_shards_fail_closed_lab_unlocks_digests() {
        let m = ManifestFile::load_default().unwrap();
        let spec = m.primary().unwrap();
        let k3 = spec
            .weights
            .quants
            .iter()
            .find(|q| q.id == "kimi-k3-shards")
            .expect("k3-shards");
        let _env = crate::weights::test_env::lock();
        let dir = std::env::temp_dir().join(format!("joule-k3-fc-{}", Uuid::new_v4()));
        let blob = std::env::temp_dir().join(format!("joule-k3-fc-b-{}", Uuid::new_v4()));
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&blob);
        std::env::set_var("JOULE_BLOBS_DIR", &blob);
        let store = WeightsStore::new(&dir);
        assert!(
            !digests_verified_for_quant(&store, &spec.id, k3),
            "kimi-k3-shards must not verify digests without staged bytes"
        );
        // Real LFS pins may unlock when content is present; empty store stays false.
        assert!(
            quant_can_unlock_service_digests(k3),
            "real production pins are eligible to unlock"
        );
        assert!(k3
            .files
            .iter()
            .all(|f| !is_synthetic_placeholder_digest(&f.sha256)));
        // Same quant digests_verified_for_primary_lab uses (pick_quant 8192 → lab-large).
        let lab = spec.pick_quant(8192).expect("lab-large");
        assert!(
            is_lab_fixture_quant(lab),
            "primary path must stay lab fixture"
        );
        let eng = ClusterEngine::new();
        let _ = prepare_and_install(&store, &eng, spec, lab).expect("lab install");
        assert!(
            digests_verified_for_quant(&store, &spec.id, lab),
            "lab fixture must unlock digests after sha256 stage"
        );
        assert!(digests_verified_for_primary_lab(&store).unwrap());
        // Critical product gate: lab complete must NOT unlock service / full-Kimi digests.
        assert!(
            !production_digests_ok(&store),
            "lab bytes must not set production_digests_ok"
        );
        assert!(
            !digests_verified_for_service_claim(&store),
            "service claim digests stay false without kimi-k3-shards residency"
        );
        eprintln!(
            "OBSERVE k3-content-gate: k3_can_unlock={} k3_verified={} lab_verified={} service_claim={} production_ok={}",
            quant_can_unlock_service_digests(k3),
            digests_verified_for_quant(&store, &spec.id, k3),
            digests_verified_for_primary_lab(&store).unwrap(),
            digests_verified_for_service_claim(&store),
            production_digests_ok(&store),
        );
        std::env::remove_var("JOULE_BLOBS_DIR");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&blob);
    }

    #[test]
    fn recommend_quant_fleet_prefers_k3_shards() {
        let m = ManifestFile::load_default().unwrap();
        let spec = m.primary().unwrap();
        let below = recommend_quant_for_pool(spec, 8192, 1).expect("lab below fleet");
        assert!(
            is_lab_fixture_quant(below),
            "below fleet must recommend lab, got {}",
            below.id
        );
        let fleet = recommend_quant_for_pool(spec, 72 * 1024, 5).expect("k3 on fleet");
        assert_eq!(
            fleet.id, "kimi-k3-shards",
            "fleet-ready must recommend production quant, got {}",
            fleet.id
        );
        eprintln!(
            "OBSERVE recommend_quant: below={} fleet={}",
            below.id, fleet.id
        );
    }

    /// Shipped agent path: PoolStatus first-light stays lab; fleet recommend upgrades
    /// to kimi-k3-shards; PlanAccept band selects K3 even for 8 GiB donors.
    #[test]
    fn resolve_agent_quant_upgrades_lab_to_k3_under_fleet_recommend() {
        let m = ManifestFile::load_default().unwrap();
        let spec = m.primary().unwrap();
        // First-light (no recommend): lab by mem.
        let first = resolve_agent_quant(spec, None, 8192).expect("first");
        assert_eq!(
            first.id, "lab-large",
            "first-light uses lab; got {}",
            first.id
        );
        // Explicit control fleet recommend.
        let upgraded = resolve_agent_quant(spec, Some("kimi-k3-shards"), 8192).expect("upgrade");
        assert_eq!(upgraded.id, "kimi-k3-shards");
        assert!(agent_should_reprepare(Some("lab-large"), "kimi-k3-shards"));
        assert!(!agent_should_reprepare(Some("lab-large"), "lab-large"));
        // PlanAccept band: even without recommend, K3 when pins real (not pick_quant lab).
        let band = resolve_plan_band_quant(spec, None, 8192).expect("band");
        assert_eq!(
            band.id, "kimi-k3-shards",
            "8GiB PlanAccept band must use K3 pins, not pick_quant lab"
        );
        let band_fleet =
            resolve_plan_band_quant(spec, Some("kimi-k3-shards"), 8192).expect("band fleet");
        assert_eq!(band_fleet.id, "kimi-k3-shards");
        assert_eq!(spec.pick_quant(8192).unwrap().id, "lab-large");
        eprintln!(
            "OBSERVE agent-quant-upgrade: first=lab-large fleet_rec=kimi-k3-shards band=kimi-k3-shards reprepare=true"
        );
    }

    #[tokio::test]
    async fn cluster_engine_stage_requires_band_weights() {
        use crate::manifest::WeightFile;
        use sha2::{Digest, Sha256};

        let eng = ClusterEngine::new();
        eng.load_plan(&demo_plan()).await.unwrap();
        let req = StageRequest {
            model: CLUSTER_MODEL.into(),
            prompt: "band-gate".into(),
            layer_start: 0,
            layer_end: 5,
            upstream: vec![],
            is_tail: false,
            require_upstream: false,
            require_band_weights: true,
            required_weight_files: vec!["model-00001-of-000096.safetensors".into()],
        };
        let err = eng.stage_layers(req.clone()).await.unwrap_err();
        assert!(
            format!("{err}").contains("missing band weights"),
            "got {err}"
        );

        // Install band stand-in via load_model_for_band (real tiny safetensors).
        let dir = std::env::temp_dir().join(format!("joule-ce-band-{}", Uuid::new_v4()));
        let _ = std::fs::remove_dir_all(&dir);
        let store = WeightsStore::new(&dir);
        let m = ManifestFile::load_default().unwrap();
        let spec = m.model("kimi-open").unwrap();
        let md = store.model_dir(&spec.id, "kimi-k3-ce-band");
        std::fs::create_dir_all(&md).unwrap();
        let path = md.join("model-00001-of-000096.safetensors");
        load::write_tiny_safetensors_fixture(&path).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let hash = hex::encode(Sha256::digest(&bytes));
        let quant = QuantSpec {
            id: "kimi-k3-ce-band".into(),
            min_node_vram_mib: 256,
            approx_file_mib: 1,
            files: vec![WeightFile {
                path: "model-00001-of-000096.safetensors".into(),
                sha256: hash,
                url: "peer://1".into(),
                size_bytes: bytes.len() as u64,
            }],
        };
        let lm = load_model_for_band(&store, spec, &quant, 0, 5).unwrap();
        eng.install_loaded(lm);
        let out = eng
            .stage_layers(req.clone())
            .await
            .expect("stage with weights");
        assert!(
            out.activation.starts_with(b"JST3"),
            "ClusterEngine with f32 weights must emit matmul JST3, got {:?}",
            &out.activation[..4.min(out.activation.len())]
        );
        assert!(!out.activation.is_empty());
        // Mutate staged file: diagonal scale 1.0 → 2.5 (real f32 matmul difference).
        let path2 = md.join("model-00001-of-000096.safetensors");
        {
            use safetensors::tensor::{serialize, TensorView};
            use safetensors::Dtype;
            use std::collections::BTreeMap;
            let mut floats = vec![0.0f32; 16];
            for i in 0..4 {
                floats[i * 4 + i] = 2.5; // non-zero diagonal
            }
            let mut data = Vec::with_capacity(64);
            for f in &floats {
                data.extend_from_slice(&f.to_le_bytes());
            }
            let tensor = TensorView::new(Dtype::F32, vec![4, 4], &data).unwrap();
            let mut map: BTreeMap<String, TensorView<'_>> = BTreeMap::new();
            map.insert("demo.weight".into(), tensor);
            let bytes = serialize(&map, &None).unwrap();
            std::fs::write(&path2, &bytes).unwrap();
            let hash = hex::encode(Sha256::digest(&bytes));
            let quant2 = QuantSpec {
                id: "kimi-k3-ce-band".into(),
                min_node_vram_mib: 256,
                approx_file_mib: 1,
                files: vec![WeightFile {
                    path: "model-00001-of-000096.safetensors".into(),
                    sha256: hash,
                    url: "peer://1".into(),
                    size_bytes: bytes.len() as u64,
                }],
            };
            let lm2 = load_model_for_band(&store, spec, &quant2, 0, 5).unwrap();
            eng.install_loaded(lm2);
        }
        let out2 = eng
            .stage_layers(req)
            .await
            .expect("stage after weight change");
        assert!(out2.activation.starts_with(b"JST3"));
        assert_ne!(
            out.activation, out2.activation,
            "different loaded weight bytes must change matmul activation"
        );
        eprintln!(
            "OBSERVE stage-matmul: jst3_a={} jst3_b={} fail_closed_then_ok",
            out.activation.len(),
            out2.activation.len()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)] // intentional: serialize JOULE_* env for whole async test
    async fn cluster_engine_prepare_stage_is_weight_backed() {
        let _env = crate::weights::test_env::lock();
        let dir = std::env::temp_dir().join(format!("joule-ce-prep-{}", Uuid::new_v4()));
        let blob = std::env::temp_dir().join(format!("joule-ce-prep-b-{}", Uuid::new_v4()));
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&blob);
        std::env::set_var("JOULE_BLOBS_DIR", &blob);
        let store = WeightsStore::new(&dir);
        let m = ManifestFile::load_default().unwrap();
        let spec = m.model("kimi-open").unwrap();
        let lab = spec
            .weights
            .quants
            .iter()
            .find(|q| q.id == "lab-tiny")
            .unwrap();
        let eng = ClusterEngine::new();
        eng.load_plan(&demo_plan()).await.unwrap();
        prepare_and_install(&store, &eng, spec, lab).unwrap();
        let out = eng
            .stage_layers(StageRequest {
                model: CLUSTER_MODEL.into(),
                prompt: "prep-stage".into(),
                layer_start: 0,
                layer_end: 5,
                upstream: vec![],
                is_tail: false,
                require_upstream: false,
                require_band_weights: true,
                required_weight_files: vec![],
            })
            .await
            .expect("lab prepare + gate");
        assert!(
            out.activation.starts_with(b"JST3") || out.activation.starts_with(b"JST2"),
            "prepared ClusterEngine stage must consume weights (JST3 matmul or JST2 fallback)"
        );
        eprintln!(
            "OBSERVE weight-stage-prepare: magic={:?} len={} require_band_weights=true",
            std::str::from_utf8(&out.activation[..4]).unwrap_or("????"),
            out.activation.len()
        );
        std::env::remove_var("JOULE_BLOBS_DIR");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&blob);
    }

    /// Per-shard band-only prepare/install: only preferred files resident; other band fails closed.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)] // intentional: serialize JOULE_* env for whole async test
    async fn prepare_and_install_for_band_only_preferred_files() {
        use crate::load::write_tiny_safetensors_fixture;
        use crate::manifest::WeightFile;
        use sha2::{Digest, Sha256};

        let _env = crate::weights::test_env::lock();
        let dir = std::env::temp_dir().join(format!("joule-band-only-{}", Uuid::new_v4()));
        let blob = std::env::temp_dir().join(format!("joule-band-only-b-{}", Uuid::new_v4()));
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&blob);
        std::env::set_var("JOULE_BLOBS_DIR", &blob);
        let store = WeightsStore::new(&dir);
        let m = ManifestFile::load_default().unwrap();
        let spec = m.model("kimi-open").unwrap();

        // Multi-file K3-named stand-ins (not whole quant on every donor).
        let model_dir = store.model_dir(&spec.id, "kimi-k3-band-only");
        std::fs::create_dir_all(&model_dir).unwrap();
        let f1 = model_dir.join("model-00001-of-000096.safetensors");
        let f2 = model_dir.join("model-00002-of-000096.safetensors");
        write_tiny_safetensors_fixture(&f1).unwrap();
        // Distinct payload so file2 is not restored from file1's content-addressed blob.
        {
            use safetensors::tensor::{serialize, TensorView};
            use safetensors::Dtype;
            use std::collections::BTreeMap;
            let mut floats = vec![1.0f32; 16];
            floats[0] = std::f32::consts::PI;
            let mut data = Vec::with_capacity(64);
            for f in &floats {
                data.extend_from_slice(&f.to_le_bytes());
            }
            let tensor = TensorView::new(Dtype::F32, vec![4, 4], &data).unwrap();
            let mut map: BTreeMap<String, TensorView<'_>> = BTreeMap::new();
            map.insert("demo.weight".into(), tensor);
            std::fs::write(&f2, serialize(&map, &None).unwrap()).unwrap();
        }
        let h1 = hex::encode(Sha256::digest(std::fs::read(&f1).unwrap()));
        let h2 = hex::encode(Sha256::digest(std::fs::read(&f2).unwrap()));
        assert_ne!(
            h1, h2,
            "fixtures must differ so blob store cannot substitute file2"
        );
        let quant = QuantSpec {
            id: "kimi-k3-band-only".into(),
            min_node_vram_mib: 256,
            approx_file_mib: 1,
            files: vec![
                WeightFile {
                    path: "model-00001-of-000096.safetensors".into(),
                    sha256: h1.clone(),
                    url: format!("peer://k3/{h1}"),
                    size_bytes: std::fs::metadata(&f1).unwrap().len(),
                },
                WeightFile {
                    path: "model-00002-of-000096.safetensors".into(),
                    sha256: h2.clone(),
                    url: format!("peer://k3/{h2}"),
                    size_bytes: std::fs::metadata(&f2).unwrap().len(),
                },
            ],
        };

        // Only file1 on disk → band 0–0 ok; band 1–1 needs file2.
        std::fs::remove_file(&f2).unwrap();
        let eng = ClusterEngine::new();
        eng.load_plan(&demo_plan()).await.unwrap();
        let report = prepare_and_install_for_band(&store, &eng, spec, &quant, 0, 0)
            .expect("band 0-0 install");
        let basenames = eng
            .loaded_report()
            .map(|r| {
                // LoadReport may not expose basenames — re-read via install invariant in report.message
                r.message.clone()
            })
            .unwrap_or_default();
        let lm_names = {
            // Re-load to assert basenames ⊆ preferred
            let lm = load_model_for_band(&store, spec, &quant, 0, 0).unwrap();
            lm.loaded_file_basenames.clone()
        };
        assert_eq!(
            lm_names,
            vec!["model-00001-of-000096.safetensors".to_string()]
        );
        assert!(
            !lm_names
                .iter()
                .any(|b| b == "model-00002-of-000096.safetensors"),
            "must not load file2 for band 0-0"
        );
        let out = eng
            .stage_layers(StageRequest {
                model: CLUSTER_MODEL.into(),
                prompt: "band-only".into(),
                layer_start: 0,
                layer_end: 0,
                upstream: vec![],
                is_tail: false,
                require_upstream: false,
                require_band_weights: true,
                required_weight_files: WeightsStore::required_weight_files_for_band(&quant, 0, 0)
                    .unwrap(),
            })
            .await
            .expect("stage with band-only weights");
        assert!(
            out.activation.starts_with(b"JST3") || out.activation.starts_with(b"JST2"),
            "magic={:?}",
            &out.activation[..4.min(out.activation.len())]
        );
        assert!(
            prepare_and_install_for_band(&store, &eng, spec, &quant, 1, 1).is_err(),
            "band 1-1 must fail closed without file2"
        );
        eprintln!(
            "OBSERVE band-only-load: basenames={lm_names:?} bytes={} stage_magic={:?} msg={basenames}",
            report.bytes_resident,
            std::str::from_utf8(&out.activation[..4]).unwrap_or("????"),
        );
        std::env::remove_var("JOULE_BLOBS_DIR");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&blob);
    }

    #[test]
    fn digests_gate_service_live_honesty() {
        let _env = crate::weights::test_env::lock();
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
        assert!(
            !out.text.starts_with("[joule-pipeline-stage:"),
            "must not be stage-tag only: {}",
            out.text
        );

        // Weight flip on multi-file mid must change tokens (real tensor path).
        assert!(eng.loaded_report().is_some());
        {
            use crate::load::load_model;
            let mut lm = load_model(&store, spec, mid).expect("reload mid");
            if let Some(emb) = lm.tensors.get_mut("tok_embeddings.weight") {
                // Invert all f32 signs so every scored row changes.
                for chunk in emb.chunks_exact_mut(4) {
                    let mut v = f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
                    v = -v - 0.37;
                    chunk.copy_from_slice(&v.to_le_bytes());
                }
            }
            eng.install_loaded(lm);
        }
        let out2 = eng
            .infer(InferRequest {
                model: CLUSTER_MODEL.into(),
                prompt: "lab-mid pool infer".into(),
                max_tokens: 24,
            })
            .await
            .expect("infer after flip");
        assert_ne!(
            out.text, out2.text,
            "lab-mid weight flip must change decode text"
        );
        // Activation-sensitive tail decode on multi-file mid.
        let a = generate_from_activation_state(
            CLUSTER_MODEL,
            "lab-mid",
            &load_model(&store, spec, mid).unwrap().tensors,
            None,
            &[0.1, 0.2, 0.3, 0.4],
            "mid-tail",
            16,
        )
        .expect("mid act decode");
        let b = generate_from_activation_state(
            CLUSTER_MODEL,
            "lab-mid",
            &load_model(&store, spec, mid).unwrap().tensors,
            None,
            &[8.0, -1.0, 0.0, 2.0],
            "mid-tail",
            16,
        )
        .expect("mid act decode b");
        assert!(a.contains("joule-decode"), "a={a}");
        assert_ne!(a, b, "activation seed must change mid tail text");
        eprintln!(
            "OBSERVE lab-mid-decode: tensors={} bytes={} text_len={} flip_diff=true act_diff=true",
            report.tensors,
            report.bytes_resident,
            out.text.len()
        );

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
        assert!(
            !out.text.starts_with("[joule-pipeline-stage:"),
            "must not be stage-tag only: {}",
            out.text
        );
        // Weight-sensitive: invert embedding floats → different tokens.
        {
            use crate::load::load_model;
            let mut lm = load_model(&store, spec, large).expect("reload large");
            if let Some(emb) = lm.tensors.get_mut("tok_embeddings.weight") {
                for chunk in emb.chunks_exact_mut(4) {
                    let mut v = f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
                    v = -v + 0.11;
                    chunk.copy_from_slice(&v.to_le_bytes());
                }
            }
            eng.install_loaded(lm);
        }
        let out2 = eng
            .infer(InferRequest {
                model: CLUSTER_MODEL.into(),
                prompt: "lab-large multi-MiB".into(),
                max_tokens: 16,
            })
            .await
            .expect("infer large flip");
        assert_ne!(
            out.text, out2.text,
            "lab-large weight flip must change decode text"
        );
        let tensors = crate::load::load_model(&store, spec, large)
            .unwrap()
            .tensors;
        let ta = generate_from_activation_state(
            CLUSTER_MODEL,
            "lab-large",
            &tensors,
            None,
            &[1.0, 0.0, 0.0, 0.0],
            "large-tail",
            16,
        )
        .expect("large act a");
        let tb = generate_from_activation_state(
            CLUSTER_MODEL,
            "lab-large",
            &tensors,
            None,
            &[0.0, 0.0, 0.0, 1.0],
            "large-tail",
            16,
        )
        .expect("large act b");
        assert!(ta.contains("joule-decode"), "{ta}");
        assert_ne!(ta, tb);
        eprintln!(
            "OBSERVE lab-large-decode: tensors={} bytes={} text_len={} flip_diff=true act_diff=true",
            report.tensors,
            report.bytes_resident,
            out.text.len()
        );
        let _ = fs::remove_dir_all(&dir);
    }
}
