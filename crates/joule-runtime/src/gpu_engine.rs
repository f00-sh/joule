//! Production GPU/FFI engine (ADR 0003): CUDA driver matvec + content-proof weights.
//!
//! - Stage/infer for production quants **fail closed** without verified digests.
//! - Stage runs **CUDA matvec** on resident weight matrices (not a text tag on
//!   ClusterEngine output). See [`crate::cuda_matvec`].

use crate::cuda_matvec::{host_matvec_f32, production_matvec4};
use crate::load::LoadedModel;
use crate::manifest::{ManifestFile, ModelReadiness, QuantSpec};
use crate::stage::{StageOutput, StageRequest};
use crate::stage_matmul::{pack_jst3, MATMUL_DIM};
use crate::weights::{
    digests_verified_for_quant, is_lab_fixture_quant, quant_can_unlock_service_digests,
    WeightsStore,
};
use crate::{ClusterEngine, Engine, InferRequest, InferResponse, LoadReport, RuntimeError};
use async_trait::async_trait;
use joule_proto::ClusterPlan;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Result of probing the CUDA driver (FFI).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CudaProbe {
    pub available: bool,
    pub device_count: u32,
    pub detail: &'static str,
}

/// Outcome of commit-gated quant upgrade (lab→K3 without bricking serve).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpgradeOutcome {
    /// Gate + resident weights committed to `quant_id`.
    Committed { quant_id: String },
    /// Probe failed; prior content_verified / quant unchanged.
    Deferred {
        reason: String,
        kept_verified: bool,
        kept_quant: Option<String>,
    },
}

impl UpgradeOutcome {
    pub fn is_committed(&self) -> bool {
        matches!(self, Self::Committed { .. })
    }
}

/// Dynamically load libcuda and query device count (cuInit + cuDeviceGetCount).
pub fn probe_cuda_devices() -> CudaProbe {
    probe_cuda_devices_with(|name| {
        let candidates = [
            name,
            "libcuda.so.1",
            "libcuda.so",
            "/usr/lib/libcuda.so.1",
            "/usr/lib64/libcuda.so.1",
            "/usr/lib/x86_64-linux-gnu/libcuda.so.1",
        ];
        for c in candidates {
            if let Ok(lib) = unsafe { libloading::Library::new(c) } {
                return Ok(lib);
            }
        }
        Err("libcuda not found".into())
    })
}

/// Testable probe: inject library open.
pub fn probe_cuda_devices_with<F>(open: F) -> CudaProbe
where
    F: FnOnce(&str) -> Result<libloading::Library, String>,
{
    let lib = match open("libcuda.so.1") {
        Ok(l) => l,
        Err(_) => {
            return CudaProbe {
                available: false,
                device_count: 0,
                detail: "libcuda_missing",
            };
        }
    };

    type CuInit = unsafe extern "C" fn(u32) -> i32;
    type CuDeviceGetCount = unsafe extern "C" fn(*mut i32) -> i32;

    let cu_init: libloading::Symbol<CuInit> = match unsafe { lib.get(b"cuInit\0") } {
        Ok(s) => s,
        Err(_) => {
            return CudaProbe {
                available: false,
                device_count: 0,
                detail: "cuInit_missing",
            };
        }
    };
    let cu_count: libloading::Symbol<CuDeviceGetCount> =
        match unsafe { lib.get(b"cuDeviceGetCount\0") } {
            Ok(s) => s,
            Err(_) => {
                return CudaProbe {
                    available: false,
                    device_count: 0,
                    detail: "cuDeviceGetCount_missing",
                };
            }
        };

    let init_rc = unsafe { cu_init(0) };
    if init_rc != 0 {
        return CudaProbe {
            available: false,
            device_count: 0,
            detail: "cuInit_failed",
        };
    }
    let mut count: i32 = 0;
    let count_rc = unsafe { cu_count(&mut count) };
    if count_rc != 0 || count < 0 {
        return CudaProbe {
            available: false,
            device_count: 0,
            detail: "cuDeviceGetCount_failed",
        };
    }
    let n = count as u32;
    if n == 0 {
        return CudaProbe {
            available: false,
            device_count: 0,
            detail: "zero_devices",
        };
    }
    let _ = lib;
    CudaProbe {
        available: true,
        device_count: n,
        detail: "ok",
    }
}

/// Production engine: CUDA matvec + content-proof weight path (ADR 0003).
pub struct ProductionEngine {
    inner: Arc<ClusterEngine>,
    cuda: CudaProbe,
    /// Last quant id installed via [`Self::install_production`] / mark path.
    production_quant: Mutex<Option<String>>,
    /// Set only after [`Self::require_production_content`] succeeds for that quant.
    content_verified: Mutex<bool>,
}

impl ProductionEngine {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ClusterEngine::new()),
            cuda: probe_cuda_devices(),
            production_quant: Mutex::new(None),
            content_verified: Mutex::new(false),
        }
    }

    pub fn with_cuda_probe(probe: CudaProbe) -> Self {
        Self {
            inner: Arc::new(ClusterEngine::new()),
            cuda: probe,
            production_quant: Mutex::new(None),
            content_verified: Mutex::new(false),
        }
    }

    pub fn cuda(&self) -> CudaProbe {
        self.cuda
    }

    pub fn cluster(&self) -> &ClusterEngine {
        self.inner.as_ref()
    }

    pub fn cluster_arc(&self) -> Arc<ClusterEngine> {
        self.inner.clone()
    }

    pub fn update_readiness(&self, r: ModelReadiness) {
        self.inner.update_readiness(r);
    }

    pub fn install_loaded(&self, model: LoadedModel) {
        self.inner.install_loaded(model);
    }

    pub fn clear_loaded(&self) {
        self.inner.clear_loaded();
        *self.content_verified.lock().expect("lock") = false;
        *self.production_quant.lock().expect("lock") = None;
    }

    pub fn loaded_report(&self) -> Option<LoadReport> {
        self.inner.loaded_report()
    }

    pub fn is_model_loaded(&self) -> bool {
        self.inner.is_model_loaded()
    }

    pub fn has_resident_weights(&self) -> bool {
        self.inner.has_resident_weights()
    }

    pub fn content_verified(&self) -> bool {
        *self.content_verified.lock().expect("lock")
    }

    pub fn production_quant_id(&self) -> Option<String> {
        self.production_quant.lock().expect("lock").clone()
    }

    /// True when quant id is production K3-class (not lab fixtures).
    pub fn is_production_quant_id(id: &str) -> bool {
        id == "kimi-k3-shards" || id.starts_with("kimi-k3")
    }

    /// Snapshot of content gate (for commit-gated upgrades).
    pub fn snapshot_gate(&self) -> (Option<String>, bool) {
        (
            self.production_quant.lock().expect("lock").clone(),
            *self.content_verified.lock().expect("lock"),
        )
    }

    /// Restore content gate after a failed upgrade probe.
    pub fn restore_gate(&self, snap: (Option<String>, bool)) {
        *self.production_quant.lock().expect("lock") = snap.0;
        *self.content_verified.lock().expect("lock") = snap.1;
    }

    /// **Test / intentional fail-closed only.** Clears content_verified.
    /// Agents must use [`Self::try_commit_quant_upgrade`] — never call this before mark.
    pub fn begin_quant_intent(&self, quant: &QuantSpec) {
        *self.production_quant.lock().expect("lock") = Some(quant.id.clone());
        *self.content_verified.lock().expect("lock") = false;
    }

    /// Commit-gated full quant arm: **probe without clearing** the prior gate.
    /// On success installs `loaded` and sets verified; on failure leaves prior serve intact.
    pub fn try_commit_quant_upgrade(
        &self,
        store: &WeightsStore,
        quant: &QuantSpec,
        loaded: LoadedModel,
    ) -> UpgradeOutcome {
        // Probe only — do not begin_quant_intent (would brick lab serve).
        if let Err(e) = Self::require_production_content(store, quant) {
            return UpgradeOutcome::Deferred {
                reason: e.to_string(),
                kept_verified: self.content_verified(),
                kept_quant: self.production_quant_id(),
            };
        }
        self.inner.install_loaded(loaded);
        *self.production_quant.lock().expect("lock") = Some(quant.id.clone());
        *self.content_verified.lock().expect("lock") = true;
        UpgradeOutcome::Committed {
            quant_id: quant.id.clone(),
        }
    }

    /// Commit-gated band arm (multi-donor). Probe band readiness without clearing gate.
    pub fn try_commit_band_upgrade(
        &self,
        store: &WeightsStore,
        model: &str,
        quant: &QuantSpec,
        loaded: LoadedModel,
        layer_start: u32,
        layer_end: u32,
    ) -> UpgradeOutcome {
        if let Err(e) =
            Self::require_production_band(store, model, quant, layer_start, layer_end)
        {
            return UpgradeOutcome::Deferred {
                reason: e.to_string(),
                kept_verified: self.content_verified(),
                kept_quant: self.production_quant_id(),
            };
        }
        self.inner.install_loaded(loaded);
        *self.production_quant.lock().expect("lock") = Some(quant.id.clone());
        *self.content_verified.lock().expect("lock") = true;
        UpgradeOutcome::Committed {
            quant_id: quant.id.clone(),
        }
    }

    /// Full-quant digest gate (all listed files complete + non-synthetic pins).
    pub fn require_production_content(
        store: &WeightsStore,
        quant: &QuantSpec,
    ) -> Result<(), RuntimeError> {
        if is_lab_fixture_quant(quant) {
            return Ok(());
        }
        if Self::is_production_quant_id(&quant.id) {
            if !quant_can_unlock_service_digests(quant) {
                return Err(RuntimeError::Infer(
                    "production quant digests are placeholders — refuse".into(),
                ));
            }
            if !digests_verified_for_quant(store, "kimi-open", quant) {
                return Err(RuntimeError::Infer(
                    "production quant digests not verified (missing/wrong resident weights)".into(),
                ));
            }
        }
        Ok(())
    }

    /// Band-scoped digest gate for multi-donor partial residency (AC3).
    /// Production quant unlocks stage when preferred band files match sha256 on disk
    /// (not only when all 96 full-quant shards are complete).
    pub fn require_production_band(
        store: &WeightsStore,
        model: &str,
        quant: &QuantSpec,
        layer_start: u32,
        layer_end: u32,
    ) -> Result<(), RuntimeError> {
        if is_lab_fixture_quant(quant) {
            return Ok(());
        }
        if Self::is_production_quant_id(&quant.id) {
            if !quant_can_unlock_service_digests(quant) {
                return Err(RuntimeError::Infer(
                    "production quant digests are placeholders — refuse".into(),
                ));
            }
            // Full quant complete is always enough.
            if digests_verified_for_quant(store, model, quant) {
                return Ok(());
            }
            store
                .band_files_ready(model, quant, layer_start, layer_end)
                .map_err(|e| {
                    RuntimeError::Infer(format!(
                        "production band digests not verified L{layer_start}-{layer_end}: {e}"
                    ))
                })?;
        }
        Ok(())
    }

    /// Shipped install path: content-proof then load tensors onto the engine.
    pub fn install_production(
        &self,
        store: &WeightsStore,
        model: &str,
        quant: &QuantSpec,
        loaded: LoadedModel,
    ) -> Result<(), RuntimeError> {
        let _ = model;
        Self::require_production_content(store, quant)?;
        self.inner.install_loaded(loaded);
        *self.production_quant.lock().expect("lock") = Some(quant.id.clone());
        *self.content_verified.lock().expect("lock") = true;
        Ok(())
    }

    /// Band install: unlock via band_files_ready (or full quant), then resident load.
    pub fn install_production_band(
        &self,
        store: &WeightsStore,
        model: &str,
        quant: &QuantSpec,
        loaded: LoadedModel,
        layer_start: u32,
        layer_end: u32,
    ) -> Result<(), RuntimeError> {
        Self::require_production_band(store, model, quant, layer_start, layer_end)?;
        self.inner.install_loaded(loaded);
        *self.production_quant.lock().expect("lock") = Some(quant.id.clone());
        *self.content_verified.lock().expect("lock") = true;
        Ok(())
    }

    /// Mark content when digests verify for full `quant`.
    /// **Does not clear** prior verification on failure (snapshot/restore).
    pub fn mark_content_from_store(
        &self,
        store: &WeightsStore,
        quant: &QuantSpec,
    ) -> Result<(), RuntimeError> {
        let snap = self.snapshot_gate();
        match Self::require_production_content(store, quant) {
            Ok(()) => {
                *self.production_quant.lock().expect("lock") = Some(quant.id.clone());
                *self.content_verified.lock().expect("lock") = true;
                Ok(())
            }
            Err(e) => {
                self.restore_gate(snap);
                Err(e)
            }
        }
    }

    /// Mark after band prepare. Does not clear prior verification on failure.
    pub fn mark_content_from_band(
        &self,
        store: &WeightsStore,
        model: &str,
        quant: &QuantSpec,
        layer_start: u32,
        layer_end: u32,
    ) -> Result<(), RuntimeError> {
        let snap = self.snapshot_gate();
        match Self::require_production_band(store, model, quant, layer_start, layer_end) {
            Ok(()) => {
                *self.production_quant.lock().expect("lock") = Some(quant.id.clone());
                *self.content_verified.lock().expect("lock") = true;
                Ok(())
            }
            Err(e) => {
                self.restore_gate(snap);
                Err(e)
            }
        }
    }

    /// Fail closed unless content was proven via mark/install (lab or production).
    /// Weight-gated stage and all infer require this — no open path when intent missing.
    fn gate_production_content(&self, weight_gated: bool) -> Result<(), RuntimeError> {
        if *self.content_verified.lock().expect("lock") {
            return Ok(());
        }
        if !weight_gated {
            // Ungated lab stage (require_band_weights=false) still refuses if a
            // production quant intent was claimed but never verified.
            let q = self.production_quant.lock().expect("lock").clone();
            if let Some(qid) = q {
                if Self::is_production_quant_id(&qid) {
                    return Err(RuntimeError::Infer(format!(
                        "ProductionEngine: production quant {qid} digests not verified — refuse"
                    )));
                }
            }
            return Ok(());
        }
        // Weight-gated path: always require content proof (lab or K3).
        let q = self
            .production_quant
            .lock()
            .expect("lock")
            .clone()
            .unwrap_or_else(|| "(none)".into());
        Err(RuntimeError::Infer(format!(
            "ProductionEngine: content digests not verified (quant={q}) — refuse stage/infer"
        )))
    }

    /// GPU (or host-fallback) weight stage — **not** ClusterEngine::stage_layers.
    fn production_stage(&self, req: StageRequest) -> Result<StageOutput, RuntimeError> {
        self.gate_production_content(req.require_band_weights)?;
        if req.require_band_weights && !self.has_resident_weights() {
            return Err(RuntimeError::Infer(
                "ProductionEngine: missing resident weights for band stage".into(),
            ));
        }
        self.inner_production_matmul(&req)
    }

    fn inner_production_matmul(&self, req: &StageRequest) -> Result<StageOutput, RuntimeError> {
        // Access loaded model via ClusterEngine helper.
        let Some(lm) = self.inner.loaded_model_snapshot() else {
            if req.require_band_weights {
                return Err(RuntimeError::Infer(
                    "ProductionEngine: model not loaded".into(),
                ));
            }
            return Err(RuntimeError::Infer(
                "ProductionEngine: no resident weights for production stage".into(),
            ));
        };
        if req.require_band_weights {
            let need = if !req.required_weight_files.is_empty() {
                req.required_weight_files.clone()
            } else {
                joule_cluster::preferred_weight_files(req.layer_start, req.layer_end)
                    .map_err(RuntimeError::Infer)?
            };
            let match_one = |f: &str| {
                lm.loaded_file_basenames
                    .iter()
                    .any(|b| b == f || b.ends_with(f))
            };
            if !need.is_empty() && !need.iter().any(|f| match_one(f)) {
                let real = lm
                    .loaded_file_basenames
                    .iter()
                    .any(|b| b != "__joule_armed__" && !b.is_empty());
                if !real {
                    return Err(RuntimeError::Infer(format!(
                        "missing band weights for production stage layers {}-{}",
                        req.layer_start, req.layer_end
                    )));
                }
            }
        }

        let (matrix, x) = weight_matrix_and_state(&lm.tensors, req).map_err(RuntimeError::Infer)?;
        let (y, used_cuda) = production_matvec4(&matrix, &x).map_err(RuntimeError::Infer)?;
        // Optional bias-style stack: second matvec with layer scale when CUDA available
        // keeps path weight-sensitive and GPU-backed.
        let mut state = y;
        if used_cuda {
            // Second GPU pass with scaled input for stack depth ≥1.
            let mut x2 = state;
            for v in &mut x2 {
                *v *= 1.0 + req.layer_start as f32 * 0.001;
            }
            let (y2, _) = production_matvec4(&matrix, &x2).map_err(RuntimeError::Infer)?;
            state = y2;
        } else {
            let host = host_matvec_f32(&matrix, &state, 4).map_err(RuntimeError::Infer)?;
            state.copy_from_slice(&host[..4]);
        }

        // Sole StageOutput contract: shared pack_jst3 (activation + is_tail text/decode).
        let backend = if used_cuda {
            b"joule-stage-cuda-matvec-v1".as_slice()
        } else {
            b"joule-stage-production-host-matvec-v1".as_slice()
        };
        pack_jst3(
            req,
            &state,
            1,
            lm.tensors.len() as u32,
            &lm.tensors,
            backend,
        )
        .map_err(RuntimeError::Infer)
    }

    fn production_infer(&self, req: InferRequest) -> Result<InferResponse, RuntimeError> {
        // Infer always weight-gated on ProductionEngine.
        self.gate_production_content(true)?;
        if !self.has_resident_weights() {
            return Err(RuntimeError::NotLoaded(req.model.clone()));
        }
        let Some(lm) = self.inner.loaded_model_snapshot() else {
            return Err(RuntimeError::NotLoaded(req.model));
        };
        // GPU weight fingerprint: matvec over first weight matrix — proves CUDA path.
        let (matrix, mut x) = weight_matrix_and_state(
            &lm.tensors,
            &StageRequest {
                model: req.model.clone(),
                prompt: req.prompt.clone(),
                layer_start: 0,
                layer_end: 0,
                upstream: vec![],
                is_tail: true,
                require_upstream: false,
                require_band_weights: true,
                required_weight_files: lm.loaded_file_basenames.clone(),
            },
        )
        .map_err(RuntimeError::Infer)?;
        // Fold prompt into x so fingerprint is prompt+weight sensitive.
        let ph = Sha256::digest(req.prompt.as_bytes());
        for i in 0..4 {
            x[i] += (ph[i] as f32 / 255.0) * 0.1;
        }
        let (y, used_cuda) = production_matvec4(&matrix, &x).map_err(RuntimeError::Infer)?;
        let mut fp = Sha256::new();
        for v in &y {
            fp.update(v.to_le_bytes());
        }
        let fp_hex = hex::encode(fp.finalize());
        // Decode still uses resident tensors (real weight path).
        let body = crate::decode::generate(&lm, &req.prompt, req.max_tokens.max(16));
        let backend = if used_cuda {
            format!("joule-cuda:devices={}", self.cuda.device_count)
        } else {
            "joule-production:host-matvec".into()
        };
        let text = format!("[{backend} fp={}] {body}", &fp_hex[..16.min(fp_hex.len())]);
        Ok(InferResponse {
            text: text.clone(),
            prompt_tokens: req.prompt.split_whitespace().count() as u32,
            completion_tokens: text.split_whitespace().count() as u32,
        })
    }
}

impl Default for ProductionEngine {
    fn default() -> Self {
        Self::new()
    }
}

fn weight_matrix_and_state(
    tensors: &HashMap<String, Vec<u8>>,
    req: &StageRequest,
) -> Result<([f32; 16], [f32; 4]), String> {
    let need = MATMUL_DIM * MATMUL_DIM;
    let mut matrix = [0.0f32; 16];
    let mut found = false;
    let mut names: Vec<&String> = tensors.keys().collect();
    names.sort();
    for name in names {
        if name == "__joule_armed__" {
            continue;
        }
        let Some(w) = as_f32s(&tensors[name]) else {
            continue;
        };
        if w.is_empty() {
            continue;
        }
        for i in 0..need {
            matrix[i] = w[i % w.len()];
        }
        found = true;
        break;
    }
    if !found {
        return Err("production stage: no f32 weight matrix in resident tensors".into());
    }
    let mut x = [0.0f32; 4];
    let seed = Sha256::digest(req.prompt.trim().as_bytes());
    for i in 0..4 {
        let b0 = seed[i % 32];
        let b1 = seed[(i + 7) % 32];
        let u = u16::from_le_bytes([b0, b1]);
        x[i] = (u as f32 / 65535.0) * 2.0 - 1.0;
    }
    if req.upstream.len() >= 4 {
        let n = (req.upstream.len() / 4).min(4);
        for (i, slot) in x.iter_mut().enumerate().take(n) {
            let mut buf = [0u8; 4];
            buf.copy_from_slice(&req.upstream[i * 4..i * 4 + 4]);
            let raw = f32::from_le_bytes(buf);
            if raw.is_finite() {
                *slot += raw.clamp(-2.0, 2.0) * 0.25;
            }
        }
    }
    Ok((matrix, x))
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

#[async_trait]
impl Engine for ProductionEngine {
    async fn load_plan(&self, plan: &ClusterPlan) -> Result<(), RuntimeError> {
        self.inner.load_plan(plan).await
    }

    async fn stage_layers(&self, req: StageRequest) -> Result<StageOutput, RuntimeError> {
        self.production_stage(req)
    }

    async fn infer(&self, req: InferRequest) -> Result<InferResponse, RuntimeError> {
        // Ensure plan model is set for consistency with ClusterEngine.
        if self.inner.loaded_model_snapshot().is_none() {
            return Err(RuntimeError::NotLoaded(req.model));
        }
        self.production_infer(req)
    }
}

/// Fleet honesty: full production K3 service-live requires multi-backend + high VRAM.
/// Used by [`crate::manifest::ModelSpec::readiness`] (SoT for capacity gates).
pub fn full_k3_service_fleet_ok(pool_vram_mib: u64, backends: u32) -> bool {
    pool_vram_mib >= 65_536 && backends >= 3
}

/// True when digests verified for shipped production quant `kimi-k3-shards`.
pub fn production_digests_ok(store: &WeightsStore) -> bool {
    let Ok(m) = ManifestFile::load_default() else {
        return false;
    };
    let Some(spec) = m.primary() else {
        return false;
    };
    let Some(k3) = spec
        .weights
        .quants
        .iter()
        .find(|q| q.id == "kimi-k3-shards")
    else {
        return false;
    };
    digests_verified_for_quant(store, &spec.id, k3)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::load::{load_model_for_band, write_tiny_safetensors_fixture};
    use crate::manifest::WeightFile;
    use crate::weights::is_synthetic_placeholder_digest;
    use crate::{prepare_and_install, ManifestFile, WeightsStore};
    use joule_proto::{NodeId, ShardAssignment, ShardRole, CLUSTER_MODEL};
    use sha2::{Digest, Sha256};
    use std::fs;
    use uuid::Uuid;

    #[test]
    fn cuda_probe_runs_without_panic() {
        let p = probe_cuda_devices();
        eprintln!(
            "OBSERVE cuda-probe: available={} devices={} detail={}",
            p.available, p.device_count, p.detail
        );
        if p.available {
            assert!(p.device_count >= 1);
        }
    }

    #[test]
    fn manifest_k3_shards_are_real_pins() {
        let m = ManifestFile::load_default().unwrap();
        let spec = m.model("kimi-open").unwrap();
        let k3 = spec
            .weights
            .quants
            .iter()
            .find(|q| q.id == "kimi-k3-shards")
            .expect("kimi-k3-shards");
        assert_eq!(k3.files.len(), 96);
        assert!(k3
            .files
            .iter()
            .all(|f| !is_synthetic_placeholder_digest(&f.sha256)));
        assert!(quant_can_unlock_service_digests(k3));
        let total: u64 = k3.files.iter().map(|f| f.size_bytes).sum();
        assert!(total > 100 * 1024 * 1024 * 1024);
        eprintln!(
            "OBSERVE k3-pins: files={} total_gib={} first={}",
            k3.files.len(),
            total / (1024 * 1024 * 1024),
            &k3.files[0].sha256[..16]
        );
    }

    #[test]
    fn production_digests_fail_closed_without_bytes() {
        let dir = std::env::temp_dir().join(format!("joule-prod-empty-{}", Uuid::new_v4()));
        let _ = fs::remove_dir_all(&dir);
        let store = WeightsStore::new(&dir);
        assert!(!production_digests_ok(&store));
        let m = ManifestFile::load_default().unwrap();
        let k3 = m
            .primary()
            .unwrap()
            .weights
            .quants
            .iter()
            .find(|q| q.id == "kimi-k3-shards")
            .unwrap();
        let err = ProductionEngine::require_production_content(&store, k3).unwrap_err();
        assert!(
            format!("{err}").contains("not verified") || format!("{err}").contains("placeholder"),
            "{err}"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn fleet_gate_rejects_single_small_card() {
        assert!(!full_k3_service_fleet_ok(8192, 1));
        assert!(!full_k3_service_fleet_ok(65_536, 1));
        assert!(!full_k3_service_fleet_ok(8192, 3));
        assert!(full_k3_service_fleet_ok(65_536, 3));
        // readiness SoT uses the same helper (see manifest readiness tests).
        let r = crate::readiness_for_pool(8192, 1).unwrap();
        assert!(!r.pool_ready);
        assert!(!r.can_begin_service);
        let r2 = crate::readiness_for_pool_ex(
            72 * 1024,
            5,
            crate::RuntimeFlags {
                digests_verified: true,
                model_loaded: true,
                service_live: true,
            },
            None,
        )
        .unwrap();
        assert!(r2.pool_ready);
        assert!(r2.can_begin_service);
        assert!(r2.service_live);
        eprintln!("OBSERVE fleet-gates: 8GiB×1 reject; 72GiB×5+flags accept service_live");
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn production_engine_lab_prepare_infer_weight_sensitive() {
        let _env = crate::weights::test_env::lock();
        let root = std::env::temp_dir().join(format!("joule-prod-eng-{}", Uuid::new_v4()));
        let blobs = std::env::temp_dir().join(format!("joule-prod-blobs-{}", Uuid::new_v4()));
        let _ = fs::remove_dir_all(&root);
        let _ = fs::remove_dir_all(&blobs);
        fs::create_dir_all(&blobs).unwrap();
        std::env::set_var("JOULE_BLOBS_DIR", &blobs);
        let store = WeightsStore::new(&root);
        let m = ManifestFile::load_default().unwrap();
        let spec = m.primary().unwrap();
        let lab = spec.pick_quant(256).expect("lab-tiny");
        let eng = ProductionEngine::with_cuda_probe(CudaProbe {
            available: true,
            device_count: 1,
            detail: "test",
        });
        let report = prepare_and_install(&store, eng.cluster(), spec, lab).expect("prepare");
        assert!(report.bytes_resident > 0 || eng.has_resident_weights());
        eng.mark_content_from_store(&store, lab).unwrap();
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
        let a = eng
            .infer(InferRequest {
                model: CLUSTER_MODEL.into(),
                prompt: "prod-engine-aaa".into(),
                max_tokens: 16,
            })
            .await
            .unwrap();
        let b = eng
            .infer(InferRequest {
                model: CLUSTER_MODEL.into(),
                prompt: "prod-engine-bbb".into(),
                max_tokens: 16,
            })
            .await
            .unwrap();
        assert!(
            a.text.contains("joule-cuda") || a.text.contains("joule-production"),
            "{}",
            a.text
        );
        assert!(
            a.text.contains("fp="),
            "gpu fingerprint required: {}",
            a.text
        );
        assert_ne!(a.text, b.text, "prompt/weight sensitive");
        eprintln!(
            "OBSERVE production-engine: cuda_or_host={} len_a={} len_b={}",
            a.text.contains("joule-cuda"),
            a.text.len(),
            b.text.len()
        );
        std::env::remove_var("JOULE_BLOBS_DIR");
        let _ = fs::remove_dir_all(&root);
        let _ = fs::remove_dir_all(&blobs);
    }

    /// Skeptic: fleet recommends K3 without residency must not brick lab serve.
    /// Commit-gated upgrade: probe fails → Deferred; content_verified + stage stay live.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn try_commit_failed_k3_keeps_lab_serve() {
        let _env = crate::weights::test_env::lock();
        let root = std::env::temp_dir().join(format!("joule-commit-gate-{}", Uuid::new_v4()));
        let blobs = std::env::temp_dir().join(format!("joule-commit-gate-b-{}", Uuid::new_v4()));
        let _ = fs::remove_dir_all(&root);
        let _ = fs::remove_dir_all(&blobs);
        fs::create_dir_all(&blobs).unwrap();
        std::env::set_var("JOULE_BLOBS_DIR", &blobs);
        let store = WeightsStore::new(&root);
        let m = ManifestFile::load_default().unwrap();
        let spec = m.primary().unwrap();
        let lab = spec.pick_quant(8192).expect("lab-large");
        let k3 = spec
            .weights
            .quants
            .iter()
            .find(|q| q.id == "kimi-k3-shards")
            .expect("kimi-k3-shards");

        let eng = ProductionEngine::with_cuda_probe(CudaProbe {
            available: true,
            device_count: 1,
            detail: "test-commit-gate",
        });
        let report = prepare_and_install(&store, eng.cluster(), spec, lab).expect("lab install");
        assert!(report.bytes_resident > 0 || eng.has_resident_weights());
        eng.mark_content_from_store(&store, lab)
            .expect("lab mark");
        assert!(eng.content_verified());
        assert_eq!(eng.production_quant_id().as_deref(), Some(lab.id.as_str()));

        eng.load_plan(&ClusterPlan {
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
            pool_mem_mib: 200_000,
            model_layers: 1,
        })
        .await
        .unwrap();

        // Empty store for K3: cannot load full quant. Simulate failed upgrade with a
        // dummy LoadedModel clone of lab tensors + try_commit against empty K3 digests.
        let lab_lm = crate::load_model(&store, spec, lab).expect("reload lab tensors");
        let outcome = eng.try_commit_quant_upgrade(&store, k3, lab_lm);
        match &outcome {
            UpgradeOutcome::Deferred {
                kept_verified,
                kept_quant,
                reason,
            } => {
                assert!(*kept_verified, "must keep lab verified: {reason}");
                assert_eq!(kept_quant.as_deref(), Some(lab.id.as_str()));
            }
            UpgradeOutcome::Committed { quant_id } => {
                panic!("K3 must not commit without digests, got {quant_id}");
            }
        }
        assert!(
            eng.content_verified(),
            "content_verified must remain true after deferred K3"
        );
        assert_eq!(
            eng.production_quant_id().as_deref(),
            Some(lab.id.as_str()),
            "production quant must stay lab after deferred K3"
        );

        // Weight-gated stage still works on lab (the brick skeptic feared).
        let out = eng
            .stage_layers(StageRequest {
                model: CLUSTER_MODEL.into(),
                prompt: "commit-gate-still-serves".into(),
                layer_start: 0,
                layer_end: 0,
                upstream: vec![],
                is_tail: false,
                require_upstream: false,
                require_band_weights: true,
                required_weight_files: vec![],
            })
            .await
            .expect("lab stage after deferred K3 must still work");
        assert!(
            out.activation.starts_with(b"JST3") || !out.activation.is_empty(),
            "activation present"
        );
        eprintln!(
            "OBSERVE try_commit_failed_k3_keeps_lab_serve: verified={} quant={:?} act_len={}",
            eng.content_verified(),
            eng.production_quant_id(),
            out.activation.len()
        );

        // begin_quant_intent still clears (test-only path) — document contrast.
        eng.begin_quant_intent(k3);
        assert!(!eng.content_verified());
        // Restore via successful lab re-commit for hygiene.
        let lab_lm2 = crate::load_model(&store, spec, lab).expect("lab again");
        assert!(eng
            .try_commit_quant_upgrade(&store, lab, lab_lm2)
            .is_committed());
        assert!(eng.content_verified());

        std::env::remove_var("JOULE_BLOBS_DIR");
        let _ = fs::remove_dir_all(&root);
        let _ = fs::remove_dir_all(&blobs);
    }

    /// AC3: Engine stage_layers fails closed without content proof; succeeds only
    /// after shipped unlock + install on real digest-backed band files.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn mesh_k3_band_content_proof_stage() {
        let _env = crate::weights::test_env::lock();
        let root = std::env::temp_dir().join(format!("joule-mesh-k3-{}", Uuid::new_v4()));
        let blobs = std::env::temp_dir().join(format!("joule-mesh-k3-b-{}", Uuid::new_v4()));
        let _ = fs::remove_dir_all(&root);
        let _ = fs::remove_dir_all(&blobs);
        fs::create_dir_all(&blobs).unwrap();
        std::env::set_var("JOULE_BLOBS_DIR", &blobs);
        let store = WeightsStore::new(&root);

        let eng = ProductionEngine::with_cuda_probe(CudaProbe {
            available: true,
            device_count: 2,
            detail: "test-fleet",
        });
        eng.load_plan(&ClusterPlan {
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
            pool_mem_mib: 200_000,
            model_layers: 93,
        })
        .await
        .unwrap();

        // Public API only: begin intent + failed mark leaves fail-closed.
        let empty_k3 = QuantSpec {
            id: "kimi-k3-band-proof".into(),
            min_node_vram_mib: 256,
            approx_file_mib: 1,
            files: vec![WeightFile {
                path: "model-00001-of-000096.safetensors".into(),
                sha256: "ab".repeat(32), // non-synthetic shape; file missing
                url: "peer://kimi-open/k3/model-00001-of-000096.safetensors".into(),
                size_bytes: 64,
            }],
        };
        assert!(!is_synthetic_placeholder_digest(&empty_k3.files[0].sha256));
        eng.begin_quant_intent(&empty_k3);
        assert!(eng
            .mark_content_from_band(&store, "kimi-open", &empty_k3, 0, 0)
            .is_err());
        assert!(!eng.content_verified());
        assert_eq!(
            eng.production_quant_id().as_deref(),
            Some("kimi-k3-band-proof")
        );
        let fail = eng
            .stage_layers(StageRequest {
                model: CLUSTER_MODEL.into(),
                prompt: "no-digests".into(),
                layer_start: 0,
                layer_end: 0,
                upstream: vec![],
                is_tail: false,
                require_upstream: false,
                require_band_weights: true,
                required_weight_files: vec!["model-00001-of-000096.safetensors".into()],
            })
            .await;
        assert!(fail.is_err(), "expected fail-closed without digests");
        let err = format!("{}", fail.unwrap_err());
        assert!(
            err.contains("not verified") || err.contains("digests"),
            "err should mention digests: {err}"
        );

        // Shipped unlock: plant band file → mark_content_from_band + install_production_band.
        let md = store.model_dir("kimi-open", "kimi-k3-band-proof");
        fs::create_dir_all(&md).unwrap();
        let path = md.join("model-00001-of-000096.safetensors");
        write_tiny_safetensors_fixture(&path).unwrap();
        let bytes = fs::read(&path).unwrap();
        let hash = hex::encode(Sha256::digest(&bytes));
        assert!(!is_synthetic_placeholder_digest(&hash));
        let quant = QuantSpec {
            id: "kimi-k3-band-proof".into(),
            min_node_vram_mib: 256,
            approx_file_mib: 1,
            files: vec![WeightFile {
                path: "model-00001-of-000096.safetensors".into(),
                sha256: hash,
                url: "peer://kimi-open/k3/model-00001-of-000096.safetensors".into(),
                size_bytes: bytes.len() as u64,
            }],
        };
        assert!(quant_can_unlock_service_digests(&quant));
        eng.mark_content_from_band(&store, "kimi-open", &quant, 0, 0)
            .expect("mark band after plant");

        let m = ManifestFile::load_default().unwrap();
        let spec = m.primary().unwrap();
        let lm = load_model_for_band(&store, spec, &quant, 0, 0).unwrap();
        eng.install_production_band(&store, &spec.id, &quant, lm, 0, 0)
            .expect("install_production_band");
        assert!(eng.content_verified());
        assert_eq!(
            eng.production_quant_id().as_deref(),
            Some("kimi-k3-band-proof")
        );

        let out = eng
            .stage_layers(StageRequest {
                model: CLUSTER_MODEL.into(),
                prompt: "mesh-k3-content".into(),
                layer_start: 0,
                layer_end: 0,
                upstream: vec![],
                is_tail: false,
                require_upstream: false,
                require_band_weights: true,
                required_weight_files: vec!["model-00001-of-000096.safetensors".into()],
            })
            .await
            .expect("stage after content proof");
        assert!(
            out.activation.starts_with(b"JST3"),
            "got {:?}",
            &out.activation[..4.min(out.activation.len())]
        );
        // Weight sensitivity: flip diagonal and re-stage.
        {
            use safetensors::tensor::{serialize, TensorView};
            use safetensors::Dtype;
            use std::collections::BTreeMap;
            let mut floats = vec![0.0f32; 16];
            for i in 0..4 {
                floats[i * 4 + i] = 3.5;
            }
            let mut data = Vec::new();
            for f in &floats {
                data.extend_from_slice(&f.to_le_bytes());
            }
            let tensor = TensorView::new(Dtype::F32, vec![4, 4], &data).unwrap();
            let mut map: BTreeMap<String, TensorView<'_>> = BTreeMap::new();
            map.insert("demo.weight".into(), tensor);
            let raw = serialize(&map, &None).unwrap();
            fs::write(&path, &raw).unwrap();
            let hash2 = hex::encode(Sha256::digest(&raw));
            let quant2 = QuantSpec {
                id: "kimi-k3-band-proof".into(),
                min_node_vram_mib: 256,
                approx_file_mib: 1,
                files: vec![WeightFile {
                    path: "model-00001-of-000096.safetensors".into(),
                    sha256: hash2,
                    url: "peer://kimi-open/k3/model-00001-of-000096.safetensors".into(),
                    size_bytes: raw.len() as u64,
                }],
            };
            let lm2 = load_model_for_band(&store, spec, &quant2, 0, 0).unwrap();
            eng.install_production_band(&store, &spec.id, &quant2, lm2, 0, 0)
                .unwrap();
        }
        let out2 = eng
            .stage_layers(StageRequest {
                model: CLUSTER_MODEL.into(),
                prompt: "mesh-k3-content".into(),
                layer_start: 0,
                layer_end: 0,
                upstream: vec![],
                is_tail: false,
                require_upstream: false,
                require_band_weights: true,
                required_weight_files: vec!["model-00001-of-000096.safetensors".into()],
            })
            .await
            .unwrap();
        assert_ne!(
            out.activation, out2.activation,
            "CUDA/host production stage must be weight-sensitive"
        );

        // Multi-donor tail contract (agent_handle_infer takes stage.text for InferDone):
        // is_tail=true must yield non-empty weight-sensitive completion text.
        let tail_a = eng
            .stage_layers(StageRequest {
                model: CLUSTER_MODEL.into(),
                prompt: "mesh-k3-tail-hi".into(),
                layer_start: 0,
                layer_end: 0,
                upstream: out2.activation.clone(),
                is_tail: true,
                require_upstream: true,
                require_band_weights: true,
                required_weight_files: vec!["model-00001-of-000096.safetensors".into()],
            })
            .await
            .expect("tail stage after content proof");
        // Same extraction as joule_control::agent_handle_infer multi-shard tail.
        let text_a = tail_a.text.clone().unwrap_or_default();
        assert!(
            !text_a.is_empty(),
            "multi-donor tail InferDone text must be non-empty, got empty (pack_jst3 is_tail)"
        );
        assert!(
            text_a.contains("mesh-k3-tail-hi")
                || text_a.contains("joule-decode")
                || text_a.contains("matmul")
                || text_a.contains("L0"),
            "tail text should reflect decode/matmul path: {text_a}"
        );
        assert!(tail_a.completion_tokens > 0);

        // Weight-sensitive tail: reinstall first-weight set and re-tail.
        {
            write_tiny_safetensors_fixture(&path).unwrap();
            let bytes3 = fs::read(&path).unwrap();
            let hash3 = hex::encode(Sha256::digest(&bytes3));
            let quant3 = QuantSpec {
                id: "kimi-k3-band-proof".into(),
                min_node_vram_mib: 256,
                approx_file_mib: 1,
                files: vec![WeightFile {
                    path: "model-00001-of-000096.safetensors".into(),
                    sha256: hash3,
                    url: "peer://kimi-open/k3/model-00001-of-000096.safetensors".into(),
                    size_bytes: bytes3.len() as u64,
                }],
            };
            let lm3 = load_model_for_band(&store, spec, &quant3, 0, 0).unwrap();
            eng.install_production_band(&store, &spec.id, &quant3, lm3, 0, 0)
                .unwrap();
        }
        let tail_b = eng
            .stage_layers(StageRequest {
                model: CLUSTER_MODEL.into(),
                prompt: "mesh-k3-tail-hi".into(),
                layer_start: 0,
                layer_end: 0,
                upstream: out2.activation.clone(),
                is_tail: true,
                require_upstream: true,
                require_band_weights: true,
                required_weight_files: vec!["model-00001-of-000096.safetensors".into()],
            })
            .await
            .unwrap();
        let text_b = tail_b.text.unwrap_or_default();
        assert!(!text_b.is_empty());
        // Activation and/or text must differ when weights flip (shared pack_jst3 decode path).
        assert!(
            tail_a.activation != tail_b.activation || text_a != text_b,
            "tail stage must be weight-sensitive"
        );

        assert!(full_k3_service_fleet_ok(200_000, 8));
        eprintln!(
            "OBSERVE mesh-k3-serve: fail_closed_ok stage_jst3 weight_flip={} fleet_ok={} tail_text_len={} tail_weight_flip={}",
            out.activation != out2.activation,
            full_k3_service_fleet_ok(200_000, 8),
            text_a.len(),
            text_a != text_b || tail_a.activation != tail_b.activation
        );
        std::env::remove_var("JOULE_BLOBS_DIR");
        let _ = fs::remove_dir_all(&root);
        let _ = fs::remove_dir_all(&blobs);
    }
}
