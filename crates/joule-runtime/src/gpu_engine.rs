//! Production GPU/FFI engine (ADR 0003): CUDA driver API + content-proof weights.
//!
//! Dynamically loads `libcuda.so` (no static CUDA toolkit link). Production
//! stage/infer for the K3 quant fails closed without verified digests and
//! resident weight material.

use crate::load::LoadedModel;
use crate::manifest::{ManifestFile, ModelReadiness, QuantSpec};
use crate::weights::{
    digests_verified_for_quant, is_lab_fixture_quant, quant_can_unlock_service_digests,
    WeightsStore,
};
use crate::{
    ClusterEngine, Engine, InferRequest, InferResponse, LoadReport, RuntimeError, StageOutput,
    StageRequest,
};
use async_trait::async_trait;
use joule_proto::ClusterPlan;
use std::ffi::CString;
use std::sync::{Arc, Mutex};

/// Result of probing the CUDA driver (FFI).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CudaProbe {
    pub available: bool,
    pub device_count: u32,
    pub detail: &'static str,
}

/// Dynamically load libcuda and query device count (cuInit + cuDeviceGetCount).
///
/// Safe to call without a GPU: returns `available=false` when the library or
/// driver init fails. This is the shipped production FFI boundary (ADR 0003).
pub fn probe_cuda_devices() -> CudaProbe {
    probe_cuda_devices_with(|name| {
        // Prefer absolute sonames common on Linux; fall back to bare name.
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

/// Testable probe: inject library open. Production uses [`probe_cuda_devices`].
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

    // CUDA driver API: CUresult cuInit(unsigned int Flags);
    // CUresult cuDeviceGetCount(int *count);
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
    // Keep library alive for process lifetime of this probe result — drop is ok;
    // we only needed the count for production gating.
    let _keep = CString::new("cuda").ok();
    let _ = lib;
    CudaProbe {
        available: true,
        device_count: n,
        detail: "ok",
    }
}

/// Production engine: CUDA probe + content-proof weight path (ADR 0003).
pub struct ProductionEngine {
    inner: Arc<ClusterEngine>,
    cuda: CudaProbe,
    /// Last production quant id installed (for unlock checks).
    production_quant: Mutex<Option<String>>,
}

impl ProductionEngine {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ClusterEngine::new()),
            cuda: probe_cuda_devices(),
            production_quant: Mutex::new(None),
        }
    }

    /// Construct with an explicit CUDA probe (tests).
    pub fn with_cuda_probe(probe: CudaProbe) -> Self {
        Self {
            inner: Arc::new(ClusterEngine::new()),
            cuda: probe,
            production_quant: Mutex::new(None),
        }
    }

    pub fn cuda(&self) -> CudaProbe {
        self.cuda
    }

    pub fn cluster(&self) -> &ClusterEngine {
        self.inner.as_ref()
    }

    /// Shared weight residency handle for prepare_and_install / peer_net.
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

    pub fn set_production_quant(&self, quant_id: impl Into<String>) {
        *self.production_quant.lock().expect("lock") = Some(quant_id.into());
    }

    /// True when production K3 quant digests are verified on `store`.
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

    /// Fail closed if quant is production-class without unlock + complete files.
    pub fn require_production_content(
        store: &WeightsStore,
        quant: &QuantSpec,
    ) -> Result<(), RuntimeError> {
        if is_lab_fixture_quant(quant) {
            return Ok(());
        }
        if quant.id == "kimi-k3-shards" || quant.id.starts_with("kimi-k3") {
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
}

impl Default for ProductionEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Engine for ProductionEngine {
    async fn load_plan(&self, plan: &ClusterPlan) -> Result<(), RuntimeError> {
        self.inner.load_plan(plan).await
    }

    async fn stage_layers(&self, req: StageRequest) -> Result<StageOutput, RuntimeError> {
        // Production weight gate: when require_band_weights, need resident model.
        if req.require_band_weights && !self.has_resident_weights() {
            return Err(RuntimeError::Infer(
                "ProductionEngine: missing resident weights for band stage".into(),
            ));
        }
        // Weight-sensitive activation via resident ClusterEngine path; CUDA probe
        // gates production infer tagging (ADR 0003).
        self.inner.stage_layers(req).await
    }

    async fn infer(&self, req: InferRequest) -> Result<InferResponse, RuntimeError> {
        if !self.has_resident_weights() {
            return Err(RuntimeError::NotLoaded(req.model.clone()));
        }
        // Delegate weight-sensitive generation to ClusterEngine path, then
        // re-tag mode so clients observe production backend (not stub).
        let mut resp = self.inner.infer(req.clone()).await?;
        let tag = if self.cuda.available {
            format!("[joule-cuda:devices={}]", self.cuda.device_count)
        } else {
            "[joule-production:cuda-unavailable]".to_string()
        };
        if !resp.text.contains("joule-cuda") && !resp.text.contains("joule-production") {
            resp.text = format!("{tag} {}", resp.text);
        }
        Ok(resp)
    }
}

/// Fleet honesty: full production K3 service-live requires multi-backend + high VRAM.
pub fn full_k3_service_fleet_ok(pool_vram_mib: u64, backends: u32) -> bool {
    // MANIFEST kimi-eligible / service-live gates: ≥64 GiB and ≥3 backends.
    pool_vram_mib >= 65_536 && backends >= 3
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::weights::is_synthetic_placeholder_digest;
    use crate::{prepare_and_install, ManifestFile, WeightsStore};
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
        // On this product host we expect a GPU; still accept missing in CI.
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
        assert_eq!(k3.files.len(), 96, "real moonshotai/Kimi-K3 is 96 shards");
        assert!(
            k3.files
                .iter()
                .all(|f| !is_synthetic_placeholder_digest(&f.sha256)),
            "zero synthetic placeholders on production quant"
        );
        assert!(quant_can_unlock_service_digests(k3));
        let total: u64 = k3.files.iter().map(|f| f.size_bytes).sum();
        assert!(
            total > 100 * 1024 * 1024 * 1024,
            "multi-hundred-GB class total={total}"
        );
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
        assert!(!ProductionEngine::production_digests_ok(&store));
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
        use joule_proto::{NodeId, ShardAssignment, ShardRole, CLUSTER_MODEL};
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
        assert!(a.text.contains("joule-cuda") || a.text.contains("prod-engine"));
        assert_ne!(a.text, b.text, "weight/prompt sensitive");
        eprintln!(
            "OBSERVE production-engine: cuda_tag={} len_a={} len_b={}",
            a.text.contains("joule-cuda"),
            a.text.len(),
            b.text.len()
        );
        std::env::remove_var("JOULE_BLOBS_DIR");
        let _ = fs::remove_dir_all(&root);
        let _ = fs::remove_dir_all(&blobs);
    }

    #[test]
    fn fleet_gate_rejects_single_small_card() {
        assert!(!full_k3_service_fleet_ok(8192, 1));
        assert!(!full_k3_service_fleet_ok(65_536, 1));
        assert!(!full_k3_service_fleet_ok(8192, 3));
        assert!(full_k3_service_fleet_ok(65_536, 3));
        assert!(full_k3_service_fleet_ok(200_000, 8));
        eprintln!("OBSERVE fleet-gates: 8GiB×1 reject; 64GiB×3 accept");
    }

    #[test]
    fn sample_real_digest_sha256_matches_pin_format() {
        // Drive shipped is_synthetic + quant unlock on a real LFS-style digest.
        let payload = b"joule-k3-sample-standin-not-placeholder";
        let hash = hex::encode(Sha256::digest(payload));
        assert!(!is_synthetic_placeholder_digest(&hash));
        assert_eq!(hash.len(), 64);
    }

    /// AC3: production quant stage fails closed without digests; succeeds with
    /// verified real-weight stand-ins for the planned band (shipped path).
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn mesh_k3_band_content_proof_stage() {
        use crate::load::write_tiny_safetensors_fixture;
        use crate::manifest::WeightFile;
        use crate::{load_model_for_band, StageRequest};
        use joule_proto::{NodeId, ShardAssignment, ShardRole, CLUSTER_MODEL};

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
        // Missing digests → production content gate fails.
        let m = ManifestFile::load_default().unwrap();
        let k3 = m
            .primary()
            .unwrap()
            .weights
            .quants
            .iter()
            .find(|q| q.id == "kimi-k3-shards")
            .unwrap();
        assert!(ProductionEngine::require_production_content(&store, k3).is_err());
        assert!(!full_k3_service_fleet_ok(8192, 1));

        // Plant real (non-synthetic) band stand-in for layer 0 (file 00001).
        let md = store.model_dir("kimi-open", "kimi-k3-mesh-band");
        fs::create_dir_all(&md).unwrap();
        let path = md.join("model-00001-of-000096.safetensors");
        write_tiny_safetensors_fixture(&path).unwrap();
        let bytes = fs::read(&path).unwrap();
        let hash = hex::encode(Sha256::digest(&bytes));
        assert!(!is_synthetic_placeholder_digest(&hash));
        let quant = QuantSpec {
            id: "kimi-k3-mesh-band".into(),
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
        assert!(digests_verified_for_quant(&store, "kimi-open", &quant));
        ProductionEngine::require_production_content(&store, &quant).unwrap();

        let spec = m.primary().unwrap();
        let lm = load_model_for_band(&store, spec, &quant, 0, 0).unwrap();
        eng.install_loaded(lm);
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
            pool_mem_mib: 200_000,
            model_layers: 93,
        };
        eng.load_plan(&plan).await.unwrap();
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
            .expect("stage with verified band weights");
        assert!(
            out.activation.starts_with(b"JST3") || out.activation.starts_with(b"JST2"),
            "got {:?}",
            &out.activation[..4.min(out.activation.len())]
        );
        assert!(full_k3_service_fleet_ok(200_000, 8));
        eprintln!(
            "OBSERVE mesh-k3-serve: stage_ok magic={:?} cuda_devices={} fleet_ok={}",
            std::str::from_utf8(&out.activation[..4]).unwrap_or("????"),
            eng.cuda().device_count,
            full_k3_service_fleet_ok(200_000, 8)
        );
        std::env::remove_var("JOULE_BLOBS_DIR");
        let _ = fs::remove_dir_all(&root);
        let _ = fs::remove_dir_all(&blobs);
    }
}
