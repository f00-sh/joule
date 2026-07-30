//! Inference backends + model manifest + weight cache + **model loading**.
//!
//! Kimi is not loaded until the logical device is large enough. When the pool
//! hits the load milestone (and weights exist), [`load_model`] maps tensors into
//! RAM. Service-live is a separate control flag after the mesh has loaded.

mod decode;
mod load;
mod manifest;
mod weights;

pub use decode::generate as generate_from_loaded;
pub use load::{load_model, LoadError, LoadReport, LoadedModel, TensorInfo};
pub use manifest::{
    InferenceMode, ManifestFile, MilestoneStatus, ModelReadiness, ModelSpec, QuantSpec,
    RuntimeFlags, EMBEDDED_MANIFEST,
};
pub use weights::{PrepareStatus, WeightsStore};

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
}
