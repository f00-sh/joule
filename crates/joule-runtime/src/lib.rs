//! Inference backends + model manifest + weight cache.
//!
//! Kimi is **not** loaded until the logical device (aggregate VRAM) is large
//! enough and weights are published. Until then: stub inference + arm cache.

mod manifest;
mod weights;

pub use manifest::{
    InferenceMode, ManifestFile, ModelReadiness, ModelSpec, QuantSpec, EMBEDDED_MANIFEST,
};
pub use weights::{PrepareStatus, WeightsStore};

use async_trait::async_trait;
use joule_proto::ClusterPlan;
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

/// Local or multi-shard inference engine.
#[async_trait]
pub trait Engine: Send + Sync {
    async fn load_plan(&self, plan: &ClusterPlan) -> Result<(), RuntimeError>;
    async fn infer(&self, req: InferRequest) -> Result<InferResponse, RuntimeError>;
}

/// Deterministic stub used until weights exist and a real backend is wired.
pub struct StubEngine {
    loaded: std::sync::Mutex<Option<String>>,
    mode_label: String,
}

impl StubEngine {
    pub fn new() -> Self {
        Self {
            loaded: std::sync::Mutex::new(None),
            mode_label: "stub".into(),
        }
    }

    pub fn with_mode_label(label: impl Into<String>) -> Self {
        Self {
            loaded: std::sync::Mutex::new(None),
            mode_label: label.into(),
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
        let reply = Self::expected_text_mode(&self.mode_label, &model, &req.prompt);
        let completion_tokens = reply.split_whitespace().count() as u32;
        Ok(InferResponse {
            text: reply,
            prompt_tokens: req.prompt.split_whitespace().count() as u32,
            completion_tokens,
        })
    }
}

/// Engine selected from pool readiness + local weight state.
pub struct ClusterEngine {
    inner: StubEngine,
    readiness: std::sync::Mutex<Option<ModelReadiness>>,
    prepared: std::sync::Mutex<bool>,
}

impl ClusterEngine {
    pub fn new() -> Self {
        Self {
            inner: StubEngine::with_mode_label("stub-awaiting-pool"),
            readiness: std::sync::Mutex::new(None),
            prepared: std::sync::Mutex::new(false),
        }
    }

    pub fn update_readiness(&self, r: ModelReadiness, prepared: bool) {
        let label = match r.inference_mode {
            InferenceMode::StubAwaitingPool => "stub-awaiting-pool",
            InferenceMode::StubPoolReady => {
                if prepared {
                    "stub-pool-ready-armed"
                } else {
                    "stub-pool-ready"
                }
            }
            InferenceMode::WeightsReady => {
                if prepared {
                    "weights-ready"
                } else {
                    "stub-weights-pending"
                }
            }
        };
        // Swap mode label via replacement of inner is hard; encode in expected text via stored readiness.
        *self.readiness.lock().expect("lock") = Some(r);
        *self.prepared.lock().expect("lock") = prepared;
        let _ = label;
        // Keep using stub path until a real backend is plugged in.
        let _ = &self.inner;
    }

    pub fn readiness(&self) -> Option<ModelReadiness> {
        self.readiness.lock().expect("lock").clone()
    }

    pub fn is_prepared(&self) -> bool {
        *self.prepared.lock().expect("lock")
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
        self.inner.load_plan(plan).await
    }

    async fn infer(&self, req: InferRequest) -> Result<InferResponse, RuntimeError> {
        let mode = self
            .readiness
            .lock()
            .expect("lock")
            .as_ref()
            .map(|r| match r.inference_mode {
                InferenceMode::StubAwaitingPool => "stub-awaiting-pool",
                InferenceMode::StubPoolReady => "stub-pool-ready",
                InferenceMode::WeightsReady => "weights-pending-engine",
            })
            .unwrap_or("stub")
            .to_string();
        let mut out = self.inner.infer(req).await?;
        // Annotate mode so clients can see pool-gate state in stub replies.
        if let Some(rest) = out.text.strip_prefix("[joule-stub:") {
            out.text = format!("[joule-{mode}:{rest}");
        }
        Ok(out)
    }
}

/// Compute readiness for the primary model given pool stats.
pub fn readiness_for_pool(pool_vram_mib: u64, backends: u32) -> Result<ModelReadiness, String> {
    let m = ManifestFile::load_default()?;
    let spec = m
        .primary()
        .ok_or_else(|| "manifest has no models".to_string())?;
    Ok(spec.readiness(pool_vram_mib, backends))
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
                prompt: "hello cluster".into(),
                max_tokens: 16,
            })
            .await
            .unwrap();
        assert!(out.text.contains("hello cluster"));
    }

    #[test]
    fn readiness_gates_kimi() {
        let r = readiness_for_pool(10 * 1024, 2).unwrap();
        assert!(!r.pool_ready);
        let r = readiness_for_pool(72 * 1024, 5).unwrap();
        assert!(r.pool_ready);
        assert!(!r.weights_published);
    }
}
