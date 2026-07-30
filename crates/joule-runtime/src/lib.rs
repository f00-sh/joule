//! Inference backend trait and stub engine.
//!
//! Product law: cluster protocol stays pure Rust. Real GPU backends land behind
//! this trait. Prefer pure-Rust engines (e.g. candle) before any FFI exception.

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

/// Deterministic stub for cluster/protocol tests without GPUs.
pub struct StubEngine {
    loaded: std::sync::Mutex<Option<String>>,
}

impl StubEngine {
    pub fn new() -> Self {
        Self {
            loaded: std::sync::Mutex::new(None),
        }
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
        let reply = format!("[joule-stub:{model}] {}", req.prompt);
        let completion_tokens = reply.split_whitespace().count() as u32;
        Ok(InferResponse {
            text: reply,
            prompt_tokens: req.prompt.split_whitespace().count() as u32,
            completion_tokens,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use joule_proto::{NodeId, ShardAssignment, ShardRole};
    use uuid::Uuid;

    #[tokio::test]
    async fn stub_roundtrip() {
        let eng = StubEngine::new();
        let plan = ClusterPlan {
            plan_id: Uuid::new_v4(),
            model: "kimi-open-q4".into(),
            shards: vec![ShardAssignment {
                node: NodeId::new(),
                role: ShardRole::Replica,
                layer_start: None,
                layer_end: None,
                tp_rank: None,
                tp_world: None,
            }],
        };
        eng.load_plan(&plan).await.unwrap();
        let out = eng
            .infer(InferRequest {
                model: "kimi-open-q4".into(),
                prompt: "hello cluster".into(),
                max_tokens: 16,
            })
            .await
            .unwrap();
        assert!(out.text.contains("hello cluster"));
    }
}
