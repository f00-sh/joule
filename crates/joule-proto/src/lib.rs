//! Protocol types for the joule mesh.
//!
//! Nodes speak a versioned message set over authenticated channels.
//! This crate is pure data + encoding; transport lives in `joule-mesh`.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Wire protocol major.minor. Bump major on breaking message changes.
pub const PROTOCOL_VERSION: &str = "0.1.0";

/// Stable node identity (public key fingerprint later; UUID for early mesh).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub Uuid);

impl NodeId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::new()
    }
}

/// Hardware class advertised by a donor for placement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeviceClass {
    /// NVIDIA / AMD discrete GPU with usable VRAM.
    Gpu,
    /// Apple Silicon unified memory GPU path.
    Metal,
    /// CPU-only fallback (low credit weight).
    Cpu,
}

/// Capability advertisement — what this node can host.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCaps {
    pub device: DeviceClass,
    /// Approximate free VRAM or unified memory budget in MiB.
    pub mem_mib: u32,
    /// Self-reported sustained token/s class for placement (verified later).
    pub throughput_class: u16,
    /// Model/quant tags this node can load (e.g. "kimi-k3-q4_k_m").
    pub models: Vec<String>,
}

/// Role a node plays in a multi-node inference graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShardRole {
    /// Full model replica on one node (mesh-simple path).
    Replica,
    /// Pipeline stage (layer range).
    Pipeline,
    /// Tensor-parallel rank.
    Tensor,
    /// Prefill specialist.
    Prefill,
    /// Decode specialist.
    Decode,
}

/// Placement of one model shard on one node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardAssignment {
    pub node: NodeId,
    pub role: ShardRole,
    /// Inclusive layer range when role is pipeline; ignored for replica.
    pub layer_start: Option<u32>,
    pub layer_end: Option<u32>,
    pub tp_rank: Option<u16>,
    pub tp_world: Option<u16>,
}

/// Active multi-node (or single-node) serving plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshPlan {
    pub plan_id: Uuid,
    pub model: String,
    pub shards: Vec<ShardAssignment>,
}

/// Envelope for all peer messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub protocol: String,
    pub from: NodeId,
    pub msg: Message,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    Hello {
        caps: NodeCaps,
    },
    Heartbeat {
        load: f32,
        healthy: bool,
    },
    /// Request a mesh plan for a model (bootstrap / rebalance).
    PlanRequest {
        model: String,
    },
    PlanOffer {
        plan: MeshPlan,
    },
    /// Inference request (OpenAI-shaped body later; opaque JSON for now).
    InferRequest {
        request_id: Uuid,
        model: String,
        body: serde_json::Value,
    },
    InferPartial {
        request_id: Uuid,
        delta: String,
    },
    InferDone {
        request_id: Uuid,
        /// Token accounting for ledger mint/burn.
        prompt_tokens: u32,
        completion_tokens: u32,
    },
    InferError {
        request_id: Uuid,
        error: String,
    },
    /// Challenge for anti-cheat / result verification.
    Challenge {
        challenge_id: Uuid,
        prompt: String,
    },
    ChallengeResult {
        challenge_id: Uuid,
        completion: String,
        latency_ms: u32,
    },
    /// Credit event gossip (append-only event id).
    CreditEvent {
        event_id: Uuid,
        account: String,
        delta_millijoules: i64,
        reason: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_hello() {
        let env = Envelope {
            protocol: PROTOCOL_VERSION.to_string(),
            from: NodeId::new(),
            msg: Message::Hello {
                caps: NodeCaps {
                    device: DeviceClass::Gpu,
                    mem_mib: 24576,
                    throughput_class: 40,
                    models: vec!["kimi-open-q4".into()],
                },
            },
        };
        let s = serde_json::to_string(&env).unwrap();
        let back: Envelope = serde_json::from_str(&s).unwrap();
        assert_eq!(back.protocol, PROTOCOL_VERSION);
        match back.msg {
            Message::Hello { caps } => assert_eq!(caps.mem_mib, 24576),
            _ => panic!("expected hello"),
        }
    }
}
