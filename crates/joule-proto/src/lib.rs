//! Protocol types for the joule distributed cluster.
//!
//! Nodes speak a versioned message set over authenticated channels.
//! Transport and membership live in `joule-cluster`. Connectivity medium
//! (fiber, cellular, satellite, whatever) is out of scope for the protocol.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Wire protocol major.minor. Bump major on breaking message changes.
pub const PROTOCOL_VERSION: &str = "0.1.0";

/// Stable node identity (public key fingerprint later; UUID for early cluster).
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
    /// Discrete GPU with usable VRAM (any vendor path the runtime supports).
    Gpu,
    /// Apple Silicon unified-memory GPU path.
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
    /// Model/quant tags this node can load (e.g. "kimi-open-q4").
    pub models: Vec<String>,
}

/// Role a node plays in a multi-node inference graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShardRole {
    /// Full model replica on one node.
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

/// Active multi-node (or single-node) serving plan for the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterPlan {
    pub plan_id: Uuid,
    pub model: String,
    pub shards: Vec<ShardAssignment>,
}

/// Live aggregate of donated compute — power the public dashboard.
///
/// Built from node heartbeats. Medium of connectivity is irrelevant; only
/// healthy, registered capacity counts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterCapacity {
    /// Nodes currently registered (including unhealthy).
    pub nodes_total: u32,
    /// Nodes passing health checks / recent heartbeat.
    pub nodes_healthy: u32,
    pub nodes_gpu: u32,
    pub nodes_metal: u32,
    pub nodes_cpu: u32,
    /// Sum of advertised mem_mib across all registered nodes.
    pub mem_mib_total: u64,
    /// Sum of mem_mib for healthy nodes only (what the dashboard should highlight).
    pub mem_mib_healthy: u64,
    /// Sum of throughput_class for healthy nodes (relative pool strength).
    pub throughput_class_sum: u64,
    /// Distinct model tags offered by at least one healthy node.
    pub models_available: Vec<String>,
}

/// Envelope for all peer / control messages.
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
    /// Request a cluster placement plan for a model.
    PlanRequest {
        model: String,
    },
    PlanOffer {
        plan: ClusterPlan,
    },
    /// Publish or request live pool capacity (dashboard / status).
    CapacitySnapshot {
        capacity: ClusterCapacity,
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

    #[test]
    fn capacity_serializes() {
        let c = ClusterCapacity {
            nodes_total: 2,
            nodes_healthy: 2,
            nodes_gpu: 2,
            nodes_metal: 0,
            nodes_cpu: 0,
            mem_mib_total: 32768,
            mem_mib_healthy: 32768,
            throughput_class_sum: 50,
            models_available: vec!["kimi-open-q4".into()],
        };
        let v = serde_json::to_value(&c).unwrap();
        assert_eq!(v["nodes_healthy"], 2);
    }
}
