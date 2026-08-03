//! Protocol types for the joule distributed compute cluster.
//!
//! Donor agents speak a versioned message set to the control plane.
//! Connectivity medium is out of scope — only reachability and capacity matter.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Wire protocol major.minor. Bump major on breaking message changes.
pub const PROTOCOL_VERSION: &str = "0.1.0";

/// The **only** model this cluster runs. All donated compute serves this model.
///
/// Product law: one public AI; the distributed pool exists solely to power it.
/// Quant/size variants are node implementation details, not separate API models.
pub const CLUSTER_MODEL: &str = "kimi-open";

/// Display name for dashboards and docs.
pub const CLUSTER_MODEL_LABEL: &str = "Kimi (open weights)";

/// Normalize client/agent model strings to [`CLUSTER_MODEL`].
/// Returns `None` if the client asked for a foreign model.
pub fn resolve_cluster_model(requested: Option<&str>) -> Result<&'static str, String> {
    let Some(raw) = requested.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(CLUSTER_MODEL);
    };
    let key = raw.to_ascii_lowercase();
    // Accept common aliases / legacy tags so old clients still hit the one model.
    if matches!(
        key.as_str(),
        "kimi-open"
            | "kimi"
            | "kimi-open-q4"
            | "kimi-open-q5"
            | "kimi-open-q8"
            | "kimi-k3"
            | "kimi-k2"
    ) || key.starts_with("kimi")
    {
        return Ok(CLUSTER_MODEL);
    }
    Err(format!(
        "this cluster only serves model `{CLUSTER_MODEL}` (got `{raw}`)"
    ))
}

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

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
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

impl DeviceClass {
    pub fn contribution_multiplier(self) -> u32 {
        match self {
            DeviceClass::Gpu => 8,
            DeviceClass::Metal => 6,
            DeviceClass::Cpu => 1,
        }
    }
}

/// Capability advertisement — compute this node donates to [`CLUSTER_MODEL`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCaps {
    pub device: DeviceClass,
    /// Approximate free VRAM or unified memory budget in MiB.
    pub mem_mib: u32,
    /// Self-reported sustained token/s class for placement (verified later).
    pub throughput_class: u16,
    /// Always [`CLUSTER_MODEL`] for this product. Kept for wire compat.
    #[serde(default = "default_models")]
    pub models: Vec<String>,
    /// Optional quant/size class the node will load (e.g. `q4_k_m`). Not an API model id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quant: Option<String>,
}

fn default_models() -> Vec<String> {
    vec![CLUSTER_MODEL.to_string()]
}

impl NodeCaps {
    /// Caps for a donor that only serves the cluster model.
    pub fn for_cluster(device: DeviceClass, mem_mib: u32, throughput_class: u16) -> Self {
        Self {
            device,
            mem_mib,
            throughput_class,
            models: vec![CLUSTER_MODEL.to_string()],
            quant: None,
        }
    }
}

/// Role a node plays in a multi-node inference graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShardRole {
    Replica,
    Pipeline,
    Tensor,
    Prefill,
    Decode,
}

/// Placement of one model shard on one node (VRAM-weighted slice of the pool).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardAssignment {
    pub node: NodeId,
    pub role: ShardRole,
    pub layer_start: Option<u32>,
    pub layer_end: Option<u32>,
    pub tp_rank: Option<u16>,
    pub tp_world: Option<u16>,
    /// This node's share of aggregate pool VRAM (MiB).
    #[serde(default)]
    pub mem_share_mib: u32,
    /// Parts-per-million of total pool VRAM (sum ≈ 1_000_000).
    #[serde(default)]
    pub mem_fraction_ppm: u32,
}

/// Active multi-node serving plan: one model sharded across the pool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterPlan {
    pub plan_id: Uuid,
    pub model: String,
    pub shards: Vec<ShardAssignment>,
    /// Sum of healthy donor VRAM used in this plan.
    #[serde(default)]
    pub pool_mem_mib: u64,
    /// Assumed total transformer layers for layer-range placement.
    #[serde(default = "default_model_layers")]
    pub model_layers: u32,
}

fn default_model_layers() -> u32 {
    80
}

/// How joule presents the pool externally: **one logical device**.
///
/// Five home GPUs with 8+16+16+16+16 GiB are not five products — they are one
/// supercomputer with ~72 GiB VRAM. Physical donors are internal plumbing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalDevice {
    /// Always a single virtual accelerator for the public API.
    pub id: String,
    pub name: String,
    pub kind: String,
    /// Aggregate VRAM (MiB) = sum of healthy donor memory.
    pub vram_mib: u64,
    /// Same in GiB (rounded down).
    pub vram_gib: u64,
    /// Donors that make up this logical device (internal detail).
    pub backends: u32,
    pub model: String,
    /// Backends online (device assembled).
    pub ready: bool,
    /// Pool large enough for the model’s min VRAM / backend gates.
    #[serde(default)]
    pub model_ready: bool,
    /// 0–100 toward model pool gate.
    #[serde(default)]
    pub model_progress_pct: u8,
    /// Human-readable readiness (stub vs waiting for pool vs weights).
    #[serde(default)]
    pub inference_mode: String,
    #[serde(default)]
    pub readiness_message: String,
}

/// Live aggregate of donated compute — powers the public dashboard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterCapacity {
    pub nodes_total: u32,
    pub nodes_healthy: u32,
    pub nodes_gpu: u32,
    pub nodes_metal: u32,
    pub nodes_cpu: u32,
    pub mem_mib_total: u64,
    pub mem_mib_healthy: u64,
    pub throughput_class_sum: u64,
    pub models_available: Vec<String>,
    /// Max concurrent generation streams the sharded pool can accept.
    #[serde(default)]
    pub stream_slots_total: u32,
    /// Streams currently reserved.
    #[serde(default)]
    pub stream_slots_used: u32,
    /// **The** public view: one device with aggregate VRAM.
    #[serde(default)]
    pub logical_device: Option<LogicalDevice>,
}

/// Envelope for control ↔ agent messages (newline-delimited JSON on the wire).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub protocol: String,
    pub from: NodeId,
    pub msg: Message,
}

impl Envelope {
    pub fn new(from: NodeId, msg: Message) -> Self {
        Self {
            protocol: PROTOCOL_VERSION.to_string(),
            from,
            msg,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    /// Donor registers (or re-registers) with the control plane.
    ///
    /// **Signed accounts** (anonymous joule codes): set `pubkey_hex` + `sig_hex` over
    /// [`hello_sign_preimage`]. The pool rejects forged account strings without the key.
    /// Lab nicknames may omit signatures (empty pubkey/sig).
    Hello {
        /// Account that earns millijoules (`j1…` fingerprint or lab nickname).
        account: String,
        caps: NodeCaps,
        /// Ed25519 public key (64 hex chars). Empty = unsigned lab hello.
        #[serde(default)]
        pubkey_hex: String,
        /// Ed25519 signature (128 hex chars) of [`hello_sign_preimage`].
        #[serde(default)]
        sig_hex: String,
        /// Unix ms when signature was made (replay window checked by control).
        #[serde(default)]
        signed_at_unix_ms: u64,
    },
    /// Control acknowledges join and returns (or reissues) the API key.
    Welcome {
        account: String,
        api_key: String,
    },
    Heartbeat {
        load: f32,
        healthy: bool,
    },
    /// Control → agents: pool readiness for the single model (size gates, weights).
    PoolStatus {
        pool_vram_mib: u64,
        backends: u32,
        pool_ready: bool,
        weights_published: bool,
        pool_progress_pct: u8,
        inference_mode: String,
        message: String,
        /// Quant this node should prepare (if any).
        recommend_quant: Option<String>,
    },
    /// Agent → control: local weight/arm status after prepare.
    PrepareOk {
        model: String,
        quant: String,
        armed: bool,
        files_complete: bool,
        message: String,
    },
    /// Agent → control: weights are resident in process memory (model loaded).
    ModelLoaded {
        model: String,
        quant: String,
        bytes_resident: u64,
        tensors: u32,
        message: String,
    },
    PlanRequest {
        model: String,
    },
    PlanOffer {
        plan: ClusterPlan,
        /// Correlates with RequestInfer / PlanAccept / InferRequest.
        #[serde(default = "Uuid::nil")]
        request_id: Uuid,
        /// Canonical plan body hash (hex). Empty = legacy offer (lab only).
        #[serde(default)]
        plan_hash_hex: String,
    },
    CapacitySnapshot {
        capacity: ClusterCapacity,
    },
    /// Control asks a donor to run its **shard** of a distributed inference.
    /// The full model is spread across `plan`; this node only owns its assignment.
    InferRequest {
        request_id: Uuid,
        model: String,
        prompt: String,
        max_tokens: u32,
        /// Full pool shard map (VRAM-weighted).
        plan: ClusterPlan,
        /// This node is the tail/coordinator shard (returns user-visible tokens in stub).
        is_tail: bool,
    },
    InferDone {
        request_id: Uuid,
        text: String,
        prompt_tokens: u32,
        completion_tokens: u32,
        /// Non-tail shards may return empty text with shard_ok.
        #[serde(default = "default_true")]
        shard_ok: bool,
    },
    InferError {
        request_id: Uuid,
        error: String,
    },
    /// Spot capacity attestation: agent must solve mem-bound work for `capacity_seed_hex`.
    /// `completion` in ChallengeResult is the capacity proof hex — **not** a public stub format.
    Challenge {
        challenge_id: Uuid,
        model: String,
        prompt: String,
        /// 32-byte seed as lowercase hex (64 chars). Empty = invalid / legacy reject.
        #[serde(default)]
        capacity_seed_hex: String,
        /// MiB of verified credit this challenge can unlock (typically 1024).
        #[serde(default)]
        credit_mib: u32,
    },
    ChallengeResult {
        challenge_id: Uuid,
        /// Capacity proof hex from `joule_cluster::capacity_proof_hex` (or fail).
        completion: String,
        latency_ms: u32,
    },
    CreditEvent {
        event_id: Uuid,
        account: String,
        delta_millijoules: i64,
        reason: String,
    },
    /// Control → agent: error string.
    Error {
        error: String,
    },
    /// Agent → control (and peer gossip): I am alive; here is how to dial me.
    /// See docs/design/decentral-discovery-v0.md Phase A.
    PeerAlive {
        /// Dial strings, e.g. `tcp://1.2.3.4:7702` (peer blob/gossip port).
        multiaddrs: Vec<String>,
        load: f32,
        healthy: bool,
        /// How many content digests this node seeds.
        #[serde(default)]
        blob_count: u32,
        /// Self-reported claim (UI only — never placement).
        #[serde(default)]
        mem_mib: u32,
        /// Protocol-verified capacity (MiB) for mesh PlanOffer — claim alone is ignored.
        #[serde(default)]
        verified_mem_mib: u32,
        /// Throughput class hint (same units as NodeCaps).
        #[serde(default)]
        throughput_class: u16,
    },
    /// Any peer → mesh: request inference on the logical device (Phase D).
    /// Coordinator answers with PlanOffer; shards answer PlanAccept; then InferRequest.
    RequestInfer {
        request_id: Uuid,
        account: String,
        model: String,
        prompt: String,
        max_tokens: u32,
    },
    /// Shard peer → coordinator: accept or reject a PlanOffer for a request.
    ///
    /// `plan_hash_hex` + `confirm_hex` are **content confirmations** (hashed
    /// agreement). Recipients verify with `joule_cluster::verify_plan_accept_confirm`.
    PlanAccept {
        plan_id: Uuid,
        request_id: Uuid,
        accepted: bool,
        #[serde(default)]
        reason: String,
        /// SHA-256 hex of the plan body (must match coordinator's plan).
        #[serde(default)]
        plan_hash_hex: String,
        /// Domain-separated accept confirmation hash (see lease module).
        #[serde(default)]
        confirm_hex: String,
    },
    /// Coordinator → shards: hashed plan body for agreement (optional; PlanOffer still carries full plan).
    PlanHash {
        plan_id: Uuid,
        request_id: Uuid,
        plan_hash_hex: String,
    },
    /// Agent → control / peer: content-addressed blobs this node can seed.
    /// f00 does **not** host these; peers do. See docs/design/distribution-v0.md.
    BlobsHave {
        blobs: Vec<BlobMeta>,
    },
    /// Agent → control or **direct to seeder peer**: need this hash from the swarm.
    BlobWant {
        sha256: String,
    },
    /// Control → agent: peers that announced this hash (empty if nobody seeding yet).
    /// `multiaddrs[i]` is the dial list for `peers[i]` when known (Phase A/B).
    BlobLocate {
        sha256: String,
        peers: Vec<NodeId>,
        sizes: Vec<u64>,
        #[serde(default)]
        multiaddrs: Vec<Vec<String>>,
    },
    /// Control → seeder: please push this blob toward the swarm directory (payload out-of-band / later chunk).
    /// For small blobs, seeder may reply with BlobChunk.
    BlobProvide {
        sha256: String,
        request_id: Uuid,
        to: NodeId,
    },
    /// Seeder → control → requester: chunk of a blob (base64). For lab-sized files; large models use peer HTTP later.
    BlobChunk {
        sha256: String,
        request_id: Uuid,
        offset: u64,
        /// raw bytes, base64-encoded on the wire via serde_bytes not available — use base64 string
        data_b64: String,
        done: bool,
    },
    /// Operator-signed order (update, model, notice, …). Peers verify + rebroadcast.
    /// See docs/design/broadcast-v0.md. f00 is not a push server — swarm floods this.
    OperatorBroadcast {
        envelope: SignedEnvelope,
    },
    /// Control → agent: digests this node should obtain (subset of model, not full).
    FetchDigests {
        digests: Vec<String>,
        reason: String,
        /// Desired replica count for rebalance hints.
        #[serde(default = "default_replica_factor")]
        replica_factor: u32,
    },
}

fn default_replica_factor() -> u32 {
    2
}

/// Kind of operator order (allow-listed actions only).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorKind {
    Notice,
    SoftwareUpdate,
    ModelUpdate,
    Policy,
    PauseService,
    ResumeService,
    Revoke,
    /// Unknown kinds: verify + store + relay only (never execute).
    #[serde(other)]
    Other,
}

/// Authenticated operator message. Heavy payloads are **hashes**, not bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedEnvelope {
    pub id: Uuid,
    pub issued_at_unix_ms: u64,
    #[serde(default)]
    pub expires_at_unix_ms: Option<u64>,
    pub kind: OperatorKind,
    /// Canonical JSON body for this kind (string to keep hashing stable).
    pub body_json: String,
    pub body_sha256: String,
    /// ed25519 signature hex over preimage (see broadcast-v0).
    pub sig_ed25519_hex: String,
    /// Optional OpenPGP detached signature (humans / GPG).
    #[serde(default)]
    pub openpgp_sig: Option<String>,
}

/// Content-addressed object a node can seed into the pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobMeta {
    pub sha256: String,
    pub size: u64,
    /// weight | software | fixture | other
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub name: String,
    /// Optional dial hints for direct peer fetch (`tcp://host:port`).
    #[serde(default)]
    pub multiaddrs: Vec<String>,
}

fn default_true() -> bool {
    true
}

/// Encode one envelope as a single newline-terminated JSON line.
pub fn encode_line(env: &Envelope) -> Result<Vec<u8>, serde_json::Error> {
    let mut v = serde_json::to_vec(env)?;
    v.push(b'\n');
    Ok(v)
}

/// Decode one JSON line (without trailing newline).
pub fn decode_line(line: &[u8]) -> Result<Envelope, serde_json::Error> {
    serde_json::from_slice(line)
}

/// Canonical preimage for a signed Hello (UTF-8 string, then sign the bytes).
///
/// The whole pool verifies the same formula — account must match the pubkey fingerprint.
pub fn hello_sign_preimage(
    account: &str,
    from: &NodeId,
    pubkey_hex: &str,
    signed_at_unix_ms: u64,
) -> String {
    format!(
        "joule-hello-v1|{account}|{from}|{pubkey}|{ts}|{proto}",
        account = account.trim(),
        from = from,
        pubkey = pubkey_hex.trim().to_ascii_lowercase(),
        ts = signed_at_unix_ms,
        proto = PROTOCOL_VERSION,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_hello() {
        let env = Envelope::new(
            NodeId::new(),
            Message::Hello {
                account: "alice".into(),
                caps: NodeCaps::for_cluster(DeviceClass::Gpu, 24576, 40),
                pubkey_hex: String::new(),
                sig_hex: String::new(),
                signed_at_unix_ms: 0,
            },
        );
        let line = encode_line(&env).unwrap();
        let back = decode_line(&line[..line.len() - 1]).unwrap();
        match back.msg {
            Message::Hello { account, caps, .. } => {
                assert_eq!(account, "alice");
                assert_eq!(caps.mem_mib, 24576);
                assert_eq!(caps.models, vec![CLUSTER_MODEL.to_string()]);
            }
            _ => panic!("expected hello"),
        }
        let pre = hello_sign_preimage("alice", &NodeId::new(), "ab", 1);
        assert!(pre.starts_with("joule-hello-v1|alice|"));
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
            models_available: vec![CLUSTER_MODEL.into()],
            stream_slots_total: 4,
            stream_slots_used: 1,
            logical_device: None,
        };
        let v = serde_json::to_value(&c).unwrap();
        assert_eq!(v["nodes_healthy"], 2);
    }

    #[test]
    fn resolve_single_model() {
        assert_eq!(resolve_cluster_model(None).unwrap(), CLUSTER_MODEL);
        assert_eq!(resolve_cluster_model(Some("kimi")).unwrap(), CLUSTER_MODEL);
        assert!(resolve_cluster_model(Some("gpt-4")).is_err());
    }
}
