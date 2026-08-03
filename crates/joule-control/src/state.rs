//! Shared control-plane state: cluster registry, ledger, accounts, pending jobs.

use crate::blobs::BlobDirectory;
use crate::broadcast::BroadcastLog;
use crate::mesh::MeshDirectory;
use crate::persist;
use joule_cluster::Cluster;
use joule_dht::DhtStore;
use joule_ledger::{score_burn, score_mint, EconomyEvent, FairnessSnapshot, Ledger, Millijoule};
use joule_proto::{ClusterPlan, DeviceClass, NodeCaps, NodeId, CLUSTER_MODEL};
use serde::Serialize;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, Notify, RwLock};
use tracing::{info, warn};
use uuid::Uuid;

pub type SharedState = Arc<RwLock<ControlState>>;

#[derive(Debug, Clone, Serialize)]
pub struct AccountInfo {
    pub account: String,
    pub api_key: String,
    pub balance_millijoules: Millijoule,
    pub donating: bool,
    /// Rolling-window millijoules earned (fairness window).
    pub contributed_mj_window: Millijoule,
    /// Rolling-window millijoules spent.
    pub consumed_mj_window: Millijoule,
    /// Continuous healthy online seconds (tenure).
    pub continuous_online_secs: u64,
    /// Current leecher mint multiplier in basis points (10_000 = 1.0×).
    pub leecher_mint_bp: u32,
    /// Current leecher usage multiplier in basis points.
    pub leecher_usage_bp: u32,
    /// Lifetime prompt tokens charged on this account (chat usage).
    #[serde(default)]
    pub prompt_tokens_used: u64,
    /// Lifetime completion tokens charged on this account.
    #[serde(default)]
    pub completion_tokens_used: u64,
}

/// Per-account fairness stats for the auditable economy (v0).
#[derive(Debug, Clone, Default, Serialize)]
pub struct AccountEconomy {
    pub contributed_mj_window: Millijoule,
    pub consumed_mj_window: Millijoule,
    /// Wall time when continuous healthy streak started (None if offline).
    #[serde(skip)]
    pub online_since: Option<Instant>,
    /// Accumulated continuous seconds at last disconnect (restored after reboot only partially).
    pub continuous_online_secs: u64,
    /// Best *verified* mem across this account's nodes (MiB) — claims ignored.
    pub best_mem_mib: u32,
    /// Disconnect events in the fairness window (churn penalty).
    #[serde(default)]
    pub disconnects_window: u32,
    /// Lifetime prompt tokens billed via chat (not persisted until snapshot v6).
    #[serde(default)]
    pub prompt_tokens_used: u64,
    /// Lifetime completion tokens billed via chat.
    #[serde(default)]
    pub completion_tokens_used: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeView {
    pub id: String,
    pub account: String,
    pub device: String,
    pub mem_mib: u32,
    /// Untrusted advertisement.
    pub claimed_mem_mib: u32,
    /// Protocol-trusted VRAM (challenges).
    pub verified_mem_mib: u32,
    /// claim_only | challenge_partial | challenge_full
    pub attestation_tier: String,
    pub throughput_class: u16,
    pub healthy: bool,
    pub load: f32,
    pub inflight: u32,
    pub max_slots: u32,
    pub free_slots: u32,
    pub compute_state: String,
    pub reputation_ok: u64,
    pub reputation_fail: u64,
    pub banned: bool,
    pub models: Vec<String>,
}

/// How a chat/infer was coordinated (Phase D mesh vs classic control stream).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinationPath {
    /// Classic: cluster registry + try_acquire_stream + InferRequest fan-out.
    ControlDispatch,
    /// Mesh: RequestInfer → plan_from_mesh_donors → PlanOffer → PlanAccept → InferRequest.
    MeshRequestInfer,
}

impl CoordinationPath {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ControlDispatch => "control_dispatch",
            Self::MeshRequestInfer => "mesh_request_infer",
        }
    }
}

/// Non-tail InferDone payload for pipeline activation handoff (phase-1 collect).
#[derive(Debug, Clone)]
pub struct ShardAck {
    pub activation: Option<joule_proto::ShardActivation>,
    pub shard_ok: bool,
}

#[derive(Debug)]
pub struct PendingInfer {
    pub account: String,
    /// Full VRAM-sharded plan; stream reserved on every shard when `stream_reserved`.
    pub plan: ClusterPlan,
    /// Shards still expected to ACK (node ids).
    pub awaiting: std::collections::HashSet<NodeId>,
    pub tail_text: Option<String>,
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub charge: bool,
    /// True if cluster.try_acquire_stream was used (must release_stream).
    pub stream_reserved: bool,
    pub coordination: CoordinationPath,
    pub tx: Option<oneshot::Sender<Result<InferOutcome, String>>>,
}

/// In-flight PlanOffer acceptance wait (mesh Phase D).
#[derive(Debug)]
pub struct PendingPlanAccept {
    pub plan_id: Uuid,
    /// Canonical plan hash required on every PlanAccept.
    pub plan_hash_hex: String,
    pub expected: std::collections::HashSet<NodeId>,
    pub accepted: std::collections::HashSet<NodeId>,
    pub tx: Option<oneshot::Sender<Result<(), String>>>,
}

#[derive(Debug, Clone)]
pub struct InferOutcome {
    pub text: String,
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub worker_account: String,
    pub device: DeviceClass,
    pub worker_id: NodeId,
    /// Aggregate pool VRAM the request was sharded over.
    pub pool_mem_mib: u64,
    pub shard_count: u32,
    /// `mesh_request_infer` or `control_dispatch`.
    pub coordination: String,
}

#[derive(Debug)]
pub struct PendingChallenge {
    pub node: NodeId,
    pub model: String,
    pub prompt: String,
    /// Capacity proof hex from `joule_cluster::capacity_proof_hex` (not stub format).
    pub expected: String,
    pub capacity_seed_hex: String,
    pub credit_mib: u32,
    pub started: Instant,
}

#[derive(Debug)]
pub struct ControlState {
    pub cluster: Cluster,
    pub ledger: Ledger,
    pub keys: HashMap<String, String>,
    pub account_keys: HashMap<String, String>,
    /// account_id → ed25519 pubkey hex (bound at first signed Hello).
    pub account_pubkeys: HashMap<String, String>,
    pub node_account: HashMap<NodeId, String>,
    pub pending: HashMap<Uuid, PendingInfer>,
    /// request_id → PlanAccept collector (mesh Phase D).
    pub pending_plan_accepts: HashMap<Uuid, PendingPlanAccept>,
    pub pending_challenges: HashMap<Uuid, PendingChallenge>,
    pub heartbeat_mint_mj: Millijoule,
    /// Every Nth chat request also runs a second-worker verify (0 = off).
    pub dual_verify_every: u64,
    /// How long chat admission waits for a free stream slot (tests may shorten).
    pub lease_wait: Duration,
    pub chat_count: u64,
    pub data_dir: Option<PathBuf>,
    /// Wake waiters when a compute slot frees.
    pub schedule_notify: Option<Arc<Notify>>,
    /// Ring buffer of (time, healthy_vram_mib) for countdown ETA.
    pub vram_history: VecDeque<(Instant, u64)>,
    /// Nodes that reported successful model load (weights resident).
    pub nodes_model_loaded: HashSet<NodeId>,
    /// Operator / automatic flag: public service serving real model.
    pub service_live: bool,
    /// Operator pause (signed bus) — blocks chat even if donors online.
    pub operator_paused: bool,
    /// Per-account rolling fairness + tenure (economy v0).
    pub account_economy: HashMap<String, AccountEconomy>,
    /// Swarm content directory (hash → seeders). Never stores payload bytes.
    pub blobs: BlobDirectory,
    /// Mesh peer directory (multiaddrs for direct dial). Phase A decentral discovery.
    pub mesh: MeshDirectory,
    /// Content-addressed DHT lite view (peer/ + blob/ keys). Phase C.
    pub dht: DhtStore,
    /// Operator-signed messages (deduped); peers flood these.
    pub broadcasts: BroadcastLog,
    /// In-flight blob transfers (seeder-attributed book for backpressure).
    pub pending_blob_xfers: crate::seeder_rank::BlobXferBook,
    /// Active model chunks from last model_update (for rebalance).
    pub active_chunks: Vec<joule_cluster::ModelChunk>,
    pub active_replica_factor: u32,
    /// Last rebalance wall time (rate-limit BlobsHave-triggered rebalance).
    pub last_rebalance: Option<Instant>,
    /// Per-node notary ed25519 secret keys (32 bytes). Generated at join with OS RNG.
    /// Checkpoints only sign with these — never deterministic lab keys.
    pub notary_secret_keys: HashMap<String, [u8; 32]>,
    /// Stream leases for chat admission (free/used truth + audit trail).
    pub leases: joule_cluster::LeaseBook,
    /// MANIFEST digests staged + sha256 verified (content-addressed).
    pub digests_verified: bool,
    /// NodeId → ed25519 device pubkey hex (from Hello).
    pub node_device_pubkeys: std::collections::HashMap<NodeId, String>,
    dirty: bool,
}

impl Default for ControlState {
    fn default() -> Self {
        Self::new()
    }
}

impl ControlState {
    pub fn new() -> Self {
        Self {
            cluster: Cluster::new(Duration::from_secs(30)),
            ledger: Ledger::new(),
            keys: HashMap::new(),
            account_keys: HashMap::new(),
            account_pubkeys: HashMap::new(),
            node_account: HashMap::new(),
            pending: HashMap::new(),
            pending_plan_accepts: HashMap::new(),
            pending_challenges: HashMap::new(),
            heartbeat_mint_mj: 10,
            dual_verify_every: 3,
            lease_wait: Duration::from_secs(20),
            chat_count: 0,
            data_dir: None,
            schedule_notify: None,
            vram_history: VecDeque::new(),
            nodes_model_loaded: HashSet::new(),
            service_live: false,
            operator_paused: false,
            account_economy: HashMap::new(),
            blobs: BlobDirectory::new(),
            mesh: MeshDirectory::new(),
            dht: DhtStore::new(),
            broadcasts: BroadcastLog::new(256),
            pending_blob_xfers: crate::seeder_rank::BlobXferBook::new(),
            active_chunks: Vec::new(),
            active_replica_factor: joule_cluster::DEFAULT_REPLICA_FACTOR,
            last_rebalance: None,
            notary_secret_keys: HashMap::new(),
            leases: joule_cluster::LeaseBook::default(),
            digests_verified: false,
            node_device_pubkeys: std::collections::HashMap::new(),
            dirty: false,
        }
    }

    pub(crate) fn economy_mut(&mut self, account: &str) -> &mut AccountEconomy {
        self.account_economy.entry(account.to_string()).or_default()
    }

    /// Build fairness snapshot for scoring (refreshes continuous tenure clock).
    ///
    /// `mem_mib` is always the **current** max verified VRAM across this account's
    /// nodes (synced from cluster) — never a sticky peak after challenge fail.
    pub fn fairness_for(&mut self, account: &str) -> FairnessSnapshot {
        self.sync_best_mem_for_account(account);
        let eco = self.economy_mut(account);
        let continuous = match eco.online_since {
            Some(since) => eco
                .continuous_online_secs
                .saturating_add(since.elapsed().as_secs()),
            None => eco.continuous_online_secs,
        };
        FairnessSnapshot {
            // CRITICAL: verified-only (never raw GPU claim).
            mem_mib: eco.best_mem_mib.max(256),
            continuous_online_secs: continuous,
            contributed_mj_window: eco.contributed_mj_window,
            consumed_mj_window: eco.consumed_mj_window,
            disconnects_window: eco.disconnects_window,
        }
    }

    fn record_contribute(&mut self, account: &str, mj: Millijoule) {
        let eco = self.economy_mut(account);
        eco.contributed_mj_window = eco.contributed_mj_window.saturating_add(mj);
        // Soft decay when window gets huge so old history does not dominate forever.
        if eco.contributed_mj_window > 1_000_000 {
            eco.contributed_mj_window /= 2;
            eco.consumed_mj_window /= 2;
        }
    }

    fn record_consume(&mut self, account: &str, mj: Millijoule) {
        let eco = self.economy_mut(account);
        eco.consumed_mj_window = eco.consumed_mj_window.saturating_add(mj);
        if eco.consumed_mj_window > 1_000_000 {
            eco.contributed_mj_window /= 2;
            eco.consumed_mj_window /= 2;
        }
    }

    pub(crate) fn note_online(&mut self, account: &str, verified_mem_mib: u32, healthy: bool) {
        let eco = self.economy_mut(account);
        // Only verified memory counts toward economic mem factor.
        if verified_mem_mib > eco.best_mem_mib {
            eco.best_mem_mib = verified_mem_mib;
        }
        if healthy {
            if eco.online_since.is_none() {
                eco.online_since = Some(Instant::now());
            }
        } else if let Some(since) = eco.online_since.take() {
            eco.continuous_online_secs = eco
                .continuous_online_secs
                .saturating_add(since.elapsed().as_secs());
            // Offline resets continuous streak (loyalty is continuous presence).
            eco.continuous_online_secs = 0;
            // Churn: count disconnect for fairness window (soft decay when huge).
            eco.disconnects_window = eco.disconnects_window.saturating_add(1);
            if eco.disconnects_window > 10_000 {
                eco.disconnects_window /= 2;
            }
        }
    }

    /// Mesh PlanOffer donors: healthy mesh peers with **cluster verified** capacity only.
    /// PeerAlive claim is never used for geometry.
    pub fn mesh_plan_donors(&self) -> Vec<(NodeId, u32)> {
        let mut v: Vec<(NodeId, u32)> = self
            .mesh
            .list()
            .into_iter()
            .filter(|p| p.healthy)
            .filter_map(|p| {
                let verified = self.cluster.verified_mem_mib(&p.node);
                if joule_cluster::placement_mem_mib(verified) == 0 {
                    return None;
                }
                Some((p.node, joule_cluster::economic_mem_mib(verified)))
            })
            .collect();
        v.sort_by(|a, b| {
            b.1.cmp(&a.1)
                .then_with(|| a.0.to_string().cmp(&b.0.to_string()))
        });
        v
    }

    /// Eligible accounts for pool donation redistrib (currently online with verified mem).
    pub fn donation_recipients(&self, exclude: &str) -> Vec<String> {
        let mut set = std::collections::BTreeSet::new();
        for n in self.cluster.nodes() {
            if n.account == exclude {
                continue;
            }
            if n.healthy && n.verified_mem_mib > 0 {
                set.insert(n.account.clone());
            }
        }
        // Also include any account with a positive sealed balance that is donating
        // (even if currently offline) — equitable share among pool participants.
        for (acct, eco) in &self.account_economy {
            if acct == exclude {
                continue;
            }
            if eco.best_mem_mib > 0 || eco.contributed_mj_window > 0 {
                set.insert(acct.clone());
            }
        }
        set.into_iter().collect()
    }

    /// Voluntary donate unused millijoules into the pool (sealed burn + equitable credits).
    pub fn donate_to_pool(
        &mut self,
        donor: &str,
        amount: Millijoule,
    ) -> Result<joule_ledger::DonateResult, String> {
        if amount <= 0 {
            return Err("amount must be positive".into());
        }
        let recipients = self.donation_recipients(donor);
        if recipients.is_empty() {
            return Err("no eligible recipients in the pool".into());
        }
        let result = self
            .ledger
            .donate_to_pool(donor, amount, &recipients)
            .map_err(|e| e.to_string())?;
        // Do **not** record_contribute for donate_receive — pool gifts must not wash
        // anti-leech (consume ≫ contribute) fairness windows.
        self.seal_and_checkpoint();
        self.mark_dirty();
        self.save_if_dirty();
        Ok(result)
    }

    pub fn record_vram_sample(&mut self, healthy_vram_mib: u64) {
        let now = Instant::now();
        self.vram_history.push_back((now, healthy_vram_mib));
        while self.vram_history.len() > 120 {
            self.vram_history.pop_front();
        }
        // Drop samples older than 2 hours.
        while self
            .vram_history
            .front()
            .is_some_and(|(t, _)| now.duration_since(*t) > Duration::from_secs(7200))
        {
            self.vram_history.pop_front();
        }
    }

    /// Recent aggregate VRAM growth (MiB/s), if observable.
    pub fn vram_growth_mib_per_sec(&self) -> Option<f64> {
        if self.vram_history.len() < 2 {
            return None;
        }
        let (t0, v0) = *self.vram_history.front()?;
        let (t1, v1) = *self.vram_history.back()?;
        let dt = t1.duration_since(t0).as_secs_f64();
        if dt < 5.0 {
            return None;
        }
        let dv = v1 as f64 - v0 as f64;
        if dv <= 0.0 {
            return None;
        }
        Some(dv / dt)
    }

    pub fn runtime_flags(&self) -> joule_runtime::RuntimeFlags {
        joule_runtime::RuntimeFlags {
            model_loaded: !self.nodes_model_loaded.is_empty(),
            // Raw operator/auto flag — readiness honesty ANDs digests + model_loaded.
            service_live: self.service_live,
            digests_verified: self.digests_verified,
        }
    }

    /// Public live claim for HTTP/dashboard: matches readiness honesty
    /// (`service_live ∧ digests_verified ∧ model_loaded`). Never report live without digests.
    pub fn service_live_public(&self) -> bool {
        self.service_live
            && self.digests_verified
            && !self.nodes_model_loaded.is_empty()
            && !self.operator_paused
    }

    /// Set operator `service_live` intent; refuse true without digests_verified.
    pub fn set_service_live_intent(&mut self, want: bool) {
        if want && !self.digests_verified {
            self.service_live = false;
            info!("service_live intent true refused — digests not verified");
        } else {
            self.service_live = want;
            // Public true still needs model_loaded (service_live_public).
        }
    }

    pub fn mark_node_loaded(&mut self, id: NodeId) {
        self.nodes_model_loaded.insert(id);
        // Auto service-live only when digests verified + pool gate + enough loaders.
        let backends = self.cluster.pool_size() as u32;
        let vram = self.cluster.capacity().mem_mib_healthy;
        if let Ok(r) = joule_runtime::readiness_for_pool_ex(
            vram,
            backends,
            self.runtime_flags(),
            self.vram_growth_mib_per_sec(),
        ) {
            if r.can_begin_service
                && self.digests_verified
                && self.nodes_model_loaded.len() >= r.required_backends as usize
            {
                self.service_live = true;
                info!("service marked live — digests verified + model loaded on mesh");
            } else if !self.digests_verified {
                self.service_live = false;
            }
        }
    }

    /// Set digests gate (tests / explicit content verify only — not agent self-report).
    pub fn set_digests_verified(&mut self, ok: bool) {
        self.digests_verified = ok;
        if !ok {
            self.service_live = false;
        }
    }

    /// **Only** digests SoT: assign `digests_verified` from WeightsStore MANIFEST sha256
    /// (`digests_verified_for_primary_lab`). Never BlobsHave catalog, PrepareOk, or ModelLoaded.
    /// Assigns the pure result (not sticky-or-true).
    pub fn refresh_digests_from_evidence(&mut self) -> bool {
        let store = joule_runtime::WeightsStore::new(joule_runtime::WeightsStore::default_root());
        let ok = joule_runtime::digests_verified_for_primary_lab(&store).unwrap_or(false);
        self.set_digests_verified(ok);
        if ok {
            info!("digests_verified from WeightsStore MANIFEST sha256");
        }
        ok
    }

    pub fn set_node_device_pubkey(&mut self, id: &NodeId, pubkey_hex: &str) {
        let pk = pubkey_hex.trim().to_ascii_lowercase();
        if pk.len() == 64 && pk.chars().all(|c| c.is_ascii_hexdigit()) {
            self.node_device_pubkeys.insert(id.clone(), pk);
        }
    }

    pub fn device_pubkey(&self, id: &NodeId) -> Option<&str> {
        self.node_device_pubkeys.get(id).map(|s| s.as_str())
    }

    pub fn shared() -> SharedState {
        Self::shared_with_notify(Arc::new(Notify::new()))
    }

    pub fn shared_with_notify(notify: Arc<Notify>) -> SharedState {
        let mut s = Self::new();
        s.schedule_notify = Some(notify);
        Arc::new(RwLock::new(s))
    }

    pub fn shared_with_data_dir(
        dir: PathBuf,
        notify: Arc<Notify>,
    ) -> Result<SharedState, anyhow::Error> {
        let mut state = Self::new();
        state.data_dir = Some(dir.clone());
        state.schedule_notify = Some(notify);
        if let Some(snap) = persist::load(&dir)? {
            info!(path = %dir.display(), "loaded persisted state");
            persist::apply_snapshot(&mut state, snap);
        }
        Ok(Arc::new(RwLock::new(state)))
    }

    pub fn wake_scheduler(&self) {
        if let Some(n) = &self.schedule_notify {
            n.notify_waiters();
        }
    }

    pub fn release_slot(&mut self, id: &NodeId) {
        self.cluster.release_worker(id);
        self.wake_scheduler();
    }

    /// Admit a stream lease (expires stale first). Fail closed when pool full.
    pub fn admit_stream_lease(
        &mut self,
        account: &str,
        request_id: Uuid,
        ttl: std::time::Duration,
    ) -> Result<joule_cluster::StreamLease, String> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let mut book = std::mem::take(&mut self.leases);
        book.expire_stale(&mut self.cluster, now);
        let r = book.try_admit(&mut self.cluster, account, request_id, ttl);
        self.leases = book;
        r
    }

    /// Release lease by request id (idempotent).
    pub fn release_stream_lease(&mut self, request_id: Uuid, event: &str, detail: &str) -> bool {
        let mut book = std::mem::take(&mut self.leases);
        let ok = book.release_by_request(&mut self.cluster, request_id, event, detail);
        self.leases = book;
        if ok {
            self.wake_scheduler();
        }
        ok
    }

    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    pub fn save_if_dirty(&mut self) {
        if !self.dirty {
            return;
        }
        let Some(dir) = self.data_dir.clone() else {
            return;
        };
        if let Err(e) = persist::save(&dir, self) {
            warn!(error = %e, "persist failed");
            return;
        }
        self.dirty = false;
    }

    pub fn prune(&mut self) {
        self.cluster.apply_staleness();
        self.cluster.reap_dead(Duration::from_secs(120));
        // Drop load flags for dead nodes.
        let alive: HashSet<_> = self.cluster.nodes().map(|n| n.id.clone()).collect();
        self.nodes_model_loaded.retain(|id| alive.contains(id));
        let vram = self.cluster.capacity().mem_mib_healthy;
        self.record_vram_sample(vram);
        // Expire old challenges as **fails** (do not silently drop — prevents
        // fully unlocked claims from never decaying when agents stop answering).
        let now = Instant::now();
        let expired: Vec<(Uuid, NodeId)> = self
            .pending_challenges
            .iter()
            .filter(|(_, c)| now.duration_since(c.started) >= Duration::from_secs(60))
            .map(|(id, c)| (*id, c.node.clone()))
            .collect();
        for (cid, node) in expired {
            self.pending_challenges.remove(&cid);
            self.cluster.record_challenge_fail(&node);
            if let Some(account) = self.node_account.get(&node).cloned() {
                self.sync_best_mem_for_account(&account);
            }
            self.mark_dirty();
            warn!(%node, %cid, "challenge expired → fail (verified decay)");
        }
        // Drop stuck control-relayed blob transfers (lab path).
        self.pending_blob_xfers
            .retain_fresh(Duration::from_secs(120));
        self.mesh.prune_stale(Duration::from_secs(180));
        // Expire stale stream leases so cancel/disconnect cannot strand slots forever.
        let unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let mut book = std::mem::take(&mut self.leases);
        let n = book.expire_stale(&mut self.cluster, unix);
        self.leases = book;
        if n > 0 {
            self.wake_scheduler();
            warn!(expired = n, "expired stale stream leases");
        }
        self.save_if_dirty();
    }

    /// Recompute account best_mem from **current** cluster verified max (can decrease).
    pub(crate) fn sync_best_mem_for_account(&mut self, account: &str) {
        let best_v = self
            .cluster
            .nodes()
            .filter(|n| n.account == account)
            .map(|n| n.verified_mem_mib)
            .max()
            .unwrap_or(0);
        if let Some(eco) = self.account_economy.get_mut(account) {
            eco.best_mem_mib = best_v;
        }
    }

    pub fn ensure_account(&mut self, account: &str) -> String {
        if let Some(k) = self.account_keys.get(account) {
            return k.clone();
        }
        let api_key = format!("joule_{}", Uuid::new_v4().simple());
        self.account_keys
            .insert(account.to_string(), api_key.clone());
        self.keys.insert(api_key.clone(), account.to_string());
        self.ledger.ensure_account(account);
        self.mark_dirty();
        api_key
    }

    pub fn account_for_key(&self, api_key: &str) -> Option<&str> {
        self.keys.get(api_key).map(|s| s.as_str())
    }

    pub fn register_node(&mut self, id: NodeId, account: &str, mut caps: NodeCaps) -> String {
        let api_key = self.ensure_account(account);
        // Single-model law: every donor is compute for CLUSTER_MODEL only.
        caps.models = vec![CLUSTER_MODEL.to_string()];
        self.cluster
            .upsert_node(id.clone(), account.to_string(), caps);
        let verified = self.cluster.verified_mem_mib(&id);
        self.node_account.insert(id.clone(), account.to_string());
        // Fresh OS-random notary key for this node (not deterministic from id).
        self.notary_secret_keys
            .entry(id.to_string())
            .or_insert_with(|| {
                use rand::RngCore;
                let mut sk = [0u8; 32];
                rand::rngs::OsRng.fill_bytes(&mut sk);
                sk
            });
        self.note_online(account, verified, true);
        self.mark_dirty();
        api_key
    }

    /// Seal a ledger checkpoint with **cryptographic notary quorum** (fail-closed).
    /// Signs only with per-node keys issued at join (`notary_secret_keys`). If a
    /// chosen notary has no key, the checkpoint is skipped (fail closed — never
    /// forge with deterministic lab keys).
    pub(crate) fn seal_and_checkpoint(&mut self) {
        let notaries = self.cluster.pick_notaries(3);
        if notaries.is_empty() {
            return;
        }
        let head = self.ledger.head().head_hash_hex;
        let mut atts = Vec::with_capacity(notaries.len());
        for nid in &notaries {
            let Some(sk_bytes) = self.notary_secret_keys.get(nid) else {
                tracing::warn!(%nid, "notary missing OS key — skip checkpoint (fail-closed)");
                return;
            };
            let sk = ed25519_dalek::SigningKey::from_bytes(sk_bytes);
            atts.push(joule_ledger::sign_head(&sk, &head, nid));
        }
        let min_ok = notaries.len().clamp(1, 2);
        match self
            .ledger
            .maybe_checkpoint_with_quorum(notaries, atts, min_ok)
        {
            Ok(Some(entry)) => {
                tracing::info!(
                    height = entry.height,
                    notaries = entry.notaries.len(),
                    sigs = entry.notary_attestations.len(),
                    "ledger checkpoint sealed with notary quorum"
                );
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(error = %e, "ledger checkpoint quorum rejected (fail-closed)");
            }
        }
    }

    pub fn on_heartbeat(
        &mut self,
        id: &NodeId,
        load: f32,
        healthy: bool,
    ) -> Result<Option<Millijoule>, joule_cluster::ClusterError> {
        self.cluster.set_health(id, healthy, load)?;
        let account = self
            .node_account
            .get(id)
            .cloned()
            .unwrap_or_else(|| "unknown".into());
        let verified = self.cluster.verified_mem_mib(id);
        self.note_online(&account, verified, healthy);
        // Refresh best verified from cluster (can decrease after challenge fail).
        self.sync_best_mem_for_account(&account);
        if !healthy {
            self.mark_dirty();
            self.save_if_dirty();
            return Ok(None);
        }
        let fair = self.fairness_for(&account);
        let breakdown = score_mint(EconomyEvent::Heartbeat, fair);
        // Allow operator heartbeat_mint_mj as scale on the scored base path (default 10 matches HEARTBEAT_BASE).
        let scale = if self.heartbeat_mint_mj > 0 {
            self.heartbeat_mint_mj
        } else {
            10
        };
        let mint = if scale == 10 {
            breakdown.total_mj
        } else {
            // rescale from default base 10
            (breakdown.total_mj.saturating_mul(scale)) / 10
        }
        .max(1);
        let reason = breakdown.reason_tag(&format!("heartbeat:{id}|vmem={verified}"));
        let _ = self
            .ledger
            .mint_contribution_verified(&account, mint, reason, Some(verified));
        self.record_contribute(&account, mint);
        self.seal_and_checkpoint();
        self.mark_dirty();
        self.save_if_dirty();
        Ok(Some(mint))
    }

    pub fn remove_node(&mut self, id: &NodeId) {
        if let Some(account) = self.node_account.get(id).cloned() {
            self.note_online(&account, 0, false);
        }
        self.cluster.remove_node(id);
        self.node_account.remove(id);
        self.blobs.remove_node(id);
        self.mesh.remove(id);
        // Drop pending challenges for this node.
        self.pending_challenges.retain(|_, c| c.node != *id);
    }

    pub fn account_info(&self, account: &str) -> Option<AccountInfo> {
        let api_key = self.account_keys.get(account)?.clone();
        let eco = self
            .account_economy
            .get(account)
            .cloned()
            .unwrap_or_default();
        let continuous = match eco.online_since {
            Some(since) => eco
                .continuous_online_secs
                .saturating_add(since.elapsed().as_secs()),
            None => eco.continuous_online_secs,
        };
        let (leecher_mint_bp, leecher_usage_bp) =
            joule_ledger::leecher_factors_bp(eco.contributed_mj_window, eco.consumed_mj_window);
        Some(AccountInfo {
            account: account.to_string(),
            api_key,
            balance_millijoules: self.ledger.balance(account),
            donating: self.cluster.account_is_donating(account),
            contributed_mj_window: eco.contributed_mj_window,
            consumed_mj_window: eco.consumed_mj_window,
            continuous_online_secs: continuous,
            leecher_mint_bp,
            leecher_usage_bp,
            prompt_tokens_used: eco.prompt_tokens_used,
            completion_tokens_used: eco.completion_tokens_used,
        })
    }

    pub fn node_views(&self) -> Vec<NodeView> {
        let now = Instant::now();
        let mut out: Vec<NodeView> = self
            .cluster
            .nodes()
            .map(|n| {
                let max = joule_cluster::max_slots(n);
                let free = joule_cluster::free_slots(n);
                let st = joule_cluster::compute_state(n);
                NodeView {
                    id: n.id.to_string(),
                    account: n.account.clone(),
                    device: match n.caps.device {
                        DeviceClass::Gpu => "gpu".into(),
                        DeviceClass::Metal => "metal".into(),
                        DeviceClass::Cpu => "cpu".into(),
                    },
                    mem_mib: n.verified_mem_mib,
                    claimed_mem_mib: n.claimed_mem_mib,
                    verified_mem_mib: n.verified_mem_mib,
                    throughput_class: n.caps.throughput_class,
                    healthy: n.healthy,
                    load: n.load,
                    inflight: n.inflight,
                    max_slots: max,
                    free_slots: free,
                    compute_state: match st {
                        joule_cluster::ComputeState::Free => "free".into(),
                        joule_cluster::ComputeState::Loaded => "loaded".into(),
                        joule_cluster::ComputeState::Full => "full".into(),
                        joule_cluster::ComputeState::Unavailable => "unavailable".into(),
                    },
                    reputation_ok: n.reputation.ok,
                    reputation_fail: n.reputation.fail,
                    banned: n.reputation.is_banned(now),
                    models: n.caps.models.clone(),
                    attestation_tier: joule_cluster::attestation_tier(
                        n.claimed_mem_mib,
                        n.verified_mem_mib,
                    )
                    .as_str()
                    .into(),
                }
            })
            .collect();
        out.sort_by(|a, b| a.account.cmp(&b.account).then(a.id.cmp(&b.id)));
        out
    }

    /// One shard of a multi-node stream finished.
    pub fn settle_shard_success(
        &mut self,
        request_id: Uuid,
        text: String,
        prompt_tokens: u32,
        completion_tokens: u32,
        worker: &NodeId,
        is_tail: bool,
    ) {
        let verified = self.cluster.verified_mem_mib(worker);
        let worker_account = self
            .node_account
            .get(worker)
            .cloned()
            .unwrap_or_else(|| "unknown".into());
        self.note_online(&worker_account, verified, true);
        self.sync_best_mem_for_account(&worker_account);
        let fair = self.fairness_for(&worker_account);
        let breakdown = score_mint(
            EconomyEvent::Work {
                completion_tokens: completion_tokens.max(1),
            },
            fair,
        );
        let mint = breakdown.total_mj;
        let reason = breakdown.reason_tag(&format!("infer-shard:{request_id}|vmem={verified}"));
        let _ =
            self.ledger
                .mint_contribution_verified(&worker_account, mint, reason, Some(verified));
        self.record_contribute(&worker_account, mint);
        self.seal_and_checkpoint();
        self.mark_dirty();

        let Some(pending) = self.pending.get_mut(&request_id) else {
            return;
        };
        pending.awaiting.remove(worker);
        if is_tail || (!text.is_empty() && pending.tail_text.is_none()) {
            pending.tail_text = Some(text);
            pending.prompt_tokens = prompt_tokens;
            pending.completion_tokens = completion_tokens;
        }
        if !pending.awaiting.is_empty() {
            return;
        }

        let Some(mut pending) = self.pending.remove(&request_id) else {
            return;
        };
        if pending.stream_reserved {
            self.cluster.release_stream(&pending.plan);
            self.wake_scheduler();
        }

        let text = pending
            .tail_text
            .unwrap_or_else(|| "[joule] empty completion from pool".into());
        let prompt_tokens = pending.prompt_tokens;
        let completion_tokens = pending.completion_tokens;
        let tail = pending
            .plan
            .shards
            .last()
            .map(|s| s.node.clone())
            .unwrap_or_else(NodeId::new);
        let device = self
            .cluster
            .get(&tail)
            .map(|n| n.caps.device)
            .unwrap_or(DeviceClass::Cpu);
        let worker_account = self
            .node_account
            .get(&tail)
            .cloned()
            .unwrap_or_else(|| "unknown".into());
        let pool_mem_mib = pending.plan.pool_mem_mib;
        let shard_count = pending.plan.shards.len() as u32;
        let coordination = pending.coordination.as_str().to_string();

        if pending.charge {
            let payer = pending.account.clone();
            let fair = self.fairness_for(&payer);
            let burn = score_burn(prompt_tokens, completion_tokens, fair);
            let reason = burn.reason_tag(&format!("chat:{request_id}"));
            if let Err(e) = self.ledger.burn_usage(&payer, burn.total_mj, reason) {
                if let Some(tx) = pending.tx.take() {
                    let _ = tx.send(Err(e.to_string()));
                }
                return;
            }
            self.record_consume(&payer, burn.total_mj);
            {
                let eco = self.economy_mut(&payer);
                eco.prompt_tokens_used = eco
                    .prompt_tokens_used
                    .saturating_add(u64::from(prompt_tokens));
                eco.completion_tokens_used = eco
                    .completion_tokens_used
                    .saturating_add(u64::from(completion_tokens));
            }
            self.seal_and_checkpoint();
            self.mark_dirty();
        }
        if let Some(tx) = pending.tx.take() {
            let _ = tx.send(Ok(InferOutcome {
                text,
                prompt_tokens,
                completion_tokens,
                worker_account,
                device,
                worker_id: tail,
                pool_mem_mib,
                shard_count,
                coordination,
            }));
        }
    }

    pub fn settle_infer_success(
        &mut self,
        request_id: Uuid,
        text: String,
        prompt_tokens: u32,
        completion_tokens: u32,
        worker: &NodeId,
    ) {
        self.settle_shard_success(
            request_id,
            text,
            prompt_tokens,
            completion_tokens,
            worker,
            true,
        );
    }

    pub fn settle_infer_error(&mut self, request_id: Uuid, error: String) {
        if let Some(mut pending) = self.pending.remove(&request_id) {
            if pending.stream_reserved {
                self.cluster.release_stream(&pending.plan);
                self.wake_scheduler();
            }
            if let Some(tx) = pending.tx.take() {
                let _ = tx.send(Err(error));
            }
        }
    }

    /// Record PlanAccept for a mesh-coordinated request; completes wait when all expected accept.
    ///
    /// Policy is pure [`joule_cluster::on_accept`] (membership → plan_id → verify → record).
    /// Device ed25519 signature is verified first for expected shards (fail closed).
    #[allow(clippy::too_many_arguments)]
    pub fn settle_plan_accept(
        &mut self,
        request_id: Uuid,
        from: &NodeId,
        plan_id: Uuid,
        accepted: bool,
        plan_hash_hex: &str,
        confirm_hex: &str,
        signer_pubkey_hex: &str,
        sig_hex: &str,
        signed_at_unix_ms: u64,
    ) {
        // Expected shards: device sig must verify and match Hello-bound pubkey.
        if self
            .pending_plan_accepts
            .get(&request_id)
            .is_some_and(|p| p.expected.contains(from))
        {
            let want_hash = self
                .pending_plan_accepts
                .get(&request_id)
                .map(|p| p.plan_hash_hex.clone())
                .unwrap_or_default();
            let reg = self.device_pubkey(from).map(|s| s.to_string());
            let sig_err = if signer_pubkey_hex.is_empty() || sig_hex.is_empty() {
                Some("missing plan accept signature".to_string())
            } else if let Some(reg_pk) = reg {
                if reg_pk != signer_pubkey_hex.trim().to_ascii_lowercase() {
                    Some("plan accept pubkey not bound to this node (Hello)".into())
                } else {
                    joule_cluster::verify_plan_accept_sig(
                        from,
                        plan_id,
                        request_id,
                        accepted,
                        plan_hash_hex,
                        confirm_hex,
                        signer_pubkey_hex,
                        sig_hex,
                        signed_at_unix_ms,
                    )
                    .err()
                }
            } else {
                Some("no device pubkey registered for node (Hello required)".into())
            };
            if let Some(e) = sig_err {
                if let Some(mut p) = self.pending_plan_accepts.remove(&request_id) {
                    if let Some(tx) = p.tx.take() {
                        let _ = tx.send(Err(format!("plan accept sig from {from}: {e}")));
                    }
                }
                self.leases.record_accepts(
                    request_id,
                    &[],
                    "plan_accept_sig_invalid",
                    &format!("{from}: {e}"),
                    Some(&want_hash),
                );
                return;
            }
        }
        let effect = {
            let pending = self.pending_plan_accepts.get(&request_id);
            let view = pending.map(|p| joule_cluster::PlanAgreeView {
                plan_id: p.plan_id,
                want_hash: p.plan_hash_hex.as_str(),
                expected: &p.expected,
                already_accepted: &p.accepted,
            });
            joule_cluster::on_accept(
                view.as_ref(),
                from,
                plan_id,
                request_id,
                accepted,
                plan_hash_hex,
                confirm_hex,
            )
        };
        match effect {
            joule_cluster::PlanAcceptEffect::Ignore => {}
            joule_cluster::PlanAcceptEffect::Abort { event, detail } => {
                let want_hash = self
                    .pending_plan_accepts
                    .get(&request_id)
                    .map(|p| p.plan_hash_hex.clone())
                    .unwrap_or_default();
                if let Some(mut p) = self.pending_plan_accepts.remove(&request_id) {
                    if let Some(tx) = p.tx.take() {
                        let _ = tx.send(Err(detail.clone()));
                    }
                }
                self.leases
                    .record_accepts(request_id, &[], event, &detail, Some(&want_hash));
            }
            joule_cluster::PlanAcceptEffect::Record { ready } => {
                let Some(p) = self.pending_plan_accepts.get_mut(&request_id) else {
                    return;
                };
                p.accepted.insert(from.clone());
                if ready {
                    if let Some(mut p) = self.pending_plan_accepts.remove(&request_id) {
                        let accepts: Vec<NodeId> = p.expected.iter().cloned().collect();
                        let agreed_hash = p.plan_hash_hex.clone();
                        self.leases.record_accepts(
                            request_id,
                            &accepts,
                            "plan_agreed",
                            "all shards confirmed",
                            Some(&agreed_hash),
                        );
                        if let Some(tx) = p.tx.take() {
                            let _ = tx.send(Ok(()));
                        }
                    }
                }
            }
        }
    }

    pub fn should_dual_verify(&mut self) -> bool {
        if self.dual_verify_every == 0 {
            return false;
        }
        self.chat_count = self.chat_count.wrapping_add(1);
        self.chat_count % self.dual_verify_every == 0
    }

    pub fn settle_challenge_result(
        &mut self,
        challenge_id: Uuid,
        completion: String,
        from: &NodeId,
    ) -> Option<bool> {
        let pending = self.pending_challenges.remove(&challenge_id)?;
        if pending.node != *from {
            // Wrong node answered.
            self.cluster.record_challenge_fail(&pending.node);
            if let Some(account) = self.node_account.get(&pending.node).cloned() {
                self.sync_best_mem_for_account(&account);
            }
            self.mark_dirty();
            return Some(false);
        }
        // Exact match on capacity proof for pending.credit_mib (proven work only).
        let ok = completion.trim() == pending.expected.trim();
        let proven = pending.credit_mib;
        if ok {
            // Unlock ≤ proven working-set MiB from this challenge (never free farm).
            self.cluster.record_challenge_ok(from, proven);
            if let Some(account) = self.node_account.get(from).cloned() {
                let verified = self.cluster.verified_mem_mib(from);
                self.note_online(&account, verified, true);
                self.sync_best_mem_for_account(&account);
                let fair = self.fairness_for(&account);
                let breakdown = score_mint(EconomyEvent::ChallengeOk, fair);
                let reason = breakdown.reason_tag(&format!(
                    "challenge:{challenge_id}|vmem={verified}|proven={proven}"
                ));
                let _ = self.ledger.mint_contribution_verified(
                    &account,
                    breakdown.total_mj,
                    reason,
                    Some(verified),
                );
                self.record_contribute(&account, breakdown.total_mj);
                self.seal_and_checkpoint();
                self.mark_dirty();
            }
        } else {
            self.cluster.record_challenge_fail(from);
            if let Some(account) = self.node_account.get(from).cloned() {
                self.sync_best_mem_for_account(&account);
            }
            self.mark_dirty();
            warn!(%from, "challenge failed (exact match required)");
        }
        Some(ok)
    }
}

#[cfg(test)]
mod challenge_integrity_tests {
    use super::*;
    use joule_ledger::{mem_factor_bp, score_mint, EconomyEvent};
    use joule_proto::{DeviceClass, NodeCaps};
    use joule_runtime::StubEngine;
    use std::time::Duration;

    fn register_claimed(state: &mut ControlState, claim: u32) -> (NodeId, String) {
        let id = NodeId::new();
        let account = format!("acct-{}", &id.to_string()[..8]);
        state.register_node(
            id.clone(),
            &account,
            NodeCaps::for_cluster(DeviceClass::Gpu, claim, 40),
        );
        (id, account)
    }

    /// Production seal_and_checkpoint attaches cryptographic notary quorum.
    #[test]
    fn seal_and_checkpoint_hotpath_writes_notary_attestations() {
        let mut state = ControlState::new();
        for _ in 0..3 {
            let _ = register_claimed(&mut state, 8192);
        }
        // Fill ledger to CHECKPOINT_EVERY boundary (32).
        for i in 0..32u32 {
            state
                .ledger
                .mint_contribution_verified("alice", 1, format!("fill-{i}"), Some(256))
                .unwrap();
        }
        assert_eq!(state.ledger.head().entries % 32, 0);
        state.seal_and_checkpoint();
        let cp = state
            .ledger
            .last_signed_checkpoint()
            .expect("signed checkpoint after seal");
        assert!(!cp.notary_attestations.is_empty());
        assert!(!cp.notaries.is_empty());
        // Public audit: each attestation verifies against pre-checkpoint head in reason
        assert!(cp.reason.contains("checkpoint|head="));
    }

    fn insert_capacity_challenge(
        state: &mut ControlState,
        id: &NodeId,
        seed: [u8; 32],
        credit_mib: u32,
        started: Instant,
    ) -> (Uuid, String) {
        let challenge_id = Uuid::new_v4();
        let expected = joule_cluster::capacity_proof_hex(&seed, credit_mib);
        state.pending_challenges.insert(
            challenge_id,
            PendingChallenge {
                node: id.clone(),
                model: CLUSTER_MODEL.into(),
                prompt: format!("joule-challenge:{challenge_id}"),
                expected: expected.clone(),
                capacity_seed_hex: hex::encode(seed),
                credit_mib,
                started,
            },
        );
        (challenge_id, expected)
    }

    #[test]
    fn pool_issues_joule_api_key_and_wrong_key_fails_closed() {
        let mut state = ControlState::new();
        let key = state.ensure_account("connect-alice");
        assert!(
            key.starts_with("joule_"),
            "pool-issued keys must use joule_ prefix, got {key}"
        );
        assert!(key.len() > 10, "key must be non-trivial");
        assert_eq!(state.account_for_key(&key), Some("connect-alice"));
        // Stable: second ensure returns same key for account.
        assert_eq!(state.ensure_account("connect-alice"), key);
        // Fail closed: invented / wrong / empty never map to an account.
        assert_eq!(state.account_for_key("joule_deadbeefnotreal"), None);
        assert_eq!(state.account_for_key(""), None);
        assert_eq!(state.account_for_key("sk-openai-style-fake"), None);
        // register_node also issues via ensure_account path.
        let id = NodeId::new();
        let reg = state.register_node(
            id,
            "connect-bob",
            NodeCaps::for_cluster(DeviceClass::Gpu, 8192, 40),
        );
        assert!(reg.starts_with("joule_"));
        assert_eq!(state.account_for_key(&reg), Some("connect-bob"));
        assert_ne!(reg, key);
    }

    #[test]
    fn wrong_completion_fails_even_if_model_loaded() {
        let mut state = ControlState::new();
        let (id, account) = register_claimed(&mut state, 24_576);
        state.cluster.set_verified_mem_mib(&id, 4096);
        assert_eq!(state.cluster.verified_mem_mib(&id), 4096);
        state.nodes_model_loaded.insert(id.clone());
        state.sync_best_mem_for_account(&account);

        let seed = [3u8; 32];
        // Lab credit=1 MiB (true 1:1 work); do not allocate 1 GiB in unit tests.
        let credit = 1u32;
        let (challenge_id, expected) =
            insert_capacity_challenge(&mut state, &id, seed, credit, Instant::now());
        // Wrong answer + model_loaded must still fail
        let ok = state
            .settle_challenge_result(challenge_id, "I am a 5070 farm".into(), &id)
            .unwrap();
        assert!(!ok);
        let v = state.cluster.verified_mem_mib(&id);
        assert!(v < 4096, "fail must reduce verified, got {v}");
        state.sync_best_mem_for_account(&account);
        assert_eq!(
            state.account_economy.get(&account).unwrap().best_mem_mib,
            v,
            "best_mem must track reduced verified"
        );
        let fair = state.fairness_for(&account);
        assert_eq!(fair.mem_mib, joule_cluster::economic_mem_mib(v));
        let mint = score_mint(EconomyEvent::Heartbeat, fair);
        let mint_full = score_mint(
            EconomyEvent::Heartbeat,
            FairnessSnapshot {
                mem_mib: 24_576,
                continuous_online_secs: fair.continuous_online_secs,
                contributed_mj_window: fair.contributed_mj_window,
                consumed_mj_window: fair.consumed_mj_window,
                disconnects_window: fair.disconnects_window,
            },
        );
        assert!(
            mint.total_mj < mint_full.total_mj,
            "mint after fail must be less than full-claim mint"
        );
        let _ = expected;
    }

    #[test]
    fn expired_challenge_records_fail_and_reduces_verified() {
        let mut state = ControlState::new();
        let (id, account) = register_claimed(&mut state, 16_384);
        state.cluster.set_verified_mem_mib(&id, 4096);
        assert_eq!(state.cluster.verified_mem_mib(&id), 4096);
        state.sync_best_mem_for_account(&account);

        let (challenge_id, _) = insert_capacity_challenge(
            &mut state,
            &id,
            [9u8; 32],
            1, // lab-scale credit (1 MiB work)
            Instant::now() - Duration::from_secs(120),
        );
        let before = state.cluster.verified_mem_mib(&id);
        state.prune();
        assert!(
            !state.pending_challenges.contains_key(&challenge_id),
            "expired challenge removed"
        );
        let after = state.cluster.verified_mem_mib(&id);
        assert!(
            after < before,
            "expiry fail must decay verified {before}->{after}"
        );
        assert_eq!(
            state.account_economy.get(&account).unwrap().best_mem_mib,
            after
        );
    }

    #[test]
    fn claim_only_join_mint_uses_floor_not_claim() {
        let mut state = ControlState::new();
        let (id, account) = register_claimed(&mut state, 65_536);
        assert_eq!(state.cluster.verified_mem_mib(&id), 0);
        let fair = state.fairness_for(&account);
        assert_eq!(
            fair.mem_mib, 256,
            "unverified join must floor mem for economy"
        );
        assert!(mem_factor_bp(fair.mem_mib) < mem_factor_bp(65_536));
        let mint = score_mint(EconomyEvent::Heartbeat, fair);
        let mint_fake = score_mint(
            EconomyEvent::Heartbeat,
            FairnessSnapshot {
                mem_mib: 65_536,
                ..Default::default()
            },
        );
        assert!(mint.total_mj < mint_fake.total_mj);
    }

    #[test]
    fn capacity_proof_challenge_raises_verified_by_proven_credit_only() {
        let mut state = ControlState::new();
        let (id, account) = register_claimed(&mut state, 8192);
        let seed = [0xABu8; 32];
        // 2 MiB proven work → +2 verified (1:1), not free CHALLENGE_CREDIT or claim.
        let credit = 2u32;
        assert_eq!(joule_cluster::capacity_work_bytes(credit), 2 * 1024 * 1024);
        let (challenge_id, expected) =
            insert_capacity_challenge(&mut state, &id, seed, credit, Instant::now());
        let ok = state
            .settle_challenge_result(challenge_id, expected, &id)
            .unwrap();
        assert!(ok);
        assert_eq!(state.cluster.verified_mem_mib(&id), credit);
        assert!(state.cluster.verified_mem_mib(&id) < 8192);
        assert_eq!(
            state.account_economy.get(&account).unwrap().best_mem_mib,
            credit
        );
    }

    /// CRITICAL: public StubEngine format string must NOT raise verified.
    #[test]
    fn public_stub_formula_does_not_unlock_verified() {
        let mut state = ControlState::new();
        let claim = 24_576u32;
        let (id, _account) = register_claimed(&mut state, claim);
        let seed = [0x11u8; 32];
        let credit = 1u32;
        let (challenge_id, real_expected) =
            insert_capacity_challenge(&mut state, &id, seed, credit, Instant::now());
        let forge =
            StubEngine::expected_text(CLUSTER_MODEL, &format!("joule-challenge:{challenge_id}"));
        assert_ne!(
            forge, real_expected,
            "stub format must not equal capacity proof"
        );
        let ok = state
            .settle_challenge_result(challenge_id, forge, &id)
            .unwrap();
        assert!(!ok, "public stub formula must fail capacity challenge");
        assert_eq!(
            state.cluster.verified_mem_mib(&id),
            0,
            "forge must leave verified at 0 (or decay from 0)"
        );
    }

    /// Smaller proof (1 MiB) must not satisfy a 2 MiB pending challenge.
    #[test]
    fn undersized_work_proof_does_not_unlock() {
        let mut state = ControlState::new();
        let (id, _) = register_claimed(&mut state, 24_576);
        let seed = [0x22u8; 32];
        let want_credit = 2u32;
        let (challenge_id, _expected) =
            insert_capacity_challenge(&mut state, &id, seed, want_credit, Instant::now());
        // Attacker solves only 1 MiB of work and submits that proof.
        let undersized = joule_cluster::capacity_proof_hex(&seed, 1);
        let ok = state
            .settle_challenge_result(challenge_id, undersized, &id)
            .unwrap();
        assert!(!ok, "1 MiB proof must not unlock 2 MiB credit");
        assert_eq!(state.cluster.verified_mem_mib(&id), 0);
    }

    /// Formula-only / public-stub-echo cannot unlock arbitrary claim.
    #[test]
    fn capacity_matrix_claim_verified_states() {
        let claim = 24_576u32;
        let mut state = ControlState::new();
        let (id, account) = register_claimed(&mut state, claim);

        assert_eq!(state.cluster.verified_mem_mib(&id), 0);
        let fair0 = state.fairness_for(&account);
        assert_eq!(fair0.mem_mib, joule_cluster::economic_mem_mib(0));
        assert!(
            state.cluster.plan_full_pool().is_err(),
            "claim-only must not form placement plan"
        );
        state.mesh.upsert(
            id.clone(),
            vec!["tcp://10.0.0.1:1".into()],
            0.1,
            true,
            0,
            claim,
            0,
            40,
        );
        assert!(state.mesh.plan_donors().is_empty());
        assert!(state.mesh_plan_donors().is_empty());

        state.cluster.set_verified_mem_mib(&id, 4096);
        state.mesh.upsert(
            id.clone(),
            vec!["tcp://10.0.0.1:1".into()],
            0.1,
            true,
            0,
            claim,
            4096,
            40,
        );
        let fair_mid = state.fairness_for(&account);
        assert_eq!(fair_mid.mem_mib, 4096);
        let plan_mid = state.cluster.plan_full_pool().unwrap();
        assert_eq!(plan_mid.pool_mem_mib, 4096);
        let donors_mid = state.mesh.plan_donors();
        assert_eq!(donors_mid.len(), 1);
        assert_eq!(donors_mid[0].1, 4096);

        state.cluster.on_challenge_result(&id, false, 0);
        let half = state.cluster.verified_mem_mib(&id);
        assert_eq!(half, 2048);
        state.mesh.upsert(
            id.clone(),
            vec!["tcp://10.0.0.1:1".into()],
            0.1,
            true,
            0,
            claim,
            half,
            40,
        );
        assert_eq!(state.mesh_plan_donors()[0].1, half);

        let m_half = score_mint(EconomyEvent::Heartbeat, state.fairness_for(&account));
        let m_claim = score_mint(
            EconomyEvent::Heartbeat,
            FairnessSnapshot {
                mem_mib: claim,
                ..Default::default()
            },
        );
        assert!(m_half.total_mj < m_claim.total_mj);
    }

    /// CRITICAL: N serial settle of credit-C cannot raise verified above peak C.
    /// Uses real settle_challenge_result + capacity proofs (not accounting-only).
    #[test]
    fn serial_settles_cannot_sum_past_peak_work_farm() {
        let mut state = ControlState::new();
        let claim = 65_536u32;
        let (id, account) = register_claimed(&mut state, claim);
        let peak_c = 2u32; // 2 MiB working set per challenge (lab scale)
        assert_eq!(
            joule_cluster::capacity_work_bytes(peak_c),
            peak_c as usize * 1024 * 1024
        );
        // Many serial honest settles of the same peak C.
        for i in 0..32u8 {
            let mut seed = [0xCCu8; 32];
            seed[0] = i;
            let (cid, expected) =
                insert_capacity_challenge(&mut state, &id, seed, peak_c, Instant::now());
            let ok = state
                .settle_challenge_result(cid, expected, &id)
                .expect("settle");
            assert!(ok, "serial settle {i} must accept valid peak proof");
            assert_eq!(
                state.cluster.verified_mem_mib(&id),
                peak_c,
                "after settle {i}: verified must stay peak={peak_c}, not sum"
            );
        }
        assert!(
            state.cluster.verified_mem_mib(&id) < claim,
            "peak {peak_c} must not unlock farm claim {claim}"
        );
        state.sync_best_mem_for_account(&account);
        assert_eq!(
            state.account_economy.get(&account).unwrap().best_mem_mib,
            peak_c
        );
        // Mint factor tracks peak verified, not claim.
        let fair = state.fairness_for(&account);
        assert_eq!(fair.mem_mib, joule_cluster::economic_mem_mib(peak_c));
        let mint_peak = score_mint(EconomyEvent::Heartbeat, fair);
        let mint_farm = score_mint(
            EconomyEvent::Heartbeat,
            FairnessSnapshot {
                mem_mib: claim,
                ..Default::default()
            },
        );
        assert!(mint_peak.total_mj < mint_farm.total_mj);
        // Raising peak once (larger single proof) updates verified to new peak only.
        let bigger = 4u32;
        let (cid, expected) =
            insert_capacity_challenge(&mut state, &id, [0xDDu8; 32], bigger, Instant::now());
        assert!(state.settle_challenge_result(cid, expected, &id).unwrap());
        assert_eq!(state.cluster.verified_mem_mib(&id), bigger);
        // Mesh placement uses cluster peak, not claim.
        state
            .mesh
            .upsert(id.clone(), vec![], 0.0, true, 0, claim, 0, 0);
        let donors = state.mesh_plan_donors();
        assert_eq!(donors.len(), 1);
        assert_eq!(donors[0].1, joule_cluster::economic_mem_mib(bigger));
    }

    /// Agent returns capacity proof; matches control oracle; stub text is not used.
    #[tokio::test]
    async fn agent_challenge_returns_capacity_proof_not_stub_formula() {
        use crate::agent_handle_challenge;
        use joule_proto::{Envelope, Message};
        let engine = StubEngine::new();
        let id = NodeId::new();
        let seed = [0x5Eu8; 32];
        // 1 MiB lab credit — true 1:1 work without 1 GiB unit-test alloc.
        let credit = 1u32;
        assert_eq!(joule_cluster::capacity_work_bytes(credit), 1024 * 1024);
        let seed_hex = hex::encode(seed);
        let want = joule_cluster::capacity_proof_hex(&seed, credit);
        let prompt = "joule-challenge:unique-nonce-9f3a";
        let env = Envelope::new(
            id.clone(),
            Message::Challenge {
                challenge_id: Uuid::new_v4(),
                model: CLUSTER_MODEL.into(),
                prompt: prompt.into(),
                capacity_seed_hex: seed_hex,
                credit_mib: credit,
            },
        );
        let reply = agent_handle_challenge(&env, &engine)
            .await
            .expect("challenge");
        match reply.msg {
            Message::ChallengeResult { completion, .. } => {
                assert_eq!(completion, want, "must be capacity proof hex");
                assert_ne!(
                    completion,
                    StubEngine::expected_text(CLUSTER_MODEL, prompt),
                    "must not return public stub formula"
                );
                assert!(joule_cluster::capacity_verify(&seed, credit, &completion));
            }
            other => panic!("expected ChallengeResult, got {other:?}"),
        }
    }
}
