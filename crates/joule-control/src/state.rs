//! Shared control-plane state: cluster registry, ledger, accounts, pending jobs.

use crate::persist;
use joule_cluster::Cluster;
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

#[derive(Debug)]
pub struct PendingInfer {
    pub account: String,
    /// Full VRAM-sharded plan; stream reserved on every shard.
    pub plan: ClusterPlan,
    /// Shards still expected to ACK (node ids).
    pub awaiting: std::collections::HashSet<NodeId>,
    pub tail_text: Option<String>,
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub charge: bool,
    pub tx: Option<oneshot::Sender<Result<InferOutcome, String>>>,
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
}

#[derive(Debug)]
pub struct PendingChallenge {
    pub node: NodeId,
    pub model: String,
    pub prompt: String,
    pub expected: String,
    pub started: Instant,
}

#[derive(Debug)]
pub struct ControlState {
    pub cluster: Cluster,
    pub ledger: Ledger,
    pub keys: HashMap<String, String>,
    pub account_keys: HashMap<String, String>,
    pub node_account: HashMap<NodeId, String>,
    pub pending: HashMap<Uuid, PendingInfer>,
    pub pending_challenges: HashMap<Uuid, PendingChallenge>,
    pub heartbeat_mint_mj: Millijoule,
    /// Every Nth chat request also runs a second-worker verify (0 = off).
    pub dual_verify_every: u64,
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
    /// Per-account rolling fairness + tenure (economy v0).
    pub account_economy: HashMap<String, AccountEconomy>,
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
            node_account: HashMap::new(),
            pending: HashMap::new(),
            pending_challenges: HashMap::new(),
            heartbeat_mint_mj: 10,
            dual_verify_every: 3,
            chat_count: 0,
            data_dir: None,
            schedule_notify: None,
            vram_history: VecDeque::new(),
            nodes_model_loaded: HashSet::new(),
            service_live: false,
            account_economy: HashMap::new(),
            dirty: false,
        }
    }

    fn economy_mut(&mut self, account: &str) -> &mut AccountEconomy {
        self.account_economy.entry(account.to_string()).or_default()
    }

    /// Build fairness snapshot for scoring (refreshes continuous tenure clock).
    pub fn fairness_for(&mut self, account: &str) -> FairnessSnapshot {
        let eco = self.economy_mut(account);
        let continuous = match eco.online_since {
            Some(since) => eco
                .continuous_online_secs
                .saturating_add(since.elapsed().as_secs()),
            None => eco.continuous_online_secs,
        };
        FairnessSnapshot {
            mem_mib: eco.best_mem_mib.max(256),
            continuous_online_secs: continuous,
            contributed_mj_window: eco.contributed_mj_window,
            consumed_mj_window: eco.consumed_mj_window,
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

    fn note_online(&mut self, account: &str, verified_mem_mib: u32, healthy: bool) {
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
        }
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
            service_live: self.service_live,
        }
    }

    pub fn mark_node_loaded(&mut self, id: NodeId) {
        self.nodes_model_loaded.insert(id);
        // Auto service-live when enough of the mesh has loaded and pool gate ok.
        let backends = self.cluster.pool_size() as u32;
        let vram = self.cluster.capacity().mem_mib_healthy;
        if let Ok(r) = joule_runtime::readiness_for_pool_ex(
            vram,
            backends,
            self.runtime_flags(),
            self.vram_growth_mib_per_sec(),
        ) {
            if r.can_begin_service && self.nodes_model_loaded.len() >= r.required_backends as usize
            {
                self.service_live = true;
                info!("service marked live — model loaded on mesh");
            }
        }
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

    fn mark_dirty(&mut self) {
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
        // Expire old challenges.
        let now = Instant::now();
        self.pending_challenges
            .retain(|_, c| now.duration_since(c.started) < Duration::from_secs(60));
        self.save_if_dirty();
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
        self.node_account.insert(id, account.to_string());
        self.note_online(account, verified, true);
        self.mark_dirty();
        api_key
    }

    fn seal_and_checkpoint(&mut self) {
        let notaries = self.cluster.pick_notaries(3);
        let _ = self.ledger.maybe_checkpoint(notaries);
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
        // Refresh best verified across all this account's nodes.
        let best_v = self
            .cluster
            .nodes()
            .filter(|n| n.account == account)
            .map(|n| n.verified_mem_mib)
            .max()
            .unwrap_or(verified);
        if let Some(eco) = self.account_economy.get_mut(&account) {
            eco.best_mem_mib = best_v;
        }
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
        self.cluster.release_stream(&pending.plan);
        self.wake_scheduler();

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
            self.cluster.release_stream(&pending.plan);
            self.wake_scheduler();
            if let Some(tx) = pending.tx.take() {
                let _ = tx.send(Err(error));
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
            return Some(false);
        }
        let ok = completion.trim() == pending.expected.trim();
        if ok {
            self.cluster.record_challenge_ok(from);
            if let Some(account) = self.node_account.get(from).cloned() {
                let verified = self.cluster.verified_mem_mib(from);
                self.note_online(&account, verified, true);
                if let Some(eco) = self.account_economy.get_mut(&account) {
                    eco.best_mem_mib = eco.best_mem_mib.max(verified);
                }
                let fair = self.fairness_for(&account);
                let breakdown = score_mint(EconomyEvent::ChallengeOk, fair);
                let reason =
                    breakdown.reason_tag(&format!("challenge:{challenge_id}|vmem={verified}"));
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
            warn!(%from, "challenge failed");
        }
        Some(ok)
    }
}
