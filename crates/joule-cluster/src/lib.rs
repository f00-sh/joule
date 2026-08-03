//! Distributed compute cluster membership, live capacity, and placement.
//!
//! Internet-wide volunteer pool. Nodes join the control plane over any path.
//! Scheduling of free/loaded compute lives in [`scheduler`].

mod capacity_challenge;
mod chunks;
mod erasure;
mod lease;
mod plan_agree;
mod plan_sign;
mod scheduler;

pub use capacity_challenge::{
    clamp_credit_mib, max_work_bytes, parse_seed_hex, proof_hex as capacity_proof_hex,
    solve as capacity_solve, verify as capacity_verify, work_bytes as capacity_work_bytes,
    BYTES_PER_CREDIT_MIB,
};
pub use chunks::{
    live_replica_counts, plan_redundant_chunks, plan_survives, required_digests_for_node,
    ChunkHold, ChunkRole, ModelChunk, NodeChunkPlan, RedundantChunkPlan, DEFAULT_REPLICA_FACTOR,
};
pub use erasure::{
    content_sha256, encode as erasure_encode, place_erasure_shards, placement_survives,
    reconstruct as erasure_reconstruct, DurablePlacement, ErasureSet,
};
pub use lease::{
    lease_receipt_hex, plan_accept_confirm_hex, plan_accept_fields, plan_hash_hex,
    verify_plan_accept_confirm, LeaseAuditEntry, LeaseBook, StreamLease, DOMAIN_ACCEPT,
    DOMAIN_LEASE, DOMAIN_PLAN,
};
pub use plan_agree::{on_accept, PlanAcceptEffect, PlanAgreeView};
pub use plan_sign::{
    lab_signing_key_for_node, plan_accept_sign_preimage, plan_offer_sign_preimage, sign_preimage,
    verify_plan_accept_sig, verify_plan_offer_sig, verify_sig, DOMAIN_PLAN_ACCEPT_SIG,
    DOMAIN_PLAN_OFFER_SIG,
};
pub use scheduler::{
    compute_state, free_slots, free_stream_slots, max_slots, max_streams, pool_max_streams,
    ComputeState, NodeSchedule, SchedulerSnapshot, DEFAULT_MODEL_LAYERS, STREAM_BUDGET_MIB,
};

/// Floor used only when a path must assign a nonzero crumb of capacity after
/// verification has begun. Unverified (0) stays 0 for eligibility gates.
pub const VERIFIED_MEM_FLOOR_MIB: u32 = 256;

/// MiB credited per successful lab challenge (not a free ramp to full claim).
pub const CHALLENGE_CREDIT_MIB: u32 = 1024;

/// **Economic** capacity from verified only. Claim is never a parameter.
/// Used for mint factors, fairness, donate eligibility.
pub fn economic_mem_mib(verified_mem_mib: u32) -> u32 {
    if verified_mem_mib == 0 {
        // Unverified: floor crumb so brand-new honest nodes still mint ≥1 mJ
        // once they join, without treating claim as real. Protocol still
        // refuses claim-sized factors until challenges raise verified.
        VERIFIED_MEM_FLOOR_MIB
    } else {
        verified_mem_mib.max(VERIFIED_MEM_FLOOR_MIB)
    }
}

/// **Placement** weight from verified only. Claim is never a parameter.
/// Returns 0 when unverified so claim-only peers are excluded from weighted plans.
pub fn placement_mem_mib(verified_mem_mib: u32) -> u32 {
    verified_mem_mib
}

/// Capacity attestation tier (trust surface for untrusted donors).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttestationTier {
    /// Claim only — verified is 0; mint uses floor only.
    ClaimOnly,
    /// Partial challenge unlock (0 < verified < claim).
    ChallengePartial,
    /// Peak proven equals claim (fully challenge-backed for current claim).
    ChallengeFull,
}

impl AttestationTier {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ClaimOnly => "claim_only",
            Self::ChallengePartial => "challenge_partial",
            Self::ChallengeFull => "challenge_full",
        }
    }
}

/// Derive attestation tier from claim vs verified (no free full-claim without proof).
pub fn attestation_tier(claimed_mem_mib: u32, verified_mem_mib: u32) -> AttestationTier {
    if verified_mem_mib == 0 {
        AttestationTier::ClaimOnly
    } else if claimed_mem_mib > 0 && verified_mem_mib >= claimed_mem_mib {
        AttestationTier::ChallengeFull
    } else {
        AttestationTier::ChallengePartial
    }
}

use joule_proto::{
    ClusterCapacity, ClusterPlan, DeviceClass, LogicalDevice, NodeCaps, NodeId, CLUSTER_MODEL,
    CLUSTER_MODEL_LABEL,
};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClusterError {
    #[error("no eligible nodes for model {0}")]
    NoEligibleNodes(String),
    #[error("unknown node {0}")]
    UnknownNode(NodeId),
}

#[derive(Debug, Clone, Default)]
pub struct Reputation {
    pub ok: u64,
    pub fail: u64,
    pub banned_until: Option<Instant>,
}

impl Reputation {
    pub fn score(&self) -> f64 {
        let total = (self.ok + self.fail).max(1) as f64;
        self.ok as f64 / total
    }

    pub fn is_banned(&self, now: Instant) -> bool {
        self.banned_until.is_some_and(|t| now < t)
    }

    pub fn record_ok(&mut self) {
        self.ok = self.ok.saturating_add(1);
    }

    pub fn record_fail(&mut self, ban_after_fails: u64, ban_for: Duration) {
        self.fail = self.fail.saturating_add(1);
        // Ban if more fails than oks and at least ban_after_fails failures.
        if self.fail >= ban_after_fails && self.fail > self.ok {
            self.banned_until = Some(Instant::now() + ban_for);
        }
    }
}

#[derive(Debug, Clone)]
pub struct Node {
    pub id: NodeId,
    pub account: String,
    pub caps: NodeCaps,
    pub healthy: bool,
    pub load: f32,
    pub last_seen: Instant,
    /// In-flight inference jobs assigned by control (for load balancing).
    pub inflight: u32,
    pub reputation: Reputation,
    /// Round-robin salt updated when selected.
    pub rr_seq: u64,
    /// Advertised VRAM (untrusted claim).
    pub claimed_mem_mib: u32,
    /// Protocol-trusted VRAM after challenges (self-govern). Starts 0.
    pub verified_mem_mib: u32,
    /// Consecutive challenge passes (audit only — never free full-claim unlock).
    pub challenge_streak: u32,
}

/// In-memory cluster registry fed by agent hellos/heartbeats.
#[derive(Debug)]
pub struct Cluster {
    nodes: HashMap<NodeId, Node>,
    stale_after: Duration,
    rr_counter: u64,
}

impl Default for Cluster {
    fn default() -> Self {
        Self::new(Duration::from_secs(30))
    }
}

impl Cluster {
    pub fn new(stale_after: Duration) -> Self {
        Self {
            nodes: HashMap::new(),
            stale_after,
            rr_counter: 0,
        }
    }

    pub fn upsert_node(&mut self, id: NodeId, account: impl Into<String>, caps: NodeCaps) {
        let account = account.into();
        let claim = caps.mem_mib;
        if let Some(existing) = self.nodes.get_mut(&id) {
            existing.account = account;
            // Claim can rise; verified never exceeds claim and is never set from claim alone.
            if claim < existing.verified_mem_mib {
                existing.verified_mem_mib = claim;
            }
            existing.claimed_mem_mib = claim;
            existing.caps = caps;
            existing.healthy = true;
            existing.last_seen = Instant::now();
            return;
        }
        self.nodes.insert(
            id.clone(),
            Node {
                id,
                account,
                claimed_mem_mib: claim,
                verified_mem_mib: 0,
                challenge_streak: 0,
                caps,
                healthy: true,
                load: 0.0,
                last_seen: Instant::now(),
                inflight: 0,
                reputation: Reputation::default(),
                rr_seq: 0,
            },
        );
    }

    /// Effective memory for economics / readiness: verified only (anti fake VRAM).
    pub fn verified_mem_mib(&self, id: &NodeId) -> u32 {
        self.nodes.get(id).map(|n| n.verified_mem_mib).unwrap_or(0)
    }

    /// Record challenge outcome → adjust verified capacity (self-govern).
    ///
    /// **Peak working-set model (anti serial farm):** success sets
    /// `verified = max(verified, proven)` where `proven` is the **single-challenge**
    /// working-set MiB (clamped to [`CHALLENGE_CREDIT_MIB`] and claim).
    ///
    /// N serial challenges each proving C MiB cannot sum to N×C — trusted capacity
    /// never exceeds the **largest single proof** (peak RAM demonstrated), not the
    /// sum of sequential deltas. Fail halves verified.
    pub fn on_challenge_result(&mut self, id: &NodeId, ok: bool, proven_credit_mib: u32) {
        let Some(n) = self.nodes.get_mut(id) else {
            return;
        };
        if ok {
            n.challenge_streak = n.challenge_streak.saturating_add(1);
            let proven = proven_credit_mib
                .min(CHALLENGE_CREDIT_MIB)
                .min(n.claimed_mem_mib);
            // Peak, not sum — serial 64×1 GiB cannot mint a 64 GiB farm.
            n.verified_mem_mib = n.verified_mem_mib.max(proven);
        } else {
            n.challenge_streak = 0;
            n.verified_mem_mib /= 2;
        }
    }

    /// Explicit set of verified capacity (ops / tests after real attestation).
    pub fn set_verified_mem_mib(&mut self, id: &NodeId, verified: u32) {
        if let Some(n) = self.nodes.get_mut(id) {
            n.verified_mem_mib = verified.min(n.claimed_mem_mib);
        }
    }

    /// Sum of verified healthy VRAM (model readiness must not trust claims).
    pub fn verified_pool_vram_mib(&self) -> u64 {
        let now = Instant::now();
        self.nodes
            .values()
            .filter(|n| n.healthy && !n.reputation.is_banned(now))
            .map(|n| u64::from(n.verified_mem_mib))
            .sum()
    }

    /// Test/ops helper: treat all current claims as fully verified (after real challenges in prod).
    pub fn trust_all_claims_for_tests(&mut self) {
        for n in self.nodes.values_mut() {
            n.verified_mem_mib = n.claimed_mem_mib;
            n.challenge_streak = 3;
        }
    }

    /// Pick up to `k` healthy node ids as notaries (deterministic-ish from rr).
    pub fn pick_notaries(&mut self, k: usize) -> Vec<String> {
        let now = Instant::now();
        let mut ids: Vec<NodeId> = self
            .nodes
            .values()
            .filter(|n| n.healthy && !n.reputation.is_banned(now))
            .map(|n| n.id.clone())
            .collect();
        ids.sort_by_key(|a| a.0);
        if ids.is_empty() {
            return vec![];
        }
        let start = (self.rr_counter as usize) % ids.len();
        self.rr_counter = self.rr_counter.wrapping_add(1);
        (0..k.min(ids.len()))
            .map(|i| ids[(start + i) % ids.len()].to_string())
            .collect()
    }

    pub fn set_health(
        &mut self,
        id: &NodeId,
        healthy: bool,
        load: f32,
    ) -> Result<(), ClusterError> {
        let node = self
            .nodes
            .get_mut(id)
            .ok_or_else(|| ClusterError::UnknownNode(id.clone()))?;
        node.healthy = healthy;
        node.load = load;
        node.last_seen = Instant::now();
        Ok(())
    }

    pub fn remove_node(&mut self, id: &NodeId) {
        self.nodes.remove(id);
    }

    pub fn apply_staleness(&mut self) {
        let now = Instant::now();
        let stale = self.stale_after;
        for n in self.nodes.values_mut() {
            if now.duration_since(n.last_seen) > stale {
                n.healthy = false;
            }
        }
    }

    pub fn reap_dead(&mut self, dead_after: Duration) {
        let now = Instant::now();
        self.nodes
            .retain(|_, n| now.duration_since(n.last_seen) <= dead_after);
    }

    pub fn nodes(&self) -> impl Iterator<Item = &Node> {
        self.nodes.values()
    }

    pub fn get(&self, id: &NodeId) -> Option<&Node> {
        self.nodes.get(id)
    }

    pub fn get_mut(&mut self, id: &NodeId) -> Option<&mut Node> {
        self.nodes.get_mut(id)
    }

    pub fn account_is_donating(&self, account: &str) -> bool {
        let now = Instant::now();
        self.nodes
            .values()
            .any(|n| n.account == account && n.healthy && !n.reputation.is_banned(now))
    }

    /// Healthy, non-banned donors (pool membership).
    pub(crate) fn eligible(&self) -> Vec<&Node> {
        let now = Instant::now();
        self.nodes
            .values()
            .filter(|p| p.healthy && !p.reputation.is_banned(now))
            .collect()
    }

    /// Rank schedulable donors (have free slots). Prefer free over loaded.
    pub fn rank_workers(&self, _model: &str) -> Vec<NodeId> {
        self.rank_schedulable()
    }

    /// How many healthy donors are in the pool (not necessarily free).
    pub fn pool_size(&self) -> usize {
        self.eligible().len()
    }

    /// Acquire one free/loaded slot (not full). Prefer free nodes.
    pub fn acquire_worker(&mut self, _model: &str) -> Option<NodeId> {
        self.try_acquire_slot()
    }

    /// Acquire up to `n` slots on distinct free/loaded nodes.
    pub fn acquire_workers(&mut self, _model: &str, n: usize) -> Vec<NodeId> {
        self.try_acquire_slots(n)
    }

    pub fn preferred_pipeline_stages(&self) -> usize {
        let n = self.pool_size();
        if n >= 4 {
            4
        } else if n >= 2 {
            n
        } else {
            1
        }
    }

    pub fn release_worker(&mut self, id: &NodeId) {
        if let Some(n) = self.nodes.get_mut(id) {
            n.inflight = n.inflight.saturating_sub(1);
        }
    }

    /// Random healthy non-banned node for spot challenges (any model).
    pub fn pick_challenge_target(&self) -> Option<&Node> {
        let now = Instant::now();
        let mut eligible: Vec<&Node> = self
            .nodes
            .values()
            .filter(|n| n.healthy && !n.reputation.is_banned(now))
            .collect();
        if eligible.is_empty() {
            return None;
        }
        // Prefer nodes with fewer recent challenges (use fail+ok as proxy) and lower inflight.
        eligible.sort_by(|a, b| {
            (a.inflight, a.reputation.ok + a.reputation.fail)
                .cmp(&(b.inflight, b.reputation.ok + b.reputation.fail))
        });
        Some(eligible[0])
    }

    pub fn record_challenge_ok(&mut self, id: &NodeId, proven_credit_mib: u32) {
        if let Some(n) = self.nodes.get_mut(id) {
            n.reputation.record_ok();
        }
        self.on_challenge_result(id, true, proven_credit_mib);
    }

    pub fn record_challenge_fail(&mut self, id: &NodeId) {
        if let Some(n) = self.nodes.get_mut(id) {
            n.reputation.record_fail(3, Duration::from_secs(120));
        }
        self.on_challenge_result(id, false, 0);
    }

    pub fn capacity(&self) -> ClusterCapacity {
        let mut nodes_total = 0u32;
        let mut nodes_healthy = 0u32;
        let mut nodes_gpu = 0u32;
        let mut nodes_metal = 0u32;
        let mut nodes_cpu = 0u32;
        let mut mem_mib_total = 0u64;
        let mut mem_mib_healthy = 0u64;
        let mut throughput_class_sum = 0u64;
        let now = Instant::now();

        for n in self.nodes.values() {
            nodes_total += 1;
            // Total shows claims; healthy for economics/readiness uses verified only.
            mem_mib_total += u64::from(n.claimed_mem_mib.max(n.caps.mem_mib));
            match n.caps.device {
                DeviceClass::Gpu => nodes_gpu += 1,
                DeviceClass::Metal => nodes_metal += 1,
                DeviceClass::Cpu => nodes_cpu += 1,
            }
            if n.healthy && !n.reputation.is_banned(now) {
                nodes_healthy += 1;
                mem_mib_healthy += u64::from(n.verified_mem_mib);
                throughput_class_sum += u64::from(n.caps.throughput_class);
            }
        }

        let models_available = if nodes_healthy > 0 {
            vec![CLUSTER_MODEL.to_string()]
        } else {
            vec![]
        };

        let stream_slots_total = scheduler::pool_max_streams(mem_mib_healthy);
        let stream_slots_used = self
            .eligible()
            .iter()
            .map(|n| n.inflight)
            .max()
            .unwrap_or(0)
            .min(stream_slots_total);

        // Public product view: one accelerator whose VRAM is the sum of donors.
        // model_ready filled by control once manifest gates are applied.
        let logical_device = Some(LogicalDevice {
            id: "joule-pool".into(),
            name: format!("joule supercomputer ({CLUSTER_MODEL_LABEL})"),
            kind: "aggregate_gpu".into(),
            vram_mib: mem_mib_healthy,
            vram_gib: mem_mib_healthy / 1024,
            backends: nodes_healthy,
            model: CLUSTER_MODEL.to_string(),
            ready: nodes_healthy > 0,
            model_ready: false,
            model_progress_pct: 0,
            inference_mode: String::new(),
            readiness_message: String::new(),
        });

        ClusterCapacity {
            nodes_total,
            nodes_healthy,
            nodes_gpu,
            nodes_metal,
            nodes_cpu,
            mem_mib_total,
            mem_mib_healthy,
            throughput_class_sum,
            models_available,
            stream_slots_total,
            stream_slots_used,
            logical_device,
        }
    }

    /// Placement for the single cluster model: always VRAM-shard across the healthy pool.
    /// `prefer_pipeline` / `pipeline_stages` are ignored (whole pool is the plan).
    pub fn plan_for(
        &self,
        _model: &str,
        _prefer_pipeline: bool,
        _pipeline_stages: usize,
    ) -> Result<ClusterPlan, ClusterError> {
        self.plan_sharded_pool()
    }

    /// Full-pool VRAM-sharded plan (alias).
    pub fn plan_full_pool(&self) -> Result<ClusterPlan, ClusterError> {
        self.plan_sharded_pool()
    }
}

/// Build a VRAM-sharded [`ClusterPlan`] from gossip membership (Phase D mesh coordinator).
///
/// `donors` is `(node, mem_mib)` for healthy peers; memory is treated as verified for plan
/// geometry (callers should only pass trusted/challenge-backed values when available).
pub fn plan_from_mesh_donors(donors: &[(NodeId, u32)]) -> Result<ClusterPlan, ClusterError> {
    use joule_proto::{ShardAssignment, ShardRole};
    use uuid::Uuid;

    if donors.is_empty() {
        return Err(ClusterError::NoEligibleNodes(CLUSTER_MODEL.to_string()));
    }
    // Drop zero-placement (unverified) entries; never floor them into the plan.
    let mut donors: Vec<(NodeId, u32)> = donors
        .iter()
        .filter_map(|(id, m)| {
            let place = placement_mem_mib(*m);
            if place == 0 {
                None
            } else {
                Some((id.clone(), place))
            }
        })
        .collect();
    if donors.is_empty() {
        return Err(ClusterError::NoEligibleNodes(CLUSTER_MODEL.to_string()));
    }
    donors.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0 .0.cmp(&b.0 .0)));

    let pool_mem: u64 = donors.iter().map(|(_, m)| u64::from(*m)).sum();
    if pool_mem == 0 {
        return Err(ClusterError::NoEligibleNodes(CLUSTER_MODEL.to_string()));
    }

    let layers = scheduler::DEFAULT_MODEL_LAYERS;
    let mut shards = Vec::with_capacity(donors.len());
    let mut layer_cursor = 0u32;
    let mut ppm_acc = 0u32;

    for (i, (id, eff)) in donors.iter().enumerate() {
        let mem = u64::from(*eff);
        let mut ppm = ((mem * 1_000_000) / pool_mem) as u32;
        let is_last = i + 1 == donors.len();
        if is_last {
            ppm = 1_000_000u32.saturating_sub(ppm_acc);
        } else {
            ppm_acc = ppm_acc.saturating_add(ppm);
        }
        let layer_start = layer_cursor.min(layers.saturating_sub(1));
        let layer_end = if is_last {
            layers.saturating_sub(1)
        } else {
            let span = ((u64::from(layers) * mem) / pool_mem).max(1) as u32;
            let end = layer_start.saturating_add(span).saturating_sub(1);
            end.min(layers.saturating_sub(1))
        };
        let layer_end = layer_end.max(layer_start);
        layer_cursor = layer_end.saturating_add(1).min(layers);
        shards.push(ShardAssignment {
            node: id.clone(),
            role: if donors.len() == 1 {
                ShardRole::Replica
            } else {
                ShardRole::Pipeline
            },
            layer_start: Some(layer_start),
            layer_end: Some(layer_end),
            tp_rank: None,
            tp_world: None,
            mem_share_mib: *eff,
            mem_fraction_ppm: ppm,
        });
    }

    Ok(ClusterPlan {
        plan_id: Uuid::new_v4(),
        model: CLUSTER_MODEL.to_string(),
        shards,
        pool_mem_mib: pool_mem,
        model_layers: layers,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use joule_proto::DeviceClass;

    fn node(mem: u32, device: DeviceClass) -> (NodeId, NodeCaps) {
        (NodeId::new(), NodeCaps::for_cluster(device, mem, 10))
    }

    #[test]
    fn plan_from_mesh_donors_shards_all() {
        let a = NodeId::new();
        let b = NodeId::new();
        let plan = plan_from_mesh_donors(&[(a.clone(), 8192), (b.clone(), 16384)]).unwrap();
        assert_eq!(plan.shards.len(), 2);
        assert_eq!(plan.pool_mem_mib, 8192 + 16384);
        assert!(plan.shards.iter().any(|s| s.node == a));
        assert!(plan.shards.iter().any(|s| s.node == b));
    }

    #[test]
    fn stream_uses_whole_pool_not_one_gpu() {
        let mut c = Cluster::default();
        let (a, ca) = node(8192, DeviceClass::Gpu);
        let (b, cb) = node(16384, DeviceClass::Gpu);
        c.upsert_node(a.clone(), "alice", ca);
        c.upsert_node(b.clone(), "bob", cb);
        c.trust_all_claims_for_tests();
        let plan = c.try_acquire_stream().unwrap();
        assert_eq!(plan.shards.len(), 2);
        assert_eq!(plan.pool_mem_mib, 8192 + 16384);
        // both nodes participate (inflight on each)
        assert_eq!(c.get(&a).unwrap().inflight, 1);
        assert_eq!(c.get(&b).unwrap().inflight, 1);
        c.release_stream(&plan);
        assert_eq!(c.get(&a).unwrap().inflight, 0);
    }

    #[test]
    fn banned_node_skipped() {
        let mut c = Cluster::default();
        let (a, ca) = node(8192, DeviceClass::Gpu);
        let (b, cb) = node(16384, DeviceClass::Gpu);
        c.upsert_node(a.clone(), "alice", ca);
        c.upsert_node(b.clone(), "bob", cb);
        c.trust_all_claims_for_tests();
        for _ in 0..5 {
            c.record_challenge_fail(&a);
        }
        let pick = c.acquire_worker(CLUSTER_MODEL).unwrap();
        assert_eq!(pick, b);
    }

    #[test]
    fn capacity_single_model_uses_full_pool() {
        let mut c = Cluster::default();
        let (a, ca) = node(8192, DeviceClass::Gpu);
        let (b, cb) = node(16384, DeviceClass::Gpu);
        c.upsert_node(a.clone(), "alice", ca);
        c.upsert_node(b, "bob", cb);
        c.set_health(&a, false, 1.0).unwrap();
        assert!(c.account_is_donating("bob"));
        assert!(!c.account_is_donating("alice"));
        let cap = c.capacity();
        assert_eq!(cap.models_available, vec![CLUSTER_MODEL.to_string()]);
        assert_eq!(cap.nodes_healthy, 1);
    }

    #[test]
    fn full_pool_pipeline_uses_all_healthy() {
        let mut c = Cluster::default();
        for _ in 0..3 {
            let (id, caps) = node(8192, DeviceClass::Gpu);
            c.upsert_node(id, "donor", caps);
        }
        c.trust_all_claims_for_tests();
        let plan = c.plan_full_pool().unwrap();
        assert_eq!(plan.model, CLUSTER_MODEL);
        assert_eq!(plan.shards.len(), 3);
        assert_eq!(plan.pool_mem_mib, 8192 * 3);
    }

    /// CRITICAL: fake high-end claim (e.g. "5070 farm") must not inflate verified capacity.
    #[test]
    fn high_claim_without_challenge_is_not_verified() {
        let mut c = Cluster::default();
        let (id, caps) = node(24_576, DeviceClass::Gpu); // claim ~24 GiB
        c.upsert_node(id.clone(), "faker", caps);
        assert_eq!(c.verified_mem_mib(&id), 0, "join must start unverified");
        assert_eq!(c.verified_pool_vram_mib(), 0);
        // Placement excludes unverified entirely (claim never enters the logical GPU).
        assert!(
            c.plan_full_pool().is_err(),
            "claim-only node must not form a pool plan"
        );
        assert_eq!(placement_mem_mib(0), 0);
        assert_eq!(max_streams(c.get(&id).unwrap()), 0);
        // After one ok with proven peak C, verified = C (not free full claim).
        let proven = 64u32;
        c.on_challenge_result(&id, true, proven);
        let v1 = c.verified_mem_mib(&id);
        assert_eq!(v1, proven);
        assert!(v1 < 24_576, "must not unlock full claim on one ok");
        // Three serial oks of same peak C → still C (peak, not sum).
        c.on_challenge_result(&id, true, proven);
        c.on_challenge_result(&id, true, proven);
        assert_eq!(
            c.verified_mem_mib(&id),
            proven,
            "serial same peak must not sum"
        );
        // Larger single proof raises peak.
        c.on_challenge_result(&id, true, 128);
        assert_eq!(c.verified_mem_mib(&id), 128);
        // Fail halves trust.
        let before_fail = c.verified_mem_mib(&id);
        c.on_challenge_result(&id, false, 0);
        let v_fail = c.verified_mem_mib(&id);
        assert!(v_fail < before_fail, "fail must reduce verified");
    }

    #[test]
    fn join_starts_with_zero_verified_regardless_of_claim() {
        let mut c = Cluster::default();
        let claim = 65_536u32; // pretend full farm
        let (id, caps) = node(claim, DeviceClass::Gpu);
        c.upsert_node(id.clone(), "a", caps);
        assert_eq!(c.verified_mem_mib(&id), 0);
        assert_eq!(c.get(&id).unwrap().claimed_mem_mib, claim);
        // N serial peak-C proofs cannot sum to N×C (anti farm).
        let proven = 128u32;
        for _ in 0..3 {
            c.on_challenge_result(&id, true, proven);
        }
        assert!(
            c.verified_mem_mib(&id) < claim,
            "3 challenges must not equal full claim"
        );
        assert_eq!(c.verified_mem_mib(&id), proven, "peak not sum");
    }

    #[test]
    fn unlock_is_peak_not_sum_and_capped_per_challenge() {
        let mut c = Cluster::default();
        let (id, caps) = node(24_576, DeviceClass::Gpu);
        c.upsert_node(id.clone(), "f", caps);
        // Ok with proven=0 → clamp to 1? No — proven 0 min with claim: we use
        // proven_credit_mib.min(CHALLENGE_CREDIT).min(claim); 0 stays 0 with max.
        c.on_challenge_result(&id, true, 0);
        assert_eq!(c.verified_mem_mib(&id), 0);
        c.on_challenge_result(&id, true, 16);
        assert_eq!(c.verified_mem_mib(&id), 16);
        // Single challenge cannot exceed CHALLENGE_CREDIT_MIB (peak cap).
        c.on_challenge_result(&id, true, CHALLENGE_CREDIT_MIB * 10);
        assert_eq!(
            c.verified_mem_mib(&id),
            CHALLENGE_CREDIT_MIB,
            "peak capped at CHALLENGE_CREDIT_MIB"
        );
    }

    /// CRITICAL: N serial credit-C unlocks cannot exceed peak work of C.
    #[test]
    fn serial_challenges_cannot_sum_past_peak_work() {
        let mut c = Cluster::default();
        let claim = 65_536u32;
        let (id, caps) = node(claim, DeviceClass::Gpu);
        c.upsert_node(id.clone(), "farm", caps);
        let peak_c = 64u32; // working-set MiB per challenge
        for _ in 0..100 {
            c.on_challenge_result(&id, true, peak_c);
        }
        assert_eq!(
            c.verified_mem_mib(&id),
            peak_c,
            "100×serial {peak_c} must stay peak={peak_c}, not 6400"
        );
        assert!(c.verified_mem_mib(&id) < claim);
        // Raising peak once unlocks only that peak.
        c.on_challenge_result(&id, true, 256);
        assert_eq!(c.verified_mem_mib(&id), 256);
        for _ in 0..50 {
            c.on_challenge_result(&id, true, 256);
        }
        assert_eq!(c.verified_mem_mib(&id), 256);
    }

    #[test]
    fn capacity_api_never_takes_claim() {
        assert_eq!(placement_mem_mib(0), 0);
        assert_eq!(economic_mem_mib(0), VERIFIED_MEM_FLOOR_MIB);
        assert_eq!(placement_mem_mib(8192), 8192);
        assert_eq!(economic_mem_mib(8192), 8192);
    }

    #[test]
    fn rank_schedulable_prefers_verified_not_claim() {
        let mut c = Cluster::default();
        let (low_claim_high_v, caps_a) = node(4096, DeviceClass::Gpu);
        let (high_claim_low_v, caps_b) = node(65_536, DeviceClass::Gpu);
        c.upsert_node(low_claim_high_v.clone(), "a", caps_a);
        c.upsert_node(high_claim_low_v.clone(), "b", caps_b);
        c.set_verified_mem_mib(&low_claim_high_v, 4096);
        c.set_verified_mem_mib(&high_claim_low_v, 1024);
        let ranked = c.rank_schedulable();
        assert_eq!(
            ranked[0], low_claim_high_v,
            "verified 4G must beat claim 64G/verified 1G"
        );
    }

    #[test]
    fn attestation_tier_from_claim_verified() {
        assert_eq!(attestation_tier(8192, 0), AttestationTier::ClaimOnly);
        assert_eq!(
            attestation_tier(8192, 1024),
            AttestationTier::ChallengePartial
        );
        assert_eq!(attestation_tier(8192, 8192), AttestationTier::ChallengeFull);
        // Claim-only never grades full without proof
        assert_ne!(attestation_tier(65_536, 0), AttestationTier::ChallengeFull);
    }
}
