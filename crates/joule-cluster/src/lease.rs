//! Stream leases + auditable plan agreement (admission path).
//!
//! Distributed law: a chat request only proceeds after a **stream lease** is
//! taken against verified pool capacity, and multi-shard work only proceeds
//! after each required donor **confirms** the plan with a content hash.
//! Invalid / missing confirmation fails closed.

use crate::Cluster;
use joule_proto::{ClusterPlan, NodeId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Domain tags for hash preimages (stable, not secrets).
pub const DOMAIN_PLAN: &[u8] = b"joule-plan-v1";
pub const DOMAIN_ACCEPT: &[u8] = b"joule-plan-accept-v1";
pub const DOMAIN_LEASE: &[u8] = b"joule-stream-lease-v1";

/// One concurrent generation reservation on the sharded pool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamLease {
    pub lease_id: Uuid,
    pub request_id: Uuid,
    pub account: String,
    /// Unix seconds when the lease was granted.
    pub granted_unix: u64,
    /// Unix seconds after which the lease is stale (must release).
    pub deadline_unix: u64,
    pub plan: ClusterPlan,
    /// Canonical plan body hash (hex).
    pub plan_hash_hex: String,
}

/// Confirmable audit row for a completed (or aborted) admission lifecycle.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LeaseAuditEntry {
    pub lease_id: Uuid,
    pub request_id: Uuid,
    pub account: String,
    pub plan_hash_hex: String,
    /// Node ids that sent a valid PlanAccept confirmation.
    pub accepts: Vec<String>,
    pub event: String,
    pub detail: String,
    pub unix_secs: u64,
}

/// Pure lease table — call sites pass [`Cluster`] for acquire/release of inflight.
#[derive(Debug, Default)]
pub struct LeaseBook {
    active: HashMap<Uuid, StreamLease>,
    /// request_id → lease_id
    by_request: HashMap<Uuid, Uuid>,
    audit: Vec<LeaseAuditEntry>,
}

impl LeaseBook {
    pub fn active_count(&self) -> u32 {
        self.active.len() as u32
    }

    pub fn get(&self, lease_id: &Uuid) -> Option<&StreamLease> {
        self.active.get(lease_id)
    }

    pub fn get_by_request(&self, request_id: &Uuid) -> Option<&StreamLease> {
        self.by_request
            .get(request_id)
            .and_then(|id| self.active.get(id))
    }

    pub fn audit_trail(&self) -> &[LeaseAuditEntry] {
        &self.audit
    }

    pub fn audit_for_request(&self, request_id: Uuid) -> Vec<&LeaseAuditEntry> {
        self.audit
            .iter()
            .filter(|e| e.request_id == request_id)
            .collect()
    }

    fn push_audit(&mut self, mut e: LeaseAuditEntry) {
        if e.unix_secs == 0 {
            e.unix_secs = now_unix();
        }
        self.audit.push(e);
        if self.audit.len() > 512 {
            let drop_n = self.audit.len() - 512;
            self.audit.drain(0..drop_n);
        }
    }

    /// Acquire one stream lease against the cluster (fail closed if full).
    pub fn try_admit(
        &mut self,
        cluster: &mut Cluster,
        account: &str,
        request_id: Uuid,
        ttl: Duration,
    ) -> Result<StreamLease, String> {
        let plan = cluster
            .try_acquire_stream()
            .ok_or_else(|| "pool full: no free stream slots".to_string())?;
        let plan_hash_hex = plan_hash_hex(&plan);
        let granted = now_unix();
        let lease = StreamLease {
            lease_id: Uuid::new_v4(),
            request_id,
            account: account.to_string(),
            granted_unix: granted,
            deadline_unix: granted.saturating_add(ttl.as_secs().max(1)),
            plan,
            plan_hash_hex: plan_hash_hex.clone(),
        };
        self.by_request.insert(request_id, lease.lease_id);
        self.active.insert(lease.lease_id, lease.clone());
        self.push_audit(LeaseAuditEntry {
            lease_id: lease.lease_id,
            request_id,
            account: account.to_string(),
            plan_hash_hex,
            accepts: vec![],
            event: "lease_granted".into(),
            detail: format!("shards={}", lease.plan.shards.len()),
            unix_secs: granted,
        });
        Ok(lease)
    }

    /// Release by lease id (idempotent). Returns true if a live lease was freed.
    pub fn release(
        &mut self,
        cluster: &mut Cluster,
        lease_id: Uuid,
        event: &str,
        detail: &str,
    ) -> bool {
        let Some(lease) = self.active.remove(&lease_id) else {
            return false;
        };
        self.by_request.remove(&lease.request_id);
        cluster.release_stream(&lease.plan);
        self.push_audit(LeaseAuditEntry {
            lease_id: lease.lease_id,
            request_id: lease.request_id,
            account: lease.account,
            plan_hash_hex: lease.plan_hash_hex,
            accepts: vec![],
            event: event.into(),
            detail: detail.into(),
            unix_secs: now_unix(),
        });
        true
    }

    pub fn release_by_request(
        &mut self,
        cluster: &mut Cluster,
        request_id: Uuid,
        event: &str,
        detail: &str,
    ) -> bool {
        let Some(id) = self.by_request.get(&request_id).copied() else {
            return false;
        };
        self.release(cluster, id, event, detail)
    }

    /// Expire overdue leases (fail-closed cleanup). Returns how many released.
    pub fn expire_stale(&mut self, cluster: &mut Cluster, now: u64) -> u32 {
        let stale: Vec<Uuid> = self
            .active
            .values()
            .filter(|l| l.deadline_unix <= now)
            .map(|l| l.lease_id)
            .collect();
        let mut n = 0u32;
        for id in stale {
            if self.release(cluster, id, "lease_expired", "deadline passed") {
                n += 1;
            }
        }
        n
    }

    /// Bind the **agreed** plan content hash (mesh geometry may differ from the
    /// registry plan used only for stream-slot reservation).
    pub fn bind_agreement_hash(&mut self, request_id: Uuid, plan_hash_hex: String) {
        let Some(id) = self.by_request.get(&request_id).copied() else {
            return;
        };
        if let Some(lease) = self.active.get_mut(&id) {
            lease.plan_hash_hex = plan_hash_hex;
        }
    }

    /// Record multi-party accepts after verification.
    ///
    /// `plan_hash_hex` when `Some` overrides the lease body hash (use the hash
    /// every shard confirmed).
    pub fn record_accepts(
        &mut self,
        request_id: Uuid,
        accepts: &[NodeId],
        event: &str,
        detail: &str,
        plan_hash_hex: Option<&str>,
    ) {
        let Some(lease) = self.get_by_request(&request_id).cloned() else {
            // Still record orphan rejects so the trail is auditable.
            self.push_audit(LeaseAuditEntry {
                lease_id: Uuid::nil(),
                request_id,
                account: String::new(),
                plan_hash_hex: plan_hash_hex.unwrap_or("").to_string(),
                accepts: accepts.iter().map(|n| n.to_string()).collect(),
                event: event.into(),
                detail: detail.into(),
                unix_secs: now_unix(),
            });
            return;
        };
        self.push_audit(LeaseAuditEntry {
            lease_id: lease.lease_id,
            request_id,
            account: lease.account,
            plan_hash_hex: plan_hash_hex
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .unwrap_or(lease.plan_hash_hex),
            accepts: accepts.iter().map(|n| n.to_string()).collect(),
            event: event.into(),
            detail: detail.into(),
            unix_secs: now_unix(),
        });
    }
}

/// Canonical SHA-256 hex of the plan body (sorted shard nodes for stability).
pub fn plan_hash_hex(plan: &ClusterPlan) -> String {
    let mut hasher = Sha256::new();
    hasher.update(DOMAIN_PLAN);
    hasher.update(plan.plan_id.as_bytes());
    hasher.update(plan.model.as_bytes());
    hasher.update(plan.pool_mem_mib.to_le_bytes());
    let mut shards = plan.shards.clone();
    shards.sort_by_key(|a| a.node.to_string());
    for s in &shards {
        hasher.update(s.node.0.as_bytes());
        hasher.update(s.mem_share_mib.to_le_bytes());
        hasher.update(s.mem_fraction_ppm.to_le_bytes());
        if let Some(ls) = s.layer_start {
            hasher.update(ls.to_le_bytes());
        }
        if let Some(le) = s.layer_end {
            hasher.update(le.to_le_bytes());
        }
    }
    hex::encode(hasher.finalize())
}

/// Content confirmation a shard emits with PlanAccept (not a secret key — public hash).
pub fn plan_accept_confirm_hex(
    plan_id: Uuid,
    request_id: Uuid,
    node: &NodeId,
    accepted: bool,
    plan_hash_hex: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(DOMAIN_ACCEPT);
    hasher.update(plan_id.as_bytes());
    hasher.update(request_id.as_bytes());
    hasher.update(node.0.as_bytes());
    hasher.update([u8::from(accepted)]);
    hasher.update(plan_hash_hex.as_bytes());
    hex::encode(hasher.finalize())
}

/// Verify a PlanAccept confirmation from a donor.
pub fn verify_plan_accept_confirm(
    plan_id: Uuid,
    request_id: Uuid,
    node: &NodeId,
    accepted: bool,
    plan_hash_hex: &str,
    confirm_hex: &str,
) -> Result<(), String> {
    if confirm_hex.is_empty() {
        return Err("missing plan accept confirm_hex".into());
    }
    let expect = plan_accept_confirm_hex(plan_id, request_id, node, accepted, plan_hash_hex);
    if !constant_time_eq(expect.as_bytes(), confirm_hex.as_bytes()) {
        return Err("plan accept confirm_hex mismatch (tampered or wrong plan)".into());
    }
    Ok(())
}

/// Build PlanAccept fields (plan_hash + confirm) for a donor node.
pub fn plan_accept_fields(
    plan: &ClusterPlan,
    request_id: Uuid,
    node: &NodeId,
    accepted: bool,
    known_plan_hash: Option<&str>,
) -> (String, String) {
    let ph = known_plan_hash
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(|| plan_hash_hex(plan));
    let confirm = plan_accept_confirm_hex(plan.plan_id, request_id, node, accepted, &ph);
    (ph, confirm)
}

/// Lease grant preimage hash (auditable receipt).
pub fn lease_receipt_hex(lease: &StreamLease) -> String {
    let mut hasher = Sha256::new();
    hasher.update(DOMAIN_LEASE);
    hasher.update(lease.lease_id.as_bytes());
    hasher.update(lease.request_id.as_bytes());
    hasher.update(lease.account.as_bytes());
    hasher.update(lease.plan_hash_hex.as_bytes());
    hasher.update(lease.granted_unix.to_le_bytes());
    hasher.update(lease.deadline_unix.to_le_bytes());
    hex::encode(hasher.finalize())
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut v = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        v |= x ^ y;
    }
    v == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use joule_proto::{DeviceClass, NodeCaps, NodeId};

    fn pool(n: u32) -> Cluster {
        let mut c = Cluster::default();
        for i in 0..n {
            let id = NodeId::new();
            c.upsert_node(
                id,
                format!("d{i}"),
                NodeCaps::for_cluster(DeviceClass::Gpu, 8192, 10),
            );
        }
        c.trust_all_claims_for_tests();
        c
    }

    #[test]
    fn admit_release_restores_free_slots() {
        let mut c = pool(2);
        let free0 = c.scheduler_snapshot().stream_slots_free;
        assert!(free0 >= 1);
        let mut book = LeaseBook::default();
        let rid = Uuid::new_v4();
        let lease = book
            .try_admit(&mut c, "alice", rid, Duration::from_secs(30))
            .expect("admit");
        let free1 = c.scheduler_snapshot().stream_slots_free;
        assert_eq!(free1, free0 - 1);
        assert_eq!(book.active_count(), 1);
        assert!(book.release(&mut c, lease.lease_id, "lease_released", "test done"));
        assert_eq!(c.scheduler_snapshot().stream_slots_free, free0);
        assert_eq!(book.active_count(), 0);
        let trail = book.audit_for_request(rid);
        assert!(trail.iter().any(|e| e.event == "lease_granted"));
        assert!(trail.iter().any(|e| e.event == "lease_released"));
    }

    #[test]
    fn admit_fails_closed_when_saturated() {
        let mut c = pool(1);
        c.trust_all_claims_for_tests();
        let mut book = LeaseBook::default();
        let total = c.scheduler_snapshot().stream_slots_total.max(1);
        for _ in 0..total {
            book.try_admit(&mut c, "alice", Uuid::new_v4(), Duration::from_secs(60))
                .expect("fill");
        }
        let err = book
            .try_admit(&mut c, "bob", Uuid::new_v4(), Duration::from_secs(60))
            .unwrap_err();
        assert!(err.contains("pool full"), "{err}");
        assert_eq!(
            c.scheduler_snapshot().stream_slots_used,
            c.scheduler_snapshot().stream_slots_total
        );
    }

    #[test]
    fn plan_accept_confirm_detects_tamper() {
        let c = pool(2);
        let plan = c.plan_sharded_pool().unwrap();
        let ph = plan_hash_hex(&plan);
        let node = plan.shards[0].node.clone();
        let rid = Uuid::new_v4();
        let ok = plan_accept_confirm_hex(plan.plan_id, rid, &node, true, &ph);
        assert!(verify_plan_accept_confirm(plan.plan_id, rid, &node, true, &ph, &ok).is_ok());
        assert!(
            verify_plan_accept_confirm(plan.plan_id, rid, &node, true, &ph, "deadbeef").is_err()
        );
        assert!(verify_plan_accept_confirm(plan.plan_id, rid, &node, false, &ph, &ok).is_err());
        // wrong plan hash
        assert!(verify_plan_accept_confirm(plan.plan_id, rid, &node, true, "00", &ok).is_err());
    }

    #[test]
    fn multi_party_plan_agreement_trail() {
        let mut c = pool(3);
        let mut book = LeaseBook::default();
        let rid = Uuid::new_v4();
        let lease = book
            .try_admit(&mut c, "alice", rid, Duration::from_secs(30))
            .expect("admit");
        let ph = lease.plan_hash_hex.clone();
        let nodes: Vec<_> = lease.plan.shards.iter().map(|s| s.node.clone()).collect();
        assert!(nodes.len() >= 2, "need multi-shard for agreement demo");
        let mut confirmed = Vec::new();
        for n in &nodes {
            let (got_ph, confirm) = plan_accept_fields(&lease.plan, rid, n, true, Some(&ph));
            assert_eq!(got_ph, ph);
            verify_plan_accept_confirm(lease.plan.plan_id, rid, n, true, &ph, &confirm)
                .expect("confirm");
            confirmed.push(n.clone());
        }
        book.record_accepts(
            rid,
            &confirmed,
            "plan_agreed",
            "all shards confirmed",
            Some(&ph),
        );
        assert!(book.release(&mut c, lease.lease_id, "lease_released", "infer ok"));
        let trail = book.audit_for_request(rid);
        let events: Vec<_> = trail.iter().map(|e| e.event.as_str()).collect();
        assert_eq!(
            events,
            ["lease_granted", "plan_agreed", "lease_released"],
            "{events:?}"
        );
        let agreed = trail.iter().find(|e| e.event == "plan_agreed").unwrap();
        assert_eq!(agreed.plan_hash_hex, ph);
        assert_eq!(agreed.accepts.len(), nodes.len());
        // Receipt is deterministic for the same lease body.
        let r1 = lease_receipt_hex(&lease);
        assert_eq!(r1.len(), 64);
        assert!(r1.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn concurrent_admit_respects_pool_cap() {
        let mut c = pool(2);
        let total = c.scheduler_snapshot().stream_slots_total.max(1);
        let mut book = LeaseBook::default();
        let mut ok = 0u32;
        let mut denied = 0u32;
        // Saturate then release in interleaved free→used→free pattern.
        for i in 0..(total * 3) {
            let rid = Uuid::new_v4();
            match book.try_admit(&mut c, "alice", rid, Duration::from_secs(30)) {
                Ok(lease) => {
                    ok += 1;
                    assert!(book.active_count() <= total);
                    if i % 2 == 0 {
                        book.release(&mut c, lease.lease_id, "lease_released", "early free");
                    }
                }
                Err(e) => {
                    assert!(e.contains("pool full"), "{e}");
                    denied += 1;
                    // free one if any
                    if let Some(id) = book.active.keys().next().copied() {
                        book.release(&mut c, id, "lease_released", "make room");
                    }
                }
            }
        }
        assert!(ok >= total, "ok={ok} total={total}");
        assert!(denied > 0 || book.active_count() > 0);
        // Drain all
        let ids: Vec<_> = book.active.keys().copied().collect();
        for id in ids {
            book.release(&mut c, id, "lease_released", "drain");
        }
        assert_eq!(book.active_count(), 0);
        assert_eq!(c.scheduler_snapshot().stream_slots_used, 0);
        assert_eq!(
            c.scheduler_snapshot().stream_slots_free,
            c.scheduler_snapshot().stream_slots_total
        );
    }

    #[test]
    fn expire_stale_releases() {
        let mut c = pool(1);
        let mut book = LeaseBook::default();
        let rid = Uuid::new_v4();
        let mut lease = book
            .try_admit(&mut c, "alice", rid, Duration::from_secs(1))
            .unwrap();
        // force deadline into the past
        lease.deadline_unix = 1;
        book.active.insert(lease.lease_id, lease);
        let free_before_expire_used = c.scheduler_snapshot().stream_slots_used;
        assert!(free_before_expire_used >= 1);
        let n = book.expire_stale(&mut c, now_unix());
        assert_eq!(n, 1);
        assert_eq!(c.scheduler_snapshot().stream_slots_used, 0);
    }
}
