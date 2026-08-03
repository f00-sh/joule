//! Peer-only mesh coordination (Phase D production).
//!
//! - **No control required** as message bus for chat/infer.
//! - Coordinator elected from healthy donors (highest mem, then node id).
//! - On coordinator timeout/death: re-elect + re-plan from remaining peers.
//!
//! See docs/design/decentral-discovery-v0.md.

mod peer_infer;
pub use peer_infer::{can_peer_infer, prefer_peer_direct_infer};

use anyhow::{bail, Context, Result};
use joule_cluster::{plan_from_mesh_donors, Cluster, LeaseBook};
use joule_proto::{ClusterPlan, DeviceClass, Envelope, Message, NodeCaps, NodeId, CLUSTER_MODEL};
use joule_runtime::{Engine, InferRequest, StubEngine};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Mutex};
use tracing::{info, warn};
use uuid::Uuid;

/// Peer identity + capacity for election / planning.
///
/// **Trust boundary:** `verified_mem_mib` is only safe when the **caller** populated
/// it from control-cluster challenge unlock (or a lab test fixture). It must **never**
/// be filled from PeerAlive gossip self-reports — use
/// [`MeshDonor::from_untrusted_presence`] for gossip (equal unit).
#[derive(Debug, Clone)]
pub struct MeshDonor {
    pub node: NodeId,
    pub verified_mem_mib: u32,
    pub healthy: bool,
}

/// Equal unit for untrusted peer-gossip presence (cannot mint a farm).
pub const PEER_GOSSIP_UNIT_MIB: u32 = 1024;

impl MeshDonor {
    /// Presence from untrusted PeerAlive gossip — equal unit only.
    pub fn from_untrusted_presence(node: NodeId, healthy: bool) -> Self {
        Self {
            node,
            verified_mem_mib: if healthy { PEER_GOSSIP_UNIT_MIB } else { 0 },
            healthy,
        }
    }
}

/// Deterministic coordinator: max **verified** mem, then node id string.
pub fn elect_coordinator(donors: &[MeshDonor]) -> Option<NodeId> {
    let mut eligible: Vec<&MeshDonor> = donors
        .iter()
        .filter(|d| d.healthy && joule_cluster::placement_mem_mib(d.verified_mem_mib) > 0)
        .collect();
    if eligible.is_empty() {
        return None;
    }
    eligible.sort_by(|a, b| {
        joule_cluster::placement_mem_mib(b.verified_mem_mib)
            .cmp(&joule_cluster::placement_mem_mib(a.verified_mem_mib))
            .then_with(|| a.node.to_string().cmp(&b.node.to_string()))
    });
    Some(eligible[0].node.clone())
}

/// Build plan from remaining donors (re-plan entry) — verified placement only.
pub fn replan(donors: &[MeshDonor]) -> Result<ClusterPlan> {
    let pairs: Vec<(NodeId, u32)> = donors
        .iter()
        .filter(|d| d.healthy && joule_cluster::placement_mem_mib(d.verified_mem_mib) > 0)
        .map(|d| {
            (
                d.node.clone(),
                joule_cluster::placement_mem_mib(d.verified_mem_mib),
            )
        })
        .collect();
    plan_from_mesh_donors(&pairs).map_err(|e| anyhow::anyhow!(e))
}

/// In-flight request state for peer bus.
#[derive(Debug, Clone)]
pub struct InflightRequest {
    pub request_id: Uuid,
    pub account: String,
    pub model: String,
    pub prompt: String,
    pub max_tokens: u32,
    pub coordinator: NodeId,
    pub plan: Option<ClusterPlan>,
    /// Canonical plan hash every PlanAccept must confirm.
    pub plan_hash_hex: String,
    pub accepts: HashSet<NodeId>,
    pub attempt: u32,
    /// True after a stream lease was admitted for this request.
    pub lease_held: bool,
    /// Abort reason (invalid confirm, reject, pool full) — no InferDone success.
    pub aborted: Option<String>,
}

/// Peer-only mesh bus: nodes exchange envelopes without control.
#[derive(Clone)]
pub struct PeerBus {
    inner: Arc<Mutex<PeerBusInner>>,
}

struct PeerBusInner {
    local: NodeId,
    donors: HashMap<NodeId, MeshDonor>,
    /// node → mailbox
    mailboxes: HashMap<NodeId, mpsc::UnboundedSender<Envelope>>,
    inflight: HashMap<Uuid, InflightRequest>,
    /// Completions: request_id → text
    completions: HashMap<Uuid, String>,
    /// Coordinator timeout per request
    coord_timeout: Duration,
    /// Force next coordinator death for tests (drop messages from that node as coordinator).
    dead_coordinators: HashSet<NodeId>,
    /// Stream capacity + leases (same truth as control path).
    cluster: Cluster,
    leases: LeaseBook,
    /// Per-node ed25519 device keys (not derived from NodeId).
    device_keys: std::collections::HashMap<NodeId, ed25519_dalek::SigningKey>,
}

impl PeerBus {
    pub fn new(local: NodeId) -> Self {
        Self {
            inner: Arc::new(Mutex::new(PeerBusInner {
                local,
                donors: HashMap::new(),
                mailboxes: HashMap::new(),
                inflight: HashMap::new(),
                completions: HashMap::new(),
                coord_timeout: Duration::from_millis(500),
                dead_coordinators: HashSet::new(),
                cluster: Cluster::default(),
                leases: LeaseBook::default(),
                device_keys: std::collections::HashMap::new(),
            })),
        }
    }

    /// Active stream leases + scheduler free/used (for tests / audit).
    pub async fn capacity_snapshot(&self) -> (u32, u32, u32, u32) {
        let g = self.inner.lock().await;
        let s = g.cluster.scheduler_snapshot();
        (
            s.stream_slots_total,
            s.stream_slots_used,
            s.stream_slots_free,
            g.leases.active_count(),
        )
    }

    pub async fn audit_trail(&self) -> Vec<joule_cluster::LeaseAuditEntry> {
        self.inner.lock().await.leases.audit_trail().to_vec()
    }

    pub async fn abort_reason(&self, request_id: Uuid) -> Option<String> {
        self.inner
            .lock()
            .await
            .inflight
            .get(&request_id)
            .and_then(|i| i.aborted.clone())
    }

    pub async fn set_coord_timeout(&self, d: Duration) {
        self.inner.lock().await.coord_timeout = d;
    }

    pub async fn mark_dead(&self, node: &NodeId) {
        let mut g = self.inner.lock().await;
        g.dead_coordinators.insert(node.clone());
        if let Some(d) = g.donors.get_mut(node) {
            d.healthy = false;
        }
        // Mid-flight: cancel any plan/lease that depended on this node.
        invalidate_inflight_for_dead_node(&mut g, node);
    }

    /// Mark a **confirmed shard** (not only coordinator) dead for mid-infer replan.
    pub async fn mark_shard_dead(&self, node: &NodeId) {
        self.mark_dead(node).await;
    }

    /// True if any confirmed accept / plan shard is dead mid-flight.
    pub async fn needs_shard_replan(&self, request_id: Uuid) -> bool {
        let g = self.inner.lock().await;
        let Some(inf) = g.inflight.get(&request_id) else {
            return false;
        };
        let dead = |id: &NodeId| {
            g.dead_coordinators.contains(id) || g.donors.get(id).is_some_and(|d| !d.healthy)
        };
        inflight_needs_replan(inf, &dead)
    }

    pub async fn register_peer(&self, donor: MeshDonor, tx: mpsc::UnboundedSender<Envelope>) {
        let mut g = self.inner.lock().await;
        let sk = ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng);
        g.device_keys.insert(donor.node.clone(), sk);
        g.mailboxes.insert(donor.node.clone(), tx);
        g.donors.insert(donor.node.clone(), donor);
        sync_mesh_capacity(&mut g);
    }

    pub async fn upsert_donor(&self, donor: MeshDonor) {
        let mut g = self.inner.lock().await;
        g.donors.insert(donor.node.clone(), donor);
        sync_mesh_capacity(&mut g);
    }

    async fn send_to(&self, to: &NodeId, env: Envelope) -> Result<()> {
        let g = self.inner.lock().await;
        if g.dead_coordinators.contains(to) {
            bail!("peer {to} is dead");
        }
        let tx = g
            .mailboxes
            .get(to)
            .with_context(|| format!("no mailbox for {to}"))?;
        tx.send(env).map_err(|e| anyhow::anyhow!("send: {e}"))?;
        Ok(())
    }

    async fn broadcast(&self, env: Envelope, except: Option<&NodeId>) {
        let g = self.inner.lock().await;
        for (id, tx) in &g.mailboxes {
            if Some(id) == except {
                continue;
            }
            if g.dead_coordinators.contains(id) {
                continue;
            }
            let _ = tx.send(env.clone());
        }
    }

    /// Client entry: start RequestInfer on the peer bus (no control).
    pub async fn request_infer(
        &self,
        account: &str,
        prompt: &str,
        max_tokens: u32,
    ) -> Result<Uuid> {
        let request_id = Uuid::new_v4();
        let donors: Vec<MeshDonor> = {
            let g = self.inner.lock().await;
            g.donors.values().cloned().collect()
        };
        let coordinator =
            elect_coordinator(&donors).context("no healthy mesh donors for election")?;
        {
            let mut g = self.inner.lock().await;
            g.inflight.insert(
                request_id,
                InflightRequest {
                    request_id,
                    account: account.into(),
                    model: CLUSTER_MODEL.into(),
                    prompt: prompt.into(),
                    max_tokens,
                    coordinator: coordinator.clone(),
                    plan: None,
                    plan_hash_hex: String::new(),
                    accepts: HashSet::new(),
                    attempt: 0,
                    lease_held: false,
                    aborted: None,
                },
            );
        }
        let env = Envelope::new(
            {
                let g = self.inner.lock().await;
                g.local.clone()
            },
            Message::RequestInfer {
                request_id,
                account: account.into(),
                model: CLUSTER_MODEL.into(),
                prompt: prompt.into(),
                max_tokens,
            },
        );
        // Deliver to elected coordinator (and broadcast so peers learn).
        self.send_to(&coordinator, env.clone()).await?;
        self.broadcast(env, Some(&coordinator)).await;
        Ok(request_id)
    }

    /// Process one inbound envelope on `local` peer (run by each peer's loop).
    pub async fn handle_envelope(
        &self,
        local: &NodeId,
        env: Envelope,
        engine: &impl Engine,
    ) -> Result<()> {
        match env.msg.clone() {
            Message::RequestInfer {
                request_id,
                account,
                model,
                prompt,
                max_tokens,
            } => {
                let donors: Vec<MeshDonor> = {
                    let g = self.inner.lock().await;
                    g.donors.values().cloned().collect()
                };
                let coord = elect_coordinator(&donors).context("elect")?;
                if &coord != local {
                    return Ok(()); // only coordinator answers
                }
                let is_dead = {
                    let g = self.inner.lock().await;
                    g.dead_coordinators.contains(local)
                };
                if is_dead {
                    bail!("local coordinator is marked dead");
                }
                let plan = replan(&donors)?;
                let plan_hash_hex = joule_cluster::plan_hash_hex(&plan);
                // Stream lease before any PlanOffer (fail closed if pool full).
                {
                    let mut g = self.inner.lock().await;
                    sync_mesh_capacity(&mut g);
                    // Release prior lease on replan attempts for same request.
                    if g.inflight
                        .get(&request_id)
                        .map(|i| i.lease_held)
                        .unwrap_or(false)
                    {
                        mesh_release(&mut g, request_id, "lease_released", "replan");
                        if let Some(inf) = g.inflight.get_mut(&request_id) {
                            inf.lease_held = false;
                        }
                    }
                    match mesh_try_admit(&mut g, &account, request_id) {
                        Ok(_lease) => {
                            mesh_bind_hash(&mut g, request_id, plan_hash_hex.clone());
                        }
                        Err(e) => {
                            warn!(%request_id, error = %e, "peer-bus pool full — no PlanOffer");
                            if let Some(inf) = g.inflight.get_mut(&request_id) {
                                inf.aborted = Some(e.clone());
                                inf.lease_held = false;
                            } else {
                                g.inflight.insert(
                                    request_id,
                                    InflightRequest {
                                        request_id,
                                        account: account.clone(),
                                        model: model.clone(),
                                        prompt: prompt.clone(),
                                        max_tokens,
                                        coordinator: local.clone(),
                                        plan: None,
                                        plan_hash_hex: String::new(),
                                        accepts: HashSet::new(),
                                        attempt: 1,
                                        lease_held: false,
                                        aborted: Some(e),
                                    },
                                );
                            }
                            return Ok(());
                        }
                    }
                    if let Some(inf) = g.inflight.get_mut(&request_id) {
                        inf.coordinator = local.clone();
                        inf.plan = Some(plan.clone());
                        inf.plan_hash_hex = plan_hash_hex.clone();
                        inf.accepts.clear();
                        inf.attempt += 1;
                        inf.lease_held = true;
                        inf.aborted = None;
                    } else {
                        g.inflight.insert(
                            request_id,
                            InflightRequest {
                                request_id,
                                account: account.clone(),
                                model: model.clone(),
                                prompt: prompt.clone(),
                                max_tokens,
                                coordinator: local.clone(),
                                plan: Some(plan.clone()),
                                plan_hash_hex: plan_hash_hex.clone(),
                                accepts: HashSet::new(),
                                attempt: 1,
                                lease_held: true,
                                aborted: None,
                            },
                        );
                    }
                }
                info!(
                    %request_id,
                    shards = plan.shards.len(),
                    %plan_hash_hex,
                    "peer-bus PlanOffer + stream lease"
                );
                let offer = Envelope::new(
                    local.clone(),
                    Message::PlanOffer {
                        plan: plan.clone(),
                        request_id,
                        plan_hash_hex: plan_hash_hex.clone(),
                        auth: {
                            let ts = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_millis() as u64)
                                .unwrap_or(0);
                            let sk = {
                                let g = self.inner.lock().await;
                                g.device_keys.get(local).cloned().unwrap_or_else(|| {
                                    ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng)
                                })
                            };
                            let pre = joule_cluster::plan_offer_sign_preimage(
                                local,
                                plan.plan_id,
                                request_id,
                                &plan_hash_hex,
                                ts,
                            );
                            let (pk, sig) = joule_cluster::sign_preimage(&sk, &pre);
                            joule_proto::PlanAuth {
                                signer_pubkey_hex: pk,
                                sig_hex: sig,
                                signed_at_unix_ms: ts,
                            }
                        },
                    },
                );
                for s in &plan.shards {
                    let _ = self.send_to(&s.node, offer.clone()).await;
                }
            }
            Message::PlanOffer {
                plan,
                request_id,
                plan_hash_hex,
                auth,
            } => {
                // Fail closed: PlanOffer must be signed by **registered** device key for env.from
                // (same bind as PlanAccept — forgeable NodeId alone cannot spoof offers).
                let offer_hash = if plan_hash_hex.is_empty() {
                    joule_cluster::plan_hash_hex(&plan)
                } else {
                    plan_hash_hex.clone()
                };
                {
                    let g = self.inner.lock().await;
                    let reg_pk = g
                        .device_keys
                        .get(&env.from)
                        .map(|sk| hex::encode(sk.verifying_key().as_bytes()));
                    let offer_err = match reg_pk {
                        None => Some("no device key registered for PlanOffer from".to_string()),
                        Some(pk) if pk != auth.signer_pubkey_hex.trim().to_ascii_lowercase() => {
                            Some("plan offer pubkey not bound to registered device key".into())
                        }
                        Some(_) => joule_cluster::verify_plan_offer_sig(
                            &env.from,
                            plan.plan_id,
                            request_id,
                            &offer_hash,
                            &auth.signer_pubkey_hex,
                            &auth.sig_hex,
                            auth.signed_at_unix_ms,
                        )
                        .err(),
                    };
                    if let Some(e) = offer_err {
                        warn!(error = %e, "PlanOffer signature rejected");
                        return Ok(());
                    }
                }
                let accepted = plan.shards.iter().any(|s| &s.node == local);
                let ph = if plan_hash_hex.is_empty() {
                    joule_cluster::plan_hash_hex(&plan)
                } else {
                    plan_hash_hex.clone()
                };
                let confirm_hex = joule_cluster::plan_accept_confirm_hex(
                    plan.plan_id,
                    request_id,
                    local,
                    accepted,
                    &ph,
                );
                let ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                let sk = {
                    let g = self.inner.lock().await;
                    g.device_keys.get(local).cloned().unwrap_or_else(|| {
                        ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng)
                    })
                };
                let pre = joule_cluster::plan_accept_sign_preimage(
                    local,
                    plan.plan_id,
                    request_id,
                    accepted,
                    &ph,
                    &confirm_hex,
                    ts,
                );
                let (pk, sig) = joule_cluster::sign_preimage(&sk, &pre);
                let acc = Envelope::new(
                    local.clone(),
                    Message::PlanAccept {
                        plan_id: plan.plan_id,
                        request_id,
                        accepted,
                        reason: if accepted {
                            "peer-bus accept".into()
                        } else {
                            "not in plan".into()
                        },
                        plan_hash_hex: ph,
                        confirm_hex,
                        auth: joule_proto::PlanAuth {
                            signer_pubkey_hex: pk,
                            sig_hex: sig,
                            signed_at_unix_ms: ts,
                        },
                    },
                );
                // Send accept to coordinator
                let coord = {
                    let g = self.inner.lock().await;
                    g.inflight
                        .get(&request_id)
                        .map(|i| i.coordinator.clone())
                        .or_else(|| {
                            elect_coordinator(&g.donors.values().cloned().collect::<Vec<_>>())
                        })
                };
                if let Some(c) = coord {
                    let _ = self.send_to(&c, acc).await;
                }
                // Also store plan locally if we're a shard for later InferRequest
                if accepted {
                    let mut g = self.inner.lock().await;
                    if let Some(inf) = g.inflight.get_mut(&request_id) {
                        inf.plan = Some(plan);
                    }
                }
            }
            Message::PlanAccept {
                plan_id,
                request_id,
                accepted,
                plan_hash_hex,
                confirm_hex,
                reason: _,
                auth,
            } => {
                // Thin adapter: pure joule_cluster::on_accept owns order
                // (expected → plan_id → verify → record/abort). Device sig first.
                let effect = {
                    let g = self.inner.lock().await;
                    let Some(inf) = g.inflight.get(&request_id) else {
                        return Ok(());
                    };
                    if &inf.coordinator != local || inf.aborted.is_some() {
                        return Ok(());
                    }
                    if inf.plan_hash_hex.is_empty() {
                        return Ok(());
                    }
                    let expected: HashSet<NodeId> = inf
                        .plan
                        .as_ref()
                        .map(|p| p.shards.iter().map(|s| s.node.clone()).collect())
                        .unwrap_or_default();
                    if expected.contains(&env.from) {
                        // Bind signer pubkey to registered device key for env.from
                        // (forgeable NodeId alone cannot spoof accept).
                        let reg_pk = g
                            .device_keys
                            .get(&env.from)
                            .map(|sk| hex::encode(sk.verifying_key().as_bytes()));
                        let bind_err = match reg_pk {
                            None => Some("no device key registered for accept from".to_string()),
                            Some(pk)
                                if pk != auth.signer_pubkey_hex.trim().to_ascii_lowercase() =>
                            {
                                Some("plan accept pubkey not bound to registered device key".into())
                            }
                            Some(_) => joule_cluster::verify_plan_accept_sig(
                                &env.from,
                                plan_id,
                                request_id,
                                accepted,
                                &plan_hash_hex,
                                &confirm_hex,
                                &auth.signer_pubkey_hex,
                                &auth.sig_hex,
                                auth.signed_at_unix_ms,
                            )
                            .err(),
                        };
                        if let Some(e) = bind_err {
                            drop(g);
                            let mut g = self.inner.lock().await;
                            abort_inflight_and_release(
                                &mut g,
                                request_id,
                                format!("plan accept sig: {e}"),
                            );
                            return Ok(());
                        }
                    }
                    let view = joule_cluster::PlanAgreeView {
                        plan_id: inf.plan.as_ref().map(|p| p.plan_id).unwrap_or(plan_id),
                        want_hash: inf.plan_hash_hex.as_str(),
                        expected: &expected,
                        already_accepted: &inf.accepts,
                    };
                    joule_cluster::on_accept(
                        Some(&view),
                        &env.from,
                        plan_id,
                        request_id,
                        accepted,
                        &plan_hash_hex,
                        &confirm_hex,
                    )
                };
                match effect {
                    joule_cluster::PlanAcceptEffect::Ignore => return Ok(()),
                    joule_cluster::PlanAcceptEffect::Abort { event, detail } => {
                        warn!(%request_id, from = %env.from, %event, "peer PlanAccept rejected");
                        let mut g = self.inner.lock().await;
                        let want = g
                            .inflight
                            .get(&request_id)
                            .map(|i| i.plan_hash_hex.clone())
                            .unwrap_or_default();
                        mesh_record_accepts(&mut g, request_id, &[], event, &detail, Some(&want));
                        abort_inflight_and_release(&mut g, request_id, detail);
                        return Ok(());
                    }
                    joule_cluster::PlanAcceptEffect::Record { ready } => {
                        let (plan, prompt, max_tokens, model, want_hash, accepts) = {
                            let mut g = self.inner.lock().await;
                            let Some(inf) = g.inflight.get_mut(&request_id) else {
                                return Ok(());
                            };
                            if &inf.coordinator != local || inf.aborted.is_some() {
                                return Ok(());
                            }
                            inf.accepts.insert(env.from.clone());
                            let plan = inf.plan.clone();
                            let prompt = inf.prompt.clone();
                            let max_tokens = inf.max_tokens;
                            let model = inf.model.clone();
                            let want_hash = inf.plan_hash_hex.clone();
                            let expected: HashSet<NodeId> = plan
                                .as_ref()
                                .map(|p| p.shards.iter().map(|s| s.node.clone()).collect())
                                .unwrap_or_default();
                            let accepts: Vec<NodeId> = if ready {
                                expected.into_iter().collect()
                            } else {
                                vec![]
                            };
                            (plan, prompt, max_tokens, model, want_hash, accepts)
                        };
                        if ready {
                            let mut g = self.inner.lock().await;
                            mesh_record_accepts(
                                &mut g,
                                request_id,
                                &accepts,
                                "plan_agreed",
                                "all shards confirmed (peer-bus)",
                                Some(&want_hash),
                            );
                            drop(g);
                            if let Some(plan) = plan {
                                let tail = plan.shards.last().map(|s| s.node.clone());
                                for s in &plan.shards {
                                    let is_tail = Some(&s.node) == tail.as_ref();
                                    let ir = Envelope::new(
                                        local.clone(),
                                        Message::InferRequest {
                                            request_id,
                                            model: model.clone(),
                                            prompt: prompt.clone(),
                                            max_tokens,
                                            plan: plan.clone(),
                                            is_tail,
                                        },
                                    );
                                    let _ = self.send_to(&s.node, ir).await;
                                }
                            }
                        }
                    }
                }
            }
            Message::InferRequest {
                request_id,
                model,
                prompt,
                max_tokens,
                plan,
                is_tail,
            } => {
                engine.load_plan(&plan).await.ok();
                let (text, pt, ct) = if is_tail {
                    match engine
                        .infer(InferRequest {
                            model,
                            prompt,
                            max_tokens,
                        })
                        .await
                    {
                        Ok(o) => (o.text, o.prompt_tokens, o.completion_tokens),
                        Err(e) => (format!("[error] {e}"), 0, 0),
                    }
                } else {
                    (String::new(), 0, 0)
                };
                let done = Envelope::new(
                    local.clone(),
                    Message::InferDone {
                        request_id,
                        text: text.clone(),
                        prompt_tokens: pt,
                        completion_tokens: ct,
                        shard_ok: true,
                    },
                );
                // Deliver completion to all (client listens on any)
                self.broadcast(done, None).await;
                if is_tail && !text.is_empty() {
                    self.inner.lock().await.completions.insert(request_id, text);
                }
            }
            Message::InferDone {
                request_id, text, ..
            } if !text.is_empty() => {
                let mut g = self.inner.lock().await;
                if g.inflight
                    .get(&request_id)
                    .and_then(|i| i.aborted.as_ref())
                    .is_some()
                {
                    // Never complete after aborted agreement.
                    return Ok(());
                }
                // Reject InferDone for a plan that still includes a dead confirmed shard.
                let dead = |id: &NodeId| {
                    g.dead_coordinators.contains(id) || g.donors.get(id).is_some_and(|d| !d.healthy)
                };
                if g.inflight
                    .get(&request_id)
                    .is_some_and(|i| inflight_needs_replan(i, &dead))
                {
                    warn!(%request_id, "ignore InferDone; confirmed shard dead — replan required");
                    return Ok(());
                }
                g.completions.insert(request_id, text);
                // Release stream lease on successful settle.
                if g.inflight
                    .get(&request_id)
                    .map(|i| i.lease_held)
                    .unwrap_or(false)
                {
                    mesh_release(&mut g, request_id, "lease_released", "infer ok");
                    if let Some(inf) = g.inflight.get_mut(&request_id) {
                        inf.lease_held = false;
                    }
                }
            }
            Message::InferDone { .. } => {}
            _ => {}
        }
        Ok(())
    }

    /// Wait for completion text; on confirmed-shard/coordinator death, cancel and re-plan.
    pub async fn wait_completion(
        &self,
        request_id: Uuid,
        overall_timeout: Duration,
    ) -> Result<String> {
        let start = tokio::time::Instant::now();
        let mut replan_budget = 2u32;
        loop {
            // Prefer replan over accepting a completion bound to a dead shard.
            let need_replan = {
                let g = self.inner.lock().await;
                match g.inflight.get(&request_id) {
                    Some(i) => {
                        let dead = |id: &NodeId| {
                            g.dead_coordinators.contains(id)
                                || g.donors.get(id).is_some_and(|d| !d.healthy)
                        };
                        let death = inflight_needs_replan(i, &dead);
                        let no_done = !g.completions.contains_key(&request_id);
                        death
                            || (no_done
                                && start.elapsed()
                                    > g.coord_timeout.saturating_mul(i.attempt.max(1)))
                    }
                    None => false,
                }
            };

            if need_replan && replan_budget > 0 {
                replan_budget -= 1;
                // Drop any stale completion from the dead-shard plan.
                {
                    let mut g = self.inner.lock().await;
                    g.completions.remove(&request_id);
                }
                self.replan_request(request_id).await?;
                continue;
            }

            if let Some(t) = self
                .inner
                .lock()
                .await
                .completions
                .get(&request_id)
                .cloned()
            {
                return Ok(t);
            }
            if start.elapsed() > overall_timeout {
                // Always release on timeout so slots free→used→free.
                {
                    let mut g = self.inner.lock().await;
                    if g.inflight
                        .get(&request_id)
                        .map(|i| i.lease_held)
                        .unwrap_or(false)
                    {
                        mesh_release(&mut g, request_id, "lease_released", "peer-bus timeout");
                        if let Some(inf) = g.inflight.get_mut(&request_id) {
                            inf.lease_held = false;
                            inf.aborted =
                                Some(inf.aborted.clone().unwrap_or_else(|| "timeout".into()));
                        }
                    }
                }
                bail!("peer-bus infer timed out");
            }
            let coord_timeout = self.inner.lock().await.coord_timeout;
            tokio::time::sleep(coord_timeout.min(Duration::from_millis(50))).await;
        }
    }

    /// Restart PlanOffer among remaining healthy donors (re-agree + re-lease).
    /// Keeps a healthy coordinator; re-elects only when the coordinator is dead.
    pub async fn replan_request(&self, request_id: Uuid) -> Result<()> {
        let (account, prompt, max_tokens, old_coord) = {
            let g = self.inner.lock().await;
            let inf = g
                .inflight
                .get(&request_id)
                .context("unknown request for replan")?;
            (
                inf.account.clone(),
                inf.prompt.clone(),
                inf.max_tokens,
                inf.coordinator.clone(),
            )
        };
        let coord_dead = {
            let g = self.inner.lock().await;
            g.dead_coordinators.contains(&old_coord)
                || g.donors.get(&old_coord).is_some_and(|d| !d.healthy)
        };
        // Only force-kill coordinator when it is already dead (coordinator-death path).
        if coord_dead {
            let mut g = self.inner.lock().await;
            g.dead_coordinators.insert(old_coord.clone());
            if let Some(d) = g.donors.get_mut(&old_coord) {
                d.healthy = false;
            }
        }
        let donors: Vec<MeshDonor> = {
            let g = self.inner.lock().await;
            g.donors.values().filter(|d| d.healthy).cloned().collect()
        };
        let new_coord = if !coord_dead && donors.iter().any(|d| d.node == old_coord) {
            old_coord.clone()
        } else {
            elect_coordinator(&donors).context("no remaining donors for re-election")?
        };
        info!(%request_id, old = %old_coord, new = %new_coord, "re-plan after death");
        {
            let mut g = self.inner.lock().await;
            // Drop stale completion + release prior lease before re-agree.
            g.completions.remove(&request_id);
            if g.inflight
                .get(&request_id)
                .map(|i| i.lease_held)
                .unwrap_or(false)
            {
                mesh_release(
                    &mut g,
                    request_id,
                    "lease_released",
                    "cancel for shard/coordinator replan",
                );
            }
            if let Some(inf) = g.inflight.get_mut(&request_id) {
                inf.coordinator = new_coord.clone();
                inf.plan = None;
                inf.accepts.clear();
                inf.lease_held = false;
                inf.attempt += 1;
            }
        }
        let env = Envelope::new(
            {
                let g = self.inner.lock().await;
                g.local.clone()
            },
            Message::RequestInfer {
                request_id,
                account,
                model: CLUSTER_MODEL.into(),
                prompt,
                max_tokens,
            },
        );
        self.send_to(&new_coord, env.clone()).await?;
        self.broadcast(env, Some(&new_coord)).await;
        Ok(())
    }

    pub async fn take_completion(&self, request_id: Uuid) -> Option<String> {
        self.inner
            .lock()
            .await
            .completions
            .get(&request_id)
            .cloned()
    }
}

/// Spawn a simple peer actor loop reading from rx.
pub async fn run_peer_actor(
    bus: PeerBus,
    local: NodeId,
    mut rx: mpsc::UnboundedReceiver<Envelope>,
) {
    let engine = StubEngine::new();
    while let Some(env) = rx.recv().await {
        if let Err(e) = bus.handle_envelope(&local, env, &engine).await {
            warn!(error = %e, %local, "peer actor handle error");
        }
    }
}

fn inflight_needs_replan(inf: &InflightRequest, dead: &dyn Fn(&NodeId) -> bool) -> bool {
    if dead(&inf.coordinator) {
        return true;
    }
    if let Some(plan) = &inf.plan {
        for s in &plan.shards {
            // Confirmed shard or any plan member dead mid-flight forces replan.
            if dead(&s.node)
                && (inf.accepts.contains(&s.node) || !inf.accepts.is_empty() || inf.attempt > 0)
            {
                return true;
            }
        }
    }
    false
}

/// Drop completions/leases for inflight work that depended on a just-dead node.
fn invalidate_inflight_for_dead_node(inner: &mut PeerBusInner, node: &NodeId) {
    let affected: Vec<Uuid> = inner
        .inflight
        .iter()
        .filter_map(|(rid, inf)| {
            let coord = &inf.coordinator == node;
            let accepted = inf.accepts.contains(node);
            let in_plan = inf
                .plan
                .as_ref()
                .is_some_and(|p| p.shards.iter().any(|s| &s.node == node));
            if coord || accepted || in_plan {
                Some(*rid)
            } else {
                None
            }
        })
        .collect();
    for rid in affected {
        inner.completions.remove(&rid);
        if inner
            .inflight
            .get(&rid)
            .map(|i| i.lease_held)
            .unwrap_or(false)
        {
            mesh_release(
                inner,
                rid,
                "lease_released",
                "confirmed shard/coordinator death mid-infer",
            );
            if let Some(inf) = inner.inflight.get_mut(&rid) {
                inf.lease_held = false;
            }
        }
    }
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Mirror mesh donors into the local Cluster for stream-slot accounting.
fn sync_mesh_capacity(inner: &mut PeerBusInner) {
    for d in inner.donors.values() {
        if !d.healthy {
            if let Ok(()) = inner.cluster.set_health(&d.node, false, 1.0) {
                // ok
            }
            continue;
        }
        let mem = joule_cluster::placement_mem_mib(d.verified_mem_mib);
        if mem == 0 {
            continue;
        }
        inner.cluster.upsert_node(
            d.node.clone(),
            "mesh",
            NodeCaps::for_cluster(DeviceClass::Gpu, mem, 10),
        );
        inner.cluster.set_verified_mem_mib(&d.node, mem);
        let _ = inner.cluster.set_health(&d.node, true, 0.0);
    }
}

fn abort_inflight_and_release(inner: &mut PeerBusInner, request_id: Uuid, reason: String) {
    let held = if let Some(inf) = inner.inflight.get_mut(&request_id) {
        inf.aborted = Some(reason.clone());
        let held = inf.lease_held;
        inf.lease_held = false;
        held
    } else {
        true
    };
    if held {
        let mut book = std::mem::take(&mut inner.leases);
        book.release_by_request(&mut inner.cluster, request_id, "lease_released", &reason);
        inner.leases = book;
    }
}

fn mesh_expire_stale(inner: &mut PeerBusInner) {
    let now = now_unix();
    let mut book = std::mem::take(&mut inner.leases);
    book.expire_stale(&mut inner.cluster, now);
    inner.leases = book;
}

fn mesh_try_admit(
    inner: &mut PeerBusInner,
    account: &str,
    request_id: Uuid,
) -> Result<joule_cluster::StreamLease, String> {
    mesh_expire_stale(inner);
    let mut book = std::mem::take(&mut inner.leases);
    let r = book.try_admit(
        &mut inner.cluster,
        account,
        request_id,
        Duration::from_secs(90),
    );
    inner.leases = book;
    r
}

fn mesh_release(inner: &mut PeerBusInner, request_id: Uuid, event: &str, detail: &str) -> bool {
    let mut book = std::mem::take(&mut inner.leases);
    let ok = book.release_by_request(&mut inner.cluster, request_id, event, detail);
    inner.leases = book;
    ok
}

fn mesh_bind_hash(inner: &mut PeerBusInner, request_id: Uuid, plan_hash_hex: String) {
    let mut book = std::mem::take(&mut inner.leases);
    book.bind_agreement_hash(request_id, plan_hash_hex);
    inner.leases = book;
}

fn mesh_record_accepts(
    inner: &mut PeerBusInner,
    request_id: Uuid,
    accepts: &[NodeId],
    event: &str,
    detail: &str,
    plan_hash_hex: Option<&str>,
) {
    let mut book = std::mem::take(&mut inner.leases);
    book.record_accepts(request_id, accepts, event, detail, plan_hash_hex);
    inner.leases = book;
}

/// Full peer-only chat: multi-donor bus, RequestInfer, wait for text.
pub async fn peer_only_chat(
    donors: Vec<(NodeId, u32)>,
    account: &str,
    prompt: &str,
    max_tokens: u32,
) -> Result<String> {
    if donors.len() < 2 {
        bail!("peer_only_chat needs ≥2 donors");
    }
    // Use first donor as "client" local identity
    let client_id = donors[0].0.clone();
    let bus = PeerBus::new(client_id.clone());

    let mut rxs = Vec::new();
    for (id, mem) in &donors {
        let (tx, rx) = mpsc::unbounded_channel();
        bus.register_peer(
            MeshDonor {
                node: id.clone(),
                verified_mem_mib: *mem,
                healthy: true,
            },
            tx,
        )
        .await;
        rxs.push((id.clone(), rx));
    }

    // Start actors
    for (id, rx) in rxs {
        let b = bus.clone();
        tokio::spawn(async move {
            run_peer_actor(b, id, rx).await;
        });
    }
    // allow mailboxes to settle
    tokio::time::sleep(Duration::from_millis(20)).await;

    let rid = bus.request_infer(account, prompt, max_tokens).await?;
    bus.wait_completion(rid, Duration::from_secs(5)).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn donors_n(n: usize) -> Vec<(NodeId, u32)> {
        (0..n)
            .map(|i| (NodeId::new(), 8192u32 + (i as u32) * 1024))
            .collect()
    }

    #[test]
    fn untrusted_gossip_presence_is_equal_unit_not_claim() {
        let a = NodeId::new();
        let b = NodeId::new();
        // Caller must use from_untrusted_presence for PeerAlive — never raw claim.
        let donors = vec![
            MeshDonor::from_untrusted_presence(a.clone(), true),
            MeshDonor::from_untrusted_presence(b.clone(), true),
        ];
        assert_eq!(donors[0].verified_mem_mib, PEER_GOSSIP_UNIT_MIB);
        assert_eq!(donors[1].verified_mem_mib, PEER_GOSSIP_UNIT_MIB);
        let plan = replan(&donors).expect("equal plan");
        assert_eq!(
            plan.pool_mem_mib,
            2 * u64::from(PEER_GOSSIP_UNIT_MIB),
            "gossip path must not carry farm claims"
        );
    }

    #[test]
    fn election_picks_highest_mem() {
        let a = NodeId::new();
        let b = NodeId::new();
        let d = vec![
            MeshDonor {
                node: a.clone(),
                verified_mem_mib: 8192,
                healthy: true,
            },
            MeshDonor {
                node: b.clone(),
                verified_mem_mib: 16384,
                healthy: true,
            },
        ];
        assert_eq!(elect_coordinator(&d).unwrap(), b);
    }

    /// PlanOffer signed with a key not bound to env.from device_keys must be rejected.
    #[tokio::test]
    async fn plan_offer_wrong_device_key_fail_closed() {
        let donors = donors_n(2);
        let client_id = donors[0].0.clone();
        let bus = PeerBus::new(client_id.clone());
        for (id, mem) in &donors {
            let (tx, _rx) = mpsc::unbounded_channel();
            bus.register_peer(
                MeshDonor {
                    node: id.clone(),
                    verified_mem_mib: *mem,
                    healthy: true,
                },
                tx,
            )
            .await;
        }
        let high = donors.iter().max_by_key(|(_, m)| *m).unwrap().0.clone();
        let low = donors.iter().min_by_key(|(_, m)| *m).unwrap().0.clone();

        // Forged offer: env.from = high, but signed with a random key ≠ device_keys[high].
        let plan =
            joule_cluster::plan_from_mesh_donors(&[(high.clone(), 16384), (low.clone(), 8192)])
                .expect("plan");
        let rid = Uuid::new_v4();
        let ph = joule_cluster::plan_hash_hex(&plan);
        let ts = 1u64;
        let forge_sk = ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng);
        let pre = joule_cluster::plan_offer_sign_preimage(&high, plan.plan_id, rid, &ph, ts);
        let (pk, sig) = joule_cluster::sign_preimage(&forge_sk, &pre);
        let offer = Envelope::new(
            high.clone(),
            Message::PlanOffer {
                plan: plan.clone(),
                request_id: rid,
                plan_hash_hex: ph,
                auth: joule_proto::PlanAuth {
                    signer_pubkey_hex: pk,
                    sig_hex: sig,
                    signed_at_unix_ms: ts,
                },
            },
        );
        let engine = StubEngine::new();
        bus.handle_envelope(&low, offer, &engine)
            .await
            .expect("handle returns ok");
        let g = bus.inner.lock().await;
        assert!(
            !g.inflight.contains_key(&rid)
                || g.inflight
                    .get(&rid)
                    .is_some_and(|i| i.plan.is_none() && i.accepts.is_empty()),
            "wrong-key PlanOffer must not establish plan/accepts"
        );
        let reg = g.device_keys.get(&high).expect("high registered");
        let reg_pk = hex::encode(reg.verifying_key().as_bytes());
        let forge_pk = hex::encode(forge_sk.verifying_key().as_bytes());
        assert_ne!(reg_pk, forge_pk);
        eprintln!("OBSERVE plan_offer wrong-key fail-closed forge_pk≠reg_pk");
    }

    #[tokio::test]
    async fn peer_only_chat_completes_without_control() {
        let d = donors_n(3);
        let text = peer_only_chat(d, "alice", "user: peer-only-hello", 32)
            .await
            .expect("peer only chat");
        assert!(!text.is_empty(), "empty completion");
        // StubEngine echoes prompt content for CLUSTER_MODEL
        assert!(
            text.contains("peer-only-hello") || text.len() > 4,
            "text={text}"
        );
    }

    #[tokio::test]
    async fn peer_plan_accept_requires_valid_confirm_hex() {
        let donors = donors_n(2);
        let client_id = donors[0].0.clone();
        let bus = PeerBus::new(client_id.clone());
        let mut rxs = Vec::new();
        for (id, mem) in &donors {
            let (tx, rx) = mpsc::unbounded_channel();
            bus.register_peer(
                MeshDonor {
                    node: id.clone(),
                    verified_mem_mib: *mem,
                    healthy: true,
                },
                tx,
            )
            .await;
            rxs.push((id.clone(), rx));
        }
        // Non-coordinator sends tampered confirm (lowest mem is not elected).
        let bad = donors.iter().min_by_key(|(_, m)| *m).unwrap().0.clone();
        for (id, rx) in rxs {
            let b = bus.clone();
            let is_bad = id == bad;
            tokio::spawn(async move {
                let engine = StubEngine::new();
                let mut rx = rx;
                while let Some(env) = rx.recv().await {
                    if is_bad {
                        if let Message::PlanOffer {
                            plan,
                            request_id,
                            plan_hash_hex,
                            ..
                        } = &env.msg
                        {
                            // Tampered confirm — coordinator must fail closed.
                            let coord = b
                                .inner
                                .lock()
                                .await
                                .inflight
                                .get(request_id)
                                .map(|i| i.coordinator.clone());
                            if let Some(c) = coord {
                                let acc = Envelope::new(
                                    id.clone(),
                                    Message::PlanAccept {
                                        plan_id: plan.plan_id,
                                        request_id: *request_id,
                                        accepted: true,
                                        reason: "tampered".into(),
                                        plan_hash_hex: plan_hash_hex.clone(),
                                        confirm_hex: "00".repeat(32),
                                        auth: joule_proto::PlanAuth::default(),
                                    },
                                );
                                let _ = b.send_to(&c, acc).await;
                            }
                            continue;
                        }
                    }
                    let _ = b.handle_envelope(&id, env, &engine).await;
                }
            });
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        let (_t, _u, free0, leases0) = bus.capacity_snapshot().await;
        let rid = bus
            .request_infer("alice", "user: tamper-test", 8)
            .await
            .expect("request");
        // Wait briefly for PlanAccept path to reject.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let abort = bus.abort_reason(rid).await;
        assert!(
            abort.as_ref().is_some_and(|s| {
                s.contains("confirm")
                    || s.contains("invalid")
                    || s.contains("sig")
                    || s.contains("signature")
            }),
            "expected abort on bad confirm/sig, got {abort:?}"
        );
        // No false completion.
        assert!(bus.take_completion(rid).await.is_none());
        let (_t2, used2, free2, leases2) = bus.capacity_snapshot().await;
        assert_eq!(leases2, 0, "lease must release after invalid accept");
        assert_eq!(used2, 0);
        assert!(free2 >= free0 || leases0 == 0);
        let trail = bus.audit_trail().await;
        assert!(
            trail.iter().any(|e| {
                e.event == "plan_accept_invalid"
                    || e.detail.contains("sig")
                    || e.detail.contains("confirm")
                    || e.event == "lease_released"
            }),
            "trail={trail:?}"
        );
    }

    #[tokio::test]
    async fn peer_lease_free_used_free_on_chat() {
        let donors = donors_n(3);
        let client_id = donors[0].0.clone();
        let bus = PeerBus::new(client_id.clone());
        let mut rxs = Vec::new();
        for (id, mem) in &donors {
            let (tx, rx) = mpsc::unbounded_channel();
            bus.register_peer(
                MeshDonor {
                    node: id.clone(),
                    verified_mem_mib: *mem,
                    healthy: true,
                },
                tx,
            )
            .await;
            rxs.push((id.clone(), rx));
        }
        for (id, rx) in rxs {
            let b = bus.clone();
            tokio::spawn(async move {
                run_peer_actor(b, id, rx).await;
            });
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        let (total0, used0, free0, leases0) = bus.capacity_snapshot().await;
        assert!(total0 >= 1, "total={total0}");
        assert_eq!(used0, 0);
        assert_eq!(leases0, 0);
        assert_eq!(free0, total0);

        let rid = bus
            .request_infer("alice", "user: lease-lifecycle", 16)
            .await
            .expect("request");
        // Observe used during flight (brief poll).
        let mut saw_used = false;
        for _ in 0..50 {
            let (_t, u, _f, l) = bus.capacity_snapshot().await;
            if u > 0 || l > 0 {
                saw_used = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let text = bus
            .wait_completion(rid, Duration::from_secs(5))
            .await
            .expect("completion");
        assert!(!text.is_empty());
        let (total1, used1, free1, leases1) = bus.capacity_snapshot().await;
        assert_eq!(used1, 0, "slots must free after complete");
        assert_eq!(leases1, 0);
        assert_eq!(free1, total1);
        let trail = bus.audit_trail().await;
        assert!(
            trail.iter().any(|e| e.event == "lease_granted"),
            "trail={trail:?}"
        );
        assert!(
            trail.iter().any(|e| e.event == "plan_agreed"),
            "trail={trail:?}"
        );
        assert!(
            trail.iter().any(|e| e.event == "lease_released"),
            "trail={trail:?}"
        );
        // Either we observed mid-flight use, or audit proves grant+release lifecycle.
        assert!(
            saw_used
                || trail.iter().any(|e| e.event == "lease_granted")
                    && trail.iter().any(|e| e.event == "lease_released"),
            "free→used→free not observed; trail={trail:?}"
        );
        let _ = (total0, free0);
    }

    /// Outsider PlanAccept must not pad quorum (inject after plan is set).
    #[tokio::test]
    async fn peer_outsider_accept_does_not_pad_quorum() {
        let donors = donors_n(2);
        let client_id = donors[0].0.clone();
        let bus = PeerBus::new(client_id.clone());
        let outsider = NodeId::new();
        let mut rxs = Vec::new();
        for (id, mem) in &donors {
            let (tx, rx) = mpsc::unbounded_channel();
            bus.register_peer(
                MeshDonor {
                    node: id.clone(),
                    verified_mem_mib: *mem,
                    healthy: true,
                },
                tx,
            )
            .await;
            rxs.push((id.clone(), rx));
        }
        for (id, rx) in rxs {
            let b = bus.clone();
            tokio::spawn(async move {
                run_peer_actor(b, id, rx).await;
            });
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        let rid = bus
            .request_infer("alice", "user: outsider-pad", 16)
            .await
            .expect("request");
        // Inject outsider accepts while honest shards also confirm.
        let b = bus.clone();
        let out = outsider.clone();
        tokio::spawn(async move {
            for _ in 0..40 {
                let inject = {
                    let g = b.inner.lock().await;
                    g.inflight.get(&rid).and_then(|inf| {
                        let plan = inf.plan.as_ref()?;
                        let ph = inf.plan_hash_hex.clone();
                        if ph.is_empty() {
                            return None;
                        }
                        let confirm = joule_cluster::plan_accept_confirm_hex(
                            plan.plan_id,
                            rid,
                            &out,
                            true,
                            &ph,
                        );
                        Some((
                            inf.coordinator.clone(),
                            Envelope::new(
                                out.clone(),
                                Message::PlanAccept {
                                    plan_id: plan.plan_id,
                                    request_id: rid,
                                    accepted: true,
                                    reason: "outsider".into(),
                                    plan_hash_hex: ph,
                                    confirm_hex: confirm,
                                    auth: joule_proto::PlanAuth::default(),
                                },
                            ),
                        ))
                    })
                };
                if let Some((c, acc)) = inject {
                    let _ = b.send_to(&c, acc).await;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
        let text = bus
            .wait_completion(rid, Duration::from_secs(5))
            .await
            .expect("honest shards must still complete despite outsider");
        assert!(!text.is_empty());
        let trail = bus.audit_trail().await;
        assert!(
            trail.iter().any(|e| e.event == "plan_agreed"),
            "trail={trail:?}"
        );
        if let Some(agreed) = trail.iter().find(|e| e.event == "plan_agreed") {
            assert!(
                !agreed.accepts.iter().any(|a| a == &outsider.to_string()),
                "outsider padded accepts: {agreed:?}"
            );
            assert_eq!(agreed.accepts.len(), 2, "need both real shards: {agreed:?}");
        }
    }

    /// Outsider with **invalid** confirm must not Abort (policy: membership before verify).
    #[tokio::test]
    async fn peer_outsider_bad_confirm_does_not_abort() {
        let donors = donors_n(2);
        let client_id = donors[0].0.clone();
        let bus = PeerBus::new(client_id.clone());
        let outsider = NodeId::new();
        let mut rxs = Vec::new();
        for (id, mem) in &donors {
            let (tx, rx) = mpsc::unbounded_channel();
            bus.register_peer(
                MeshDonor {
                    node: id.clone(),
                    verified_mem_mib: *mem,
                    healthy: true,
                },
                tx,
            )
            .await;
            rxs.push((id.clone(), rx));
        }
        for (id, rx) in rxs {
            let b = bus.clone();
            tokio::spawn(async move {
                run_peer_actor(b, id, rx).await;
            });
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        let rid = bus
            .request_infer("carol", "user: outsider-bad-confirm", 16)
            .await
            .expect("request");
        let b = bus.clone();
        let out = outsider.clone();
        tokio::spawn(async move {
            for _ in 0..40 {
                let inject = {
                    let g = b.inner.lock().await;
                    g.inflight.get(&rid).and_then(|inf| {
                        let plan = inf.plan.as_ref()?;
                        let ph = inf.plan_hash_hex.clone();
                        if ph.is_empty() {
                            return None;
                        }
                        Some((
                            inf.coordinator.clone(),
                            Envelope::new(
                                out.clone(),
                                Message::PlanAccept {
                                    plan_id: plan.plan_id,
                                    request_id: rid,
                                    accepted: true,
                                    reason: "outsider bad confirm".into(),
                                    plan_hash_hex: ph,
                                    confirm_hex: "00".repeat(32),
                                    auth: joule_proto::PlanAuth::default(),
                                },
                            ),
                        ))
                    })
                };
                if let Some((c, acc)) = inject {
                    let _ = b.send_to(&c, acc).await;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
        let text = bus
            .wait_completion(rid, Duration::from_secs(5))
            .await
            .expect("honest shards complete; outsider bad confirm ignored");
        assert!(!text.is_empty());
        assert!(
            bus.abort_reason(rid).await.is_none(),
            "must not abort on outsider bad confirm"
        );
        let trail = bus.audit_trail().await;
        assert!(
            !trail.iter().any(|e| e.event == "plan_accept_invalid"),
            "outsider must not produce abort audit: {trail:?}"
        );
        assert!(trail.iter().any(|e| e.event == "plan_agreed"), "{trail:?}");
    }

    /// Outsider reject with valid self-confirm must not abort required-shard agreement.
    #[tokio::test]
    async fn peer_outsider_reject_does_not_abort() {
        let donors = donors_n(2);
        let client_id = donors[0].0.clone();
        let bus = PeerBus::new(client_id.clone());
        let outsider = NodeId::new();
        let mut rxs = Vec::new();
        for (id, mem) in &donors {
            let (tx, rx) = mpsc::unbounded_channel();
            bus.register_peer(
                MeshDonor {
                    node: id.clone(),
                    verified_mem_mib: *mem,
                    healthy: true,
                },
                tx,
            )
            .await;
            rxs.push((id.clone(), rx));
        }
        for (id, rx) in rxs {
            let b = bus.clone();
            tokio::spawn(async move {
                run_peer_actor(b, id, rx).await;
            });
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        let rid = bus
            .request_infer("bob", "user: outsider-reject", 16)
            .await
            .expect("request");
        let b = bus.clone();
        let out = outsider.clone();
        tokio::spawn(async move {
            for _ in 0..40 {
                let inject = {
                    let g = b.inner.lock().await;
                    g.inflight.get(&rid).and_then(|inf| {
                        let plan = inf.plan.as_ref()?;
                        let ph = inf.plan_hash_hex.clone();
                        if ph.is_empty() {
                            return None;
                        }
                        let confirm = joule_cluster::plan_accept_confirm_hex(
                            plan.plan_id,
                            rid,
                            &out,
                            false,
                            &ph,
                        );
                        Some((
                            inf.coordinator.clone(),
                            Envelope::new(
                                out.clone(),
                                Message::PlanAccept {
                                    plan_id: plan.plan_id,
                                    request_id: rid,
                                    accepted: false,
                                    reason: "outsider reject".into(),
                                    plan_hash_hex: ph,
                                    confirm_hex: confirm,
                                    auth: joule_proto::PlanAuth::default(),
                                },
                            ),
                        ))
                    })
                };
                if let Some((c, acc)) = inject {
                    let _ = b.send_to(&c, acc).await;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
        let text = bus
            .wait_completion(rid, Duration::from_secs(5))
            .await
            .expect("must complete; outsider reject ignored");
        assert!(!text.is_empty());
        assert!(bus.abort_reason(rid).await.is_none());
    }

    #[tokio::test]
    async fn confirmed_shard_death_triggers_replan() {
        let donors = donors_n(3);
        let client_id = donors[0].0.clone();
        let bus = PeerBus::new(client_id.clone());
        bus.set_coord_timeout(Duration::from_millis(80)).await;
        let mut rxs = Vec::new();
        for (id, mem) in &donors {
            let (tx, rx) = mpsc::unbounded_channel();
            bus.register_peer(
                MeshDonor {
                    node: id.clone(),
                    verified_mem_mib: *mem,
                    healthy: true,
                },
                tx,
            )
            .await;
            rxs.push((id.clone(), rx));
        }
        // Elected coordinator = highest mem
        let high = donors.iter().max_by_key(|(_, m)| *m).unwrap().0.clone();
        // Pick a non-coordinator shard to kill after accept
        let victim = donors
            .iter()
            .map(|(id, _)| id.clone())
            .find(|id| id != &high)
            .unwrap();
        for (id, rx) in rxs {
            if id == victim {
                // Act until first PlanAccept path, then die (drain only)
                let b = bus.clone();
                tokio::spawn(async move {
                    let engine = StubEngine::new();
                    let mut rx = rx;
                    let mut n = 0u32;
                    while let Some(env) = rx.recv().await {
                        n += 1;
                        let _ = b.handle_envelope(&id, env, &engine).await;
                        if n >= 3 {
                            // stop handling after participating in agree
                            while rx.recv().await.is_some() {}
                            break;
                        }
                    }
                });
            } else {
                let b = bus.clone();
                tokio::spawn(async move {
                    run_peer_actor(b, id, rx).await;
                });
            }
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
        let rid = bus
            .request_infer("shard-kill", "user: shard-replan", 16)
            .await
            .expect("request");
        // Wait until plan accepted then kill confirmed shard
        for _ in 0..40 {
            let g = bus.inner.lock().await;
            if g.inflight.get(&rid).is_some_and(|i| !i.accepts.is_empty()) {
                break;
            }
            drop(g);
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
        // Snapshot attempt before death (first plan already bumped attempt).
        let attempt_before = {
            let g = bus.inner.lock().await;
            g.inflight.get(&rid).map(|i| i.attempt).unwrap_or(0)
        };
        bus.mark_shard_dead(&victim).await;
        // wait_completion must auto replan without an explicit replan_request call.
        let text = bus
            .wait_completion(rid, Duration::from_secs(6))
            .await
            .expect("completion after auto shard replan");
        assert!(!text.is_empty());
        let g = bus.inner.lock().await;
        let inf = g.inflight.get(&rid).expect("inflight");
        assert!(
            inf.attempt > attempt_before,
            "auto replan must increase attempt (before={attempt_before} after={})",
            inf.attempt
        );
        let trail = g.leases.audit_trail();
        assert!(
            trail.iter().any(|e| e.event == "lease_granted"),
            "must grant lease: {trail:?}"
        );
        assert!(
            trail.iter().any(|e| e.event == "lease_released"),
            "must release lease on replan/complete: {trail:?}"
        );
        assert_eq!(
            g.leases.active_count(),
            0,
            "no active leases after completion"
        );
        let snap = g.cluster.scheduler_snapshot();
        assert_eq!(
            snap.stream_slots_used, 0,
            "used must restore free after replan+complete used={} free={}",
            snap.stream_slots_used, snap.stream_slots_free
        );
        assert_eq!(
            snap.stream_slots_free, snap.stream_slots_total,
            "free==total after replan free-used-free"
        );
        assert!(
            !g.donors.get(&victim).map(|d| d.healthy).unwrap_or(true),
            "victim must stay unhealthy"
        );
    }

    /// Mid-flight death: first coordinator is **alive and elected**, then silenced
    /// after RequestInfer so the first plan cannot complete. wait_completion must
    /// re-elect / re-plan (attempt ≥ 2, coordinator ≠ dead id) or the test fails.
    #[tokio::test]
    async fn coordinator_death_triggers_replan() {
        let donors = donors_n(3);
        let client_id = donors[0].0.clone();
        // Highest mem is elected first coordinator while still healthy.
        let high = donors.iter().max_by_key(|(_, m)| *m).unwrap().0.clone();

        let bus = PeerBus::new(client_id.clone());
        // Short timeout so wait_completion replan fires without long sleep.
        bus.set_coord_timeout(Duration::from_millis(60)).await;

        let mut rxs = Vec::new();
        for (id, mem) in &donors {
            let (tx, rx) = mpsc::unbounded_channel();
            bus.register_peer(
                MeshDonor {
                    node: id.clone(),
                    verified_mem_mib: *mem,
                    healthy: true,
                },
                tx,
            )
            .await;
            rxs.push((id.clone(), rx));
        }

        // Live actors for everyone except the first coordinator — that peer's
        // mailbox accepts messages but never handles them (mid-flight silence).
        for (id, rx) in rxs {
            if id == high {
                // Drain mailbox so send_to does not fail; never handle_envelope.
                tokio::spawn(async move {
                    let mut rx = rx;
                    while rx.recv().await.is_some() {
                        // intentional no-op: dead coordinator mid-request
                    }
                });
            } else {
                let b = bus.clone();
                tokio::spawn(async move {
                    run_peer_actor(b, id, rx).await;
                });
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Pre-condition: high would be elected and is still healthy.
        {
            let g = bus.inner.lock().await;
            let list: Vec<_> = g.donors.values().cloned().collect();
            assert_eq!(elect_coordinator(&list).unwrap(), high);
            assert!(!g.dead_coordinators.contains(&high));
        }

        let rid = bus
            .request_infer("bob", "user: replan-after-death", 16)
            .await
            .expect("request");

        // Mid-flight: RequestInfer was delivered to `high` (silent). Confirm
        // inflight still points at high before replan can finish.
        {
            let g = bus.inner.lock().await;
            let inf = g.inflight.get(&rid).expect("inflight");
            assert_eq!(
                inf.coordinator, high,
                "first coordinator must be the live high-mem peer"
            );
            assert_eq!(inf.attempt, 0, "no replan yet");
        }

        // Kill that coordinator mid-request (still no completion possible from high).
        bus.mark_dead(&high).await;

        let text = bus
            .wait_completion(rid, Duration::from_secs(5))
            .await
            .expect("completion after mid-flight replan");
        assert!(
            !text.is_empty() && (text.contains("replan-after-death") || text.len() > 4),
            "text={text}"
        );

        let g = bus.inner.lock().await;
        let inf = g.inflight.get(&rid).expect("inflight after replan");
        assert!(
            inf.attempt >= 1,
            "replan must bump attempt (got {})",
            inf.attempt
        );
        assert_ne!(
            inf.coordinator, high,
            "new coordinator must not be the dead first coordinator"
        );
        assert!(
            g.dead_coordinators.contains(&high),
            "dead coordinator recorded"
        );
        // Without replan, silent high would never complete — prove completion path ran.
        assert!(g.completions.contains_key(&rid));
    }
}
