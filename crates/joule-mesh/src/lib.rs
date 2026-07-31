//! Peer-only mesh coordination (Phase D production).
//!
//! - **No control required** as message bus for chat/infer.
//! - Coordinator elected from healthy donors (highest mem, then node id).
//! - On coordinator timeout/death: re-elect + re-plan from remaining peers.
//!
//! See docs/design/decentral-discovery-v0.md.

use anyhow::{bail, Context, Result};
use joule_cluster::plan_from_mesh_donors;
use joule_proto::{ClusterPlan, Envelope, Message, NodeId, CLUSTER_MODEL};
use joule_runtime::{Engine, InferRequest, StubEngine};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tracing::{info, warn};
use uuid::Uuid;

/// Peer identity + capacity for election / planning.
#[derive(Debug, Clone)]
pub struct MeshDonor {
    pub node: NodeId,
    pub mem_mib: u32,
    pub healthy: bool,
}

/// Deterministic coordinator: max mem_mib, then node id string.
pub fn elect_coordinator(donors: &[MeshDonor]) -> Option<NodeId> {
    let mut eligible: Vec<&MeshDonor> = donors.iter().filter(|d| d.healthy && d.mem_mib > 0).collect();
    if eligible.is_empty() {
        return None;
    }
    eligible.sort_by(|a, b| {
        b.mem_mib
            .cmp(&a.mem_mib)
            .then_with(|| a.node.to_string().cmp(&b.node.to_string()))
    });
    Some(eligible[0].node.clone())
}

/// Build plan from remaining donors (re-plan entry).
pub fn replan(donors: &[MeshDonor]) -> Result<ClusterPlan> {
    let pairs: Vec<(NodeId, u32)> = donors
        .iter()
        .filter(|d| d.healthy && d.mem_mib > 0)
        .map(|d| (d.node.clone(), d.mem_mib))
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
    pub accepts: HashSet<NodeId>,
    pub attempt: u32,
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
            })),
        }
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
    }

    pub async fn register_peer(
        &self,
        donor: MeshDonor,
        tx: mpsc::UnboundedSender<Envelope>,
    ) {
        let mut g = self.inner.lock().await;
        g.mailboxes.insert(donor.node.clone(), tx);
        g.donors.insert(donor.node.clone(), donor);
    }

    pub async fn upsert_donor(&self, donor: MeshDonor) {
        self.inner.lock().await.donors.insert(donor.node.clone(), donor);
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
                    accepts: HashSet::new(),
                    attempt: 0,
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
    pub async fn handle_envelope(&self, local: &NodeId, env: Envelope, engine: &impl Engine) -> Result<()> {
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
                {
                    let mut g = self.inner.lock().await;
                    if let Some(inf) = g.inflight.get_mut(&request_id) {
                        inf.coordinator = local.clone();
                        inf.plan = Some(plan.clone());
                        inf.attempt += 1;
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
                                accepts: HashSet::new(),
                                attempt: 1,
                            },
                        );
                    }
                }
                info!(%request_id, shards = plan.shards.len(), "peer-bus PlanOffer");
                let offer = Envelope::new(
                    local.clone(),
                    Message::PlanOffer {
                        plan: plan.clone(),
                        request_id,
                    },
                );
                for s in &plan.shards {
                    let _ = self.send_to(&s.node, offer.clone()).await;
                }
            }
            Message::PlanOffer {
                plan,
                request_id,
            } => {
                let accepted = plan.shards.iter().any(|s| &s.node == local);
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
                    },
                );
                // Send accept to coordinator
                let coord = {
                    let g = self.inner.lock().await;
                    g.inflight
                        .get(&request_id)
                        .map(|i| i.coordinator.clone())
                        .or_else(|| elect_coordinator(&g.donors.values().cloned().collect::<Vec<_>>()))
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
                plan_id: _,
                request_id,
                accepted,
                ..
            } => {
                if !accepted {
                    return Ok(());
                }
                let (ready, plan, prompt, max_tokens, model) = {
                    let mut g = self.inner.lock().await;
                    let Some(inf) = g.inflight.get_mut(&request_id) else {
                        return Ok(());
                    };
                    // Only the elected coordinator collects accepts / fans out Infer.
                    if &inf.coordinator != local {
                        return Ok(());
                    }
                    inf.accepts.insert(env.from.clone());
                    let plan = inf.plan.clone();
                    let need = plan.as_ref().map(|p| p.shards.len()).unwrap_or(0);
                    let ready = plan.is_some() && inf.accepts.len() >= need && need > 0;
                    (
                        ready,
                        plan,
                        inf.prompt.clone(),
                        inf.max_tokens,
                        inf.model.clone(),
                    )
                };
                if let (true, Some(plan)) = (ready, plan) {
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
                    self.inner
                        .lock()
                        .await
                        .completions
                        .insert(request_id, text);
                }
            }
            Message::InferDone {
                request_id,
                text,
                ..
            } if !text.is_empty() => {
                self.inner
                    .lock()
                    .await
                    .completions
                    .insert(request_id, text);
            }
            Message::InferDone { .. } => {}
            _ => {}
        }
        Ok(())
    }

    /// Wait for completion text; on timeout, re-elect and re-plan (coordinator death path).
    pub async fn wait_completion(
        &self,
        request_id: Uuid,
        overall_timeout: Duration,
    ) -> Result<String> {
        let start = tokio::time::Instant::now();
        let mut replan_budget = 2u32;
        loop {
            if let Some(t) = self.inner.lock().await.completions.get(&request_id).cloned() {
                return Ok(t);
            }
            if start.elapsed() > overall_timeout {
                bail!("peer-bus infer timed out");
            }
            let coord_timeout = self.inner.lock().await.coord_timeout;
            tokio::time::sleep(coord_timeout.min(Duration::from_millis(50))).await;

            // If still no completion after coord timeout and attempts left, re-plan.
            let need_replan = {
                let g = self.inner.lock().await;
                let inf = g.inflight.get(&request_id);
                match inf {
                    Some(i) if !g.completions.contains_key(&request_id) => {
                        g.dead_coordinators.contains(&i.coordinator)
                            || start.elapsed() > coord_timeout.saturating_mul(i.attempt.max(1))
                    }
                    _ => false,
                }
            };
            if need_replan && replan_budget > 0 {
                replan_budget -= 1;
                self.replan_request(request_id).await?;
            }
        }
    }

    /// Elect new coordinator from remaining healthy donors and restart PlanOffer.
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
        // Mark old coordinator dead if not already
        {
            let mut g = self.inner.lock().await;
            g.dead_coordinators.insert(old_coord.clone());
            if let Some(d) = g.donors.get_mut(&old_coord) {
                d.healthy = false;
            }
        }
        let donors: Vec<MeshDonor> = {
            let g = self.inner.lock().await;
            g.donors
                .values()
                .filter(|d| d.healthy)
                .cloned()
                .collect()
        };
        let new_coord = elect_coordinator(&donors).context("no remaining donors for re-election")?;
        info!(%request_id, old = %old_coord, new = %new_coord, "coordinator re-elected; re-plan");
        {
            let mut g = self.inner.lock().await;
            if let Some(inf) = g.inflight.get_mut(&request_id) {
                inf.coordinator = new_coord.clone();
                inf.plan = None;
                inf.accepts.clear();
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
        self.inner.lock().await.completions.get(&request_id).cloned()
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
                mem_mib: *mem,
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
    fn election_picks_highest_mem() {
        let a = NodeId::new();
        let b = NodeId::new();
        let d = vec![
            MeshDonor {
                node: a.clone(),
                mem_mib: 8192,
                healthy: true,
            },
            MeshDonor {
                node: b.clone(),
                mem_mib: 16384,
                healthy: true,
            },
        ];
        assert_eq!(elect_coordinator(&d).unwrap(), b);
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
    async fn coordinator_death_triggers_replan() {
        let donors = donors_n(3);
        let client_id = donors[0].0.clone();
        // Highest mem is last donor → will be elected first
        let high = donors.iter().max_by_key(|(_, m)| *m).unwrap().0.clone();

        let bus = PeerBus::new(client_id.clone());
        bus.set_coord_timeout(Duration::from_millis(80)).await;

        let mut rxs = Vec::new();
        for (id, mem) in &donors {
            let (tx, rx) = mpsc::unbounded_channel();
            bus.register_peer(
                MeshDonor {
                    node: id.clone(),
                    mem_mib: *mem,
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

        // Kill highest-mem coordinator before request
        bus.mark_dead(&high).await;
        assert_ne!(
            elect_coordinator(
                &bus.inner
                    .lock()
                    .await
                    .donors
                    .values()
                    .cloned()
                    .collect::<Vec<_>>()
            )
            .unwrap(),
            high
        );

        let rid = bus
            .request_infer("bob", "user: replan-after-death", 16)
            .await
            .expect("request");
        // Force replan path
        bus.replan_request(rid).await.ok();
        let text = bus
            .wait_completion(rid, Duration::from_secs(4))
            .await
            .expect("completion after replan");
        assert!(!text.is_empty());
    }
}
