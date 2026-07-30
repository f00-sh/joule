//! TCP agent protocol: newline-delimited JSON envelopes.

use crate::app::{AgentRoutes, App};
use crate::state::{InferOutcome, PendingChallenge, PendingInfer};
use anyhow::{Context, Result};
use joule_proto::{
    decode_line, encode_line, resolve_cluster_model, Envelope, Message, NodeId, CLUSTER_MODEL,
};
use joule_runtime::{Engine, InferRequest, StubEngine};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};
use uuid::Uuid;

pub async fn run_agent_listener(app: App, listener: TcpListener) -> Result<()> {
    loop {
        let (sock, peer) = listener.accept().await?;
        let app = app.clone();
        tokio::spawn(async move {
            if let Err(e) = run_agent_session(app, sock).await {
                warn!(%peer, error = %e, "agent session ended");
            }
        });
    }
}

pub async fn run_agent_session(app: App, sock: TcpStream) -> Result<()> {
    let peer = sock.peer_addr().ok();
    let (reader, mut writer) = sock.into_split();
    let mut lines = BufReader::new(reader).lines();

    let (tx, mut rx) = mpsc::unbounded_channel::<Envelope>();
    let write_task = tokio::spawn(async move {
        while let Some(env) = rx.recv().await {
            let bytes = match encode_line(&env) {
                Ok(b) => b,
                Err(e) => {
                    warn!(error = %e, "encode failed");
                    continue;
                }
            };
            if writer.write_all(&bytes).await.is_err() {
                break;
            }
        }
    });

    let mut node_id: Option<NodeId> = None;

    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let env: Envelope = match decode_line(line.as_bytes()) {
            Ok(e) => e,
            Err(e) => {
                warn!(error = %e, "bad agent line");
                continue;
            }
        };

        match env.msg {
            Message::Hello { account, caps } => {
                if account.trim().is_empty() {
                    let err = Envelope::new(
                        env.from.clone(),
                        Message::Error {
                            error: "account required".into(),
                        },
                    );
                    let _ = tx.send(err);
                    continue;
                }
                let id = env.from.clone();
                let api_key = {
                    let mut g = app.state.write().await;
                    let key = g.register_node(id.clone(), &account, caps);
                    crate::edge::publish_snapshot_async(&g, Some(&app.identity), true);
                    key
                };
                {
                    let mut r = app.routes.lock().await;
                    r.insert(id.clone(), tx.clone());
                }
                node_id = Some(id.clone());
                info!(%id, %account, ?peer, "agent joined");
                let welcome = Envelope::new(
                    id.clone(),
                    Message::Welcome {
                        account: account.clone(),
                        api_key,
                    },
                );
                let _ = tx.send(welcome);
                // Catch-up: flood recent operator envelopes (already verified/deduped).
                let recent = {
                    let g = app.state.read().await;
                    g.broadcasts.recent().to_vec()
                };
                for env in recent.iter() {
                    let out = Envelope::new(
                        id.clone(),
                        Message::OperatorBroadcast {
                            envelope: env.clone(),
                        },
                    );
                    let _ = tx.send(out);
                }
                // Re-plan model digests so late joiners get a share of the mesh
                // (not full model — redundant placement includes them).
                if let Some(mu) = recent
                    .iter()
                    .rev()
                    .find(|e| e.kind == joule_proto::OperatorKind::ModelUpdate)
                {
                    crate::model_update::apply_model_update(&app, mu).await;
                } else {
                    crate::model_update::rebalance_replicas(&app).await;
                }
            }
            Message::Heartbeat { load, healthy } => {
                let id = env.from.clone();
                let mut g = app.state.write().await;
                match g.on_heartbeat(&id, load, healthy) {
                    Ok(Some(mint)) => {
                        crate::edge::publish_snapshot_async(&g, Some(&app.identity), false);
                        tracing::debug!(%id, mint, "heartbeat mint");
                    }
                    Ok(None) => {}
                    Err(e) => {
                        let err = Envelope::new(
                            id,
                            Message::Error {
                                error: e.to_string(),
                            },
                        );
                        let _ = tx.send(err);
                    }
                }
            }
            Message::PeerAlive {
                multiaddrs,
                load,
                healthy,
                blob_count,
                mem_mib,
                throughput_class,
            } => {
                let id = env.from.clone();
                {
                    let mut g = app.state.write().await;
                    g.mesh.upsert(
                        id.clone(),
                        multiaddrs.clone(),
                        load,
                        healthy,
                        blob_count,
                        mem_mib,
                        throughput_class,
                    );
                    // Phase C: mirror into DHT peer/<id> (seq = wall ms for LWW).
                    let seq = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0);
                    g.dht.put_peer(joule_dht::PeerRecord {
                        node_id: id.to_string(),
                        multiaddrs: multiaddrs.clone(),
                        load_milli: (load * 1000.0) as u32,
                        healthy,
                        blob_count,
                        seq,
                        updated_unix_ms: seq,
                    });
                }
                // Gossip to other agents (control as temporary flood hub).
                let routes = app.routes.lock().await;
                let msg = Message::PeerAlive {
                    multiaddrs,
                    load,
                    healthy,
                    blob_count,
                    mem_mib,
                    throughput_class,
                };
                for (node, peer_tx) in routes.iter() {
                    if node != &id {
                        let _ = peer_tx.send(Envelope::new(node.clone(), msg.clone()));
                    }
                }
                tracing::debug!(%id, "PeerAlive mesh update + gossip");
            }
            Message::BlobsHave { blobs } => {
                let id = env.from.clone();
                let need_rebalance = {
                    let mut g = app.state.write().await;
                    // Phase C DHT blob/<sha> seeders.
                    for b in &blobs {
                        g.dht.put_blob_seeder(
                            &b.sha256,
                            &id.to_string(),
                            b.size,
                            b.multiaddrs.clone(),
                        );
                    }
                    // Full inventory replace from agent (authoritative local scan).
                    g.blobs.announce(id.clone(), blobs);
                    tracing::debug!(%id, "blob inventory updated");
                    if g.active_chunks.is_empty() {
                        false
                    } else {
                        let now = Instant::now();
                        let due = g
                            .last_rebalance
                            .map(|t| now.duration_since(t) >= Duration::from_secs(10))
                            .unwrap_or(true);
                        if due {
                            g.last_rebalance = Some(now);
                        }
                        due
                    }
                };
                if need_rebalance {
                    // New seed may heal under-replication (rate-limited).
                    crate::model_update::rebalance_replicas(&app).await;
                }
            }
            Message::BlobWant { sha256 } => {
                let requester = env.from.clone();
                let hash = sha256.to_lowercase();
                let g = app.state.read().await;
                let peers = g.blobs.peers_for(&hash);
                let mut peer_ids = Vec::new();
                let mut sizes = Vec::new();
                let mut multiaddrs = Vec::new();
                for (n, m) in peers {
                    let addrs = if m.multiaddrs.is_empty() {
                        g.mesh.multiaddrs_for(&n)
                    } else {
                        m.multiaddrs.clone()
                    };
                    peer_ids.push(n);
                    sizes.push(m.size);
                    multiaddrs.push(addrs);
                }
                let seeder = g.blobs.pick_seeder(&hash, &requester);
                drop(g);

                let locate = Envelope::new(
                    requester.clone(),
                    Message::BlobLocate {
                        sha256: hash.clone(),
                        peers: peer_ids,
                        sizes,
                        multiaddrs,
                    },
                );
                let _ = tx.send(locate);

                // Orchestrate transfer: ask seeder to push chunks to control → requester.
                if let Some((seeder_id, _meta)) = seeder {
                    let request_id = Uuid::new_v4();
                    {
                        let mut g = app.state.write().await;
                        // Cap concurrent control-relayed transfers (lab path).
                        if g.pending_blob_xfers.len() >= 64 {
                            warn!("BlobWant: too many in-flight transfers");
                            continue;
                        }
                        g.pending_blob_xfers.insert(
                            request_id,
                            (requester.clone(), hash.clone(), std::time::Instant::now()),
                        );
                    }
                    let routes = app.routes.lock().await;
                    if let Some(stx) = routes.get(&seeder_id) {
                        let provide = Envelope::new(
                            seeder_id.clone(),
                            Message::BlobProvide {
                                sha256: hash.clone(),
                                request_id,
                                to: requester,
                            },
                        );
                        let _ = stx.send(provide);
                        tracing::debug!(%seeder_id, %hash, %request_id, "blob provide requested");
                    }
                } else {
                    tracing::debug!(%hash, %requester, "BlobWant: no seeder yet");
                }
            }
            Message::BlobChunk {
                sha256,
                request_id,
                offset,
                data_b64,
                done,
            } => {
                // Forward chunk from seeder to requester.
                let dest = {
                    let g = app.state.read().await;
                    g.pending_blob_xfers
                        .get(&request_id)
                        .map(|(n, h, _)| (n.clone(), h.clone()))
                };
                if let Some((to, want_hash)) = dest {
                    let got = sha256.to_lowercase();
                    if want_hash.to_lowercase() != got {
                        warn!(%request_id, "blob chunk hash mismatch for pending xfer");
                    } else {
                        let routes = app.routes.lock().await;
                        if let Some(rtx) = routes.get(&to) {
                            let fwd = Envelope::new(
                                to.clone(),
                                Message::BlobChunk {
                                    sha256: got,
                                    request_id,
                                    offset,
                                    data_b64,
                                    done,
                                },
                            );
                            let _ = rtx.send(fwd);
                        }
                        if done {
                            let mut g = app.state.write().await;
                            g.pending_blob_xfers.remove(&request_id);
                        }
                    }
                }
            }
            Message::OperatorBroadcast { envelope } => {
                let accept = {
                    let mut g = app.state.write().await;
                    let now = crate::broadcast::now_ms();
                    g.broadcasts.accept(envelope.clone(), now)
                };
                match accept {
                    Ok(true) => {
                        info!(id = %envelope.id, "operator broadcast accepted — flooding agents");
                        let routes = app.routes.lock().await;
                        let msg = Message::OperatorBroadcast {
                            envelope: envelope.clone(),
                        };
                        for (node, peer_tx) in routes.iter() {
                            let out = Envelope::new(node.clone(), msg.clone());
                            let _ = peer_tx.send(out);
                        }
                        drop(routes);
                        // Allow-listed operator actions (model/software digests, pause, policy…).
                        crate::operator_actions::apply_operator_actions(&app, &envelope).await;
                    }
                    Ok(false) => {
                        tracing::debug!(id = %envelope.id, "operator broadcast duplicate");
                    }
                    Err(e) => {
                        warn!(error = %e, "operator broadcast rejected");
                        let err = Envelope::new(
                            env.from.clone(),
                            Message::Error {
                                error: format!("broadcast rejected: {e}"),
                            },
                        );
                        let _ = tx.send(err);
                    }
                }
            }
            Message::InferDone {
                request_id,
                text,
                prompt_tokens,
                completion_tokens,
                shard_ok: _,
            } => {
                let mut g = app.state.write().await;
                // Tail detection: pending plan last shard or non-empty text.
                let is_tail = g
                    .pending
                    .get(&request_id)
                    .and_then(|p| p.plan.shards.last())
                    .map(|s| s.node == env.from)
                    .unwrap_or(!text.is_empty());
                g.settle_shard_success(
                    request_id,
                    text,
                    prompt_tokens,
                    completion_tokens,
                    &env.from,
                    is_tail,
                );
            }
            Message::InferError { request_id, error } => {
                let mut g = app.state.write().await;
                g.settle_infer_error(request_id, error);
            }
            Message::ChallengeResult {
                challenge_id,
                completion,
                latency_ms,
            } => {
                let mut g = app.state.write().await;
                if let Some(ok) = g.settle_challenge_result(challenge_id, completion, &env.from) {
                    info!(%challenge_id, ok, latency_ms, "challenge settled");
                }
            }
            Message::PrepareOk {
                model,
                quant,
                armed,
                files_complete,
                message,
            } => {
                info!(
                    %model,
                    %quant,
                    armed,
                    files_complete,
                    from = %env.from,
                    "{message}"
                );
            }
            Message::ModelLoaded {
                model,
                quant,
                bytes_resident,
                tensors,
                message,
            } => {
                info!(
                    %model,
                    %quant,
                    bytes_resident,
                    tensors,
                    from = %env.from,
                    "{message}"
                );
                app.state.write().await.mark_node_loaded(env.from.clone());
            }
            other => {
                warn!(msg = ?other, "ignored agent→control message");
            }
        }
    }

    if let Some(id) = node_id {
        app.routes.lock().await.remove(&id);
        app.state.write().await.remove_node(&id);
        info!(%id, "agent left");
    }
    write_task.abort();
    Ok(())
}

async fn send_to_agent(routes: &AgentRoutes, node: &NodeId, env: Envelope) -> bool {
    let r = routes.lock().await;
    if let Some(q) = r.get(node) {
        q.send(env).is_ok()
    } else {
        false
    }
}

/// Dispatch one generation across the **VRAM-sharded pool** (all healthy donors).
///
/// One request is spread over aggregate pool memory (e.g. 8+16×4 GiB), not parked
/// exclusively on a single GPU. Concurrent users share stream slots on that mesh.
pub async fn dispatch_infer(
    app: &App,
    account: &str,
    model: &str,
    prompt: &str,
    max_tokens: u32,
) -> Result<InferOutcome, String> {
    let model = resolve_cluster_model(Some(model))?.to_string();

    {
        let g = app.state.read().await;
        if !g.cluster.account_is_donating(account) {
            return Err(
                "contribution required: run `joule agent` for this account before using the API"
                    .into(),
            );
        }
    }

    let plan = acquire_stream_with_wait(app, Duration::from_secs(20)).await?;
    let request_id = Uuid::new_v4();
    let (tx, rx) = oneshot::channel();

    let awaiting: std::collections::HashSet<NodeId> =
        plan.shards.iter().map(|s| s.node.clone()).collect();
    let tail = plan
        .shards
        .last()
        .map(|s| s.node.clone())
        .ok_or_else(|| "empty shard plan".to_string())?;

    {
        let mut g = app.state.write().await;
        g.pending.insert(
            request_id,
            PendingInfer {
                account: account.to_string(),
                plan: plan.clone(),
                awaiting,
                tail_text: None,
                prompt_tokens: 0,
                completion_tokens: 0,
                charge: true,
                tx: Some(tx),
            },
        );
    }

    info!(
        %request_id,
        shards = plan.shards.len(),
        pool_mem_mib = plan.pool_mem_mib,
        "dispatching sharded infer across pool"
    );

    // Fan-out: every shard runs its slice; tail produces user-visible tokens (stub).
    let mut sent = 0usize;
    for shard in &plan.shards {
        let is_tail = shard.node == tail;
        let env = Envelope::new(
            shard.node.clone(),
            Message::InferRequest {
                request_id,
                model: model.clone(),
                prompt: prompt.to_string(),
                max_tokens,
                plan: plan.clone(),
                is_tail,
            },
        );
        if send_to_agent(&app.routes, &shard.node, env).await {
            sent += 1;
        } else {
            warn!(node = %shard.node, "shard agent not connected");
        }
    }

    if sent == 0 {
        let mut g = app.state.write().await;
        g.pending.remove(&request_id);
        g.cluster.release_stream(&plan);
        g.wake_scheduler();
        return Err("no connected agents for sharded plan".into());
    }

    // If some shards offline, drop them from awaiting so we don't hang.
    if sent < plan.shards.len() {
        let connected: std::collections::HashSet<_> =
            app.routes.lock().await.keys().cloned().collect();
        let mut g = app.state.write().await;
        if let Some(p) = g.pending.get_mut(&request_id) {
            p.awaiting.retain(|n| connected.contains(n));
            if p.awaiting.is_empty() {
                g.pending.remove(&request_id);
                g.cluster.release_stream(&plan);
                g.wake_scheduler();
                return Err("all shards disconnected mid-dispatch".into());
            }
        }
    }

    let first = match tokio::time::timeout(Duration::from_secs(45), rx).await {
        Ok(Ok(Ok(outcome))) => outcome,
        Ok(Ok(Err(e))) => return Err(e),
        Ok(Err(_)) => return Err("infer channel closed".into()),
        Err(_) => {
            let mut g = app.state.write().await;
            if let Some(mut p) = g.pending.remove(&request_id) {
                g.cluster.release_stream(&p.plan);
                g.wake_scheduler();
                let _ = p.tx.take();
            }
            return Err("sharded infer timed out".into());
        }
    };

    // Optional dual-verify: second full pool pass; log mismatch (tensor decode may differ).
    let dual = {
        let mut g = app.state.write().await;
        g.should_dual_verify()
    };
    if dual {
        match Box::pin(dispatch_infer_once(
            app, account, &model, prompt, max_tokens,
        ))
        .await
        {
            Ok(second) => {
                if second.text.trim() != first.text.trim() {
                    warn!(
                        first_len = first.text.len(),
                        second_len = second.text.len(),
                        "dual_verify text mismatch (accepted primary; tensors may be non-deterministic)"
                    );
                } else {
                    info!("dual_verify matched");
                }
            }
            Err(e) => warn!(error = %e, "dual_verify second pass failed"),
        }
    }
    Ok(first)
}

/// One sharded infer without dual-verify recursion.
async fn dispatch_infer_once(
    app: &App,
    account: &str,
    model: &str,
    prompt: &str,
    max_tokens: u32,
) -> Result<InferOutcome, String> {
    let model = resolve_cluster_model(Some(model))?.to_string();
    {
        let g = app.state.read().await;
        if !g.cluster.account_is_donating(account) {
            return Err(
                "contribution required: run `joule agent` for this account before using the API"
                    .into(),
            );
        }
    }
    let plan = acquire_stream_with_wait(app, Duration::from_secs(20)).await?;
    let request_id = Uuid::new_v4();
    let (tx, rx) = oneshot::channel();
    let awaiting: std::collections::HashSet<NodeId> =
        plan.shards.iter().map(|s| s.node.clone()).collect();
    let tail = plan
        .shards
        .last()
        .map(|s| s.node.clone())
        .ok_or_else(|| "empty shard plan".to_string())?;
    {
        let mut g = app.state.write().await;
        g.pending.insert(
            request_id,
            PendingInfer {
                account: account.to_string(),
                plan: plan.clone(),
                awaiting,
                tail_text: None,
                prompt_tokens: 0,
                completion_tokens: 0,
                charge: false, // dual pass does not double-charge
                tx: Some(tx),
            },
        );
    }
    let mut sent = 0usize;
    for shard in &plan.shards {
        let is_tail = shard.node == tail;
        let env = Envelope::new(
            shard.node.clone(),
            Message::InferRequest {
                request_id,
                model: model.clone(),
                prompt: prompt.to_string(),
                max_tokens,
                plan: plan.clone(),
                is_tail,
            },
        );
        if send_to_agent(&app.routes, &shard.node, env).await {
            sent += 1;
        }
    }
    if sent == 0 {
        let mut g = app.state.write().await;
        g.pending.remove(&request_id);
        g.cluster.release_stream(&plan);
        g.wake_scheduler();
        return Err("no connected agents for dual_verify".into());
    }
    match tokio::time::timeout(Duration::from_secs(45), rx).await {
        Ok(Ok(Ok(outcome))) => Ok(outcome),
        Ok(Ok(Err(e))) => Err(e),
        Ok(Err(_)) => Err("dual_verify channel closed".into()),
        Err(_) => {
            let mut g = app.state.write().await;
            if let Some(mut p) = g.pending.remove(&request_id) {
                g.cluster.release_stream(&p.plan);
                g.wake_scheduler();
                let _ = p.tx.take();
            }
            Err("dual_verify timed out".into())
        }
    }
}

/// Wait until a shared stream can be reserved on the whole mesh.
async fn acquire_stream_with_wait(
    app: &App,
    timeout: Duration,
) -> Result<joule_proto::ClusterPlan, String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        {
            let mut g = app.state.write().await;
            if g.cluster.pool_size() == 0 {
                return Err(format!(
                    "no healthy workers for cluster model {CLUSTER_MODEL}"
                ));
            }
            if let Some(plan) = g.cluster.try_acquire_stream() {
                return Ok(plan);
            }
        }

        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for free stream capacity on {CLUSTER_MODEL} pool"
            ));
        }

        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let wait = remaining.min(Duration::from_millis(100));
        tokio::select! {
            _ = app.schedule_notify.notified() => {}
            _ = tokio::time::sleep(wait) => {}
        }
    }
}

/// Background: send spot challenges to donors.
pub async fn challenge_loop(app: App) {
    let mut tick = tokio::time::interval(Duration::from_secs(12));
    loop {
        tick.tick().await;
        let challenge = {
            let mut g = app.state.write().await;
            g.prune();
            let target = g.cluster.pick_challenge_target().map(|n| n.id.clone());
            if let Some(id) = target {
                let challenge_id = Uuid::new_v4();
                let model = CLUSTER_MODEL.to_string();
                let prompt = format!("joule-challenge:{challenge_id}");
                let expected = StubEngine::expected_text(&model, &prompt);
                g.pending_challenges.insert(
                    challenge_id,
                    PendingChallenge {
                        node: id.clone(),
                        model: model.clone(),
                        prompt: prompt.clone(),
                        expected,
                        started: Instant::now(),
                    },
                );
                Some((id, model, prompt, challenge_id))
            } else {
                None
            }
        };
        let Some((node, model, prompt, challenge_id)) = challenge else {
            continue;
        };

        let env = Envelope::new(
            node.clone(),
            Message::Challenge {
                challenge_id,
                model,
                prompt,
            },
        );
        if !send_to_agent(&app.routes, &node, env).await {
            let mut g = app.state.write().await;
            g.pending_challenges.remove(&challenge_id);
            g.cluster.record_challenge_fail(&node);
            warn!(%node, "challenge send failed; recorded fail");
        } else {
            info!(%node, %challenge_id, "spot challenge sent");
        }
    }
}

/// Agent-side: run this node's **shard** of a pool-wide inference.
///
/// Non-tail shards only ACK (activation handoff stub). Tail produces tokens.
/// `engine` may be a stub or a [`joule_runtime::ClusterEngine`] with tensors loaded.
pub async fn agent_handle_infer(env: &Envelope, engine: &impl Engine) -> Result<Envelope> {
    match &env.msg {
        Message::InferRequest {
            request_id,
            model,
            prompt,
            max_tokens,
            plan,
            is_tail,
        } => {
            engine.load_plan(plan).await.context("engine load_plan")?;
            if !*is_tail {
                // Intermediate pipeline stage: consume compute, no user text yet.
                return Ok(Envelope::new(
                    env.from.clone(),
                    Message::InferDone {
                        request_id: *request_id,
                        text: String::new(),
                        prompt_tokens: 0,
                        completion_tokens: 0,
                        shard_ok: true,
                    },
                ));
            }
            match engine
                .infer(InferRequest {
                    model: model.clone(),
                    prompt: prompt.clone(),
                    max_tokens: *max_tokens,
                })
                .await
            {
                Ok(out) => Ok(Envelope::new(
                    env.from.clone(),
                    Message::InferDone {
                        request_id: *request_id,
                        text: out.text,
                        prompt_tokens: out.prompt_tokens,
                        completion_tokens: out.completion_tokens,
                        shard_ok: true,
                    },
                )),
                Err(e) => Ok(Envelope::new(
                    env.from.clone(),
                    Message::InferError {
                        request_id: *request_id,
                        error: e.to_string(),
                    },
                )),
            }
        }
        _ => anyhow::bail!("not an infer request"),
    }
}

/// Agent-side challenge handler.
pub async fn agent_handle_challenge(env: &Envelope, engine: &impl Engine) -> Result<Envelope> {
    match &env.msg {
        Message::Challenge {
            challenge_id,
            model,
            prompt,
        } => {
            let started = Instant::now();
            use joule_proto::{ClusterPlan, ShardAssignment, ShardRole};
            let plan = ClusterPlan {
                plan_id: Uuid::new_v4(),
                model: model.clone(),
                shards: vec![ShardAssignment {
                    node: env.from.clone(),
                    role: ShardRole::Replica,
                    layer_start: Some(0),
                    layer_end: Some(0),
                    tp_rank: None,
                    tp_world: None,
                    mem_share_mib: 0,
                    mem_fraction_ppm: 1_000_000,
                }],
                pool_mem_mib: 0,
                model_layers: 1,
            };
            engine.load_plan(&plan).await.context("engine load_plan")?;
            let out = engine
                .infer(InferRequest {
                    model: model.clone(),
                    prompt: prompt.clone(),
                    max_tokens: 64,
                })
                .await
                .context("challenge infer")?;
            let latency_ms = started.elapsed().as_millis() as u32;
            Ok(Envelope::new(
                env.from.clone(),
                Message::ChallengeResult {
                    challenge_id: *challenge_id,
                    completion: out.text,
                    latency_ms,
                },
            ))
        }
        _ => anyhow::bail!("not a challenge"),
    }
}
