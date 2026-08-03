//! TCP agent protocol: newline-delimited JSON envelopes.

use crate::app::{AgentRoutes, App};
use crate::state::{
    CoordinationPath, InferOutcome, PendingChallenge, PendingInfer, PendingPlanAccept,
};
use anyhow::{Context, Result};
use joule_proto::{
    decode_line, encode_line, resolve_cluster_model, Envelope, Message, NodeId, CLUSTER_MODEL,
};
use joule_runtime::{Engine, InferRequest};
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
            Message::Hello {
                account,
                caps,
                pubkey_hex,
                sig_hex,
                signed_at_unix_ms,
            } => {
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
                // Cryptographic acceptance: j1… accounts must present a valid ed25519 Hello.
                if crate::account_auth::requires_signature(&account) {
                    let now = crate::account_auth::now_unix_ms();
                    match crate::account_auth::verify_hello(
                        &account,
                        &id,
                        &pubkey_hex,
                        &sig_hex,
                        signed_at_unix_ms,
                        now,
                    ) {
                        Ok(pk) => {
                            let mut g = app.state.write().await;
                            if let Some(bound) = g.account_pubkeys.get(&account) {
                                if bound != &pk {
                                    let err = Envelope::new(
                                        id.clone(),
                                        Message::Error {
                                            error: "account already bound to a different key"
                                                .into(),
                                        },
                                    );
                                    let _ = tx.send(err);
                                    continue;
                                }
                            } else {
                                g.account_pubkeys.insert(account.clone(), pk);
                                g.mark_dirty();
                            }
                        }
                        Err(e) => {
                            warn!(%account, error = %e, "signed Hello rejected");
                            let err = Envelope::new(
                                id.clone(),
                                Message::Error {
                                    error: format!("hello auth failed: {e}"),
                                },
                            );
                            let _ = tx.send(err);
                            continue;
                        }
                    }
                }
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
                verified_mem_mib: _peer_verified,
                throughput_class,
            } => {
                let id = env.from.clone();
                let verified = {
                    let mut g = app.state.write().await;
                    // Placement uses **cluster verified** only — PeerAlive claim/self-report ignored.
                    let verified = g.cluster.verified_mem_mib(&id);
                    g.mesh.upsert(
                        id.clone(),
                        multiaddrs.clone(),
                        load,
                        healthy,
                        blob_count,
                        mem_mib,
                        verified,
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
                    verified
                };
                // Gossip to other agents (control as temporary flood hub).
                let routes = app.routes.lock().await;
                let msg = Message::PeerAlive {
                    multiaddrs,
                    load,
                    healthy,
                    blob_count,
                    mem_mib,
                    verified_mem_mib: verified,
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
            Message::PlanAccept {
                plan_id,
                request_id,
                accepted,
                reason,
                plan_hash_hex,
                confirm_hex,
            } => {
                tracing::debug!(
                    %plan_id,
                    %request_id,
                    accepted,
                    %reason,
                    from = %env.from,
                    "PlanAccept"
                );
                app.state.write().await.settle_plan_accept(
                    request_id,
                    &env.from,
                    plan_id,
                    accepted,
                    &plan_hash_hex,
                    &confirm_hex,
                );
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
/// **Phase D:** when mesh PeerAlive donors with `mem_mib` are connected, prefer
/// [`dispatch_mesh_infer`] (RequestInfer → PlanOffer → PlanAccept → InferRequest)
/// over classic control-only `try_acquire_stream`. Control stream remains fallback.
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

    // Mesh-happy path: geometry from PeerAlive mem, not control registry acquire.
    if mesh_donors_ready(app).await {
        match dispatch_mesh_infer(app, account, &model, prompt, max_tokens).await {
            Ok(out) => return Ok(out),
            Err(e) => {
                warn!(error = %e, "mesh RequestInfer path failed; falling back to control_dispatch");
            }
        }
    }

    dispatch_control_stream(app, account, &model, prompt, max_tokens, true).await
}

/// True when ≥1 mesh donor has mem_mib and is on agent routes.
pub async fn mesh_donors_ready(app: &App) -> bool {
    let donors = {
        let g = app.state.read().await;
        g.mesh_plan_donors()
    };
    if donors.is_empty() {
        return false;
    }
    let routes = app.routes.lock().await;
    donors.iter().any(|(id, _)| routes.contains_key(id))
}

/// Phase D mesh coordination: **stream lease** → RequestInfer → PlanOffer →
/// hashed PlanAccept (all shards) → InferRequest → InferDone → **lease release**.
pub async fn dispatch_mesh_infer(
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

    let connected: std::collections::HashSet<NodeId> =
        app.routes.lock().await.keys().cloned().collect();
    let donors: Vec<(NodeId, u32)> = {
        let g = app.state.read().await;
        g.mesh_plan_donors()
            .into_iter()
            .filter(|(id, _)| connected.contains(id))
            .collect()
    };
    if donors.is_empty() {
        return Err("mesh has no connected donors with verified capacity".into());
    }

    // Geometry from mesh; capacity truth still requires a stream lease.
    let plan_geometry =
        joule_cluster::plan_from_mesh_donors(&donors).map_err(|e| e.to_string())?;
    let request_id = Uuid::new_v4();
    let lease = {
        let mut g = app.state.write().await;
        g.admit_stream_lease(account, request_id, Duration::from_secs(90))?
    };
    // Prefer lease plan shards (registry) when they match mesh geometry size;
    // mesh plan is used for PlanOffer agreement among mesh donors.
    let plan = plan_geometry;
    let plan_hash_hex = joule_cluster::plan_hash_hex(&plan);
    // Capacity lease may have been granted against registry geometry; bind the
    // **mesh** plan hash every donor will confirm.
    {
        let mut g = app.state.write().await;
        g.leases
            .bind_agreement_hash(request_id, plan_hash_hex.clone());
    }

    info!(
        %request_id,
        lease_id = %lease.lease_id,
        shards = plan.shards.len(),
        pool_mem_mib = plan.pool_mem_mib,
        donors = donors.len(),
        %plan_hash_hex,
        "mesh RequestInfer + stream lease"
    );

    for (node, _) in &donors {
        let env = Envelope::new(
            node.clone(),
            Message::RequestInfer {
                request_id,
                account: account.to_string(),
                model: model.clone(),
                prompt: prompt.to_string(),
                max_tokens,
            },
        );
        let _ = send_to_agent(&app.routes, node, env).await;
    }

    let expected: std::collections::HashSet<NodeId> =
        plan.shards.iter().map(|s| s.node.clone()).collect();
    let (accept_tx, accept_rx) = oneshot::channel();
    {
        let mut g = app.state.write().await;
        g.pending_plan_accepts.insert(
            request_id,
            PendingPlanAccept {
                plan_id: plan.plan_id,
                plan_hash_hex: plan_hash_hex.clone(),
                expected: expected.clone(),
                accepted: std::collections::HashSet::new(),
                tx: Some(accept_tx),
            },
        );
    }

    for shard in &plan.shards {
        let env = Envelope::new(
            shard.node.clone(),
            Message::PlanOffer {
                plan: plan.clone(),
                request_id,
                plan_hash_hex: plan_hash_hex.clone(),
            },
        );
        if !send_to_agent(&app.routes, &shard.node, env).await {
            let mut g = app.state.write().await;
            g.pending_plan_accepts.remove(&request_id);
            g.release_stream_lease(request_id, "lease_released", "plan offer fail");
            return Err(format!("PlanOffer: shard {} not connected", shard.node));
        }
    }

    let agree = match tokio::time::timeout(Duration::from_secs(8), accept_rx).await {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(e))) => Err(e),
        Ok(Err(_)) => Err("PlanAccept channel closed".into()),
        Err(_) => Err("timed out waiting for PlanAccept from mesh shards".into()),
    };
    if let Err(e) = agree {
        let mut g = app.state.write().await;
        g.pending_plan_accepts.remove(&request_id);
        g.release_stream_lease(request_id, "lease_released", &e);
        return Err(e);
    }

    // Lease holds cluster inflight; fanout does NOT double-reserve.
    let out = fanout_infer(
        app,
        account,
        &model,
        prompt,
        max_tokens,
        plan,
        request_id,
        true,
        false,
        CoordinationPath::MeshRequestInfer,
    )
    .await;
    // Always release lease (success, error, or timeout inside fanout).
    {
        let mut g = app.state.write().await;
        let detail = match &out {
            Ok(_) => "infer ok",
            Err(e) => e.as_str(),
        };
        g.release_stream_lease(request_id, "lease_released", detail);
    }
    out
}

/// Classic control path: stream **lease** + multi-shard PlanOffer agree + InferRequest.
async fn dispatch_control_stream(
    app: &App,
    account: &str,
    model: &str,
    prompt: &str,
    max_tokens: u32,
    charge: bool,
) -> Result<InferOutcome, String> {
    let request_id = Uuid::new_v4();
    let lease = acquire_lease_with_wait(app, account, request_id, Duration::from_secs(20)).await?;
    let plan = lease.plan.clone();
    let plan_hash_hex = lease.plan_hash_hex.clone();
    info!(
        %request_id,
        lease_id = %lease.lease_id,
        shards = plan.shards.len(),
        pool_mem_mib = plan.pool_mem_mib,
        %plan_hash_hex,
        "control_dispatch sharded infer (stream lease)"
    );

    // Multi-party agreement even on control path: PlanOffer → hashed PlanAccept.
    let expected: std::collections::HashSet<NodeId> =
        plan.shards.iter().map(|s| s.node.clone()).collect();
    let (accept_tx, accept_rx) = oneshot::channel();
    {
        let mut g = app.state.write().await;
        g.pending_plan_accepts.insert(
            request_id,
            PendingPlanAccept {
                plan_id: plan.plan_id,
                plan_hash_hex: plan_hash_hex.clone(),
                expected,
                accepted: std::collections::HashSet::new(),
                tx: Some(accept_tx),
            },
        );
    }
    for shard in &plan.shards {
        let env = Envelope::new(
            shard.node.clone(),
            Message::PlanOffer {
                plan: plan.clone(),
                request_id,
                plan_hash_hex: plan_hash_hex.clone(),
            },
        );
        if !send_to_agent(&app.routes, &shard.node, env).await {
            let mut g = app.state.write().await;
            g.pending_plan_accepts.remove(&request_id);
            g.release_stream_lease(request_id, "lease_released", "plan offer fail");
            return Err(format!("PlanOffer: shard {} not connected", shard.node));
        }
    }
    let agree = match tokio::time::timeout(Duration::from_secs(8), accept_rx).await {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(e))) => Err(e),
        Ok(Err(_)) => Err("PlanAccept channel closed".into()),
        Err(_) => Err("timed out waiting for PlanAccept from control shards".into()),
    };
    if let Err(e) = agree {
        let mut g = app.state.write().await;
        g.pending_plan_accepts.remove(&request_id);
        g.release_stream_lease(request_id, "lease_released", &e);
        return Err(e);
    }

    let out = fanout_infer(
        app,
        account,
        model,
        prompt,
        max_tokens,
        plan,
        request_id,
        charge,
        false,
        CoordinationPath::ControlDispatch,
    )
    .await;

    {
        let mut g = app.state.write().await;
        let detail = match &out {
            Ok(_) => "infer ok",
            Err(e) => e.as_str(),
        };
        g.release_stream_lease(request_id, "lease_released", detail);
    }

    let out = out?;

    if charge {
        let dual = {
            let mut g = app.state.write().await;
            g.should_dual_verify()
        };
        if dual {
            match Box::pin(dispatch_control_stream(
                app, account, model, prompt, max_tokens, false,
            ))
            .await
            {
                Ok(second) => {
                    if second.text.trim() != out.text.trim() {
                        warn!(
                            first_len = out.text.len(),
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
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
async fn fanout_infer(
    app: &App,
    account: &str,
    model: &str,
    prompt: &str,
    max_tokens: u32,
    plan: joule_proto::ClusterPlan,
    request_id: Uuid,
    charge: bool,
    stream_reserved: bool,
    coordination: CoordinationPath,
) -> Result<InferOutcome, String> {
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
                charge,
                stream_reserved,
                coordination,
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
                model: model.to_string(),
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
        if stream_reserved {
            g.cluster.release_stream(&plan);
            g.wake_scheduler();
        }
        return Err("no connected agents for sharded plan".into());
    }

    if sent < plan.shards.len() {
        let connected: std::collections::HashSet<_> =
            app.routes.lock().await.keys().cloned().collect();
        let mut g = app.state.write().await;
        if let Some(p) = g.pending.get_mut(&request_id) {
            p.awaiting.retain(|n| connected.contains(n));
            if p.awaiting.is_empty() {
                g.pending.remove(&request_id);
                if stream_reserved {
                    g.cluster.release_stream(&plan);
                    g.wake_scheduler();
                }
                return Err("all shards disconnected mid-dispatch".into());
            }
        }
    }

    match tokio::time::timeout(Duration::from_secs(45), rx).await {
        Ok(Ok(Ok(outcome))) => Ok(outcome),
        Ok(Ok(Err(e))) => Err(e),
        Ok(Err(_)) => Err("infer channel closed".into()),
        Err(_) => {
            let mut g = app.state.write().await;
            if let Some(mut p) = g.pending.remove(&request_id) {
                if p.stream_reserved {
                    g.cluster.release_stream(&p.plan);
                    g.wake_scheduler();
                }
                let _ = p.tx.take();
            }
            Err("sharded infer timed out".into())
        }
    }
}

/// Wait until a stream **lease** can be admitted (fail closed after timeout).
async fn acquire_lease_with_wait(
    app: &App,
    account: &str,
    request_id: Uuid,
    timeout: Duration,
) -> Result<joule_cluster::StreamLease, String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        {
            let mut g = app.state.write().await;
            if g.cluster.pool_size() == 0 {
                return Err(format!(
                    "no healthy workers for cluster model {CLUSTER_MODEL}"
                ));
            }
            match g.admit_stream_lease(account, request_id, Duration::from_secs(90)) {
                Ok(lease) => return Ok(lease),
                Err(e) if e.contains("pool full") => {
                    // wait for release
                }
                Err(e) => return Err(e),
            }
        }

        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "pool full: no free stream slots for {CLUSTER_MODEL} (timed out waiting)"
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
///
/// Memory-hard oracle is computed on `spawn_blocking` **outside** the control
/// write lock so heartbeats/HTTP/settles stay responsive.
pub async fn challenge_loop(app: App) {
    let mut tick = tokio::time::interval(Duration::from_secs(12));
    loop {
        tick.tick().await;
        // 1) Brief write: prune + pick target only (no multi-GiB work under lock).
        let picked = {
            let mut g = app.state.write().await;
            g.prune();
            g.cluster.pick_challenge_target().map(|n| {
                let claim = n.claimed_mem_mib;
                let verified = n.verified_mem_mib;
                (n.id.clone(), claim, verified)
            })
        };
        let Some((node, claim, verified)) = picked else {
            continue;
        };

        // Peak model: issue up to CHALLENGE_CREDIT_MIB (single-challenge working set).
        // Prefer challenging nodes still below claim so they can raise peak.
        let credit_mib = joule_cluster::CHALLENGE_CREDIT_MIB.min(claim.max(1)).max(1);
        let _ = verified; // reserved for future progressive target = min(claim, verified+step) with full peak work

        let challenge_id = Uuid::new_v4();
        let model = CLUSTER_MODEL.to_string();
        let prompt = format!("joule-challenge:{challenge_id}");
        let mut seed = [0u8; 32];
        rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut seed);
        let capacity_seed_hex = hex::encode(seed);

        // 2) Oracle off the async runtime and off the write lock.
        let expected = match tokio::task::spawn_blocking(move || {
            joule_cluster::capacity_proof_hex(&seed, credit_mib)
        })
        .await
        {
            Ok(e) => e,
            Err(e) => {
                warn!(error = %e, "capacity oracle join failed");
                continue;
            }
        };

        // 3) Short write: register pending + send.
        {
            let mut g = app.state.write().await;
            // Node may have left during oracle work.
            if g.cluster.get(&node).is_none() {
                continue;
            }
            g.pending_challenges.insert(
                challenge_id,
                PendingChallenge {
                    node: node.clone(),
                    model: model.clone(),
                    prompt: prompt.clone(),
                    expected,
                    capacity_seed_hex: capacity_seed_hex.clone(),
                    credit_mib,
                    started: Instant::now(),
                },
            );
        }

        let env = Envelope::new(
            node.clone(),
            Message::Challenge {
                challenge_id,
                model,
                prompt,
                capacity_seed_hex,
                credit_mib,
            },
        );
        if !send_to_agent(&app.routes, &node, env).await {
            let mut g = app.state.write().await;
            g.pending_challenges.remove(&challenge_id);
            g.cluster.record_challenge_fail(&node);
            warn!(%node, "challenge send failed; recorded fail");
        } else {
            info!(%node, %challenge_id, credit_mib, "spot challenge sent");
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

/// Agent-side capacity challenge handler.
///
/// Unlocks verified capacity only via **mem-bound capacity proof**
/// (`joule_cluster::capacity_proof_hex`). Public stub strings like
/// `[joule-stub:…]` never unlock. Loaded vs unloaded engines both pass by
/// performing the same work unit (infer text is not the oracle).
pub async fn agent_handle_challenge(env: &Envelope, _engine: &impl Engine) -> Result<Envelope> {
    match &env.msg {
        Message::Challenge {
            challenge_id,
            capacity_seed_hex,
            credit_mib,
            ..
        } => {
            let started = Instant::now();
            let seed = joule_cluster::parse_seed_hex(capacity_seed_hex)
                .context("challenge missing/invalid capacity_seed_hex")?;
            let credit = if *credit_mib == 0 {
                joule_cluster::CHALLENGE_CREDIT_MIB
            } else {
                *credit_mib
            };
            // Blocking mem-bound work — run off the async scheduler when large.
            let proof = tokio::task::spawn_blocking(move || {
                joule_cluster::capacity_proof_hex(&seed, credit)
            })
            .await
            .context("capacity challenge join")?;
            let latency_ms = started.elapsed().as_millis() as u32;
            Ok(Envelope::new(
                env.from.clone(),
                Message::ChallengeResult {
                    challenge_id: *challenge_id,
                    completion: proof,
                    latency_ms,
                },
            ))
        }
        _ => anyhow::bail!("not a challenge"),
    }
}
