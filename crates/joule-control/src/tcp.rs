//! TCP agent protocol: newline-delimited JSON envelopes.

use crate::app::{AgentRoutes, App};
use crate::state::{
    CoordinationPath, InferOutcome, PendingChallenge, PendingInfer, PendingPlanAccept,
};
use anyhow::{Context, Result};
use joule_proto::{
    decode_line, encode_line, resolve_cluster_model, Envelope, Message, NodeId, CLUSTER_MODEL,
};
use joule_runtime::Engine;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};
use uuid::Uuid;

/// PlanOffer auth: signed by **pool identity** over preimage `from=pool_offerer_node_id`.
/// Envelope.from must be the same offerer id so recipients bind sig ↔ offerer device.
fn control_plan_offer_auth(
    app: &App,
    plan_id: uuid::Uuid,
    request_id: uuid::Uuid,
    plan_hash_hex: &str,
) -> (NodeId, joule_proto::PlanAuth) {
    let offerer = joule_cluster::pool_offerer_node_id(&app.identity.pool_id);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let pre =
        joule_cluster::plan_offer_sign_preimage(&offerer, plan_id, request_id, plan_hash_hex, ts);
    let pk = app.identity.public_info().verifying_key_hex;
    let sig = app.identity.sign_bytes(pre.as_bytes());
    (
        offerer,
        joule_proto::PlanAuth {
            signer_pubkey_hex: pk,
            sig_hex: sig,
            signed_at_unix_ms: ts,
        },
    )
}

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
                    // Bind device pubkey for plan-bus authenticity (Hello pubkey or lab).
                    if !pubkey_hex.is_empty() {
                        g.set_node_device_pubkey(&id, &pubkey_hex);
                    }
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
                        pool_pubkey_hex: app.identity.public_info().verifying_key_hex,
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
                let mut presence = Vec::new();
                let mesh_list = g.mesh.list();
                for (n, m) in peers {
                    let addrs = if m.multiaddrs.is_empty() {
                        g.mesh.multiaddrs_for(&n)
                    } else {
                        m.multiaddrs.clone()
                    };
                    let mesh_h = mesh_list.iter().find(|p| p.node == n);
                    presence.push(crate::seeder_rank::SeederPresence {
                        node: n.clone(),
                        multiaddrs: addrs.clone(),
                        healthy: mesh_h.map(|p| p.healthy).unwrap_or(true),
                        load: mesh_h.map(|p| p.load).unwrap_or(0.1),
                    });
                    peer_ids.push(n);
                    sizes.push(m.size);
                    multiaddrs.push(addrs);
                }
                // Candidates only via book projection (seeder-side active_transfers).
                let free = g.cluster.scheduler_snapshot().stream_slots_free;
                let candidates = crate::seeder_rank::seeder_candidates_from(
                    &presence,
                    &g.pending_blob_xfers,
                    free,
                );
                let seeder = g.blobs.pick_seeder_ranked(&hash, &requester, &candidates);
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
                    let request_id = {
                        let mut g = app.state.write().await;
                        // Cap concurrent control-relayed transfers (lab path).
                        if g.pending_blob_xfers.len() >= 64 {
                            warn!("BlobWant: too many in-flight transfers");
                            continue;
                        }
                        // Attribute load to **seeder**, not requester.
                        g.pending_blob_xfers.begin(
                            seeder_id.clone(),
                            requester.clone(),
                            hash.clone(),
                        )
                    };
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
                    g.pending_blob_xfers.requester_and_hash(&request_id)
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
                            g.pending_blob_xfers.end(&request_id);
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
                auth,
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
                    &auth.signer_pubkey_hex,
                    &auth.sig_hex,
                    auth.signed_at_unix_ms,
                );
            }
            Message::InferDone {
                request_id,
                text,
                prompt_tokens,
                completion_tokens,
                shard_ok,
                activation_hex,
                activation_layer_start,
                activation_layer_end,
                activation_payload_b64,
            } => {
                // Phase-1 pipeline: fulfill non-tail activation waiter if present.
                let ack_tx = {
                    let mut w = app.shard_acks.lock().await;
                    w.remove(&(request_id, env.from.clone()))
                };
                if let Some(tx) = ack_tx {
                    let activation =
                        if activation_hex.is_empty() || activation_payload_b64.is_empty() {
                            None
                        } else {
                            Some(joule_proto::ShardActivation {
                                node: env.from.clone(),
                                layer_start: activation_layer_start.unwrap_or(0),
                                layer_end: activation_layer_end.unwrap_or(0),
                                activation_hex,
                                payload_b64: activation_payload_b64,
                            })
                        };
                    let _ = tx.send(Ok(crate::state::ShardAck {
                        activation,
                        shard_ok,
                    }));
                    // Non-tail still settles mint path without completing user oneshot.
                    let mut g = app.state.write().await;
                    g.settle_shard_success(
                        request_id,
                        text,
                        prompt_tokens,
                        completion_tokens,
                        &env.from,
                        false,
                    );
                    continue;
                }
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
                // Agent files_complete is untrusted alone — corroborate MANIFEST/blob evidence.
                if files_complete {
                    let mut g = app.state.write().await;
                    if g.refresh_digests_from_evidence() {
                        info!("PrepareOk: digests corroborated (store or catalog)");
                    } else {
                        info!("PrepareOk: files_complete ignored without MANIFEST/blob evidence");
                    }
                }
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
                let mut g = app.state.write().await;
                // Never set digests from tensors/bytes self-report — content evidence only.
                let _ = g.refresh_digests_from_evidence();
                g.mark_node_loaded(env.from.clone());
            }
            other => {
                warn!(msg = ?other, "ignored agent→control message");
            }
        }
    }

    if let Some(id) = node_id {
        app.routes.lock().await.remove(&id);
        // Fail-fast any pipeline stage waiters for this node (control replan path).
        {
            let mut w = app.shard_acks.lock().await;
            let keys: Vec<_> = w.keys().filter(|(_, n)| n == &id).cloned().collect();
            for k in keys {
                if let Some(tx) = w.remove(&k) {
                    let _ = tx.send(Err(format!("shard disconnected: {id}")));
                }
            }
        }
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

/// Dial a donor multiaddr, send InferRequest, read InferDone (peer-direct path).
/// Returns InferDone envelope on success; None falls back to control relay.
async fn send_infer_peer_direct(multiaddr: &str, env: &Envelope) -> Option<Envelope> {
    let bytes = encode_line(env).ok()?;
    let addr = parse_tcp_multiaddr_local(multiaddr)?;
    let sock = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(addr))
        .await
        .ok()?
        .ok()?;
    let (reader, mut writer) = sock.into_split();
    writer.write_all(&bytes).await.ok()?;
    let _ = writer.flush().await;
    let mut lines = BufReader::new(reader).lines();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if tokio::time::Instant::now() > deadline {
            return None;
        }
        let line = match tokio::time::timeout(Duration::from_secs(10), lines.next_line()).await {
            Ok(Ok(Some(l))) => l,
            _ => return None,
        };
        if line.trim().is_empty() {
            continue;
        }
        let reply: Envelope = decode_line(line.as_bytes()).ok()?;
        match &reply.msg {
            Message::InferDone { .. } => return Some(reply),
            Message::Error { error } => {
                warn!(%error, %multiaddr, "peer-direct Infer error");
                return None;
            }
            _ => continue,
        }
    }
}

/// Parse `tcp://host:port` (or bare `host:port`) for peer-direct Infer dial.
fn parse_tcp_multiaddr_local(s: &str) -> Option<std::net::SocketAddr> {
    let s = s.trim();
    let rest = s.strip_prefix("tcp://").unwrap_or(s);
    rest.parse().ok()
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
///
/// Confirmed-shard death mid-infer: release + replan once on remaining connected
/// mesh donors, or fail closed if no capacity remains.
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

    let mut last_err = String::new();
    for attempt in 1u32..=2 {
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
            if attempt == 1 {
                return Err("mesh has no connected donors with verified capacity".into());
            }
            return Err(format!(
                "control replan fail-closed (no connected mesh donors): prior={last_err}"
            ));
        }

        // Geometry from mesh; capacity truth still requires a stream lease.
        let plan_geometry =
            joule_cluster::plan_from_mesh_donors(&donors).map_err(|e| e.to_string())?;
        let request_id = Uuid::new_v4();
        let lease = {
            let mut g = app.state.write().await;
            match g.admit_stream_lease(account, request_id, Duration::from_secs(90)) {
                Ok(l) => l,
                Err(e) => {
                    if attempt == 1 {
                        return Err(e);
                    }
                    return Err(format!(
                        "control replan fail-closed (no healthy capacity): {e}; prior={last_err}"
                    ));
                }
            }
        };
        let mut guard = StreamLeaseGuard::new(app, request_id);
        let plan = plan_geometry;
        let plan_hash_hex = joule_cluster::plan_hash_hex(&plan);
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
            attempt,
            "mesh RequestInfer + stream lease"
        );

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

        for shard in &plan.shards {
            let (offerer, auth) =
                control_plan_offer_auth(app, plan.plan_id, request_id, &plan_hash_hex);
            let env = Envelope::new(
                offerer,
                Message::PlanOffer {
                    plan: plan.clone(),
                    request_id,
                    plan_hash_hex: plan_hash_hex.clone(),
                    auth,
                },
            );
            if !send_to_agent(&app.routes, &shard.node, env).await {
                let mut g = app.state.write().await;
                g.pending_plan_accepts.remove(&request_id);
                guard.release_now("lease_released", "plan offer fail").await;
                last_err = format!("PlanOffer: shard {} not connected", shard.node);
                if attempt < 2 && is_control_shard_death_err(&last_err) {
                    continue;
                }
                return Err(last_err);
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
            drop(g);
            guard.release_now("lease_released", &e).await;
            last_err = e;
            if attempt < 2 && is_control_shard_death_err(&last_err) {
                continue;
            }
            return Err(last_err);
        }

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
        match out {
            Ok(o) => {
                guard.release_now("lease_released", "infer ok").await;
                if attempt > 1 {
                    info!(attempt, "mesh/control replan succeeded after shard death");
                }
                return Ok(o);
            }
            Err(e) => {
                guard.release_now("lease_released", &e).await;
                last_err = e;
                if attempt < 2 && is_control_shard_death_err(&last_err) {
                    info!(
                        attempt,
                        error = %last_err,
                        "mesh/control replan after confirmed shard death mid-infer"
                    );
                    continue;
                }
                return Err(last_err);
            }
        }
    }
    Err(format!("control replan exhausted: {last_err}"))
}

/// True when fanout failed because a confirmed plan shard died/disconnected mid-infer.
fn is_control_shard_death_err(e: &str) -> bool {
    e.contains("not connected")
        || e.contains("missing activation")
        || e.contains("shard disconnected")
        || e.contains("all shards disconnected")
        || e.contains("timed out")
        || e.contains("pipeline sequential stage")
}

/// Classic control path: stream **lease** + multi-shard PlanOffer agree + InferRequest.
///
/// On confirmed-shard death mid-infer: release lease, replan against remaining
/// healthy/connected donors (up to 1 retry), or fail closed if no capacity.
async fn dispatch_control_stream(
    app: &App,
    account: &str,
    model: &str,
    prompt: &str,
    max_tokens: u32,
    charge: bool,
) -> Result<InferOutcome, String> {
    let mut last_err = String::new();
    for attempt in 1u32..=2 {
        let request_id = Uuid::new_v4();
        let wait = {
            let g = app.state.read().await;
            g.lease_wait
        };
        let lease = match acquire_lease_with_wait(app, account, request_id, wait).await {
            Ok(l) => l,
            Err(e) => {
                if attempt == 1 {
                    return Err(e);
                }
                // Replan attempt: no capacity left after shard death.
                return Err(format!(
                    "control replan fail-closed (no healthy capacity after shard death): {e}; prior={last_err}"
                ));
            }
        };
        let mut guard = StreamLeaseGuard::new(app, request_id);
        let plan = lease.plan.clone();
        let plan_hash_hex = lease.plan_hash_hex.clone();
        info!(
            %request_id,
            lease_id = %lease.lease_id,
            shards = plan.shards.len(),
            pool_mem_mib = plan.pool_mem_mib,
            %plan_hash_hex,
            attempt,
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
            let (offerer, auth) =
                control_plan_offer_auth(app, plan.plan_id, request_id, &plan_hash_hex);
            let env = Envelope::new(
                offerer,
                Message::PlanOffer {
                    plan: plan.clone(),
                    request_id,
                    plan_hash_hex: plan_hash_hex.clone(),
                    auth,
                },
            );
            if !send_to_agent(&app.routes, &shard.node, env).await {
                let mut g = app.state.write().await;
                g.pending_plan_accepts.remove(&request_id);
                drop(g);
                guard.release_now("lease_released", "plan offer fail").await;
                last_err = format!("PlanOffer: shard {} not connected", shard.node);
                if attempt < 2 && is_control_shard_death_err(&last_err) {
                    info!(attempt, error = %last_err, "control replan after plan-offer shard death");
                    continue;
                }
                return Err(last_err);
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
            drop(g);
            guard.release_now("lease_released", &e).await;
            last_err = e;
            if attempt < 2 && is_control_shard_death_err(&last_err) {
                info!(attempt, error = %last_err, "control replan after PlanAccept failure");
                continue;
            }
            return Err(last_err);
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

        match out {
            Ok(outcome) => {
                guard.release_now("lease_released", "infer ok").await;
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
                                if second.text.trim() != outcome.text.trim() {
                                    warn!(
                                        first_len = outcome.text.len(),
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
                if attempt > 1 {
                    info!(attempt, "control replan succeeded after shard death");
                }
                return Ok(outcome);
            }
            Err(e) => {
                guard.release_now("lease_released", &e).await;
                last_err = e;
                if attempt < 2 && is_control_shard_death_err(&last_err) {
                    info!(
                        attempt,
                        error = %last_err,
                        "control replan: confirmed shard death mid-infer; retrying remaining pool"
                    );
                    continue;
                }
                return Err(last_err);
            }
        }
    }
    Err(format!("control replan exhausted: {last_err}"))
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
                started: std::time::Instant::now(),
                tx: Some(tx),
            },
        );
    }

    // Prefer peer-direct Infer hop when all shards advertise dial multiaddrs.
    let shard_addrs: Vec<Vec<String>> = {
        let g = app.state.read().await;
        plan.shards
            .iter()
            .map(|s| g.mesh.multiaddrs_for(&s.node))
            .collect()
    };
    let peer_direct = joule_mesh::prefer_peer_direct_infer(&shard_addrs);
    info!(peer_direct, shards = plan.shards.len(), "infer fanout path");

    // Sequential multi-stage chain: each non-tail (layer order) receives prior
    // activations as upstream; first stage has empty upstream.
    let non_tails = joule_cluster::non_tail_nodes(&plan, &tail);
    let mut upstream: Vec<joule_proto::ShardActivation> = Vec::new();
    let mut sent = 0usize;
    for (stage_i, node) in non_tails.iter().enumerate() {
        if stage_i > 0 {
            if let Err(e) =
                joule_cluster::verify_prefix_activations(&plan, &tail, &upstream, stage_i)
            {
                let mut g = app.state.write().await;
                g.pending.remove(&request_id);
                if stream_reserved {
                    g.cluster.release_stream(&plan);
                    g.wake_scheduler();
                }
                return Err(format!("pipeline mid-chain handoff failed: {e}"));
            }
        }
        let env = Envelope::new(
            node.clone(),
            Message::InferRequest {
                request_id,
                model: model.to_string(),
                prompt: prompt.to_string(),
                max_tokens,
                plan: plan.clone(),
                is_tail: false,
                // Prior stages only (sequential PP), not empty parallel fanout.
                upstream_activations: upstream.clone(),
            },
        );
        let idx = plan.shards.iter().position(|s| &s.node == node);
        let mut delivered = false;
        let mut got_act: Option<joule_proto::ShardActivation> = None;
        if peer_direct {
            if let Some(addr) = idx.and_then(|i| shard_addrs.get(i)).and_then(|a| a.first()) {
                if let Some(done) = send_infer_peer_direct(addr, &env).await {
                    delivered = true;
                    if let Message::InferDone {
                        activation_hex,
                        activation_layer_start,
                        activation_layer_end,
                        activation_payload_b64,
                        shard_ok,
                        ..
                    } = done.msg
                    {
                        if shard_ok
                            && !activation_hex.is_empty()
                            && !activation_payload_b64.is_empty()
                        {
                            got_act = Some(joule_proto::ShardActivation {
                                node: node.clone(),
                                layer_start: activation_layer_start.unwrap_or(0),
                                layer_end: activation_layer_end.unwrap_or(0),
                                activation_hex,
                                payload_b64: activation_payload_b64,
                            });
                        }
                        let mut g = app.state.write().await;
                        g.settle_shard_success(request_id, String::new(), 0, 0, node, false);
                    }
                }
            }
        }
        if !delivered {
            let (tx, rx) = oneshot::channel();
            {
                let mut w = app.shard_acks.lock().await;
                w.insert((request_id, node.clone()), tx);
            }
            if send_to_agent(&app.routes, node, env).await {
                delivered = true;
                match tokio::time::timeout(Duration::from_secs(20), rx).await {
                    Ok(Ok(Ok(ack))) => {
                        if let Some(a) = ack.activation {
                            got_act = Some(a);
                        }
                    }
                    Ok(Ok(Err(e))) => {
                        warn!(node = %node, error = %e, "non-tail activation error");
                    }
                    _ => {
                        let mut w = app.shard_acks.lock().await;
                        w.remove(&(request_id, node.clone()));
                        warn!(node = %node, "non-tail activation timeout");
                    }
                }
            } else {
                let mut w = app.shard_acks.lock().await;
                w.remove(&(request_id, node.clone()));
            }
        }
        if delivered {
            sent += 1;
            if let Some(a) = got_act {
                upstream.push(a);
            } else {
                let mut g = app.state.write().await;
                g.pending.remove(&request_id);
                if stream_reserved {
                    g.cluster.release_stream(&plan);
                    g.wake_scheduler();
                }
                return Err(format!(
                    "pipeline sequential stage {stage_i} from {node}: missing activation payload"
                ));
            }
        } else {
            let mut g = app.state.write().await;
            g.pending.remove(&request_id);
            if stream_reserved {
                g.cluster.release_stream(&plan);
                g.wake_scheduler();
            }
            return Err(format!(
                "pipeline sequential stage {stage_i}: non-tail {node} not connected"
            ));
        }
    }
    if !non_tails.is_empty() {
        if let Err(e) =
            joule_cluster::verify_upstream_activations(&plan, request_id, prompt, &tail, &upstream)
        {
            let mut g = app.state.write().await;
            g.pending.remove(&request_id);
            if stream_reserved {
                g.cluster.release_stream(&plan);
                g.wake_scheduler();
            }
            return Err(format!("pipeline activation handoff failed: {e}"));
        }
        info!(
            stages = upstream.len(),
            "pipeline sequential activations verified; dispatching tail"
        );
    }

    // Phase 2: tail with verified upstream activations.
    {
        let env = Envelope::new(
            tail.clone(),
            Message::InferRequest {
                request_id,
                model: model.to_string(),
                prompt: prompt.to_string(),
                max_tokens,
                plan: plan.clone(),
                is_tail: true,
                upstream_activations: upstream,
            },
        );
        let i = plan.shards.iter().position(|s| s.node == tail);
        let mut delivered = false;
        if peer_direct {
            if let Some(addr) = i.and_then(|i| shard_addrs.get(i)).and_then(|a| a.first()) {
                if let Some(done) = send_infer_peer_direct(addr, &env).await {
                    delivered = true;
                    if let Message::InferDone {
                        request_id: rid,
                        text,
                        prompt_tokens,
                        completion_tokens,
                        ..
                    } = done.msg
                    {
                        let mut g = app.state.write().await;
                        g.settle_shard_success(
                            rid,
                            text,
                            prompt_tokens,
                            completion_tokens,
                            &tail,
                            true,
                        );
                    }
                }
            }
        }
        if !delivered && send_to_agent(&app.routes, &tail, env).await {
            delivered = true;
        }
        if delivered {
            sent += 1;
        } else {
            warn!(node = %tail, "tail shard agent not connected");
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

/// Cancel-safe lease hold: releases on Drop if not disarmed (client cancel / panic path).
struct StreamLeaseGuard {
    app: App,
    request_id: Uuid,
    released: bool,
}

impl StreamLeaseGuard {
    fn new(app: &App, request_id: Uuid) -> Self {
        Self {
            app: app.clone(),
            request_id,
            released: false,
        }
    }

    async fn release_now(&mut self, event: &str, detail: &str) {
        if self.released {
            return;
        }
        self.released = true;
        let mut g = self.app.state.write().await;
        g.release_stream_lease(self.request_id, event, detail);
    }
}

impl Drop for StreamLeaseGuard {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        let app = self.app.clone();
        let rid = self.request_id;
        // Prefer immediate try_write so cancel paths free slots without waiting a task.
        if let Ok(mut g) = app.state.try_write() {
            g.release_stream_lease(rid, "lease_released", "drop guard");
            return;
        }
        tokio::spawn(async move {
            let mut g = app.state.write().await;
            g.release_stream_lease(rid, "lease_released", "drop guard async");
        });
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

/// Options for agent-side infer (production vs stub/lab fixtures).
#[derive(Debug, Clone, Copy, Default)]
pub struct InferAgentOpts {
    /// When true, `stage_layers` requires preferred band weights loaded (ClusterEngine path).
    pub require_band_weights: bool,
}

/// Agent-side: run this node's **layer-band pipeline stage** (stub-safe defaults).
pub async fn agent_handle_infer(env: &Envelope, engine: &impl Engine) -> Result<Envelope> {
    agent_handle_infer_with(env, engine, InferAgentOpts::default()).await
}

/// Agent-side infer with production options (e.g. band-weight gate after prepare).
///
/// Non-tail: sequential chain — stage receives prior activations as upstream.
/// Tail: verify full upstream set, then layer-sliced stage.
pub async fn agent_handle_infer_with(
    env: &Envelope,
    engine: &impl Engine,
    opts: InferAgentOpts,
) -> Result<Envelope> {
    match &env.msg {
        Message::InferRequest {
            request_id,
            model,
            prompt,
            max_tokens,
            plan,
            is_tail,
            upstream_activations,
        } => {
            engine.load_plan(plan).await.context("engine load_plan")?;
            let shard = plan
                .shards
                .iter()
                .find(|s| s.node == env.from)
                .ok_or_else(|| anyhow::anyhow!("node not in plan"))?;
            let ls = shard.layer_start.unwrap_or(0);
            let le = shard.layer_end.unwrap_or(ls);
            if !*is_tail {
                let required = joule_cluster::preferred_weight_files(ls, le).unwrap_or_default();
                // Mid-chain non-tails must see prior stage tensor payloads.
                let require_up = !upstream_activations.is_empty();
                if require_up {
                    if let Err(e) = joule_cluster::concat_prior_payloads(upstream_activations) {
                        return Ok(Envelope::new(
                            env.from.clone(),
                            Message::InferError {
                                request_id: *request_id,
                                error: format!("pipeline mid-chain activation: {e}"),
                            },
                        ));
                    }
                }
                let upstream_bytes = if upstream_activations.is_empty() {
                    vec![]
                } else {
                    joule_cluster::concat_prior_payloads(upstream_activations)
                        .map_err(|e| anyhow::anyhow!(e))?
                };
                let stage = engine
                    .stage_layers(joule_runtime::StageRequest {
                        model: model.clone(),
                        prompt: prompt.clone(),
                        layer_start: ls,
                        layer_end: le,
                        upstream: upstream_bytes,
                        is_tail: false,
                        require_upstream: require_up,
                        require_band_weights: opts.require_band_weights,
                        required_weight_files: required,
                    })
                    .await
                    .context("stage_layers non-tail")?;
                let act = joule_cluster::activation_from_payload(
                    env.from.clone(),
                    ls,
                    le,
                    &stage.activation,
                )
                .map_err(|e| anyhow::anyhow!(e))?;
                return Ok(Envelope::new(
                    env.from.clone(),
                    Message::InferDone {
                        request_id: *request_id,
                        text: String::new(),
                        prompt_tokens: stage.prompt_tokens,
                        completion_tokens: 0,
                        shard_ok: true,
                        activation_hex: act.activation_hex,
                        activation_layer_start: Some(ls),
                        activation_layer_end: Some(le),
                        activation_payload_b64: act.payload_b64,
                    },
                ));
            }
            // Tail: multi-shard verifies real upstream tensors then layer-sliced stage;
            // single-shard runs full engine.infer (tensor path when ClusterEngine loaded).
            if plan.shards.len() == 1 {
                let out = engine
                    .infer(joule_runtime::InferRequest {
                        model: model.clone(),
                        prompt: prompt.clone(),
                        max_tokens: *max_tokens,
                    })
                    .await
                    .context("single-shard tail infer")?;
                return Ok(Envelope::new(
                    env.from.clone(),
                    Message::InferDone {
                        request_id: *request_id,
                        text: out.text,
                        prompt_tokens: out.prompt_tokens,
                        completion_tokens: out.completion_tokens,
                        shard_ok: true,
                        activation_hex: String::new(),
                        activation_layer_start: None,
                        activation_layer_end: None,
                        activation_payload_b64: String::new(),
                    },
                ));
            }
            if let Err(e) = joule_cluster::verify_upstream_activations(
                plan,
                *request_id,
                prompt,
                &env.from,
                upstream_activations,
            ) {
                return Ok(Envelope::new(
                    env.from.clone(),
                    Message::InferError {
                        request_id: *request_id,
                        error: format!("pipeline activation: {e}"),
                    },
                ));
            }
            let upstream_bytes =
                joule_cluster::concat_upstream_payloads(plan, &env.from, upstream_activations)
                    .map_err(|e| anyhow::anyhow!(e))?;
            let required = joule_cluster::preferred_weight_files(ls, le).unwrap_or_default();
            let stage = engine
                .stage_layers(joule_runtime::StageRequest {
                    model: model.clone(),
                    prompt: prompt.clone(),
                    layer_start: ls,
                    layer_end: le,
                    upstream: upstream_bytes,
                    is_tail: true,
                    require_upstream: true,
                    require_band_weights: opts.require_band_weights,
                    required_weight_files: required,
                })
                .await
                .context("stage_layers tail")?;
            let text = stage.text.unwrap_or_default();
            Ok(Envelope::new(
                env.from.clone(),
                Message::InferDone {
                    request_id: *request_id,
                    text,
                    prompt_tokens: stage.prompt_tokens,
                    completion_tokens: stage.completion_tokens,
                    shard_ok: true,
                    activation_hex: String::new(),
                    activation_layer_start: None,
                    activation_layer_end: None,
                    activation_payload_b64: String::new(),
                },
            ))
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

#[cfg(test)]
mod tests {
    use super::*;
    use joule_proto::{ClusterPlan, ShardAssignment, ShardRole, CLUSTER_MODEL};
    use joule_runtime::StubEngine;
    use uuid::Uuid;

    fn demo_plan(layers: u32) -> ClusterPlan {
        ClusterPlan {
            plan_id: Uuid::new_v4(),
            model: CLUSTER_MODEL.into(),
            shards: vec![
                ShardAssignment {
                    node: NodeId::new(),
                    role: ShardRole::Pipeline,
                    layer_start: Some(0),
                    layer_end: Some(layers / 2),
                    tp_rank: None,
                    tp_world: None,
                    mem_share_mib: 4096,
                    mem_fraction_ppm: 500_000,
                },
                ShardAssignment {
                    node: NodeId::new(),
                    role: ShardRole::Pipeline,
                    layer_start: Some(layers / 2 + 1),
                    layer_end: Some(layers.saturating_sub(1)),
                    tp_rank: None,
                    tp_world: None,
                    mem_share_mib: 4096,
                    mem_fraction_ppm: 500_000,
                },
            ],
            pool_mem_mib: 8192,
            model_layers: layers,
        }
    }

    #[tokio::test]
    async fn multi_donor_infer_non_tail_empty_ack_only() {
        let plan = demo_plan(joule_runtime::placement_model_layers());
        let eng = StubEngine::new();
        let env = Envelope::new(
            plan.shards[0].node.clone(),
            Message::InferRequest {
                request_id: Uuid::new_v4(),
                model: CLUSTER_MODEL.into(),
                prompt: "should-not-infer".into(),
                max_tokens: 16,
                plan: plan.clone(),
                is_tail: false,
                upstream_activations: vec![],
            },
        );
        let reply = agent_handle_infer(&env, &eng).await.expect("handle");
        match reply.msg {
            Message::InferDone {
                text,
                prompt_tokens,
                completion_tokens,
                shard_ok,
                activation_hex,
                activation_payload_b64,
                ..
            } => {
                assert!(text.is_empty(), "non-tail must not run full infer: {text}");
                let _ = prompt_tokens; // stage may report prompt token count
                assert_eq!(completion_tokens, 0);
                assert!(shard_ok);
                assert!(
                    !activation_hex.is_empty() && !activation_payload_b64.is_empty(),
                    "non-tail must produce real stage tensor payload"
                );
            }
            other => panic!("expected InferDone, got {other:?}"),
        }
        // Call site: non-tail StageRequest carries preferred weight basenames for the band.
        let ls = plan.shards[0].layer_start.unwrap_or(0);
        let le = plan.shards[0].layer_end.unwrap_or(ls);
        let prefs = joule_cluster::preferred_weight_files(ls, le).unwrap();
        assert!(
            !prefs.is_empty(),
            "shard band must map to preferred weight files"
        );
        eprintln!(
            "OBSERVE pipeline-handoff: non-tail activation_hex set; model_layers={} band={}-{} preferred_n={} first={}",
            plan.model_layers,
            ls,
            le,
            prefs.len(),
            prefs.first().map(|s| s.as_str()).unwrap_or("")
        );
    }

    #[tokio::test]
    async fn multi_donor_infer_tail_runs_pipeline_stage() {
        let plan = demo_plan(joule_runtime::placement_model_layers());
        let eng = StubEngine::new();
        eng.load_plan(&plan).await.unwrap();
        let rid = Uuid::new_v4();
        let prompt = "tail-full-infer";
        let stage = eng
            .stage_layers(joule_runtime::StageRequest {
                model: CLUSTER_MODEL.into(),
                prompt: prompt.into(),
                layer_start: 0,
                layer_end: plan.model_layers / 2,
                upstream: vec![],
                is_tail: false,
                require_upstream: false,
                require_band_weights: false,
                required_weight_files: joule_cluster::preferred_weight_files(
                    0,
                    plan.model_layers / 2,
                )
                .unwrap_or_default(),
            })
            .await
            .unwrap();
        assert!(
            stage.activation.starts_with(b"JST1") && stage.activation.len() >= 16,
            "non-tail stage must emit real tensor payload"
        );
        let upstream = vec![joule_cluster::activation_from_payload(
            plan.shards[0].node.clone(),
            0,
            plan.model_layers / 2,
            &stage.activation,
        )
        .unwrap()];
        let env = Envelope::new(
            plan.shards[1].node.clone(),
            Message::InferRequest {
                request_id: rid,
                model: CLUSTER_MODEL.into(),
                prompt: prompt.into(),
                max_tokens: 8,
                plan: plan.clone(),
                is_tail: true,
                upstream_activations: upstream.clone(),
            },
        );
        let reply = agent_handle_infer(&env, &eng).await.expect("handle");
        match reply.msg {
            Message::InferDone { text, .. } => {
                // Multi-shard tail must use stage_layers path, not independent engine.infer.
                assert!(
                    text.contains("joule-pipeline-stage"),
                    "multi-shard tail must be pipeline stage, not stub infer: {text}"
                );
                assert!(
                    text.contains("upstream_bytes=")
                        && !text.contains("upstream_bytes=0]")
                        && !text.contains("upstream_bytes=0:"),
                    "tail stage must consume non-empty upstream: {text}"
                );
                assert!(
                    text.contains("tail-full-infer"),
                    "prompt must appear: {text}"
                );
                eprintln!(
                    "OBSERVE real-pp agent_handle_infer: text_len={} upstream_payload_len={}",
                    text.len(),
                    stage.activation.len()
                );
            }
            other => panic!("expected InferDone, got {other:?}"),
        }

        // Wrong/corrupt upstream payload must fail closed at verify (not run independent infer).
        let mut bad = upstream;
        bad[0].payload_b64 = String::new();
        let env_bad = Envelope::new(
            plan.shards[1].node.clone(),
            Message::InferRequest {
                request_id: Uuid::new_v4(),
                model: CLUSTER_MODEL.into(),
                prompt: prompt.into(),
                max_tokens: 8,
                plan: plan.clone(),
                is_tail: true,
                upstream_activations: bad,
            },
        );
        let reply_bad = agent_handle_infer(&env_bad, &eng).await.expect("handle");
        match reply_bad.msg {
            Message::InferError { error, .. } => {
                assert!(
                    error.contains("activation")
                        || error.contains("pipeline")
                        || error.contains("payload"),
                    "wrong upstream must fail closed via agent_handle_infer: {error}"
                );
                eprintln!("OBSERVE real-pp wrong-upstream fail-closed: {error}");
            }
            Message::InferDone { text, .. } => {
                panic!("wrong upstream must not complete: {text}");
            }
            other => panic!("expected InferError for wrong upstream, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn multi_donor_tail_rejects_missing_activation() {
        let plan = demo_plan(joule_runtime::placement_model_layers());
        let eng = StubEngine::new();
        eng.load_plan(&plan).await.unwrap();
        let env = Envelope::new(
            plan.shards[1].node.clone(),
            Message::InferRequest {
                request_id: Uuid::new_v4(),
                model: CLUSTER_MODEL.into(),
                prompt: "no-upstream".into(),
                max_tokens: 8,
                plan: plan.clone(),
                is_tail: true,
                upstream_activations: vec![],
            },
        );
        let reply = agent_handle_infer(&env, &eng).await.expect("handle");
        match reply.msg {
            Message::InferError { error, .. } => {
                assert!(
                    error.contains("activation") || error.contains("pipeline"),
                    "{error}"
                );
            }
            other => panic!("expected InferError without upstream, got {other:?}"),
        }
    }

    fn demo_plan_3(layers: u32) -> ClusterPlan {
        let a = NodeId::new();
        let b = NodeId::new();
        let c = NodeId::new();
        ClusterPlan {
            plan_id: Uuid::new_v4(),
            model: CLUSTER_MODEL.into(),
            shards: vec![
                ShardAssignment {
                    node: a,
                    role: ShardRole::Pipeline,
                    layer_start: Some(0),
                    layer_end: Some(layers / 3),
                    tp_rank: None,
                    tp_world: None,
                    mem_share_mib: 4096,
                    mem_fraction_ppm: 333_333,
                },
                ShardAssignment {
                    node: b,
                    role: ShardRole::Pipeline,
                    layer_start: Some(layers / 3 + 1),
                    layer_end: Some(2 * layers / 3),
                    tp_rank: None,
                    tp_world: None,
                    mem_share_mib: 4096,
                    mem_fraction_ppm: 333_333,
                },
                ShardAssignment {
                    node: c,
                    role: ShardRole::Pipeline,
                    layer_start: Some(2 * layers / 3 + 1),
                    layer_end: Some(layers.saturating_sub(1)),
                    tp_rank: None,
                    tp_world: None,
                    mem_share_mib: 4096,
                    mem_fraction_ppm: 333_334,
                },
            ],
            pool_mem_mib: 12288,
            model_layers: layers,
        }
    }

    /// ≥3 shards sequential: stage0 empty upstream; stage1 consumes stage0; tail full chain.
    #[tokio::test]
    async fn sequential_three_shard_chain_via_agent_handle_infer() {
        let plan = demo_plan_3(joule_runtime::placement_model_layers());
        let eng = StubEngine::new();
        eng.load_plan(&plan).await.unwrap();
        let rid = Uuid::new_v4();
        let prompt = "seq-chain-3";

        // Stage 0 (first non-tail): empty upstream.
        let env0 = Envelope::new(
            plan.shards[0].node.clone(),
            Message::InferRequest {
                request_id: rid,
                model: CLUSTER_MODEL.into(),
                prompt: prompt.into(),
                max_tokens: 16,
                plan: plan.clone(),
                is_tail: false,
                upstream_activations: vec![],
            },
        );
        let r0 = agent_handle_infer(&env0, &eng).await.expect("stage0");
        let act0 = match r0.msg {
            Message::InferDone {
                activation_hex,
                activation_payload_b64,
                activation_layer_start,
                activation_layer_end,
                ..
            } => {
                assert!(!activation_payload_b64.is_empty());
                joule_proto::ShardActivation {
                    node: plan.shards[0].node.clone(),
                    layer_start: activation_layer_start.unwrap_or(0),
                    layer_end: activation_layer_end.unwrap_or(0),
                    activation_hex,
                    payload_b64: activation_payload_b64,
                }
            }
            other => panic!("stage0 InferDone expected: {other:?}"),
        };
        let up0 = joule_cluster::decode_payload(&act0).unwrap();
        assert!(up0.starts_with(b"JST1"));
        eprintln!(
            "OBSERVE seq-multi-stage: stage0 upstream_in=0 act_len={}",
            up0.len()
        );

        // Stage 1 (second non-tail): prior activation required.
        let env1 = Envelope::new(
            plan.shards[1].node.clone(),
            Message::InferRequest {
                request_id: rid,
                model: CLUSTER_MODEL.into(),
                prompt: prompt.into(),
                max_tokens: 16,
                plan: plan.clone(),
                is_tail: false,
                upstream_activations: vec![act0.clone()],
            },
        );
        let r1 = agent_handle_infer(&env1, &eng).await.expect("stage1");
        let act1 = match r1.msg {
            Message::InferDone {
                activation_hex,
                activation_payload_b64,
                activation_layer_start,
                activation_layer_end,
                ..
            } => {
                assert!(!activation_payload_b64.is_empty());
                joule_proto::ShardActivation {
                    node: plan.shards[1].node.clone(),
                    layer_start: activation_layer_start.unwrap_or(0),
                    layer_end: activation_layer_end.unwrap_or(0),
                    activation_hex,
                    payload_b64: activation_payload_b64,
                }
            }
            other => panic!("stage1 InferDone expected: {other:?}"),
        };
        let up1 = joule_cluster::decode_payload(&act1).unwrap();
        assert_ne!(up0, up1, "mid stage must depend on prior activation");
        eprintln!(
            "OBSERVE seq-multi-stage: stage1 upstream_in={} act_len={}",
            up0.len(),
            up1.len()
        );

        // Mid-chain missing prior fails closed.
        let env_bad = Envelope::new(
            plan.shards[1].node.clone(),
            Message::InferRequest {
                request_id: Uuid::new_v4(),
                model: CLUSTER_MODEL.into(),
                prompt: prompt.into(),
                max_tokens: 16,
                plan: plan.clone(),
                is_tail: false,
                upstream_activations: vec![joule_proto::ShardActivation {
                    node: plan.shards[0].node.clone(),
                    layer_start: 0,
                    layer_end: 1,
                    activation_hex: "00".repeat(32),
                    payload_b64: String::new(),
                }],
            },
        );
        match agent_handle_infer(&env_bad, &eng)
            .await
            .expect("bad mid")
            .msg
        {
            Message::InferError { error, .. } => {
                assert!(
                    error.contains("mid-chain")
                        || error.contains("payload")
                        || error.contains("activation"),
                    "{error}"
                );
                eprintln!("OBSERVE seq-multi-stage: mid-chain fail-closed: {error}");
            }
            other => panic!("expected InferError mid-chain, got {other:?}"),
        }

        // Tail with full chain.
        let env_t = Envelope::new(
            plan.shards[2].node.clone(),
            Message::InferRequest {
                request_id: rid,
                model: CLUSTER_MODEL.into(),
                prompt: prompt.into(),
                max_tokens: 16,
                plan: plan.clone(),
                is_tail: true,
                upstream_activations: vec![act0, act1],
            },
        );
        match agent_handle_infer(&env_t, &eng).await.expect("tail").msg {
            Message::InferDone { text, .. } => {
                assert!(text.contains("joule-pipeline-stage"));
                assert!(text.contains("upstream_bytes="));
                assert!(!text.contains("upstream_bytes=0]"));
                eprintln!(
                    "OBSERVE seq-multi-stage: shards=3 tail_text_len={} ok",
                    text.len()
                );
            }
            other => panic!("tail InferDone expected: {other:?}"),
        }
    }

    /// Production path: `require_band_weights` after prepare_and_install (shipped agent opts).
    #[tokio::test]
    async fn agent_production_band_gate_after_prepare() {
        use joule_runtime::{ClusterEngine, ManifestFile, WeightsStore};
        use std::fs;

        let eng = ClusterEngine::new();
        let plan = demo_plan(93);
        eng.load_plan(&plan).await.unwrap();
        let env = Envelope::new(
            plan.shards[0].node.clone(),
            Message::InferRequest {
                request_id: Uuid::new_v4(),
                model: CLUSTER_MODEL.into(),
                prompt: "band-gate-prod".into(),
                max_tokens: 8,
                plan: plan.clone(),
                is_tail: false,
                upstream_activations: vec![],
            },
        );
        let opts_on = InferAgentOpts {
            require_band_weights: true,
        };
        // No prepare → stage_layers fails closed (Err from agent_handle_infer_with).
        let fail = agent_handle_infer_with(&env, &eng, opts_on).await;
        let fail_dbg = format!("{fail:?}");
        assert!(
            fail.is_err()
                && (fail_dbg.contains("missing band weights")
                    || fail_dbg.contains("band weights")
                    || fail_dbg.contains("not loaded")),
            "must fail without prepare: {fail_dbg}"
        );
        eprintln!("OBSERVE agent-band-gate: fail without prepare: {fail_dbg}");

        // Shipped prepare_and_install (lab-tiny) → resident weights → gate allows lab path.
        let dir = std::env::temp_dir().join(format!("joule-agent-band-{}", Uuid::new_v4()));
        let _ = fs::remove_dir_all(&dir);
        let store = WeightsStore::new(&dir);
        let m = ManifestFile::load_default().unwrap();
        let spec = m.model("kimi-open").unwrap();
        let lab = spec
            .weights
            .quants
            .iter()
            .find(|q| q.id == "lab-tiny")
            .unwrap();
        let report = joule_runtime::prepare_and_install(&store, &eng, spec, lab).expect("prepare");
        assert!(eng.has_resident_weights() || eng.is_model_loaded());
        // Mirror production agent: opts from has_resident_weights after prepare.
        let opts_prod = InferAgentOpts {
            require_band_weights: eng.has_resident_weights() || eng.is_model_loaded(),
        };
        assert!(opts_prod.require_band_weights);
        let ok = agent_handle_infer_with(&env, &eng, opts_prod)
            .await
            .expect("stage after prepare");
        match ok.msg {
            Message::InferDone {
                activation_payload_b64,
                ..
            } => {
                assert!(!activation_payload_b64.is_empty());
                eprintln!(
                    "OBSERVE agent-band-gate: after prepare require_band_weights=true ok tensors={} bytes={}",
                    report.tensors, report.bytes_resident
                );
            }
            other => panic!("expected InferDone after prepare: {other:?}"),
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// Integrated criterion: sequential ≥3-shard path + ClusterEngine f32 weights → JST3 matmul.
    #[tokio::test]
    async fn sequential_jst3_chain_weight_resident_production_path() {
        use joule_runtime::{ClusterEngine, LoadedModel, MATMUL_DIM};
        use std::collections::HashMap;

        fn f32_diag_model(scale: f32) -> LoadedModel {
            let need = MATMUL_DIM * MATMUL_DIM + MATMUL_DIM;
            let mut w = vec![0.0f32; need];
            for i in 0..MATMUL_DIM {
                w[i * MATMUL_DIM + i] = scale;
                w[MATMUL_DIM * MATMUL_DIM + i] = 0.01 * (i as f32);
            }
            let mut bytes = Vec::with_capacity(need * 4);
            for v in &w {
                bytes.extend_from_slice(&v.to_le_bytes());
            }
            let mut tensors = HashMap::new();
            tensors.insert("blk.0.weight".into(), bytes);
            let mut sources = HashMap::new();
            sources.insert("blk.0.weight".into(), "model.safetensors".into());
            LoadedModel {
                model_id: CLUSTER_MODEL.into(),
                quant: "f32-diag".into(),
                source_dir: std::path::PathBuf::from("/tmp/joule-f32-diag"),
                tensors,
                tensor_info: vec![],
                bytes_resident: (need * 4) as u64,
                loaded_at_unix: 0,
                loaded_file_basenames: vec!["model.safetensors".into()],
                tensor_sources: sources,
            }
        }

        fn payload_bytes(act: &joule_proto::ShardActivation) -> Vec<u8> {
            joule_cluster::decode_payload(act).expect("decode payload")
        }

        fn jst3_stack_depth(payload: &[u8]) -> u32 {
            assert!(
                payload.starts_with(b"JST3"),
                "want JST3 got {:?}",
                &payload[..4.min(payload.len())]
            );
            // magic(4)+ls(4)+le(4)+dim(4)=16 → layers_applied
            u32::from_le_bytes(payload[16..20].try_into().unwrap())
        }

        let eng = ClusterEngine::new();
        let plan = demo_plan_3(joule_runtime::placement_model_layers());
        eng.load_plan(&plan).await.unwrap();
        eng.install_loaded(f32_diag_model(1.0));
        assert!(eng.has_resident_weights());
        let opts = InferAgentOpts {
            require_band_weights: true,
        };
        let rid = Uuid::new_v4();
        let prompt = "full-jst3-chain";

        // Stage 0: empty upstream, production gate on → JST3.
        let env0 = Envelope::new(
            plan.shards[0].node.clone(),
            Message::InferRequest {
                request_id: rid,
                model: CLUSTER_MODEL.into(),
                prompt: prompt.into(),
                max_tokens: 16,
                plan: plan.clone(),
                is_tail: false,
                upstream_activations: vec![],
            },
        );
        let r0 = agent_handle_infer_with(&env0, &eng, opts)
            .await
            .expect("stage0");
        let act0 = match r0.msg {
            Message::InferDone {
                activation_hex,
                activation_payload_b64,
                activation_layer_start,
                activation_layer_end,
                ..
            } => joule_proto::ShardActivation {
                node: plan.shards[0].node.clone(),
                layer_start: activation_layer_start.unwrap_or(0),
                layer_end: activation_layer_end.unwrap_or(0),
                activation_hex,
                payload_b64: activation_payload_b64,
            },
            other => panic!("stage0 InferDone: {other:?}"),
        };
        let p0 = payload_bytes(&act0);
        assert!(p0.starts_with(b"JST3"));
        let stack0 = jst3_stack_depth(&p0);
        let span0 = act0
            .layer_end
            .saturating_sub(act0.layer_start)
            .saturating_add(1);
        assert_eq!(stack0, span0.clamp(1, 32));
        eprintln!(
            "OBSERVE full-jst3-chain: stage0 upstream_in=0 magic=JST3 stack={stack0} span={span0}"
        );

        // Stage 1: prior activation required; JST3 depends on upstream.
        let env1 = Envelope::new(
            plan.shards[1].node.clone(),
            Message::InferRequest {
                request_id: rid,
                model: CLUSTER_MODEL.into(),
                prompt: prompt.into(),
                max_tokens: 16,
                plan: plan.clone(),
                is_tail: false,
                upstream_activations: vec![act0.clone()],
            },
        );
        let r1 = agent_handle_infer_with(&env1, &eng, opts)
            .await
            .expect("stage1");
        let act1 = match r1.msg {
            Message::InferDone {
                activation_hex,
                activation_payload_b64,
                activation_layer_start,
                activation_layer_end,
                ..
            } => joule_proto::ShardActivation {
                node: plan.shards[1].node.clone(),
                layer_start: activation_layer_start.unwrap_or(0),
                layer_end: activation_layer_end.unwrap_or(0),
                activation_hex,
                payload_b64: activation_payload_b64,
            },
            other => panic!("stage1 InferDone: {other:?}"),
        };
        let p1 = payload_bytes(&act1);
        assert!(p1.starts_with(b"JST3"));
        assert_ne!(p0, p1, "mid stage must depend on prior JST3 upstream");
        let stack1 = jst3_stack_depth(&p1);
        eprintln!(
            "OBSERVE full-jst3-chain: stage1 upstream_in={} stack={} act_len={}",
            p0.len(),
            stack1,
            p1.len()
        );

        // Wider span (shard0 often wider or different) vs narrow mid → stack differs when spans differ.
        assert!(
            stack0 != stack1
                || span0
                    != act1
                        .layer_end
                        .saturating_sub(act1.layer_start)
                        .saturating_add(1),
            "layer spans drive stack; got stack0={stack0} stack1={stack1}"
        );

        // Mid-chain corrupt prior fails closed on shipped handler.
        let env_bad = Envelope::new(
            plan.shards[1].node.clone(),
            Message::InferRequest {
                request_id: Uuid::new_v4(),
                model: CLUSTER_MODEL.into(),
                prompt: prompt.into(),
                max_tokens: 16,
                plan: plan.clone(),
                is_tail: false,
                upstream_activations: vec![joule_proto::ShardActivation {
                    node: plan.shards[0].node.clone(),
                    layer_start: 0,
                    layer_end: 1,
                    activation_hex: "00".repeat(32),
                    payload_b64: String::new(),
                }],
            },
        );
        match agent_handle_infer_with(&env_bad, &eng, opts)
            .await
            .expect("bad mid")
            .msg
        {
            Message::InferError { error, .. } => {
                assert!(
                    error.contains("mid-chain")
                        || error.contains("payload")
                        || error.contains("activation"),
                    "{error}"
                );
                eprintln!("OBSERVE full-jst3-chain: mid fail-closed: {error}");
            }
            other => panic!("expected InferError, got {other:?}"),
        }

        // Tail: full upstream chain, production gate, JST3 + matmul text.
        let env_t = Envelope::new(
            plan.shards[2].node.clone(),
            Message::InferRequest {
                request_id: rid,
                model: CLUSTER_MODEL.into(),
                prompt: prompt.into(),
                max_tokens: 16,
                plan: plan.clone(),
                is_tail: true,
                upstream_activations: vec![act0.clone(), act1.clone()],
            },
        );
        match agent_handle_infer_with(&env_t, &eng, opts)
            .await
            .expect("tail")
            .msg
        {
            Message::InferDone { text, .. } => {
                assert!(
                    text.contains("joule-decode")
                        || text.contains("matmul")
                        || text.contains("upstream_bytes=")
                        || text.contains("joule-pipeline-stage"),
                    "tail text={text}"
                );
                assert!(
                    text.contains("matmul") || text.contains("upstream_bytes="),
                    "tail={text}"
                );
                eprintln!(
                    "OBSERVE full-jst3-chain: shards=3 tail_ok text_len={}",
                    text.len()
                );
            }
            other => panic!("tail InferDone: {other:?}"),
        }

        // Zero diagonal kills matmul signal → different JST3 (scale-1.0 vs 0.0).
        eng.install_loaded(f32_diag_model(0.0));
        let r0b = agent_handle_infer_with(&env0, &eng, opts)
            .await
            .expect("stage0 reweight");
        let act0b = match r0b.msg {
            Message::InferDone {
                activation_hex,
                activation_payload_b64,
                activation_layer_start,
                activation_layer_end,
                ..
            } => joule_proto::ShardActivation {
                node: plan.shards[0].node.clone(),
                layer_start: activation_layer_start.unwrap_or(0),
                layer_end: activation_layer_end.unwrap_or(0),
                activation_hex,
                payload_b64: activation_payload_b64,
            },
            other => panic!("{other:?}"),
        };
        let p0b = payload_bytes(&act0b);
        assert!(p0b.starts_with(b"JST3"));
        assert_ne!(
            p0, p0b,
            "weight diagonal 1.0 vs 0.0 must change JST3 activation"
        );
        eprintln!(
            "OBSERVE full-jst3-chain: weight_flip act0_len={} act0b_len={} differ=true",
            p0.len(),
            p0b.len()
        );
    }
}
