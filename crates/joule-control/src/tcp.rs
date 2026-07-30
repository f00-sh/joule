//! TCP agent protocol: newline-delimited JSON envelopes.

use crate::app::{AgentRoutes, App};
use crate::state::{InferOutcome, PendingChallenge, PendingInfer};
use anyhow::{Context, Result};
use joule_proto::{decode_line, encode_line, Envelope, Message, NodeId};
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
                    g.register_node(id.clone(), &account, caps)
                };
                {
                    let mut r = app.routes.lock().await;
                    r.insert(id.clone(), tx.clone());
                }
                node_id = Some(id.clone());
                info!(%id, %account, ?peer, "agent joined");
                let welcome = Envelope::new(
                    id,
                    Message::Welcome {
                        account: account.clone(),
                        api_key,
                    },
                );
                let _ = tx.send(welcome);
            }
            Message::Heartbeat { load, healthy } => {
                let id = env.from.clone();
                let mut g = app.state.write().await;
                match g.on_heartbeat(&id, load, healthy) {
                    Ok(Some(mint)) => {
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
            Message::InferDone {
                request_id,
                text,
                prompt_tokens,
                completion_tokens,
            } => {
                let mut g = app.state.write().await;
                g.settle_infer_success(
                    request_id,
                    text,
                    prompt_tokens,
                    completion_tokens,
                    &env.from,
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

/// Dispatch inference across donors with load balancing + failover.
pub async fn dispatch_infer(
    app: &App,
    account: &str,
    model: &str,
    prompt: &str,
    max_tokens: u32,
) -> Result<InferOutcome, String> {
    {
        let g = app.state.read().await;
        if !g.cluster.account_is_donating(account) {
            return Err(
                "contribution required: run `joule agent` for this account before using the API"
                    .into(),
            );
        }
    }

    let dual = {
        let mut g = app.state.write().await;
        g.should_dual_verify()
    };

    // Acquire ranked workers; try until one accepts the job.
    let candidates = {
        let mut g = app.state.write().await;
        let n = if dual { 2 } else { 4 };
        let mut ids = g.cluster.acquire_workers(model, n.max(1));
        if ids.is_empty() {
            return Err(format!("no healthy workers for model {model}"));
        }
        // If dual, keep 2; for failover keep extras without dual verify requirement.
        if !dual && ids.len() > 3 {
            // release extras
            for extra in ids.drain(3..) {
                g.cluster.release_worker(&extra);
            }
        }
        ids
    };

    if candidates.is_empty() {
        return Err(format!("no healthy workers for model {model}"));
    }

    // Dual-verify path: run two workers in parallel, compare.
    if dual && candidates.len() >= 2 {
        let primary = candidates[0].clone();
        let secondary = candidates[1].clone();
        // release any extra beyond 2
        {
            let mut g = app.state.write().await;
            for extra in candidates.iter().skip(2) {
                g.cluster.release_worker(extra);
            }
        }

        let p_out = dispatch_one(
            app,
            account,
            model,
            prompt,
            max_tokens,
            primary.clone(),
            true,
        )
        .await;
        let s_out = dispatch_one(
            app,
            account,
            model,
            prompt,
            max_tokens,
            secondary.clone(),
            false,
        )
        .await;

        match (p_out, s_out) {
            (Ok(a), Ok(b)) => {
                let mut g = app.state.write().await;
                if a.text.trim() == b.text.trim() {
                    g.cluster.record_challenge_ok(&a.worker_id);
                    g.cluster.record_challenge_ok(&b.worker_id);
                    // secondary already minted via settle; return primary
                    return Ok(a);
                }
                // Mismatch: fail both reputations, still return primary if usable
                g.cluster.record_challenge_fail(&a.worker_id);
                g.cluster.record_challenge_fail(&b.worker_id);
                warn!(
                    primary = %a.worker_id,
                    secondary = %b.worker_id,
                    "dual-verify mismatch"
                );
                return Ok(a);
            }
            (Ok(a), Err(e)) => {
                let mut g = app.state.write().await;
                g.cluster.record_challenge_fail(&secondary);
                warn!(error = %e, "secondary verify failed");
                return Ok(a);
            }
            (Err(e), Ok(b)) => {
                let mut g = app.state.write().await;
                g.cluster.record_challenge_fail(&primary);
                warn!(error = %e, "primary failed; using secondary");
                return Ok(b);
            }
            (Err(e1), Err(e2)) => {
                return Err(format!("all workers failed: {e1}; {e2}"));
            }
        }
    }

    // Single path with failover across acquired candidates.
    let mut last_err = String::from("no worker accepted job");
    let mut remaining = candidates;
    while let Some(worker) = remaining.first().cloned() {
        remaining.remove(0);
        match dispatch_one(
            app,
            account,
            model,
            prompt,
            max_tokens,
            worker.clone(),
            true,
        )
        .await
        {
            Ok(out) => {
                // release unused candidates
                let mut g = app.state.write().await;
                for extra in remaining {
                    g.cluster.release_worker(&extra);
                }
                return Ok(out);
            }
            Err(e) => {
                last_err = e;
                // worker already released in settle error / timeout path for that request
            }
        }
    }
    Err(last_err)
}

async fn dispatch_one(
    app: &App,
    account: &str,
    model: &str,
    prompt: &str,
    max_tokens: u32,
    worker_id: NodeId,
    charge: bool,
) -> Result<InferOutcome, String> {
    let request_id = Uuid::new_v4();
    let (tx, rx) = oneshot::channel();
    {
        let mut g = app.state.write().await;
        g.pending.insert(
            request_id,
            PendingInfer {
                account: account.to_string(),
                worker: worker_id.clone(),
                charge,
                tx,
            },
        );
    }

    let env = Envelope::new(
        worker_id.clone(),
        Message::InferRequest {
            request_id,
            model: model.to_string(),
            prompt: prompt.to_string(),
            max_tokens,
        },
    );

    if !send_to_agent(&app.routes, &worker_id, env).await {
        let mut g = app.state.write().await;
        g.pending.remove(&request_id);
        g.cluster.release_worker(&worker_id);
        return Err(format!("agent not connected: {worker_id}"));
    }

    match tokio::time::timeout(Duration::from_secs(30), rx).await {
        Ok(Ok(Ok(outcome))) => Ok(outcome),
        Ok(Ok(Err(e))) => Err(e),
        Ok(Err(_)) => Err("infer channel closed".into()),
        Err(_) => {
            let mut g = app.state.write().await;
            if g.pending.remove(&request_id).is_some() {
                g.cluster.release_worker(&worker_id);
            }
            Err("infer timed out".into())
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
            let target = g.cluster.pick_challenge_target().map(|n| {
                let model = n
                    .caps
                    .models
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "kimi-open-q4".into());
                (n.id.clone(), model)
            });
            if let Some((id, model)) = target {
                let challenge_id = Uuid::new_v4();
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

/// Agent-side: handle InferRequest from control using local stub engine.
pub async fn agent_handle_infer(env: &Envelope, stub: &StubEngine) -> Result<Envelope> {
    match &env.msg {
        Message::InferRequest {
            request_id,
            model,
            prompt,
            max_tokens,
        } => {
            use joule_proto::{ClusterPlan, ShardAssignment, ShardRole};
            let plan = ClusterPlan {
                plan_id: Uuid::new_v4(),
                model: model.clone(),
                shards: vec![ShardAssignment {
                    node: env.from.clone(),
                    role: ShardRole::Replica,
                    layer_start: None,
                    layer_end: None,
                    tp_rank: None,
                    tp_world: None,
                }],
            };
            stub.load_plan(&plan).await.context("stub load")?;
            match stub
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
pub async fn agent_handle_challenge(env: &Envelope, stub: &StubEngine) -> Result<Envelope> {
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
                    layer_start: None,
                    layer_end: None,
                    tp_rank: None,
                    tp_world: None,
                }],
            };
            stub.load_plan(&plan).await.context("stub load")?;
            let out = stub
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
