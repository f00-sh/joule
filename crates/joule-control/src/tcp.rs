//! TCP agent protocol: newline-delimited JSON envelopes.

use crate::state::SharedState;
use anyhow::{Context, Result};
use joule_proto::{decode_line, encode_line, Envelope, Message, NodeId};
use joule_runtime::{Engine, InferRequest, StubEngine};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};
use tracing::{info, warn};
use uuid::Uuid;

/// node_id → outbound queue of envelopes to that agent connection.
pub type AgentRoutes = Arc<Mutex<HashMap<NodeId, mpsc::UnboundedSender<Envelope>>>>;

pub async fn run_agent_listener(state: SharedState, listener: TcpListener) -> Result<()> {
    let routes: AgentRoutes = Arc::new(Mutex::new(HashMap::new()));
    // Stash routes on... we need shared routes for HTTP dispatch.
    // Store in a static companion: put routes inside ControlState would require
    // more invasive changes. Use extension via Arc pair in serve — instead attach
    // routes to SharedState by wrapping.
    //
    // Simpler: global once_cell for routes keyed by process — fine for single control.
    set_routes(Arc::clone(&routes));

    loop {
        let (sock, peer) = listener.accept().await?;
        let state = Arc::clone(&state);
        let routes = Arc::clone(&routes);
        tokio::spawn(async move {
            if let Err(e) = run_agent_session(state, routes, sock).await {
                warn!(%peer, error = %e, "agent session ended");
            }
        });
    }
}

static ROUTES: std::sync::OnceLock<AgentRoutes> = std::sync::OnceLock::new();

fn set_routes(r: AgentRoutes) {
    let _ = ROUTES.set(r);
}

pub fn agent_routes() -> Option<AgentRoutes> {
    ROUTES.get().cloned()
}

pub async fn run_agent_session(
    state: SharedState,
    routes: AgentRoutes,
    sock: TcpStream,
) -> Result<()> {
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
                    let mut g = state.write().await;
                    g.register_node(id.clone(), &account, caps)
                };
                {
                    let mut r = routes.lock().await;
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
                let mut g = state.write().await;
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
                let mut g = state.write().await;
                g.settle_infer_success(
                    request_id,
                    text,
                    prompt_tokens,
                    completion_tokens,
                    &env.from,
                );
            }
            Message::InferError { request_id, error } => {
                let mut g = state.write().await;
                g.settle_infer_error(request_id, error);
            }
            other => {
                // InferRequest is control→agent (written on this socket), not read here.
                warn!(msg = ?other, "ignored agent→control message");
            }
        }
    }

    if let Some(id) = node_id {
        routes.lock().await.remove(&id);
        state.write().await.remove_node(&id);
        info!(%id, "agent left");
    }
    write_task.abort();
    Ok(())
}

/// Dispatch inference to a connected agent; falls back to local stub if none.
pub async fn dispatch_infer(
    state: &SharedState,
    account: &str,
    model: &str,
    prompt: &str,
    max_tokens: u32,
) -> Result<crate::state::InferOutcome, String> {
    let (worker_id, worker_account, device) = {
        let g = state.read().await;
        if !g.cluster.account_is_donating(account) {
            return Err(
                "contribution required: run `joule agent` for this account before using the API"
                    .into(),
            );
        }
        let Some(node) = g.cluster.pick_worker(model) else {
            return Err(format!("no healthy workers for model {model}"));
        };
        (node.id.clone(), node.account.clone(), node.caps.device)
    };

    let request_id = Uuid::new_v4();
    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let mut g = state.write().await;
        g.pending.insert(
            request_id,
            crate::state::PendingInfer {
                account: account.to_string(),
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

    let sent = if let Some(routes) = agent_routes() {
        let r = routes.lock().await;
        if let Some(q) = r.get(&worker_id) {
            q.send(env).is_ok()
        } else {
            false
        }
    } else {
        false
    };

    if !sent {
        // Local stub fallback (agent not routable) — still settle economy.
        let stub = StubEngine::new();
        // load empty plan not needed for stub if we call infer after synthetic load
        use joule_proto::{ClusterPlan, ShardAssignment, ShardRole};
        let plan = ClusterPlan {
            plan_id: Uuid::new_v4(),
            model: model.to_string(),
            shards: vec![ShardAssignment {
                node: worker_id.clone(),
                role: ShardRole::Replica,
                layer_start: None,
                layer_end: None,
                tp_rank: None,
                tp_world: None,
            }],
        };
        let _ = stub.load_plan(&plan).await;
        match stub
            .infer(InferRequest {
                model: model.to_string(),
                prompt: prompt.to_string(),
                max_tokens,
            })
            .await
        {
            Ok(out) => {
                let mut g = state.write().await;
                g.settle_infer_success(
                    request_id,
                    out.text,
                    out.prompt_tokens,
                    out.completion_tokens,
                    &worker_id,
                );
            }
            Err(e) => {
                let mut g = state.write().await;
                g.settle_infer_error(request_id, e.to_string());
            }
        }
    }

    match tokio::time::timeout(Duration::from_secs(30), rx).await {
        Ok(Ok(Ok(outcome))) => Ok(outcome),
        Ok(Ok(Err(e))) => Err(e),
        Ok(Err(_)) => Err("infer channel closed".into()),
        Err(_) => {
            let mut g = state.write().await;
            g.pending.remove(&request_id);
            let _ = (worker_account, device);
            Err("infer timed out".into())
        }
    }
}

use std::time::Duration;

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
