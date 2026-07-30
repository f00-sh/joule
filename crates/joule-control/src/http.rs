//! HTTP API: dashboard, capacity, OpenAI-compatible chat (incl. SSE stream).

use crate::app::App;
use crate::state::{AccountInfo, NodeView};
use crate::tcp::dispatch_infer;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::stream::Stream;
use joule_proto::{resolve_cluster_model, ClusterCapacity, CLUSTER_MODEL, CLUSTER_MODEL_LABEL};
use joule_runtime::{readiness_for_pool_ex, RuntimeFlags};
use serde::{Deserialize, Serialize};
// Query + Path already imported above
use serde_json::{json, Value};
use std::convert::Infallible;
use std::time::Duration;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use uuid::Uuid;

const DASHBOARD_HTML: &str = include_str!("dashboard.html");

pub fn router(app: App) -> Router {
    Router::new()
        .route("/", get(dashboard))
        .route("/dashboard", get(dashboard))
        .route("/healthz", get(healthz))
        .route("/v1/cluster/capacity", get(capacity))
        .route("/v1/cluster/scheduler", get(scheduler))
        .route("/v1/cluster/nodes", get(nodes))
        .route("/v1/models", get(models))
        .route("/v1/models/readiness", get(readiness))
        .route("/v1/public/snapshot", get(public_snapshot))
        .route("/v1/public/pubkey", get(public_pubkey))
        .route("/v1/public/ledger", get(public_ledger))
        .route("/v1/public/ledger/head", get(public_ledger_head))
        .route("/v1/public/audit/{account}", get(public_audit))
        .route("/v1/chat/completions", post(chat_completions))
        .route("/v1/account", get(account))
        .with_state(app)
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
}

async fn dashboard() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

async fn healthz(State(app): State<App>) -> impl IntoResponse {
    let g = app.state.read().await;
    let cap = g.cluster.capacity();
    let agents = app.routes.lock().await.len();
    let sched = g.cluster.scheduler_snapshot();
    let dev = cap.logical_device.as_ref();
    Json(json!({
        "ok": true,
        "service": "joule-control",
        "logical_device": {
            "id": dev.map(|d| d.id.as_str()).unwrap_or("joule-pool"),
            "vram_gib": dev.map(|d| d.vram_gib).unwrap_or(0),
            "backends": dev.map(|d| d.backends).unwrap_or(0),
            "ready": dev.map(|d| d.ready).unwrap_or(false),
        },
        "agents_connected": agents,
        "stream_slots_free": sched.stream_slots_free,
        "stream_slots_used": sched.stream_slots_used,
        "can_accept_work": sched.can_accept_work,
    }))
}

fn enrich_capacity(
    mut cap: ClusterCapacity,
    flags: RuntimeFlags,
    growth: Option<f64>,
) -> ClusterCapacity {
    if let Some(ref mut ld) = cap.logical_device {
        if let Ok(r) = readiness_for_pool_ex(ld.vram_mib, ld.backends, flags, growth) {
            ld.model_ready = r.pool_ready;
            ld.model_progress_pct = r.pool_progress_pct;
            ld.inference_mode = serde_json::to_value(r.inference_mode)
                .ok()
                .and_then(|v| v.as_str().map(|s| s.to_string()))
                .unwrap_or_else(|| "stub_awaiting_pool".into());
            ld.readiness_message = r.message;
            if r.service_live {
                ld.ready = true;
            }
        }
    }
    cap
}

async fn capacity(State(app): State<App>) -> Json<ClusterCapacity> {
    let mut g = app.state.write().await;
    g.prune();
    let flags = g.runtime_flags();
    let growth = g.vram_growth_mib_per_sec();
    Json(enrich_capacity(g.cluster.capacity(), flags, growth))
}

async fn scheduler(State(app): State<App>) -> impl IntoResponse {
    let mut g = app.state.write().await;
    g.prune();
    Json(g.cluster.scheduler_snapshot())
}

#[derive(Serialize)]
struct NodesResponse {
    nodes: Vec<NodeView>,
}

async fn nodes(State(app): State<App>) -> Json<NodesResponse> {
    let mut g = app.state.write().await;
    g.prune();
    Json(NodesResponse {
        nodes: g.node_views(),
    })
}

async fn models(State(app): State<App>) -> impl IntoResponse {
    let g = app.state.read().await;
    let flags = g.runtime_flags();
    let growth = g.vram_growth_mib_per_sec();
    let cap = enrich_capacity(g.cluster.capacity(), flags, growth);
    let online = g.cluster.pool_size() > 0;
    let data = if online {
        let ld = cap.logical_device.as_ref();
        vec![json!({
            "id": CLUSTER_MODEL,
            "object": "model",
            "owned_by": "joule",
            "name": CLUSTER_MODEL_LABEL,
            "description": "Single cluster model on one logical device (aggregate donor VRAM).",
            "pool_nodes": g.cluster.pool_size(),
            "logical_vram_gib": ld.map(|d| d.vram_gib).unwrap_or(0),
            "model_ready": ld.map(|d| d.model_ready).unwrap_or(false),
            "model_progress_pct": ld.map(|d| d.model_progress_pct).unwrap_or(0),
            "inference_mode": ld.map(|d| d.inference_mode.clone()).unwrap_or_default(),
            "readiness_message": ld.map(|d| d.readiness_message.clone()).unwrap_or_default(),
            "model_loaded": flags.model_loaded,
            "service_live": flags.service_live,
        })]
    } else {
        vec![]
    };
    Json(json!({ "object": "list", "data": data }))
}

async fn readiness(State(app): State<App>) -> impl IntoResponse {
    let g = app.state.read().await;
    let cap = g.cluster.capacity();
    let vram = cap.mem_mib_healthy;
    let backends = cap.nodes_healthy;
    let flags = g.runtime_flags();
    let growth = g.vram_growth_mib_per_sec();
    match readiness_for_pool_ex(vram, backends, flags, growth) {
        Ok(r) => Json(json!(r)).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e}))).into_response(),
    }
}

/// Signed public pool snapshot — anyone may mirror; site multi-sources these.
async fn public_snapshot(State(app): State<App>) -> impl IntoResponse {
    let g = app.state.read().await;
    Json(crate::edge::build_signed_snapshot(&g, &app.identity))
}

async fn public_pubkey(State(app): State<App>) -> impl IntoResponse {
    Json(json!(app.identity.public_info()))
}

#[derive(Debug, Deserialize)]
struct LedgerQuery {
    #[serde(default)]
    from: u64,
    #[serde(default = "default_ledger_limit")]
    limit: usize,
}

fn default_ledger_limit() -> usize {
    256
}

/// Paginated sealed millijoule chain — recompute balances yourself.
async fn public_ledger(State(app): State<App>, Query(q): Query<LedgerQuery>) -> impl IntoResponse {
    let g = app.state.read().await;
    let limit = q.limit.clamp(1, 2000);
    let slice = g.ledger.sealed().slice_from(q.from, limit);
    Json(json!({
        "ok": true,
        "protocol": "joule-sealed-ledger-v0",
        "no_money": true,
        "head": g.ledger.head(),
        "from": q.from,
        "count": slice.len(),
        "entries": slice,
        "verify": "sha256 chain; balances = sum(delta) per account",
    }))
}

async fn public_ledger_head(State(app): State<App>) -> impl IntoResponse {
    let g = app.state.read().await;
    let head = g.ledger.head();
    let chain_ok = g.ledger.verify_chain().is_ok();
    Json(json!({
        "ok": true,
        "chain_ok": chain_ok,
        "head": head,
        "no_money": true,
        "law": "balances only from sealed chain replay; claims ≠ verified VRAM",
    }))
}

async fn public_audit(State(app): State<App>, Path(account): Path<String>) -> impl IntoResponse {
    let g = app.state.read().await;
    let audit = g.ledger.sealed().audit_account(&account, 32);
    Json(json!({
        "ok": true,
        "no_money": true,
        "audit": audit,
        "chain_ok": g.ledger.verify_chain().is_ok(),
    }))
}

fn bearer_key(headers: &HeaderMap) -> Option<String> {
    let auth = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let key = auth.strip_prefix("Bearer ").unwrap_or(auth).trim();
    if key.is_empty() {
        None
    } else {
        Some(key.to_string())
    }
}

async fn account(
    State(app): State<App>,
    headers: HeaderMap,
) -> Result<Json<AccountInfo>, (StatusCode, Json<Value>)> {
    let key = bearer_key(&headers).ok_or_else(|| {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "missing Bearer API key"})),
        )
    })?;
    let g = app.state.read().await;
    let account = g.account_for_key(&key).ok_or_else(|| {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "invalid API key"})),
        )
    })?;
    let info = g.account_info(account).ok_or_else(|| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "account missing"})),
        )
    })?;
    Ok(Json(info))
}

#[derive(Debug, Deserialize)]
struct ChatRequest {
    model: Option<String>,
    messages: Vec<ChatMessage>,
    max_tokens: Option<u32>,
    stream: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ChatMessage {
    role: String,
    content: String,
}

#[derive(Debug, Serialize)]
struct ChatResponse {
    id: String,
    object: &'static str,
    created: u64,
    model: String,
    choices: Vec<Choice>,
    usage: Usage,
}

#[derive(Debug, Serialize)]
struct Choice {
    index: u32,
    message: OutMessage,
    finish_reason: &'static str,
}

#[derive(Debug, Serialize)]
struct OutMessage {
    role: &'static str,
    content: String,
}

#[derive(Debug, Serialize)]
struct Usage {
    prompt_tokens: u32,
    completion_tokens: u32,
    total_tokens: u32,
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn map_err(e: String) -> (StatusCode, Json<Value>) {
    let status = if e.contains("contribution required") {
        StatusCode::FORBIDDEN
    } else if e.contains("no healthy workers") || e.contains("agent not connected") {
        StatusCode::SERVICE_UNAVAILABLE
    } else if e.contains("insufficient balance") {
        StatusCode::PAYMENT_REQUIRED
    } else {
        StatusCode::BAD_REQUEST
    };
    (status, Json(json!({"error": e})))
}

async fn chat_completions(
    State(app): State<App>,
    headers: HeaderMap,
    Json(body): Json<ChatRequest>,
) -> Result<Response, (StatusCode, Json<Value>)> {
    let key = bearer_key(&headers).ok_or_else(|| {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "missing Bearer API key"})),
        )
    })?;
    let account = {
        let g = app.state.read().await;
        g.account_for_key(&key)
            .map(|s| s.to_string())
            .ok_or_else(|| {
                (
                    StatusCode::UNAUTHORIZED,
                    Json(json!({"error": "invalid API key"})),
                )
            })?
    };

    let model = resolve_cluster_model(body.model.as_deref())
        .map_err(|e| (StatusCode::BAD_REQUEST, Json(json!({"error": e}))))?
        .to_string();
    let prompt = body
        .messages
        .iter()
        .map(|m| format!("{}: {}", m.role, m.content))
        .collect::<Vec<_>>()
        .join("\n");
    let max_tokens = body.max_tokens.unwrap_or(256);
    let stream = body.stream.unwrap_or(false);

    let out = dispatch_infer(&app, &account, &model, &prompt, max_tokens)
        .await
        .map_err(map_err)?;

    let id = format!("chatcmpl-{}", Uuid::new_v4());
    let created = now_secs();

    if stream {
        let stream = sse_chat_stream(id, created, model, out.text);
        return Ok(Sse::new(stream)
            .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
            .into_response());
    }

    let resp = ChatResponse {
        id,
        object: "chat.completion",
        created,
        model,
        choices: vec![Choice {
            index: 0,
            message: OutMessage {
                role: "assistant",
                content: out.text,
            },
            finish_reason: "stop",
        }],
        usage: Usage {
            prompt_tokens: out.prompt_tokens,
            completion_tokens: out.completion_tokens,
            total_tokens: out.prompt_tokens + out.completion_tokens,
        },
    };
    Ok(Json(resp).into_response())
}

fn sse_chat_stream(
    id: String,
    created: u64,
    model: String,
    text: String,
) -> impl Stream<Item = Result<Event, Infallible>> {
    async_stream::stream! {
        let tokens: Vec<&str> = text.split_inclusive(char::is_whitespace).collect();
        for (i, tok) in tokens.iter().enumerate() {
            let delta = if i == 0 {
                json!({"role": "assistant", "content": tok})
            } else {
                json!({"content": tok})
            };
            let chunk = json!({
                "id": id,
                "object": "chat.completion.chunk",
                "created": created,
                "model": model,
                "choices": [{
                    "index": 0,
                    "delta": delta,
                    "finish_reason": null
                }]
            });
            yield Ok(Event::default().data(chunk.to_string()));
        }
        let done = json!({
            "id": id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [{
                "index": 0,
                "delta": {},
                "finish_reason": "stop"
            }]
        });
        yield Ok(Event::default().data(done.to_string()));
        yield Ok(Event::default().data("[DONE]"));
    }
}
