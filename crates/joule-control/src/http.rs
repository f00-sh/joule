//! HTTP API: dashboard, capacity, OpenAI-compatible chat (incl. SSE stream).

use crate::app::App;
use crate::state::{AccountInfo, NodeView};
use crate::tcp::dispatch_infer;
use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::stream::Stream;
use joule_proto::{resolve_cluster_model, ClusterCapacity, CLUSTER_MODEL, CLUSTER_MODEL_LABEL};
use serde::{Deserialize, Serialize};
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
    Json(json!({
        "ok": true,
        "service": "joule-control",
        "agents_connected": agents,
        "nodes_healthy": cap.nodes_healthy,
        "slots_free": sched.slots_free,
        "slots_used": sched.slots_used,
        "can_accept_work": sched.can_accept_work,
    }))
}

async fn capacity(State(app): State<App>) -> Json<ClusterCapacity> {
    let mut g = app.state.write().await;
    g.prune();
    Json(g.cluster.capacity())
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
    let online = g.cluster.pool_size() > 0;
    let data = if online {
        vec![json!({
            "id": CLUSTER_MODEL,
            "object": "model",
            "owned_by": "joule",
            "name": CLUSTER_MODEL_LABEL,
            "description": "Single cluster model; all healthy donors serve this model.",
            "pool_nodes": g.cluster.pool_size(),
        })]
    } else {
        vec![]
    };
    Json(json!({ "object": "list", "data": data }))
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
