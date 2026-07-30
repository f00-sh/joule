//! HTTP API: capacity dashboard feed + OpenAI-compatible chat.

use crate::state::{AccountInfo, SharedState};
use crate::tcp::dispatch_infer;
use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use joule_proto::ClusterCapacity;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use uuid::Uuid;

pub fn router(state: SharedState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/cluster/capacity", get(capacity))
        .route("/v1/models", get(models))
        .route("/v1/chat/completions", post(chat_completions))
        .route("/v1/account", get(account))
        .with_state(state)
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
}

async fn healthz() -> impl IntoResponse {
    Json(json!({"ok": true, "service": "joule-control"}))
}

async fn capacity(State(state): State<SharedState>) -> Json<ClusterCapacity> {
    let mut g = state.write().await;
    g.prune();
    Json(g.cluster.capacity())
}

async fn models(State(state): State<SharedState>) -> impl IntoResponse {
    let g = state.read().await;
    let cap = g.cluster.capacity();
    let data: Vec<Value> = cap
        .models_available
        .iter()
        .map(|id| {
            json!({
                "id": id,
                "object": "model",
                "owned_by": "joule"
            })
        })
        .collect();
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
    State(state): State<SharedState>,
    headers: HeaderMap,
) -> Result<Json<AccountInfo>, (StatusCode, Json<Value>)> {
    let key = bearer_key(&headers).ok_or_else(|| {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "missing Bearer API key"})),
        )
    })?;
    let g = state.read().await;
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

async fn chat_completions(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Json(body): Json<ChatRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<Value>)> {
    if body.stream == Some(true) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "stream not implemented yet"})),
        ));
    }
    let key = bearer_key(&headers).ok_or_else(|| {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "missing Bearer API key"})),
        )
    })?;
    let account = {
        let g = state.read().await;
        g.account_for_key(&key)
            .map(|s| s.to_string())
            .ok_or_else(|| {
                (
                    StatusCode::UNAUTHORIZED,
                    Json(json!({"error": "invalid API key"})),
                )
            })?
    };

    let model = body.model.unwrap_or_else(|| "kimi-open-q4".to_string());
    let prompt = body
        .messages
        .iter()
        .map(|m| format!("{}: {}", m.role, m.content))
        .collect::<Vec<_>>()
        .join("\n");
    let max_tokens = body.max_tokens.unwrap_or(256);

    match dispatch_infer(&state, &account, &model, &prompt, max_tokens).await {
        Ok(out) => {
            let created = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let resp = ChatResponse {
                id: format!("chatcmpl-{}", Uuid::new_v4()),
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
        Err(e) => {
            let status = if e.contains("contribution required") {
                StatusCode::FORBIDDEN
            } else if e.contains("no healthy workers") {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::BAD_REQUEST
            };
            Err((status, Json(json!({"error": e}))))
        }
    }
}
