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
use joule_proto::SignedEnvelope;
use joule_proto::{
    resolve_cluster_model, ClusterCapacity, Envelope, Message, CLUSTER_MODEL, CLUSTER_MODEL_LABEL,
};
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
        .route("/v1/cluster/leases", get(lease_audit))
        .route("/v1/cluster/nodes", get(nodes))
        .route("/v1/models", get(models))
        .route("/v1/models/readiness", get(readiness))
        .route("/v1/public/snapshot", get(public_snapshot))
        .route("/v1/public/pubkey", get(public_pubkey))
        .route("/v1/public/ledger", get(public_ledger))
        .route("/v1/public/ledger/head", get(public_ledger_head))
        .route("/v1/public/audit/{account}", get(public_audit))
        .route("/v1/blobs", get(blob_catalog))
        .route("/v1/blobs/{sha256}", get(blob_locate))
        .route("/v1/broadcasts", get(list_broadcasts))
        .route("/v1/broadcasts/inject", post(inject_broadcast))
        .route("/v1/notices", get(list_notices))
        .route("/v1/operator/status", get(operator_status))
        .route("/v1/operator/pins", get(operator_pins))
        .route("/v1/operator/audit", get(operator_key_audit))
        .route("/v1/mesh/peers", get(mesh_peers))
        .route("/v1/mesh/plan", get(mesh_plan))
        .route("/v1/dht/keys", get(dht_keys))
        .route("/v1/dht/get/{*key}", get(dht_get))
        .route("/v1/bootstrap", get(bootstrap_info))
        .route("/v1/chat/completions", post(chat_completions))
        .route("/v1/account", get(account))
        .route("/v1/account/donate", post(account_donate))
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
        "operator_paused": g.operator_paused,
        "service_live": g.service_live,
        "blob_digests": g.blobs.catalog().len(),
        "mesh_peers": g.mesh.healthy_count(),
        "mesh_total": g.mesh.snapshot().total,
        "dht_records": g.dht.len(),
    }))
}

async fn mesh_peers(State(app): State<App>) -> impl IntoResponse {
    let g = app.state.read().await;
    let snap = g.mesh.snapshot();
    Json(json!({
        "ok": true,
        "law": "decentral discovery Phase A — multiaddrs for direct dial (docs/design/decentral-discovery-v0.md)",
        "healthy": snap.healthy,
        "total": snap.total,
        "peers": snap.peers.iter().map(|p| json!({
            "node": p.node.to_string(),
            "multiaddrs": p.multiaddrs,
            "load": p.load,
            "healthy": p.healthy,
            "blob_count": p.blob_count,
            "mem_mib": p.mem_mib,
            "verified_mem_mib": p.verified_mem_mib,
            "throughput_class": p.throughput_class,
        })).collect::<Vec<_>>(),
    }))
}

/// Phase D: PlanOffer geometry from mesh **verified** mem (claims excluded).
async fn mesh_plan(State(app): State<App>) -> impl IntoResponse {
    let g = app.state.read().await;
    let donors = g.mesh_plan_donors();
    // Fall back to cluster verified registry when mesh has no verified donors yet.
    let plan = if !donors.is_empty() {
        joule_cluster::plan_from_mesh_donors(&donors)
    } else {
        g.cluster.plan_full_pool()
    };
    match plan {
        Ok(p) => Json(json!({
            "ok": true,
            "law": "decentral Phase D PlanOffer from mesh membership (docs/design/decentral-discovery-v0.md)",
            "source": if donors.is_empty() { "cluster_registry" } else { "mesh_peer_alive" },
            "donors": donors.len(),
            "plan": p,
        }))
        .into_response(),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "ok": false, "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn dht_keys(State(app): State<App>) -> impl IntoResponse {
    let g = app.state.read().await;
    Json(json!({
        "ok": true,
        "law": "decentral discovery Phase C — content-addressed DHT lite (docs/design/decentral-discovery-v0.md)",
        "count": g.dht.len(),
        "keys": g.dht.snapshot_keys(),
    }))
}

async fn dht_get(State(app): State<App>, Path(key): Path<String>) -> impl IntoResponse {
    let g = app.state.read().await;
    // Axum may pass path with leading slash stripped; accept peer/… and blob/….
    let key = key.trim_start_matches('/').to_string();
    match g.dht.get_raw(&key) {
        Some(v) => {
            let parsed: Value = serde_json::from_str(&v.value_json).unwrap_or(Value::Null);
            (
                StatusCode::OK,
                Json(json!({
                    "ok": true,
                    "key": v.key,
                    "seq": v.seq,
                    "updated_unix_ms": v.updated_unix_ms,
                    "value": parsed,
                })),
            )
                .into_response()
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({ "ok": false, "error": "key not found", "key": key })),
        )
            .into_response(),
    }
}

async fn bootstrap_info() -> impl IntoResponse {
    let loaded = joule_dht::BootstrapList::load_default();
    Json(json!({
        "ok": true,
        "law": "bootstrap lists are replaceable — not f00 payload origin",
        "loaded": loaded.is_some(),
        "bootstrap": loaded,
        "search_paths": joule_dht::BootstrapList::default_search_paths()
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>(),
        "env": "JOULE_BOOTSTRAP",
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

/// Auditable stream-lease trail: active holds + recent grant/agree/release events.
async fn lease_audit(State(app): State<App>) -> impl IntoResponse {
    let g = app.state.read().await;
    let sched = g.cluster.scheduler_snapshot();
    let active: Vec<Value> = g
        .leases
        .audit_trail()
        .iter()
        .rev()
        .take(64)
        .map(|e| {
            json!({
                "lease_id": e.lease_id.to_string(),
                "request_id": e.request_id.to_string(),
                "account": e.account,
                "plan_hash_hex": e.plan_hash_hex,
                "accepts": e.accepts,
                "event": e.event,
                "detail": e.detail,
                "unix_secs": e.unix_secs,
            })
        })
        .collect();
    Json(json!({
        "ok": true,
        "law": "stream lease free→used→free; PlanAccept confirm_hex fail-closed (joule-cluster::lease)",
        "stream_slots_total": sched.stream_slots_total,
        "stream_slots_used": sched.stream_slots_used,
        "stream_slots_free": sched.stream_slots_free,
        "active_leases": g.leases.active_count(),
        "audit": active,
    }))
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
    let operator_paused = g.operator_paused;
    let nodes_model_loaded = g.nodes_model_loaded.len();
    match readiness_for_pool_ex(vram, backends, flags, growth) {
        Ok(r) => {
            let mut v = serde_json::to_value(r).unwrap_or_else(|_| json!({}));
            if let Some(obj) = v.as_object_mut() {
                obj.insert("operator_paused".into(), json!(operator_paused));
                obj.insert("nodes_model_loaded".into(), json!(nodes_model_loaded));
                obj.insert("operator_pubkey_configured".into(), json!(true));
            }
            Json(v).into_response()
        }
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
    let last_cp = g.ledger.last_signed_checkpoint();
    Json(json!({
        "ok": true,
        "chain_ok": chain_ok,
        "head": head,
        "no_money": true,
        "law": "balances only from sealed chain replay; claims ≠ verified VRAM",
        "last_signed_checkpoint": last_cp.map(|e| json!({
            "height": e.height,
            "entry_hash_hex": e.entry_hash_hex,
            "notaries": e.notaries,
            "notary_attestations": e.notary_attestations,
            "reason": e.reason,
        })),
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

/// Content-addressed seed directory (who has which hash). **No payload bytes on f00.**
async fn blob_catalog(State(app): State<App>) -> impl IntoResponse {
    let g = app.state.read().await;
    Json(json!({
        "ok": true,
        "law": "website only — peers seed by sha256 (docs/design/distribution-v0.md)",
        "blobs": g.blobs.catalog(),
    }))
}

async fn blob_locate(State(app): State<App>, Path(sha256): Path<String>) -> impl IntoResponse {
    let g = app.state.read().await;
    let peers = g.blobs.peers_for(&sha256);
    Json(json!({
        "ok": true,
        "sha256": sha256,
        "seeders": peers.iter().map(|(n, m)| json!({
            "node": n.to_string(),
            "size": m.size,
            "kind": m.kind,
            "name": m.name,
        })).collect::<Vec<_>>(),
        "count": peers.len(),
    }))
}

/// Recent operator-signed broadcasts (verify client-side with public key too).
async fn list_broadcasts(State(app): State<App>) -> impl IntoResponse {
    let g = app.state.read().await;
    Json(json!({
        "ok": true,
        "law": "signed by operator key; swarm relays; f00 is not a push CDN",
        "operator_pubkey": crate::broadcast::operator_pubkey_hex(),
        "official_fingerprint": crate::pins::MASTER_OPENPGP_FINGERPRINT,
        "unofficial_override": crate::pins::unofficial_operator_allowed(),
        "messages": g.broadcasts.recent(),
    }))
}

/// Notices only (dashboard strip).
async fn list_notices(State(app): State<App>) -> impl IntoResponse {
    let g = app.state.read().await;
    let notices: Vec<_> = g
        .broadcasts
        .recent()
        .iter()
        .filter(|e| e.kind == joule_proto::OperatorKind::Notice)
        .cloned()
        .collect();
    Json(json!({
        "ok": true,
        "notices": notices,
    }))
}

async fn operator_status(State(app): State<App>) -> impl IntoResponse {
    let g = app.state.read().await;
    Json(json!({
        "ok": true,
        "service_live": g.service_live,
        "operator_paused": g.operator_paused,
        "heartbeat_mint_mj": g.heartbeat_mint_mj,
        "dual_verify_every": g.dual_verify_every,
        "active_chunks": g.active_chunks.len(),
        "active_replica_factor": g.active_replica_factor,
        "blob_digests": g.blobs.catalog().len(),
        "broadcasts_recent": g.broadcasts.recent().len(),
        "revoked_envelopes": g.broadcasts.revoked_count(),
        "pending_blob_xfers": g.pending_blob_xfers.len(),
        "operator_pubkey": crate::broadcast::operator_pubkey_hex(),
        "official_fingerprint": crate::pins::MASTER_OPENPGP_FINGERPRINT,
        "unofficial_override": crate::pins::unofficial_operator_allowed(),
        "law": "pause/resume/policy via signed operator bus; digests peer-seeded",
    }))
}

async fn operator_pins() -> impl IntoResponse {
    Json(json!({
        "ok": true,
        "master_openpgp_fingerprint": crate::pins::MASTER_OPENPGP_FINGERPRINT,
        "protocol_ed25519_hex": crate::pins::PROTOCOL_ED25519_PUBKEY_HEX,
        "effective_protocol_hex": crate::broadcast::operator_pubkey_hex(),
        "unofficial_override": crate::pins::unofficial_operator_allowed(),
        "official_master_url": crate::pins::OFFICIAL_MASTER_ASC_URL,
        "official_protocol_url": crate::pins::OFFICIAL_PROTOCOL_PUB_URL,
        "law": "embed is root of trust; website must match embed (docs/design/master-key-trust-v0.md)",
    }))
}

async fn operator_key_audit() -> impl IntoResponse {
    let audit = crate::official_fetch::audit_official_keys().await;
    let status = if audit.ok {
        StatusCode::OK
    } else {
        StatusCode::CONFLICT
    };
    (status, Json(audit))
}

/// Inject a pre-signed operator envelope. Always verifies against official embed
/// (or lab override with JOULE_ALLOW_UNOFFICIAL_OPERATOR=1).
async fn inject_broadcast(
    State(app): State<App>,
    Json(envelope): Json<SignedEnvelope>,
) -> impl IntoResponse {
    let accept = {
        let mut g = app.state.write().await;
        let now = crate::broadcast::now_ms();
        g.broadcasts.accept(envelope.clone(), now)
    };
    match accept {
        Ok(true) => {
            let routes = app.routes.lock().await;
            let msg = Message::OperatorBroadcast {
                envelope: envelope.clone(),
            };
            let mut n = 0u32;
            for (node, peer_tx) in routes.iter() {
                let out = Envelope::new(node.clone(), msg.clone());
                if peer_tx.send(out).is_ok() {
                    n += 1;
                }
            }
            drop(routes);
            crate::operator_actions::apply_operator_actions(&app, &envelope).await;
            Json(json!({
                "ok": true,
                "flooded_agents": n,
                "id": envelope.id,
                "kind": envelope.kind,
            }))
            .into_response()
        }
        Ok(false) => Json(json!({
            "ok": true,
            "duplicate": true,
            "id": envelope.id,
        }))
        .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": e })),
        )
            .into_response(),
    }
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
struct DonateRequest {
    /// Millijoules to donate into the pool (voluntary).
    amount: i64,
}

/// POST /v1/account/donate — burn unused mJ from the caller and spread equitably.
async fn account_donate(
    State(app): State<App>,
    headers: HeaderMap,
    Json(body): Json<DonateRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
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
    let mut g = app.state.write().await;
    match g.donate_to_pool(&account, body.amount) {
        Ok(r) => Ok(Json(json!({
            "ok": true,
            "law": "voluntary donate; sealed burn + equitable redistribute (eco=v0)",
            "amount": r.amount,
            "donor": account,
            "donor_balance": g.ledger.balance(&account),
            "recipients": r.recipient_credits.iter().map(|c| json!({
                "account": c.account,
                "delta_millijoules": c.delta_millijoules,
                "reason": c.reason,
            })).collect::<Vec<_>>(),
            "ledger_head": g.ledger.head(),
        }))),
        Err(e) => {
            let status = if e.contains("insufficient") {
                StatusCode::PAYMENT_REQUIRED
            } else {
                StatusCode::BAD_REQUEST
            };
            Err((status, Json(json!({ "ok": false, "error": e }))))
        }
    }
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
    /// Phase D coordinator: `mesh_request_infer` or `control_dispatch`.
    joule_coordination: String,
    joule_pool_mem_mib: u64,
    joule_shard_count: u32,
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
    } else if e.contains("pool full")
        || e.contains("no free stream")
        || e.contains("no healthy workers")
        || e.contains("agent not connected")
        || e.contains("timed out waiting")
        || e.contains("PlanAccept")
        || e.contains("plan ")
    {
        StatusCode::SERVICE_UNAVAILABLE
    } else if e.contains("insufficient balance") {
        StatusCode::PAYMENT_REQUIRED
    } else {
        StatusCode::BAD_REQUEST
    };
    let mut body = json!({"error": e});
    if status == StatusCode::SERVICE_UNAVAILABLE && e.contains("pool full") {
        body = json!({
            "error": e,
            "code": "pool_full",
            "retryable": true,
        });
    }
    (status, Json(body))
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
        if g.operator_paused {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "error": "operator paused service (signed pause_service / policy)",
                    "operator_paused": true,
                })),
            ));
        }
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

    // Phase D: dispatch_infer prefers mesh RequestInfer path when mesh donors
    // advertise mem_mib; falls back to control try_acquire_stream.
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
        joule_coordination: out.coordination,
        joule_pool_mem_mib: out.pool_mem_mib,
        joule_shard_count: out.shard_count,
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
