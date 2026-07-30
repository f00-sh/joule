//! Public snapshot publish + multi-path secret loading.
//!
//! Edge ingest (`joule.f00.sh/api/ingest`) is **one optional mirror**, not authority.
//! Controls also expose `GET /v1/public/snapshot` (signed) for direct multi-source fetch.
//!
//! Secrets resolution order for `JOULE_EDGE_TOKEN`:
//! 1. env `JOULE_EDGE_TOKEN`
//! 2. env `JOULE_EDGE_TOKEN_FILE` (path)
//! 3. `./.ingest-token`
//! 4. `$JOULE_DATA_DIR/edge.token` / `~/.local/share/joule/edge.token`
//! 5. `~/.config/f00/joule/edge.token`  (f00 operator core path)

use crate::identity::{announce_preimage, snapshot_preimage, PoolIdentity};
use crate::state::ControlState;
use joule_runtime::readiness_for_pool_ex;
use serde::Serialize;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, warn};

static LAST_PUBLISH_MS: AtomicU64 = AtomicU64::new(0);
static EDGE_TOKEN: OnceLock<Option<String>> = OnceLock::new();

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn home() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

fn read_token_file(path: &std::path::Path) -> Option<String> {
    let s = std::fs::read_to_string(path).ok()?;
    let t = s.trim().to_string();
    if t.is_empty() {
        None
    } else {
        Some(t)
    }
}

/// Resolve edge ingest bearer token from env / f00 core paths.
pub fn resolve_edge_token() -> Option<String> {
    EDGE_TOKEN
        .get_or_init(|| {
            if let Ok(t) = std::env::var("JOULE_EDGE_TOKEN") {
                let t = t.trim().to_string();
                if !t.is_empty() {
                    return Some(t);
                }
            }
            if let Ok(p) = std::env::var("JOULE_EDGE_TOKEN_FILE") {
                if let Some(t) = read_token_file(std::path::Path::new(&p)) {
                    return Some(t);
                }
            }
            let mut candidates: Vec<PathBuf> = vec![PathBuf::from(".ingest-token")];
            if let Ok(d) = std::env::var("JOULE_DATA_DIR") {
                candidates.push(PathBuf::from(d).join("edge.token"));
            }
            if let Some(h) = home() {
                candidates.push(h.join(".local/share/joule/edge.token"));
                candidates.push(h.join(".config/f00/joule/edge.token"));
            }
            for p in candidates {
                if let Some(t) = read_token_file(&p) {
                    debug!(path = %p.display(), "loaded JOULE_EDGE_TOKEN from file");
                    return Some(t);
                }
            }
            None
        })
        .clone()
}

/// Whether edge publish is configured (token present and not disabled).
pub fn edge_enabled() -> bool {
    if std::env::var("JOULE_EDGE_DISABLE").ok().as_deref() == Some("1") {
        return false;
    }
    resolve_edge_token().is_some()
}

fn edge_url() -> String {
    std::env::var("JOULE_EDGE_URL").unwrap_or_else(|_| "https://joule.f00.sh/api/ingest".into())
}

#[derive(Serialize)]
struct BodyForSign<'a> {
    capacity: &'a Value,
    readiness: &'a Value,
    scheduler: &'a Value,
    nodes: &'a Value,
}

/// Build a signed public snapshot (also used for multi-source decentralization).
pub fn build_signed_snapshot(state: &ControlState, identity: &PoolIdentity) -> Value {
    let cap = state.cluster.capacity();
    let flags = state.runtime_flags();
    let growth = state.vram_growth_mib_per_sec();
    let readiness =
        readiness_for_pool_ex(cap.mem_mib_healthy, cap.nodes_healthy, flags, growth).ok();
    let scheduler = state.cluster.scheduler_snapshot();
    let nodes = state.node_views();

    let capacity_v = serde_json::to_value(&cap).unwrap_or(Value::Null);
    let readiness_v = serde_json::to_value(&readiness).unwrap_or(Value::Null);
    let scheduler_v = serde_json::to_value(&scheduler).unwrap_or(Value::Null);
    let nodes_v = serde_json::to_value(&nodes).unwrap_or_else(|_| json!([]));

    let body = BodyForSign {
        capacity: &capacity_v,
        readiness: &readiness_v,
        scheduler: &scheduler_v,
        nodes: &nodes_v,
    };
    let body_json = serde_json::to_string(&body).unwrap_or_else(|_| "{}".into());
    let updated_unix_ms = now_ms();
    let pre = snapshot_preimage(&identity.pool_id, updated_unix_ms, &body_json);
    let signature_hex = identity.sign_bytes(&pre);
    let pub_info = identity.public_info();

    json!({
        "ok": true,
        "source": "control",
        "pool_id": identity.pool_id,
        "updated_at": chrono_like(updated_unix_ms),
        "updated_unix_ms": updated_unix_ms,
        "capacity": capacity_v,
        "readiness": readiness_v,
        "scheduler": scheduler_v,
        "nodes": nodes_v,
        "signature": {
            "algorithm": "ed25519",
            "scheme": "sha256(pool_id\\nupdated_unix_ms\\nbody_json)",
            "verifying_key_hex": pub_info.verifying_key_hex,
            "signature_hex": signature_hex,
            "body_json": body_json,
        }
    })
}

fn chrono_like(ms: u64) -> String {
    // RFC3339-ish without chrono dep: leave as unix ms string fallback if needed.
    // Prefer ISO from control host UTC via simple format is hard without chrono —
    // store unix; site uses updated_unix_ms.
    format!("{ms}")
}

/// Legacy unsigned body (edge ingest still accepts either).
pub fn build_snapshot(state: &ControlState) -> Value {
    let cap = state.cluster.capacity();
    let flags = state.runtime_flags();
    let growth = state.vram_growth_mib_per_sec();
    let readiness =
        readiness_for_pool_ex(cap.mem_mib_healthy, cap.nodes_healthy, flags, growth).ok();
    let scheduler = state.cluster.scheduler_snapshot();
    let nodes = state.node_views();
    json!({
        "source": "control",
        "capacity": cap,
        "readiness": readiness,
        "scheduler": scheduler,
        "nodes": nodes,
    })
}

fn announce_url() -> String {
    std::env::var("JOULE_ANNOUNCE_URL")
        .unwrap_or_else(|_| "https://joule.f00.sh/api/announce".into())
}

/// Public base URL of this control (e.g. https://pool.example.com) for decentralization.
pub fn public_base_url() -> Option<String> {
    std::env::var("JOULE_PUBLIC_URL")
        .ok()
        .map(|s| s.trim().trim_end_matches('/').to_string())
        .filter(|s| !s.is_empty())
}

/// Fire-and-forget:
/// 1) signed announce to public directory (no token — needs JOULE_PUBLIC_URL)
/// 2) optional privileged edge ingest if token present
pub fn publish_snapshot_async(state: &ControlState, identity: Option<&PoolIdentity>, force: bool) {
    let now = now_ms();
    let last = LAST_PUBLISH_MS.load(Ordering::Relaxed);
    if !force && now.saturating_sub(last) < 2000 {
        return;
    }
    LAST_PUBLISH_MS.store(now, Ordering::Relaxed);

    let id = identity.cloned();
    let body = match identity {
        Some(i) => build_signed_snapshot(state, i),
        None => build_snapshot(state),
    };
    let token = resolve_edge_token();
    let do_ingest = edge_enabled();
    let public = public_base_url();

    tokio::spawn(async move {
        let client = reqwest::Client::new();

        // --- decentralized announce (no privilege) ---
        if let (Some(base), Some(id)) = (public, id.as_ref()) {
            let snapshot_url = format!("{base}/v1/public/snapshot");
            let updated = body
                .get("updated_unix_ms")
                .and_then(|v| v.as_u64())
                .unwrap_or_else(now_ms);
            let pre = announce_preimage(&id.pool_id, &snapshot_url, updated);
            let signature_hex = id.sign_bytes(&pre);
            let pub_info = id.public_info();
            let announce = json!({
                "pool_id": id.pool_id,
                "snapshot_url": snapshot_url,
                "verifying_key_hex": pub_info.verifying_key_hex,
                "signature_hex": signature_hex,
                "updated_unix_ms": updated,
            });
            let url = announce_url();
            match client
                .post(&url)
                .header("content-type", "application/json")
                .json(&announce)
                .timeout(std::time::Duration::from_secs(5))
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    debug!(%url, "public announce ok");
                }
                Ok(resp) => {
                    warn!(%url, status = %resp.status(), "public announce failed");
                }
                Err(e) => warn!(%url, error = %e, "public announce error"),
            }
        }

        // --- optional privileged mirror ---
        if do_ingest {
            if let Some(token) = token {
                let url = edge_url();
                match client
                    .post(&url)
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", "application/json")
                    .json(&body)
                    .timeout(std::time::Duration::from_secs(5))
                    .send()
                    .await
                {
                    Ok(resp) if resp.status().is_success() => {
                        debug!(%url, "edge snapshot published");
                    }
                    Ok(resp) => {
                        warn!(%url, status = %resp.status(), "edge snapshot publish failed");
                    }
                    Err(e) => warn!(%url, error = %e, "edge snapshot publish error"),
                }
            }
        }
    });
}
