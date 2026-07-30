//! Publish live pool snapshots to the public Cloudflare edge (`joule.f00.sh/api/ingest`).
//!
//! Env:
//! - `JOULE_EDGE_URL` — full ingest URL (default `https://joule.f00.sh/api/ingest`)
//! - `JOULE_EDGE_TOKEN` — Bearer token matching Pages `INGEST_TOKEN`
//! - `JOULE_EDGE_DISABLE=1` — turn off publishing

use crate::state::ControlState;
use joule_runtime::readiness_for_pool_ex;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, warn};

static LAST_PUBLISH_MS: AtomicU64 = AtomicU64::new(0);

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Whether edge publish is configured (token present and not disabled).
pub fn edge_enabled() -> bool {
    if std::env::var("JOULE_EDGE_DISABLE").ok().as_deref() == Some("1") {
        return false;
    }
    std::env::var("JOULE_EDGE_TOKEN")
        .map(|t| !t.trim().is_empty())
        .unwrap_or(false)
}

fn edge_url() -> String {
    std::env::var("JOULE_EDGE_URL").unwrap_or_else(|_| "https://joule.f00.sh/api/ingest".into())
}

/// Build the public snapshot JSON from control state (call under state lock).
pub fn build_snapshot(state: &ControlState) -> serde_json::Value {
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

/// Fire-and-forget publish. Rate-limited to at most once per ~2s unless `force`.
pub fn publish_snapshot_async(state: &ControlState, force: bool) {
    if !edge_enabled() {
        return;
    }
    let now = now_ms();
    let last = LAST_PUBLISH_MS.load(Ordering::Relaxed);
    if !force && now.saturating_sub(last) < 2000 {
        return;
    }
    LAST_PUBLISH_MS.store(now, Ordering::Relaxed);

    let body = build_snapshot(state);
    let url = edge_url();
    let token = std::env::var("JOULE_EDGE_TOKEN").unwrap_or_default();

    tokio::spawn(async move {
        let client = reqwest::Client::new();
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
                debug!(%url, status = %resp.status(), "edge snapshot published");
            }
            Ok(resp) => {
                let status = resp.status();
                let text = resp.text().await.unwrap_or_default();
                warn!(%url, %status, %text, "edge snapshot publish failed");
            }
            Err(e) => warn!(%url, error = %e, "edge snapshot publish error"),
        }
    });
}
