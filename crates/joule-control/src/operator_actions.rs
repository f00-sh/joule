//! Allow-listed operator broadcast side-effects on control state + agent fan-out.
//!
//! Unknown kinds: store + flood only (never execute). Heavy payloads = digests only.

use crate::app::App;
use crate::model_update;
use joule_proto::{Envelope, Message, OperatorKind, SignedEnvelope};
use serde::Deserialize;
use tracing::{info, warn};

#[derive(Debug, Deserialize)]
struct PolicyBody {
    #[serde(default)]
    service_live: Option<bool>,
    #[serde(default)]
    heartbeat_mint_mj: Option<i64>,
    #[serde(default)]
    dual_verify_every: Option<u64>,
    #[serde(default)]
    pause: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct SoftwareBody {
    #[serde(default)]
    version: String,
    #[serde(default)]
    targets: Vec<SoftwareTargetBody>,
}

#[derive(Debug, Deserialize)]
struct SoftwareTargetBody {
    #[serde(default)]
    sha256: String,
}

/// After a verified envelope is accepted and flooded: run allow-listed control actions.
pub async fn apply_operator_actions(app: &App, envelope: &SignedEnvelope) {
    match envelope.kind {
        OperatorKind::ModelUpdate => {
            model_update::apply_model_update(app, envelope).await;
        }
        OperatorKind::SoftwareUpdate => {
            fanout_software_digests(app, envelope).await;
        }
        OperatorKind::PauseService => {
            let mut g = app.state.write().await;
            g.operator_paused = true;
            g.service_live = false;
            g.mark_dirty();
            info!("operator: service paused");
        }
        OperatorKind::ResumeService => {
            let mut g = app.state.write().await;
            g.operator_paused = false;
            g.mark_dirty();
            info!("operator: service unpaused (service_live may still be false until mesh ready)");
        }
        OperatorKind::Policy => {
            apply_policy(app, &envelope.body_json).await;
        }
        OperatorKind::Revoke => {
            // Ids applied inside BroadcastLog::accept; log for ops.
            info!("operator revoke processed (ids blacklisted)");
        }
        OperatorKind::Notice | OperatorKind::Other => {
            // Notice: already in broadcast log for dashboard; agents print locally.
            // Other: relay only.
        }
    }
}

async fn apply_policy(app: &App, body_json: &str) {
    let body: PolicyBody = match serde_json::from_str(body_json) {
        Ok(b) => b,
        Err(e) => {
            warn!(error = %e, "policy body parse failed");
            return;
        }
    };
    let mut g = app.state.write().await;
    if let Some(v) = body.service_live {
        g.service_live = v;
    }
    if let Some(true) = body.pause {
        g.operator_paused = true;
        g.service_live = false;
    }
    if let Some(false) = body.pause {
        g.operator_paused = false;
    }
    if let Some(v) = body.heartbeat_mint_mj {
        if (0..=1_000_000).contains(&v) {
            g.heartbeat_mint_mj = v;
        }
    }
    if let Some(v) = body.dual_verify_every {
        g.dual_verify_every = v;
    }
    g.mark_dirty();
    info!(
        service_live = g.service_live,
        heartbeat_mint_mj = g.heartbeat_mint_mj,
        dual_verify_every = g.dual_verify_every,
        "operator policy applied"
    );
}

/// Tell all agents to obtain software digests (they match os/arch themselves).
async fn fanout_software_digests(app: &App, envelope: &SignedEnvelope) {
    let body: SoftwareBody = match serde_json::from_str(&envelope.body_json) {
        Ok(b) => b,
        Err(e) => {
            warn!(error = %e, "software_update parse failed");
            return;
        }
    };
    let digests: Vec<String> = body
        .targets
        .iter()
        .map(|t| t.sha256.to_lowercase())
        .filter(|h| h.len() == 64)
        .collect();
    if digests.is_empty() {
        warn!("software_update: no target digests");
        return;
    }
    info!(
        version = %body.version,
        digests = digests.len(),
        "software_update: FetchDigests to agents (peer seed only)"
    );
    let routes = app.routes.lock().await;
    for (node, tx) in routes.iter() {
        let msg = Message::FetchDigests {
            digests: digests.clone(),
            reason: format!("software_update:{}:{}", body.version, envelope.id),
            replica_factor: 2,
        };
        let _ = tx.send(Envelope::new(node.clone(), msg));
    }
}
