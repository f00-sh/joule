//! Control plane for the joule distributed compute cluster.
//!
//! - TCP agent port: hello / heartbeat / infer dispatch / challenges
//! - HTTP API: live dashboard + capacity + OpenAI-shaped chat (contribute-to-consume)

mod app;
mod blobs;
mod broadcast;
mod edge;
mod http;
mod identity;
mod mesh;
mod model_update;
mod official_fetch;
mod operator_actions;
mod persist;
mod pins;
mod state;
mod tcp;

pub use app::App;
pub use broadcast::{
    body_sha256_hex, now_ms, operator_preimage, operator_pubkey_hex, verify_operator_sig,
    BroadcastLog,
};
pub use http::router;
pub use identity::{verify_preimage, PoolIdentity};
pub use persist::default_data_dir;
pub use pins::{
    unofficial_operator_allowed, MASTER_OPENPGP_ASC, MASTER_OPENPGP_FINGERPRINT,
    PROTOCOL_ED25519_PUBKEY_HEX,
};
pub use state::{AccountInfo, ControlState, NodeView, SharedState};
pub use state::{CoordinationPath, InferOutcome};
pub use tcp::{
    agent_handle_challenge, agent_handle_infer, challenge_loop, dispatch_infer, dispatch_mesh_infer,
    mesh_donors_ready, run_agent_listener, run_agent_session,
};

use anyhow::{Context, Result};
use std::net::SocketAddr;
use std::path::PathBuf;
use tokio::net::TcpListener;
use tracing::info;

/// Run control plane until cancelled (agent TCP + HTTP API + challenges).
pub async fn serve(app: App, agent_addr: SocketAddr, http_addr: SocketAddr) -> Result<()> {
    info!(
        fingerprint = crate::pins::MASTER_OPENPGP_FINGERPRINT,
        protocol = crate::pins::PROTOCOL_ED25519_PUBKEY_HEX,
        unofficial = crate::pins::unofficial_operator_allowed(),
        "operator trust pins (official embed)"
    );
    tokio::spawn(async {
        let audit = crate::official_fetch::audit_official_keys().await;
        if audit.ok {
            info!(message = %audit.message, "official key audit");
        } else {
            tracing::error!(message = %audit.message, "official key audit FAILED");
        }
    });

    let agent_listener = TcpListener::bind(agent_addr)
        .await
        .with_context(|| format!("bind agent listener {agent_addr}"))?;
    let agent_addr = agent_listener.local_addr()?;
    info!(%agent_addr, "agent listener");

    let http_listener = TcpListener::bind(http_addr)
        .await
        .with_context(|| format!("bind http listener {http_addr}"))?;
    let http_addr = http_listener.local_addr()?;
    info!(%http_addr, "http api + dashboard");

    let agent_app = app.clone();
    let agent_task = tokio::spawn(async move {
        if let Err(e) = run_agent_listener(agent_app, agent_listener).await {
            tracing::error!(error = %e, "agent listener exited");
        }
    });

    let prune_app = app.clone();
    let prune_task = tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
        let mut rebalance_n = 0u32;
        loop {
            tick.tick().await;
            {
                let mut g = prune_app.state.write().await;
                g.prune();
                // Optional edge mirror; landing page multi-sources signed /v1/public/snapshot too.
                edge::publish_snapshot_async(&g, Some(&prune_app.identity), false);
            }
            broadcast_pool_status(&prune_app).await;
            rebalance_n = rebalance_n.wrapping_add(1);
            // Every ~30s: pull replicas for under-replicated model chunks.
            if rebalance_n % 6 == 0 {
                model_update::rebalance_replicas(&prune_app).await;
            }
        }
    });

    let challenge_app = app.clone();
    let challenge_task = tokio::spawn(async move {
        challenge_loop(challenge_app).await;
    });

    let router = router(app);
    let result = axum::serve(http_listener, router).await;
    agent_task.abort();
    prune_task.abort();
    challenge_task.abort();
    result?;
    Ok(())
}

/// Bind ephemeral ports for tests. Returns (agent_addr, http_addr, join handle).
pub async fn serve_ephemeral(
    app: App,
) -> Result<(SocketAddr, SocketAddr, tokio::task::JoinHandle<()>)> {
    let agent_listener = TcpListener::bind("127.0.0.1:0").await?;
    let http_listener = TcpListener::bind("127.0.0.1:0").await?;
    let agent_addr = agent_listener.local_addr()?;
    let http_addr = http_listener.local_addr()?;

    let agent_app = app.clone();
    tokio::spawn(async move {
        let _ = run_agent_listener(agent_app, agent_listener).await;
    });

    let prune_app = app.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(2));
        loop {
            tick.tick().await;
            prune_app.state.write().await.prune();
        }
    });

    let challenge_app = app.clone();
    tokio::spawn(async move {
        // Faster challenges in tests if any donors online.
        challenge_loop(challenge_app).await;
    });

    let router = router(app);
    let handle = tokio::spawn(async move {
        let _ = axum::serve(http_listener, router).await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    Ok((agent_addr, http_addr, handle))
}

pub fn load_or_init_app(data_dir: Option<PathBuf>) -> Result<App> {
    App::load_or_init(data_dir)
}

// Back-compat name used by CLI earlier.
pub fn load_or_init_state(data_dir: Option<PathBuf>) -> Result<App> {
    load_or_init_app(data_dir)
}

/// Push pool readiness to every connected agent so they can arm/prepare weights.
pub async fn broadcast_pool_status(app: &App) {
    use joule_proto::{Envelope, Message, NodeId};
    use joule_runtime::ManifestFile;

    let (vram, backends, flags, growth) = {
        let g = app.state.read().await;
        let cap = g.cluster.capacity();
        (
            cap.mem_mib_healthy,
            cap.nodes_healthy,
            g.runtime_flags(),
            g.vram_growth_mib_per_sec(),
        )
    };
    let Ok(r) = joule_runtime::readiness_for_pool_ex(vram, backends, flags, growth) else {
        return;
    };
    let quant = ManifestFile::load_default()
        .ok()
        .and_then(|m| m.primary().cloned())
        .and_then(|spec| {
            // Control does not know each node VRAM here in bulk; agents re-pick.
            spec.pick_quant(8192).map(|q| q.id.clone())
        });

    let msg = Message::PoolStatus {
        pool_vram_mib: r.pool_vram_mib,
        backends: r.backends,
        pool_ready: r.pool_ready,
        weights_published: r.weights_published,
        pool_progress_pct: r.pool_progress_pct,
        inference_mode: serde_json::to_value(r.inference_mode)
            .ok()
            .and_then(|v| v.as_str().map(|s| s.to_string()))
            .unwrap_or_default(),
        message: r.message,
        recommend_quant: quant,
    };

    let routes = app.routes.lock().await;
    for (node, tx) in routes.iter() {
        let env = Envelope::new(node.clone(), msg.clone());
        let _ = tx.send(env);
    }
    let _ = std::mem::size_of::<NodeId>();
}
