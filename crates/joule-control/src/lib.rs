//! Control plane for the joule distributed compute cluster.
//!
//! - TCP agent port: hello / heartbeat / infer dispatch
//! - HTTP API: live capacity + OpenAI-shaped chat (contribute-to-consume)

mod http;
mod state;
mod tcp;

pub use http::router;
pub use state::{AccountInfo, ControlState, SharedState};
pub use tcp::{agent_handle_infer, run_agent_listener, run_agent_session};

use anyhow::Result;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::info;

/// Run control plane until cancelled (agent TCP + HTTP API).
pub async fn serve(
    state: SharedState,
    agent_addr: SocketAddr,
    http_addr: SocketAddr,
) -> Result<()> {
    let agent_listener = TcpListener::bind(agent_addr).await?;
    info!(%agent_addr, "agent listener");

    let app = router(Arc::clone(&state));
    let http_listener = TcpListener::bind(http_addr).await?;
    info!(%http_addr, "http api");

    let agent_state = Arc::clone(&state);
    let agent_task = tokio::spawn(async move {
        if let Err(e) = run_agent_listener(agent_state, agent_listener).await {
            tracing::error!(error = %e, "agent listener exited");
        }
    });

    let prune_state = Arc::clone(&state);
    let prune_task = tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            tick.tick().await;
            prune_state.write().await.prune();
        }
    });

    axum::serve(http_listener, app).await?;
    agent_task.abort();
    prune_task.abort();
    Ok(())
}
