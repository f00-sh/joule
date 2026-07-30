//! Control plane for the joule distributed compute cluster.
//!
//! - TCP agent port: hello / heartbeat / infer dispatch
//! - HTTP API: live dashboard + capacity + OpenAI-shaped chat (contribute-to-consume)

mod http;
mod persist;
mod state;
mod tcp;

pub use http::router;
pub use persist::default_data_dir;
pub use state::{AccountInfo, ControlState, NodeView, SharedState};
pub use tcp::{agent_handle_infer, run_agent_listener, run_agent_session};

use anyhow::Result;
use std::net::SocketAddr;
use std::path::PathBuf;
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
    let agent_addr = agent_listener.local_addr()?;
    info!(%agent_addr, "agent listener");

    let app = router(Arc::clone(&state));
    let http_listener = TcpListener::bind(http_addr).await?;
    let http_addr = http_listener.local_addr()?;
    info!(%http_addr, "http api + dashboard");

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

/// Bind ephemeral ports and return (agent_addr, http_addr) for tests.
pub async fn serve_ephemeral(state: SharedState) -> Result<(SocketAddr, SocketAddr, tokio::task::JoinHandle<()>)> {
    let agent_listener = TcpListener::bind("127.0.0.1:0").await?;
    let http_listener = TcpListener::bind("127.0.0.1:0").await?;
    let agent_addr = agent_listener.local_addr()?;
    let http_addr = http_listener.local_addr()?;

    let agent_state = Arc::clone(&state);
    tokio::spawn(async move {
        let _ = run_agent_listener(agent_state, agent_listener).await;
    });

    let prune_state = Arc::clone(&state);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(2));
        loop {
            tick.tick().await;
            prune_state.write().await.prune();
        }
    });

    let app = router(state);
    let handle = tokio::spawn(async move {
        let _ = axum::serve(http_listener, app).await;
    });

    // tiny delay for listeners
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    Ok((agent_addr, http_addr, handle))
}

pub fn load_or_init_state(data_dir: Option<PathBuf>) -> Result<SharedState> {
    match data_dir {
        Some(dir) => ControlState::shared_with_data_dir(dir),
        None => Ok(ControlState::shared()),
    }
}
