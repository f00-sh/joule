//! Process-wide control app: shared state + live agent routes + schedule wakeups.

use crate::identity::PoolIdentity;
use crate::state::{ControlState, SharedState};
use joule_proto::{Envelope, NodeId};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, Notify};

pub type AgentRoutes = Arc<Mutex<HashMap<NodeId, mpsc::UnboundedSender<Envelope>>>>;

#[derive(Clone)]
pub struct App {
    pub state: SharedState,
    pub routes: AgentRoutes,
    /// Wakes jobs waiting for free compute slots.
    pub schedule_notify: Arc<Notify>,
    /// Operator identity for signed public snapshots (multi-source decentralization).
    pub identity: Arc<PoolIdentity>,
}

impl App {
    pub fn new(state: SharedState) -> Self {
        // Share notify with control state for release_slot wakeups.
        let schedule_notify = {
            let g = state.try_read().ok();
            g.and_then(|s| s.schedule_notify.clone())
                .unwrap_or_else(|| Arc::new(Notify::new()))
        };
        // Ensure state holds the same notify.
        if let Ok(mut g) = state.try_write() {
            g.schedule_notify = Some(Arc::clone(&schedule_notify));
        }
        let identity = PoolIdentity::load_or_create(None).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "pool identity load failed; ephemeral key");
            PoolIdentity::load_or_create(Some(std::path::Path::new("./.joule-data")))
                .expect("ephemeral pool identity")
        });
        Self {
            state,
            routes: Arc::new(Mutex::new(HashMap::new())),
            schedule_notify,
            identity: Arc::new(identity),
        }
    }

    pub fn load_or_init(data_dir: Option<PathBuf>) -> anyhow::Result<Self> {
        let notify = Arc::new(Notify::new());
        let identity = Arc::new(PoolIdentity::load_or_create(data_dir.as_deref())?);
        let state = match data_dir {
            Some(dir) => ControlState::shared_with_data_dir(dir, Arc::clone(&notify))?,
            None => ControlState::shared_with_notify(Arc::clone(&notify)),
        };
        Ok(Self {
            state,
            routes: Arc::new(Mutex::new(HashMap::new())),
            schedule_notify: notify,
            identity,
        })
    }
}
