//! Process-wide control app: shared state + live agent routes.

use crate::state::{ControlState, SharedState};
use joule_proto::{Envelope, NodeId};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

pub type AgentRoutes = Arc<Mutex<HashMap<NodeId, mpsc::UnboundedSender<Envelope>>>>;

#[derive(Clone)]
pub struct App {
    pub state: SharedState,
    pub routes: AgentRoutes,
}

impl App {
    pub fn new(state: SharedState) -> Self {
        Self {
            state,
            routes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn load_or_init(data_dir: Option<PathBuf>) -> anyhow::Result<Self> {
        let state = match data_dir {
            Some(dir) => ControlState::shared_with_data_dir(dir)?,
            None => ControlState::shared(),
        };
        Ok(Self::new(state))
    }
}
