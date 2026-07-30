//! Shared control-plane state: cluster registry, ledger, accounts, pending jobs.

use crate::persist;
use joule_cluster::Cluster;
use joule_ledger::{
    estimate_contribution_millijoules, estimate_usage_millijoules, Ledger, Millijoule,
};
use joule_proto::{DeviceClass, NodeCaps, NodeId};
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, RwLock};
use tracing::{info, warn};
use uuid::Uuid;

pub type SharedState = Arc<RwLock<ControlState>>;

#[derive(Debug, Clone, Serialize)]
pub struct AccountInfo {
    pub account: String,
    pub api_key: String,
    pub balance_millijoules: Millijoule,
    pub donating: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeView {
    pub id: String,
    pub account: String,
    pub device: String,
    pub mem_mib: u32,
    pub throughput_class: u16,
    pub healthy: bool,
    pub load: f32,
    pub models: Vec<String>,
}

#[derive(Debug)]
pub struct PendingInfer {
    pub account: String,
    pub tx: oneshot::Sender<Result<InferOutcome, String>>,
}

#[derive(Debug, Clone)]
pub struct InferOutcome {
    pub text: String,
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub worker_account: String,
    pub device: DeviceClass,
}

#[derive(Debug)]
pub struct ControlState {
    pub cluster: Cluster,
    pub ledger: Ledger,
    /// api_key → account
    pub keys: HashMap<String, String>,
    /// account → api_key
    pub account_keys: HashMap<String, String>,
    /// node → account (for mint routing)
    pub node_account: HashMap<NodeId, String>,
    /// outstanding infer jobs waiting on an agent reply
    pub pending: HashMap<Uuid, PendingInfer>,
    /// millijoules minted per healthy heartbeat (earn-by-providing)
    pub heartbeat_mint_mj: Millijoule,
    /// optional on-disk state directory
    pub data_dir: Option<PathBuf>,
    dirty: bool,
}

impl Default for ControlState {
    fn default() -> Self {
        Self::new()
    }
}

impl ControlState {
    pub fn new() -> Self {
        Self {
            cluster: Cluster::new(Duration::from_secs(30)),
            ledger: Ledger::new(),
            keys: HashMap::new(),
            account_keys: HashMap::new(),
            node_account: HashMap::new(),
            pending: HashMap::new(),
            heartbeat_mint_mj: 10,
            data_dir: None,
            dirty: false,
        }
    }

    pub fn shared() -> SharedState {
        Arc::new(RwLock::new(Self::new()))
    }

    pub fn shared_with_data_dir(dir: PathBuf) -> Result<SharedState, anyhow::Error> {
        let mut state = Self::new();
        state.data_dir = Some(dir.clone());
        if let Some(snap) = persist::load(&dir)? {
            info!(path = %dir.display(), "loaded persisted state");
            persist::apply_snapshot(&mut state, snap);
        }
        Ok(Arc::new(RwLock::new(state)))
    }

    fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    pub fn save_if_dirty(&mut self) {
        if !self.dirty {
            return;
        }
        let Some(dir) = self.data_dir.clone() else {
            return;
        };
        if let Err(e) = persist::save(&dir, self) {
            warn!(error = %e, "persist failed");
            return;
        }
        self.dirty = false;
    }

    pub fn prune(&mut self) {
        self.cluster.apply_staleness();
        self.cluster.reap_dead(Duration::from_secs(120));
        self.save_if_dirty();
    }

    pub fn ensure_account(&mut self, account: &str) -> String {
        if let Some(k) = self.account_keys.get(account) {
            return k.clone();
        }
        let api_key = format!("joule_{}", Uuid::new_v4().simple());
        self.account_keys
            .insert(account.to_string(), api_key.clone());
        self.keys.insert(api_key.clone(), account.to_string());
        self.ledger.ensure_account(account);
        self.mark_dirty();
        api_key
    }

    pub fn account_for_key(&self, api_key: &str) -> Option<&str> {
        self.keys.get(api_key).map(|s| s.as_str())
    }

    pub fn register_node(&mut self, id: NodeId, account: &str, caps: NodeCaps) -> String {
        let api_key = self.ensure_account(account);
        self.cluster
            .upsert_node(id.clone(), account.to_string(), caps);
        self.node_account.insert(id, account.to_string());
        api_key
    }

    pub fn on_heartbeat(
        &mut self,
        id: &NodeId,
        load: f32,
        healthy: bool,
    ) -> Result<Option<Millijoule>, joule_cluster::ClusterError> {
        self.cluster.set_health(id, healthy, load)?;
        if !healthy {
            return Ok(None);
        }
        let account = self
            .node_account
            .get(id)
            .cloned()
            .unwrap_or_else(|| "unknown".into());
        let mult = self
            .cluster
            .get(id)
            .map(|n| n.caps.device.contribution_multiplier())
            .unwrap_or(1);
        let mint = self.heartbeat_mint_mj.saturating_mul(i64::from(mult));
        let _ = self
            .ledger
            .mint_contribution(&account, mint, format!("heartbeat:{id}"));
        self.mark_dirty();
        // Persist often so restarts keep earned compute credit.
        self.save_if_dirty();
        Ok(Some(mint))
    }

    pub fn remove_node(&mut self, id: &NodeId) {
        self.cluster.remove_node(id);
        self.node_account.remove(id);
    }

    pub fn account_info(&self, account: &str) -> Option<AccountInfo> {
        let api_key = self.account_keys.get(account)?.clone();
        Some(AccountInfo {
            account: account.to_string(),
            api_key,
            balance_millijoules: self.ledger.balance(account),
            donating: self.cluster.account_is_donating(account),
        })
    }

    pub fn node_views(&self) -> Vec<NodeView> {
        let mut out: Vec<NodeView> = self
            .cluster
            .nodes()
            .map(|n| NodeView {
                id: n.id.to_string(),
                account: n.account.clone(),
                device: match n.caps.device {
                    DeviceClass::Gpu => "gpu".into(),
                    DeviceClass::Metal => "metal".into(),
                    DeviceClass::Cpu => "cpu".into(),
                },
                mem_mib: n.caps.mem_mib,
                throughput_class: n.caps.throughput_class,
                healthy: n.healthy,
                load: n.load,
                models: n.caps.models.clone(),
            })
            .collect();
        out.sort_by(|a, b| a.account.cmp(&b.account).then(a.id.cmp(&b.id)));
        out
    }

    pub fn settle_infer_success(
        &mut self,
        request_id: Uuid,
        text: String,
        prompt_tokens: u32,
        completion_tokens: u32,
        worker: &NodeId,
    ) {
        let device = self
            .cluster
            .get(worker)
            .map(|n| n.caps.device)
            .unwrap_or(DeviceClass::Cpu);
        let worker_account = self
            .node_account
            .get(worker)
            .cloned()
            .unwrap_or_else(|| "unknown".into());

        let mint =
            estimate_contribution_millijoules(completion_tokens, device.contribution_multiplier());
        let _ = self
            .ledger
            .mint_contribution(&worker_account, mint, format!("infer:{request_id}"));

        if let Some(pending) = self.pending.remove(&request_id) {
            let burn = estimate_usage_millijoules(prompt_tokens, completion_tokens);
            let burn_res =
                self.ledger
                    .burn_usage(&pending.account, burn, format!("chat:{request_id}"));
            self.mark_dirty();
            if let Err(e) = burn_res {
                let _ = pending.tx.send(Err(e.to_string()));
                return;
            }
            let _ = pending.tx.send(Ok(InferOutcome {
                text,
                prompt_tokens,
                completion_tokens,
                worker_account,
                device,
            }));
        } else {
            self.mark_dirty();
        }
    }

    pub fn settle_infer_error(&mut self, request_id: Uuid, error: String) {
        if let Some(pending) = self.pending.remove(&request_id) {
            let _ = pending.tx.send(Err(error));
        }
    }
}
