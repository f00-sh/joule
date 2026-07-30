//! Disk persistence for accounts, API keys, and millijoule balances.

use crate::state::ControlState;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Snapshot {
    pub version: u32,
    pub account_keys: HashMap<String, String>,
    pub balances: HashMap<String, i64>,
}

impl Snapshot {
    pub const VERSION: u32 = 1;
}

pub fn default_data_dir() -> PathBuf {
    if let Ok(p) = std::env::var("JOULE_DATA_DIR") {
        return PathBuf::from(p);
    }
    dirs_next_home()
        .map(|h| h.join(".local/share/joule"))
        .unwrap_or_else(|| PathBuf::from("./.joule-data"))
}

fn dirs_next_home() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

pub fn snapshot_path(dir: &Path) -> PathBuf {
    dir.join("state.json")
}

pub fn load(dir: &Path) -> Result<Option<Snapshot>> {
    let path = snapshot_path(dir);
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    let snap: Snapshot = serde_json::from_str(&raw).context("parse state.json")?;
    Ok(Some(snap))
}

pub fn save(dir: &Path, state: &ControlState) -> Result<()> {
    std::fs::create_dir_all(dir).with_context(|| format!("mkdir {}", dir.display()))?;
    let snap = Snapshot {
        version: Snapshot::VERSION,
        account_keys: state.account_keys.clone(),
        balances: state.ledger.balances().clone(),
    };
    let path = snapshot_path(dir);
    let tmp = path.with_extension("json.tmp");
    let raw = serde_json::to_string_pretty(&snap)?;
    std::fs::write(&tmp, raw).with_context(|| format!("write {}", tmp.display()))?;
    std::fs::rename(&tmp, &path).with_context(|| format!("rename {}", path.display()))?;
    Ok(())
}

pub fn apply_snapshot(state: &mut ControlState, snap: Snapshot) {
    state.account_keys = snap.account_keys;
    state.keys.clear();
    for (account, key) in &state.account_keys {
        state.keys.insert(key.clone(), account.clone());
    }
    state.ledger.restore_balances(snap.balances);
}
