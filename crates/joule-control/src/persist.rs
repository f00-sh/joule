//! Disk persistence: accounts, keys, **sealed ledger chain**, economy windows.

use crate::state::{AccountEconomy, ControlState};
use anyhow::{Context, Result};
use joule_ledger::SealedEntry;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct EconomySnap {
    pub contributed_mj_window: i64,
    pub consumed_mj_window: i64,
    pub continuous_online_secs: u64,
    pub best_mem_mib: u32,
    /// Churn counter (disconnects in fairness window) — must survive restart.
    #[serde(default)]
    pub disconnects_window: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Snapshot {
    pub version: u32,
    pub account_keys: HashMap<String, String>,
    /// Legacy field — ignored on load (balances only from chain).
    #[serde(default)]
    pub balances: HashMap<String, i64>,
    #[serde(default)]
    pub economy: HashMap<String, EconomySnap>,
    /// Authoritative sealed ledger events (self-govern v0).
    #[serde(default)]
    pub chain: Vec<SealedEntry>,
    /// Signed-bus operator pause (blocks chat).
    #[serde(default)]
    pub operator_paused: bool,
    #[serde(default)]
    pub service_live: bool,
    #[serde(default)]
    pub heartbeat_mint_mj: Option<i64>,
    #[serde(default)]
    pub dual_verify_every: Option<u64>,
    /// Recent operator envelopes (for dashboard after restart; re-verify on load if key set).
    #[serde(default)]
    pub broadcasts: Vec<joule_proto::SignedEnvelope>,
}

impl Snapshot {
    pub const VERSION: u32 = 5;
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
    let mut economy = HashMap::new();
    for (acct, eco) in &state.account_economy {
        let continuous = match eco.online_since {
            Some(since) => eco
                .continuous_online_secs
                .saturating_add(since.elapsed().as_secs()),
            None => eco.continuous_online_secs,
        };
        economy.insert(
            acct.clone(),
            EconomySnap {
                contributed_mj_window: eco.contributed_mj_window,
                consumed_mj_window: eco.consumed_mj_window,
                continuous_online_secs: continuous,
                best_mem_mib: eco.best_mem_mib,
                disconnects_window: eco.disconnects_window,
            },
        );
    }
    let snap = Snapshot {
        version: Snapshot::VERSION,
        account_keys: state.account_keys.clone(),
        balances: HashMap::new(), // not authoritative
        economy,
        chain: state.ledger.sealed().entries().to_vec(),
        operator_paused: state.operator_paused,
        service_live: state.service_live,
        heartbeat_mint_mj: Some(state.heartbeat_mint_mj),
        dual_verify_every: Some(state.dual_verify_every),
        broadcasts: state.broadcasts.recent().to_vec(),
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
    if !snap.chain.is_empty() {
        if let Err(e) = state.ledger.restore_chain(snap.chain) {
            tracing::error!(error = %e, "sealed chain restore failed — starting empty ledger");
            state.ledger = joule_ledger::Ledger::new();
        }
    } else if !snap.balances.is_empty() {
        // Migration: old v1/v2 snapshots had raw balances only — cannot verify.
        // Refuse to rehydrate unverified balances (self-govern).
        tracing::warn!(
            "ignoring legacy balances without chain ({} accounts) — self-govern requires sealed ledger",
            snap.balances.len()
        );
    }
    state.account_economy.clear();
    for (acct, e) in snap.economy {
        state.account_economy.insert(
            acct,
            AccountEconomy {
                contributed_mj_window: e.contributed_mj_window,
                consumed_mj_window: e.consumed_mj_window,
                online_since: None,
                continuous_online_secs: e.continuous_online_secs,
                best_mem_mib: e.best_mem_mib,
                disconnects_window: e.disconnects_window,
                prompt_tokens_used: 0,
                completion_tokens_used: 0,
            },
        );
    }
    state.operator_paused = snap.operator_paused;
    state.service_live = snap.service_live;
    if let Some(v) = snap.heartbeat_mint_mj {
        if (0..=1_000_000).contains(&v) {
            state.heartbeat_mint_mj = v;
        }
    }
    if let Some(v) = snap.dual_verify_every {
        state.dual_verify_every = v;
    }
    // Re-accept stored broadcasts (sig checked if key pinned).
    let now = crate::broadcast::now_ms();
    for env in snap.broadcasts {
        let _ = state.broadcasts.accept(env, now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::ControlState;
    use joule_ledger::leecher_factors_bp;
    use joule_proto::{DeviceClass, NodeCaps, NodeId};
    use uuid::Uuid;

    #[test]
    fn disconnects_window_survives_save_load() {
        let dir = std::env::temp_dir().join(format!("joule-persist-churn-{}", Uuid::new_v4()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let mut state = ControlState::new();
        state.data_dir = Some(dir.clone());
        state.ensure_account("alice");
        {
            let eco = state.economy_mut("alice");
            eco.disconnects_window = 17;
            eco.contributed_mj_window = 100;
            eco.consumed_mj_window = 50;
            eco.best_mem_mib = 4096;
            eco.continuous_online_secs = 3600;
        }
        save(&dir, &state).unwrap();

        let mut state2 = ControlState::new();
        let snap = load(&dir).unwrap().expect("snapshot");
        apply_snapshot(&mut state2, snap);
        let eco = state2.account_economy.get("alice").expect("alice eco");
        assert_eq!(
            eco.disconnects_window, 17,
            "churn counter must survive control restart"
        );
        assert_eq!(eco.contributed_mj_window, 100);
        assert_eq!(eco.best_mem_mib, 4096);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn control_donate_does_not_wash_leecher_window() {
        let mut state = ControlState::new();
        // Seed accounts on sealed ledger
        state.ledger.mint_contribution("rich", 1000, "seed").unwrap();
        state.ledger.mint_contribution("leecher", 10, "seed").unwrap();
        state.ensure_account("rich");
        state.ensure_account("leecher");
        // Leecher has consume ≫ contribute in fairness window
        {
            let eco = state.economy_mut("leecher");
            eco.contributed_mj_window = 10;
            eco.consumed_mj_window = 1000;
            eco.best_mem_mib = 4096;
        }
        {
            let eco = state.economy_mut("rich");
            eco.best_mem_mib = 8192;
            eco.contributed_mj_window = 500;
        }
        let (mint_before, _) = {
            let eco = state.account_economy.get("leecher").unwrap();
            leecher_factors_bp(eco.contributed_mj_window, eco.consumed_mj_window)
        };
        assert!(mint_before < 10_000, "precondition: leeching");

        let r = state
            .donate_to_pool("rich", 300)
            .expect("ControlState.donate_to_pool");
        assert_eq!(r.amount, 300);
        assert_eq!(state.ledger.balance("rich"), 700);
        // Leecher received sealed credit but contribute window unchanged
        let eco = state.account_economy.get("leecher").unwrap();
        assert_eq!(
            eco.contributed_mj_window, 10,
            "donate_receive must not wash anti-leech contribute window"
        );
        assert_eq!(eco.consumed_mj_window, 1000);
        let (mint_after, _) = leecher_factors_bp(eco.contributed_mj_window, eco.consumed_mj_window);
        assert_eq!(
            mint_after, mint_before,
            "leecher mint factor must be unchanged by pool gift"
        );
        // Balance may increase from donate_receive
        assert!(state.ledger.balance("leecher") > 10);
    }

    #[test]
    fn note_online_disconnect_increments_churn_and_persists() {
        let dir = std::env::temp_dir().join(format!("joule-persist-note-{}", Uuid::new_v4()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let mut state = ControlState::new();
        state.data_dir = Some(dir.clone());
        let id = NodeId::new();
        state.register_node(
            id.clone(),
            "bob",
            NodeCaps::for_cluster(DeviceClass::Gpu, 8192, 40),
        );
        // Simulate online then offline (churn)
        state.note_online("bob", 4096, true);
        state.note_online("bob", 0, false);
        state.note_online("bob", 4096, true);
        state.note_online("bob", 0, false);
        let d = state
            .account_economy
            .get("bob")
            .map(|e| e.disconnects_window)
            .unwrap_or(0);
        assert!(d >= 2, "disconnects_window={d}");
        save(&dir, &state).unwrap();

        let mut state2 = ControlState::new();
        apply_snapshot(&mut state2, load(&dir).unwrap().unwrap());
        assert_eq!(
            state2
                .account_economy
                .get("bob")
                .map(|e| e.disconnects_window)
                .unwrap_or(0),
            d
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
