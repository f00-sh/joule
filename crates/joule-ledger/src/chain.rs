//! Sealed hash-chained millijoule ledger — balances exist only as chain replay.
//!
//! Users cannot set balances. Operators cannot silently edit a number without
//! breaking `entry_hash` linkage. See `docs/design/self-govern-v0.md`.

use crate::economy::ECONOMY_VERSION;
use crate::{LedgerError, Millijoule};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

pub const GENESIS_HASH: &str = "genesis";
pub const CHECKPOINT_EVERY: u64 = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryKind {
    MintContribute,
    BurnUsage,
    Checkpoint,
}

impl EntryKind {
    pub fn as_str(self) -> &'static str {
        match self {
            EntryKind::MintContribute => "mint_contribute",
            EntryKind::BurnUsage => "burn_usage",
            EntryKind::Checkpoint => "checkpoint",
        }
    }
}

/// One sealed, content-addressed ledger entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SealedEntry {
    pub height: u64,
    pub prev_hash_hex: String,
    pub entry_hash_hex: String,
    pub id: Uuid,
    pub account: String,
    pub delta_millijoules: Millijoule,
    pub reason: String,
    pub kind: EntryKind,
    pub unix_ms: u64,
    pub economy_version: String,
    /// Verified mem used for this mint (anti claim fraud), if applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verified_mem_mib: Option<u32>,
    /// Optional notary node ids recorded at checkpoints.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notaries: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainHead {
    pub height: u64,
    pub head_hash_hex: String,
    pub entries: u64,
    pub economy_version: String,
    pub protocol: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountAudit {
    pub account: String,
    pub balance_millijoules: Millijoule,
    pub event_count: u64,
    pub head: ChainHead,
    pub recent: Vec<SealedEntry>,
}

/// Append-only sealed ledger. Balances are always derived from the chain.
#[derive(Debug, Clone, Default)]
pub struct SealedLedger {
    entries: Vec<SealedEntry>,
    balances: HashMap<String, Millijoule>,
}

impl SealedLedger {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn balance(&self, account: &str) -> Millijoule {
        self.balances.get(account).copied().unwrap_or(0)
    }

    pub fn balances(&self) -> &HashMap<String, Millijoule> {
        &self.balances
    }

    pub fn entries(&self) -> &[SealedEntry] {
        &self.entries
    }

    pub fn head(&self) -> ChainHead {
        let (height, head_hash_hex) = match self.entries.last() {
            Some(e) => (e.height, e.entry_hash_hex.clone()),
            None => (0, GENESIS_HASH.to_string()),
        };
        ChainHead {
            height,
            head_hash_hex,
            entries: self.entries.len() as u64,
            economy_version: ECONOMY_VERSION.into(),
            protocol: "joule-sealed-ledger-v0".into(),
        }
    }

    pub fn ensure_account(&mut self, account: impl Into<String>) {
        self.balances.entry(account.into()).or_insert(0);
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    fn prev_hash(&self) -> String {
        self.entries
            .last()
            .map(|e| e.entry_hash_hex.clone())
            .unwrap_or_else(|| GENESIS_HASH.to_string())
    }

    /// Canonical hash for an entry body (excluding entry_hash itself).
    #[allow(clippy::too_many_arguments)]
    pub fn compute_entry_hash(
        height: u64,
        prev_hash_hex: &str,
        id: Uuid,
        account: &str,
        delta: Millijoule,
        reason: &str,
        kind: EntryKind,
        unix_ms: u64,
        economy_version: &str,
        verified_mem_mib: Option<u32>,
        notaries: &[String],
    ) -> String {
        let mut h = Sha256::new();
        h.update(height.to_string().as_bytes());
        h.update(b"|");
        h.update(prev_hash_hex.as_bytes());
        h.update(b"|");
        h.update(id.as_bytes());
        h.update(b"|");
        h.update(account.as_bytes());
        h.update(b"|");
        h.update(delta.to_string().as_bytes());
        h.update(b"|");
        h.update(reason.as_bytes());
        h.update(b"|");
        h.update(kind.as_str().as_bytes());
        h.update(b"|");
        h.update(unix_ms.to_string().as_bytes());
        h.update(b"|");
        h.update(economy_version.as_bytes());
        h.update(b"|");
        if let Some(m) = verified_mem_mib {
            h.update(m.to_string().as_bytes());
        }
        h.update(b"|");
        for n in notaries {
            h.update(n.as_bytes());
            h.update(b",");
        }
        hex::encode(h.finalize())
    }

    fn append_raw(
        &mut self,
        account: String,
        delta: Millijoule,
        reason: String,
        kind: EntryKind,
        verified_mem_mib: Option<u32>,
        notaries: Vec<String>,
    ) -> Result<SealedEntry, LedgerError> {
        let have = self.balance(&account);
        if delta < 0 && have + delta < 0 {
            return Err(LedgerError::Insufficient { have, need: -delta });
        }
        let height = self.entries.len() as u64;
        let prev_hash_hex = self.prev_hash();
        let id = Uuid::new_v4();
        let unix_ms = Self::now_ms();
        let economy_version = ECONOMY_VERSION.to_string();
        let entry_hash_hex = Self::compute_entry_hash(
            height,
            &prev_hash_hex,
            id,
            &account,
            delta,
            &reason,
            kind,
            unix_ms,
            &economy_version,
            verified_mem_mib,
            &notaries,
        );
        let entry = SealedEntry {
            height,
            prev_hash_hex,
            entry_hash_hex,
            id,
            account: account.clone(),
            delta_millijoules: delta,
            reason,
            kind,
            unix_ms,
            economy_version,
            verified_mem_mib,
            notaries,
        };
        *self.balances.entry(account).or_insert(0) += delta;
        self.entries.push(entry.clone());
        Ok(entry)
    }

    pub fn mint_contribution(
        &mut self,
        account: impl Into<String>,
        millijoules: Millijoule,
        detail: impl Into<String>,
        verified_mem_mib: Option<u32>,
    ) -> Result<SealedEntry, LedgerError> {
        assert!(millijoules >= 0, "mint must be non-negative");
        self.append_raw(
            account.into(),
            millijoules,
            format!("contribute:{}", detail.into()),
            EntryKind::MintContribute,
            verified_mem_mib,
            vec![],
        )
    }

    pub fn burn_usage(
        &mut self,
        account: impl Into<String>,
        millijoules: Millijoule,
        detail: impl Into<String>,
    ) -> Result<SealedEntry, LedgerError> {
        assert!(millijoules >= 0, "burn must be non-negative");
        self.append_raw(
            account.into(),
            -millijoules,
            format!("usage:{}", detail.into()),
            EntryKind::BurnUsage,
            None,
            vec![],
        )
    }

    /// Protocol checkpoint: zero-value entry binding head + notary set.
    pub fn checkpoint(&mut self, notaries: Vec<String>) -> Result<SealedEntry, LedgerError> {
        let head = self.head().head_hash_hex;
        let reason = format!("checkpoint|head={head}|notaries={}", notaries.join(","));
        self.append_raw(
            "_protocol".into(),
            0,
            reason,
            EntryKind::Checkpoint,
            None,
            notaries,
        )
    }

    pub fn maybe_checkpoint(&mut self, notaries: Vec<String>) -> Option<SealedEntry> {
        if self.entries.is_empty() {
            return None;
        }
        let h = self.entries.len() as u64;
        if h % CHECKPOINT_EVERY == 0 {
            self.checkpoint(notaries).ok()
        } else {
            None
        }
    }

    /// Full chain integrity check (detects tampering).
    pub fn verify_chain(&self) -> Result<(), String> {
        let mut prev = GENESIS_HASH.to_string();
        let mut balances: HashMap<String, Millijoule> = HashMap::new();
        for (i, e) in self.entries.iter().enumerate() {
            if e.height != i as u64 {
                return Err(format!("height mismatch at {i}"));
            }
            if e.prev_hash_hex != prev {
                return Err(format!("prev_hash break at height {}", e.height));
            }
            let expect = Self::compute_entry_hash(
                e.height,
                &e.prev_hash_hex,
                e.id,
                &e.account,
                e.delta_millijoules,
                &e.reason,
                e.kind,
                e.unix_ms,
                &e.economy_version,
                e.verified_mem_mib,
                &e.notaries,
            );
            if expect != e.entry_hash_hex {
                return Err(format!("entry_hash mismatch at height {}", e.height));
            }
            let bal = balances.entry(e.account.clone()).or_insert(0);
            *bal += e.delta_millijoules;
            if *bal < 0 {
                return Err(format!("negative balance at height {}", e.height));
            }
            prev = e.entry_hash_hex.clone();
        }
        for (acct, b) in &balances {
            if self.balance(acct) != *b {
                return Err(format!("balance drift for {acct}"));
            }
        }
        Ok(())
    }

    /// Replace chain and recompute balances (load from disk).
    pub fn restore_chain(&mut self, entries: Vec<SealedEntry>) -> Result<(), String> {
        self.entries = entries;
        self.balances.clear();
        for e in &self.entries {
            *self.balances.entry(e.account.clone()).or_insert(0) += e.delta_millijoules;
        }
        self.verify_chain()?;
        Ok(())
    }

    pub fn slice_from(&self, from_height: u64, limit: usize) -> &[SealedEntry] {
        let start = from_height as usize;
        if start >= self.entries.len() {
            return &[];
        }
        let end = (start + limit).min(self.entries.len());
        &self.entries[start..end]
    }

    pub fn audit_account(&self, account: &str, recent: usize) -> AccountAudit {
        let recent_entries: Vec<SealedEntry> = self
            .entries
            .iter()
            .rev()
            .filter(|e| e.account == account)
            .take(recent)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        let event_count = self.entries.iter().filter(|e| e.account == account).count() as u64;
        AccountAudit {
            account: account.to_string(),
            balance_millijoules: self.balance(account),
            event_count,
            head: self.head(),
            recent: recent_entries,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chain_links_and_balances() {
        let mut led = SealedLedger::new();
        led.mint_contribution("alice", 100, "work", Some(8192))
            .unwrap();
        led.burn_usage("alice", 40, "chat").unwrap();
        assert_eq!(led.balance("alice"), 60);
        led.verify_chain().unwrap();
        let head = led.head();
        assert_eq!(head.entries, 2);
        assert_ne!(head.head_hash_hex, GENESIS_HASH);
    }

    #[test]
    fn tamper_detected() {
        let mut led = SealedLedger::new();
        led.mint_contribution("bob", 50, "x", None).unwrap();
        led.entries[0].delta_millijoules = 9999;
        assert!(led.verify_chain().is_err());
    }

    #[test]
    fn restore_replays() {
        let mut led = SealedLedger::new();
        led.mint_contribution("a", 10, "t", None).unwrap();
        led.mint_contribution("b", 20, "t", None).unwrap();
        let entries = led.entries().to_vec();
        let mut led2 = SealedLedger::new();
        led2.restore_chain(entries).unwrap();
        assert_eq!(led2.balance("a"), 10);
        assert_eq!(led2.balance("b"), 20);
    }

    #[test]
    fn overdraft_rejected() {
        let mut led = SealedLedger::new();
        led.mint_contribution("z", 5, "t", None).unwrap();
        assert!(led.burn_usage("z", 10, "x").is_err());
        assert_eq!(led.len(), 1);
    }
}
