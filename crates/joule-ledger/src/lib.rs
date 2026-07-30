//! Append-only credit ledger denominated in **millijoules**.
//!
//! **Self-governing law:** balances exist only as the replay of a sealed hash chain
//! ([`chain::SealedLedger`]). Users cannot set balances. See `docs/design/self-govern-v0.md`.
//!
//! Fair scoring: [`economy`] — tenure, √VRAM, leecher penalties (`eco=v0`).

pub mod chain;
pub mod economy;

pub use chain::{
    AccountAudit, ChainHead, EntryKind, SealedEntry, SealedLedger, CHECKPOINT_EVERY, GENESIS_HASH,
};
pub use economy::{
    estimate_contribution_millijoules, estimate_usage_millijoules, leecher_factors_bp, score_burn,
    score_mint, BurnBreakdown, EconomyEvent, FairnessSnapshot, MintBreakdown, ECONOMY_VERSION,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

/// 1 joule = 1000 millijoules (integer math only).
pub type Millijoule = i64;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum LedgerError {
    #[error("insufficient balance: have {have} mJ, need {need} mJ")]
    Insufficient { have: Millijoule, need: Millijoule },
    #[error("account not found: {0}")]
    NotFound(String),
    #[error("chain integrity: {0}")]
    Chain(String),
}

/// Legacy event view (derived from sealed entries for older callers).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreditEvent {
    pub id: Uuid,
    pub account: String,
    pub delta_millijoules: Millijoule,
    pub reason: String,
}

/// Facade: sealed chain is the only ledger implementation.
#[derive(Debug, Default, Clone)]
pub struct Ledger {
    inner: SealedLedger,
}

impl Ledger {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn sealed(&self) -> &SealedLedger {
        &self.inner
    }

    pub fn sealed_mut(&mut self) -> &mut SealedLedger {
        &mut self.inner
    }

    pub fn balance(&self, account: &str) -> Millijoule {
        self.inner.balance(account)
    }

    pub fn ensure_account(&mut self, account: impl Into<String>) {
        self.inner.ensure_account(account);
    }

    pub fn mint_contribution(
        &mut self,
        account: impl Into<String>,
        millijoules: Millijoule,
        detail: impl Into<String>,
    ) -> Result<CreditEvent, LedgerError> {
        self.mint_contribution_verified(account, millijoules, detail, None)
    }

    /// Mint with attested verified memory (anti GPU-claim fraud).
    pub fn mint_contribution_verified(
        &mut self,
        account: impl Into<String>,
        millijoules: Millijoule,
        detail: impl Into<String>,
        verified_mem_mib: Option<u32>,
    ) -> Result<CreditEvent, LedgerError> {
        let e = self
            .inner
            .mint_contribution(account, millijoules, detail, verified_mem_mib)?;
        Ok(CreditEvent {
            id: e.id,
            account: e.account,
            delta_millijoules: e.delta_millijoules,
            reason: e.reason,
        })
    }

    pub fn burn_usage(
        &mut self,
        account: impl Into<String>,
        millijoules: Millijoule,
        detail: impl Into<String>,
    ) -> Result<CreditEvent, LedgerError> {
        let e = self.inner.burn_usage(account, millijoules, detail)?;
        Ok(CreditEvent {
            id: e.id,
            account: e.account,
            delta_millijoules: e.delta_millijoules,
            reason: e.reason,
        })
    }

    pub fn events(&self) -> Vec<CreditEvent> {
        self.inner
            .entries()
            .iter()
            .map(|e| CreditEvent {
                id: e.id,
                account: e.account.clone(),
                delta_millijoules: e.delta_millijoules,
                reason: e.reason.clone(),
            })
            .collect()
    }

    pub fn balances(&self) -> &std::collections::HashMap<String, Millijoule> {
        self.inner.balances()
    }

    /// Load sealed chain (only legitimate restore path).
    pub fn restore_chain(&mut self, entries: Vec<SealedEntry>) -> Result<(), LedgerError> {
        self.inner
            .restore_chain(entries)
            .map_err(LedgerError::Chain)
    }

    /// Deprecated path: restore balances **without** chain is rejected for self-govern.
    /// Kept name for compile; rebuilds empty chain (balances discarded) unless chain provided via restore_chain.
    pub fn restore_balances(&mut self, _balances: std::collections::HashMap<String, Millijoule>) {
        // Intentionally do not accept raw balances — they are not authoritative.
        // Callers must use restore_chain. Leaving ledger empty if only balances were stored.
        tracing_stub_restore();
    }

    pub fn verify_chain(&self) -> Result<(), LedgerError> {
        self.inner.verify_chain().map_err(LedgerError::Chain)
    }

    pub fn head(&self) -> ChainHead {
        self.inner.head()
    }

    pub fn maybe_checkpoint(&mut self, notaries: Vec<String>) -> Option<SealedEntry> {
        self.inner.maybe_checkpoint(notaries)
    }
}

fn tracing_stub_restore() {
    // Avoid hard dependency on tracing in ledger for this no-op.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mint_and_burn() {
        let mut led = Ledger::new();
        led.mint_contribution("alice", 1000, "gpu-job").unwrap();
        assert_eq!(led.balance("alice"), 1000);
        led.burn_usage("alice", 250, "chat").unwrap();
        assert_eq!(led.balance("alice"), 750);
        led.verify_chain().unwrap();
    }

    #[test]
    fn reject_overdraft() {
        let mut led = Ledger::new();
        led.mint_contribution("bob", 10, "cpu").unwrap();
        let err = led.burn_usage("bob", 50, "chat").unwrap_err();
        assert!(matches!(err, LedgerError::Insufficient { .. }));
    }
}
