//! Append-only credit ledger denominated in **millijoules**.
//!
//! Product law: API access requires recent contribution and non-negative spendable balance
//! (or an active-donor window — see design doc). Mint from verified cluster work; burn on usage.
//!
//! Fair scoring lives in [`economy`] — tenure boosts, √VRAM dampening, leecher penalties.

pub mod economy;

pub use economy::{
    estimate_contribution_millijoules, estimate_usage_millijoules, leecher_factors_bp, score_burn,
    score_mint, BurnBreakdown, EconomyEvent, FairnessSnapshot, MintBreakdown, ECONOMY_VERSION,
};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreditEvent {
    pub id: Uuid,
    pub account: String,
    pub delta_millijoules: Millijoule,
    pub reason: String,
}

#[derive(Debug, Default)]
pub struct Ledger {
    balances: HashMap<String, Millijoule>,
    events: Vec<CreditEvent>,
}

impl Ledger {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn balance(&self, account: &str) -> Millijoule {
        self.balances.get(account).copied().unwrap_or(0)
    }

    pub fn ensure_account(&mut self, account: impl Into<String>) {
        self.balances.entry(account.into()).or_insert(0);
    }

    pub fn apply(
        &mut self,
        account: impl Into<String>,
        delta: Millijoule,
        reason: impl Into<String>,
    ) -> Result<CreditEvent, LedgerError> {
        let account = account.into();
        let have = self.balance(&account);
        if delta < 0 && have + delta < 0 {
            return Err(LedgerError::Insufficient { have, need: -delta });
        }
        let event = CreditEvent {
            id: Uuid::new_v4(),
            account: account.clone(),
            delta_millijoules: delta,
            reason: reason.into(),
        };
        *self.balances.entry(account).or_insert(0) += delta;
        self.events.push(event.clone());
        Ok(event)
    }

    /// Mint credits for verified cluster contribution.
    pub fn mint_contribution(
        &mut self,
        account: impl Into<String>,
        millijoules: Millijoule,
        detail: impl Into<String>,
    ) -> Result<CreditEvent, LedgerError> {
        assert!(millijoules >= 0, "mint amount must be non-negative");
        self.apply(
            account,
            millijoules,
            format!("contribute:{}", detail.into()),
        )
    }

    /// Burn credits for API inference spend.
    pub fn burn_usage(
        &mut self,
        account: impl Into<String>,
        millijoules: Millijoule,
        detail: impl Into<String>,
    ) -> Result<CreditEvent, LedgerError> {
        assert!(millijoules >= 0, "burn amount must be non-negative");
        self.apply(account, -millijoules, format!("usage:{}", detail.into()))
    }

    pub fn events(&self) -> &[CreditEvent] {
        &self.events
    }

    /// Snapshot balances for persistence.
    pub fn balances(&self) -> &HashMap<String, Millijoule> {
        &self.balances
    }

    /// Restore balances from a snapshot (does not replay events).
    pub fn restore_balances(&mut self, balances: HashMap<String, Millijoule>) {
        self.balances = balances;
    }
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
    }

    #[test]
    fn reject_overdraft() {
        let mut led = Ledger::new();
        led.mint_contribution("bob", 10, "cpu").unwrap();
        let err = led.burn_usage("bob", 50, "chat").unwrap_err();
        assert!(matches!(err, LedgerError::Insufficient { .. }));
    }
}
