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
    churn_bp, estimate_contribution_millijoules, estimate_usage_millijoules, leecher_factors_bp,
    mem_factor_bp, score_burn, score_mint, split_donate_equitable, tenure_bp, BurnBreakdown,
    EconomyEvent, FairnessSnapshot, MintBreakdown, ECONOMY_VERSION,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

/// 1 joule = 1000 millijoules (integer math only).
pub type Millijoule = i64;

/// Result of a sealed pool donation + equitable redistribution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DonateResult {
    pub donor_burn: CreditEvent,
    pub recipient_credits: Vec<CreditEvent>,
    pub amount: Millijoule,
}

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

    /// Donate `amount` from donor into the pool; redistribute equitably to `recipients`.
    ///
    /// Conservation: donor −amount; sum(recipient +shares) = amount. Fails closed on
    /// insufficient balance or empty recipients. Pure split via [`split_donate_equitable`].
    pub fn donate_to_pool(
        &mut self,
        donor: impl Into<String>,
        amount: Millijoule,
        recipients: &[String],
    ) -> Result<DonateResult, LedgerError> {
        let donor = donor.into();
        if amount <= 0 {
            return Err(LedgerError::Chain("donate amount must be positive".into()));
        }
        if recipients.is_empty() {
            return Err(LedgerError::Chain(
                "donate requires ≥1 eligible recipient".into(),
            ));
        }
        // Exclude donor from receiving their own donation.
        let filtered: Vec<String> = recipients
            .iter()
            .filter(|a| a.as_str() != donor.as_str())
            .cloned()
            .collect();
        if filtered.is_empty() {
            return Err(LedgerError::Chain(
                "donate requires ≥1 eligible recipient other than donor".into(),
            ));
        }
        let splits = split_donate_equitable(amount, &filtered);
        let split_sum: Millijoule = splits.iter().map(|(_, s)| *s).sum();
        if split_sum != amount {
            return Err(LedgerError::Chain(format!(
                "split conservation failed: {split_sum} != {amount}"
            )));
        }
        let burn = self.inner.donate_pool(
            &donor,
            amount,
            format!("pool_donate|recipients={}", filtered.len()),
        )?;
        let mut credits = Vec::with_capacity(splits.len());
        for (acct, share) in &splits {
            let e = self.inner.donate_receive(
                acct,
                *share,
                format!("from:{donor}|share={share}|of={amount}"),
            )?;
            credits.push(CreditEvent {
                id: e.id,
                account: e.account,
                delta_millijoules: e.delta_millijoules,
                reason: e.reason,
            });
        }
        Ok(DonateResult {
            donor_burn: CreditEvent {
                id: burn.id,
                account: burn.account,
                delta_millijoules: burn.delta_millijoules,
                reason: burn.reason,
            },
            recipient_credits: credits,
            amount,
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

    #[test]
    fn raw_balance_restore_does_not_create_spendable_credits() {
        let mut led = Ledger::new();
        led.mint_contribution("alice", 50, "real").unwrap();
        // Forged cache dump — must not invent balance.
        let mut fake = std::collections::HashMap::new();
        fake.insert("eve".into(), 1_000_000i64);
        led.restore_balances(fake);
        assert_eq!(led.balance("eve"), 0, "raw balances are not authoritative");
        // Alice still has her sealed mint (restore_balances is intentional no-op on chain).
        assert_eq!(led.balance("alice"), 50);
        led.verify_chain().unwrap();
    }

    #[test]
    fn donate_to_pool_conserves_and_is_deterministic() {
        let mut led = Ledger::new();
        led.mint_contribution("rich", 1000, "seed").unwrap();
        led.ensure_account("alice");
        led.ensure_account("bob");
        led.ensure_account("carol");
        let recips = vec!["carol".into(), "alice".into(), "bob".into()];
        let r1 = led
            .donate_to_pool("rich", 300, &recips)
            .expect("donate");
        assert_eq!(r1.amount, 300);
        assert_eq!(led.balance("rich"), 700);
        let sum_credits: i64 = r1.recipient_credits.iter().map(|c| c.delta_millijoules).sum();
        assert_eq!(sum_credits, 300);
        assert_eq!(
            led.balance("alice") + led.balance("bob") + led.balance("carol"),
            300
        );
        // Same split for same inputs (second donate independent).
        led.mint_contribution("rich", 300, "seed2").unwrap();
        let r2 = led.donate_to_pool("rich", 300, &recips).unwrap();
        assert_eq!(
            r1.recipient_credits
                .iter()
                .map(|c| (c.account.clone(), c.delta_millijoules))
                .collect::<Vec<_>>(),
            r2.recipient_credits
                .iter()
                .map(|c| (c.account.clone(), c.delta_millijoules))
                .collect::<Vec<_>>()
        );
        led.verify_chain().unwrap();
    }

    #[test]
    fn donate_fails_closed_on_insufficient_or_empty() {
        let mut led = Ledger::new();
        led.mint_contribution("a", 10, "x").unwrap();
        assert!(led.donate_to_pool("a", 50, &["b".into()]).is_err());
        assert!(led.donate_to_pool("a", 5, &[]).is_err());
        assert!(led.donate_to_pool("a", 5, &["a".into()]).is_err());
    }

    #[test]
    fn tamper_breaks_verify() {
        let mut led = Ledger::new();
        led.mint_contribution("x", 10, "y").unwrap();
        // Tamper via sealed mut
        led.sealed_mut().entries(); // just touch
        let mut entries = led.sealed().entries().to_vec();
        entries[0].delta_millijoules = 999_999;
        let mut led2 = Ledger::new();
        assert!(led2.restore_chain(entries).is_err());
    }
}
