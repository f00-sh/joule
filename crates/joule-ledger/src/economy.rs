//! Fair millijoule economy — pure, integer-auditable scoring (v0 + churn).
//!
//! Goals:
//! - **Free** access paid only in donated compute (no cash path).
//! - **Small donors matter** — mint scales with √VRAM, not linear VRAM or GPU aristocracy.
//! - **Tenure boost** — continuous healthy time in the cluster earns more.
//! - **Churn penalty** — frequent disconnect/reconnect earns less than stable presence.
//! - **Leecher penalty** — consume ≫ contribute → earn less, pay more (auditable).
//! - **Deterministic** — same inputs ⇒ same outputs; every mint/burn reason embeds a breakdown.
//!
//! All multipliers use **basis points** (10_000 = 1.0×). No floats in the public API totals.
//! **Verified mem only** for `mem_mib` in FairnessSnapshot (callers must not pass raw claims).

use serde::{Deserialize, Serialize};

use crate::Millijoule;

/// Economy algorithm version string (embedded in ledger reasons).
pub const ECONOMY_VERSION: &str = "v0";

/// 10_000 basis points = 1.0×.
pub const BP_ONE: u32 = 10_000;

/// Base millijoules per healthy heartbeat (before factors).
pub const HEARTBEAT_BASE_MJ: Millijoule = 10;

/// Work mint: millijoules per completion token (before factors).
pub const WORK_PER_TOKEN_MJ: Millijoule = 2;

/// Challenge success base mint (before factors).
pub const CHALLENGE_BASE_MJ: Millijoule = 5;

/// Usage: millijoules per prompt token.
pub const USAGE_PROMPT_MJ: Millijoule = 1;

/// Usage: millijoules per completion token.
pub const USAGE_COMPLETION_MJ: Millijoule = 4;

/// √mem factor floor (0.5×) — tiny nodes still participate.
pub const MEM_FACTOR_MIN_BP: u32 = 5_000;

/// √mem factor cap (8×) — a huge card does not dominate forever.
pub const MEM_FACTOR_MAX_BP: u32 = 80_000;

/// Max tenure boost above 1.0× (5_000 bp ⇒ 1.5×).
pub const TENURE_MAX_BOOST_BP: u32 = 5_000;

/// Hard floor on mint mult under heavy leeching (0.25×).
pub const LEECHER_MINT_FLOOR_BP: u32 = 2_500;

/// Hard ceiling on usage mult under heavy leeching (4.0×).
pub const LEECHER_USAGE_CEIL_BP: u32 = 40_000;

/// Churn mint floor (0.40×) under extreme disconnect spam.
pub const CHURN_MINT_FLOOR_BP: u32 = 4_000;

/// Disconnects in the fairness window before churn penalty begins.
pub const CHURN_FREE_DISCONNECTS: u32 = 2;

/// Kind of economic event being scored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EconomyEvent {
    Heartbeat,
    /// Verified inference shard / work units.
    Work {
        completion_tokens: u32,
    },
    ChallengeOk,
}

/// Snapshot of account fairness state at score time (rolling window).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct FairnessSnapshot {
    /// **Verified** donor memory for this account (MiB). Never raw GPU claims.
    pub mem_mib: u32,
    /// Continuous healthy online seconds for this account (tenure).
    pub continuous_online_secs: u64,
    /// Millijoules contributed in the fairness window (minted).
    pub contributed_mj_window: Millijoule,
    /// Millijoules burned in the fairness window (usage).
    pub consumed_mj_window: Millijoule,
    /// Disconnects in the fairness window (churn). Stable presence → 0.
    #[serde(default)]
    pub disconnects_window: u32,
}

/// Auditable mint calculation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MintBreakdown {
    pub economy: &'static str,
    pub event: String,
    pub base_mj: Millijoule,
    /// √(GiB) dampened memory factor.
    pub mem_factor_bp: u32,
    /// Tenure loyalty multiplier.
    pub tenure_bp: u32,
    /// Leecher mint penalty (≤ 10_000 when leeching).
    pub leecher_mint_bp: u32,
    /// Churn / stability mint penalty (≤ 10_000 when disconnecting often).
    pub churn_bp: u32,
    pub total_mj: Millijoule,
}

impl MintBreakdown {
    /// Compact reason token for the ledger (machine + human readable).
    pub fn reason_tag(&self, detail: &str) -> String {
        format!(
            "{event}|{detail}|eco={eco}|base={base}|mem_bp={mem}|ten_bp={ten}|lee_bp={lee}|churn_bp={churn}|total={total}",
            event = self.event,
            detail = detail,
            eco = self.economy,
            base = self.base_mj,
            mem = self.mem_factor_bp,
            ten = self.tenure_bp,
            lee = self.leecher_mint_bp,
            churn = self.churn_bp,
            total = self.total_mj,
        )
    }
}

/// Auditable usage burn calculation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BurnBreakdown {
    pub economy: &'static str,
    pub base_mj: Millijoule,
    /// Leecher usage surcharge (≥ 10_000 when leeching).
    pub leecher_usage_bp: u32,
    pub total_mj: Millijoule,
}

impl BurnBreakdown {
    pub fn reason_tag(&self, detail: &str) -> String {
        format!(
            "usage|{detail}|eco={eco}|base={base}|lee_usage_bp={lee}|total={total}",
            detail = detail,
            eco = self.economy,
            base = self.base_mj,
            lee = self.leecher_usage_bp,
            total = self.total_mj,
        )
    }
}

/// Memory factor in basis points: `√(mem_mib / 1024)`, clamped.
///
/// | VRAM | ≈ factor |
/// |------|----------|
/// | 1 GiB | 1.0× |
/// | 4 GiB | 2.0× |
/// | 16 GiB | 4.0× |
/// | 64 GiB | 8.0× (cap) |
///
/// Linear VRAM would let a 24 GiB card earn 24× a 1 GiB laptop. √ dampens that so
/// small donors are not frozen out while bigger cards still earn more.
pub fn mem_factor_bp(mem_mib: u32) -> u32 {
    // Integer sqrt of (mem_mib * 10_000² / 1024) ≈ 10000 * sqrt(GiB)
    // Use i128 for intermediate to avoid overflow.
    let mem = u64::from(mem_mib.max(256)); // floor 0.25 GiB for pure CPU crumbs
                                           // bp = 10000 * sqrt(mem/1024) = 10000 * sqrt(mem) / sqrt(1024)
                                           // sqrt(1024)=32 → bp = 10000 * isqrt(mem) / 32
    let root = isqrt_u64(mem);
    let bp = root.saturating_mul(10_000) / 32;
    (bp as u32).clamp(MEM_FACTOR_MIN_BP, MEM_FACTOR_MAX_BP)
}

/// Tenure multiplier: starts at 1.0×, grows with log2(1 + days), capped at 1.5×.
///
/// | continuous | ≈ mult |
/// |------------|--------|
/// | 0 | 1.00× |
/// | 1 day | ~1.12× |
/// | 7 days | ~1.36× |
/// | ≥ ~30 days | 1.50× cap |
pub fn tenure_bp(continuous_online_secs: u64) -> u32 {
    let days = continuous_online_secs / 86_400;
    // boost_bp ≈ 1200 * log2(1+days) using integer log2
    // log2(1+days) in 16.16 fixed via bit length of (1+days)
    let n = days.saturating_add(1);
    let lg = floor_log2_u64(n); // 0 for 1, 1 for 2-3, 2 for 4-7, 3 for 8-15, ...
                                // fractional: linear between powers of two
    let lo = 1u64 << lg;
    let hi = lo.saturating_mul(2).max(lo + 1);
    let frac_bp = if hi > lo {
        ((n - lo) * 10_000) / (hi - lo)
    } else {
        0
    };
    // log2 ≈ lg + frac; boost = 1200 * log2
    let log2_milli = lg.saturating_mul(1_000) + (frac_bp / 10); // ~ milli-units
    let boost = (log2_milli.saturating_mul(1_200) / 1_000).min(u64::from(TENURE_MAX_BOOST_BP));
    BP_ONE.saturating_add(boost as u32)
}

/// Leecher factors from rolling contribute vs consume.
///
/// Returns `(mint_mult_bp, usage_mult_bp)`.
///
/// - Fair (`consumed ≤ contributed`): both 1.0×
/// - Leeching: mint falls toward 0.25×, usage rises toward 4.0×
/// - Pure consumer with zero contribute in window: 0.25× mint / 4.0× usage
///
/// Let `L = clamp((consumed/contributed) - 1, 0, 3)` when contributed > 0.
/// Then mint = 1/(1+L), usage = 1+L (in bp).
pub fn leecher_factors_bp(contributed: Millijoule, consumed: Millijoule) -> (u32, u32) {
    let contributed = contributed.max(0);
    let consumed = consumed.max(0);
    if contributed == 0 && consumed > 0 {
        return (LEECHER_MINT_FLOOR_BP, LEECHER_USAGE_CEIL_BP);
    }
    if contributed == 0 || consumed <= contributed {
        return (BP_ONE, BP_ONE);
    }
    // L in milli: ((consumed - contributed) * 1000 / contributed), cap 3000 (=3.0)
    let over = (consumed - contributed) as u128;
    let base = contributed as u128;
    let l_milli = ((over * 1_000) / base).min(3_000);
    // mint_bp = 10000 * 1000 / (1000 + l_milli)
    let mint = ((10_000u128 * 1_000) / (1_000 + l_milli)) as u32;
    // usage_bp = 10000 * (1000 + l_milli) / 1000
    let usage = ((10_000u128 * (1_000 + l_milli)) / 1_000) as u32;
    (
        mint.clamp(LEECHER_MINT_FLOOR_BP, BP_ONE),
        usage.clamp(BP_ONE, LEECHER_USAGE_CEIL_BP),
    )
}

/// Churn mint multiplier: stable presence keeps 1.0×; frequent dropouts pull toward 0.40×.
///
/// | disconnects_window | ≈ mult |
/// |--------------------|--------|
/// | 0–2 (free band) | 1.00× |
/// | 5 | ~0.85× |
/// | 10 | ~0.65× |
/// | ≥20 | 0.40× floor |
///
/// Each disconnect beyond [`CHURN_FREE_DISCONNECTS`] costs 500 bp of mint factor.
pub fn churn_bp(disconnects_window: u32) -> u32 {
    let excess = disconnects_window.saturating_sub(CHURN_FREE_DISCONNECTS);
    let penalty = excess.saturating_mul(500);
    BP_ONE.saturating_sub(penalty).max(CHURN_MINT_FLOOR_BP)
}

/// Score a mint event.
pub fn score_mint(event: EconomyEvent, fair: FairnessSnapshot) -> MintBreakdown {
    let base = match event {
        EconomyEvent::Heartbeat => HEARTBEAT_BASE_MJ,
        EconomyEvent::Work { completion_tokens } => {
            WORK_PER_TOKEN_MJ.saturating_mul(i64::from(completion_tokens.max(1)))
        }
        EconomyEvent::ChallengeOk => CHALLENGE_BASE_MJ,
    };
    let mem = mem_factor_bp(fair.mem_mib);
    let ten = tenure_bp(fair.continuous_online_secs);
    let (lee_mint, _) = leecher_factors_bp(fair.contributed_mj_window, fair.consumed_mj_window);
    let churn = churn_bp(fair.disconnects_window);
    let total = apply_factors(base, &[mem, ten, lee_mint, churn]);
    MintBreakdown {
        economy: ECONOMY_VERSION,
        event: match event {
            EconomyEvent::Heartbeat => "heartbeat".into(),
            EconomyEvent::Work { .. } => "work".into(),
            EconomyEvent::ChallengeOk => "challenge".into(),
        },
        base_mj: base,
        mem_factor_bp: mem,
        tenure_bp: ten,
        leecher_mint_bp: lee_mint,
        churn_bp: churn,
        total_mj: total.max(1),
    }
}

/// Deterministic equal split of `amount` across sorted `recipients` (empty → empty vec).
/// Remainder mJ go to the first recipients (1 mJ each) so sum(splits) == amount.
pub fn split_donate_equitable(
    amount: Millijoule,
    recipients: &[String],
) -> Vec<(String, Millijoule)> {
    if amount <= 0 || recipients.is_empty() {
        return Vec::new();
    }
    let mut ids = recipients.to_vec();
    ids.sort();
    ids.dedup();
    if ids.is_empty() {
        return Vec::new();
    }
    let n = ids.len() as i64;
    let each = amount / n;
    let mut rem = amount % n;
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        let mut share = each;
        if rem > 0 {
            share += 1;
            rem -= 1;
        }
        if share > 0 {
            out.push((id, share));
        }
    }
    out
}

/// Score a usage burn (prompt + completion tokens).
pub fn score_burn(
    prompt_tokens: u32,
    completion_tokens: u32,
    fair: FairnessSnapshot,
) -> BurnBreakdown {
    let base = i64::from(prompt_tokens)
        .saturating_mul(USAGE_PROMPT_MJ)
        .saturating_add(i64::from(completion_tokens).saturating_mul(USAGE_COMPLETION_MJ))
        .max(1);
    let (_, lee_usage) = leecher_factors_bp(fair.contributed_mj_window, fair.consumed_mj_window);
    let total = apply_factors(base, &[lee_usage]);
    BurnBreakdown {
        economy: ECONOMY_VERSION,
        base_mj: base,
        leecher_usage_bp: lee_usage,
        total_mj: total.max(1),
    }
}

/// Multiply `base` by a list of bp factors (integer, rounded down each step).
fn apply_factors(base: Millijoule, factors_bp: &[u32]) -> Millijoule {
    let mut v = base.max(0) as i128;
    for &bp in factors_bp {
        v = (v * i128::from(bp)) / i128::from(BP_ONE);
    }
    v.max(0) as Millijoule
}

fn isqrt_u64(n: u64) -> u64 {
    if n == 0 {
        return 0;
    }
    let mut x = n;
    let mut y = x.div_ceil(2);
    while y < x {
        x = y;
        y = (x + n / x) / 2;
    }
    x
}

fn floor_log2_u64(n: u64) -> u64 {
    if n == 0 {
        return 0;
    }
    63 - n.leading_zeros() as u64
}

/// Legacy helpers — route through fair economy with neutral snapshot.
pub fn estimate_usage_millijoules(prompt_tokens: u32, completion_tokens: u32) -> Millijoule {
    score_burn(
        prompt_tokens,
        completion_tokens,
        FairnessSnapshot::default(),
    )
    .total_mj
}

pub fn estimate_contribution_millijoules(
    completion_tokens: u32,
    _device_multiplier: u32,
) -> Millijoule {
    // device_multiplier ignored: fairness uses √mem + tenure + leecher, not GPU class aristocracy.
    score_mint(
        EconomyEvent::Work { completion_tokens },
        FairnessSnapshot {
            mem_mib: 8192,
            continuous_online_secs: 0,
            contributed_mj_window: 0,
            consumed_mj_window: 0,
            disconnects_window: 0,
        },
    )
    .total_mj
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqrt_mem_dampens_big_cards() {
        let small = mem_factor_bp(1024); // 1 GiB → ~1.0×
        let mid = mem_factor_bp(4096); // 4 GiB → ~2.0×
        let big = mem_factor_bp(16_384); // 16 GiB → ~4.0×
        let huge = mem_factor_bp(65_536); // 64 GiB → cap 8.0×
        assert!((9_000..=11_000).contains(&small), "1GiB={small}");
        assert!((18_000..=22_000).contains(&mid), "4GiB={mid}");
        assert!((38_000..=42_000).contains(&big), "16GiB={big}");
        assert_eq!(huge, MEM_FACTOR_MAX_BP);
        // 16× memory is not 16× credit
        assert!(big < small.saturating_mul(8));
    }

    #[test]
    fn small_donor_still_beats_zero() {
        let m = score_mint(
            EconomyEvent::Heartbeat,
            FairnessSnapshot {
                mem_mib: 512,
                ..Default::default()
            },
        );
        assert!(m.total_mj >= 1);
    }

    #[test]
    fn tenure_boosts_up_to_cap() {
        let fresh = tenure_bp(0);
        let day = tenure_bp(86_400);
        let month = tenure_bp(86_400 * 40);
        assert_eq!(fresh, BP_ONE);
        assert!(day > fresh);
        assert_eq!(month, BP_ONE + TENURE_MAX_BOOST_BP);
    }

    #[test]
    fn fair_ratio_no_penalty() {
        let (m, u) = leecher_factors_bp(1000, 500);
        assert_eq!(m, BP_ONE);
        assert_eq!(u, BP_ONE);
    }

    #[test]
    fn leechers_pay_more_earn_less() {
        let (m, u) = leecher_factors_bp(100, 400); // 4× consume
        assert!(m < BP_ONE, "mint {m}");
        assert!(u > BP_ONE, "usage {u}");
        assert!(m >= LEECHER_MINT_FLOOR_BP);
        assert!(u <= LEECHER_USAGE_CEIL_BP);
    }

    #[test]
    fn pure_leecher_hits_floor() {
        let (m, u) = leecher_factors_bp(0, 999);
        assert_eq!(m, LEECHER_MINT_FLOOR_BP);
        assert_eq!(u, LEECHER_USAGE_CEIL_BP);
    }

    #[test]
    fn mint_reason_is_auditable() {
        let b = score_mint(
            EconomyEvent::Heartbeat,
            FairnessSnapshot {
                mem_mib: 8192,
                continuous_online_secs: 86_400 * 3,
                contributed_mj_window: 500,
                consumed_mj_window: 100,
                disconnects_window: 0,
            },
        );
        let tag = b.reason_tag("node-1");
        assert!(tag.contains("eco=v0"));
        assert!(tag.contains("base="));
        assert!(tag.contains("total="));
        assert_eq!(
            b.total_mj,
            score_mint(
                EconomyEvent::Heartbeat,
                FairnessSnapshot {
                    mem_mib: 8192,
                    continuous_online_secs: 86_400 * 3,
                    contributed_mj_window: 500,
                    consumed_mj_window: 100,
                    disconnects_window: 0,
                },
            )
            .total_mj
        );
        assert!(tag.contains("churn_bp="));
    }

    #[test]
    fn burn_scales_with_leecher() {
        let fair = score_burn(
            10,
            10,
            FairnessSnapshot {
                contributed_mj_window: 1000,
                consumed_mj_window: 0,
                ..Default::default()
            },
        );
        let leech = score_burn(
            10,
            10,
            FairnessSnapshot {
                contributed_mj_window: 10,
                consumed_mj_window: 1000,
                ..Default::default()
            },
        );
        assert!(leech.total_mj > fair.total_mj);
    }

    #[test]
    fn larger_verified_mem_mints_at_least_as_much() {
        let small = score_mint(
            EconomyEvent::Heartbeat,
            FairnessSnapshot {
                mem_mib: 1024,
                continuous_online_secs: 86_400,
                contributed_mj_window: 100,
                consumed_mj_window: 10,
                disconnects_window: 0,
            },
        );
        let big = score_mint(
            EconomyEvent::Heartbeat,
            FairnessSnapshot {
                mem_mib: 16_384,
                continuous_online_secs: 86_400,
                contributed_mj_window: 100,
                consumed_mj_window: 10,
                disconnects_window: 0,
            },
        );
        assert!(big.total_mj >= small.total_mj);
        assert!(big.mem_factor_bp > small.mem_factor_bp);
    }

    #[test]
    fn longer_tenure_mints_more() {
        let fresh = score_mint(
            EconomyEvent::Heartbeat,
            FairnessSnapshot {
                mem_mib: 8192,
                continuous_online_secs: 0,
                ..Default::default()
            },
        );
        let loyal = score_mint(
            EconomyEvent::Heartbeat,
            FairnessSnapshot {
                mem_mib: 8192,
                continuous_online_secs: 86_400 * 10,
                ..Default::default()
            },
        );
        assert!(loyal.total_mj > fresh.total_mj);
        assert!(loyal.tenure_bp > fresh.tenure_bp);
    }

    #[test]
    fn churn_penalizes_frequent_dropout() {
        assert_eq!(churn_bp(0), BP_ONE);
        assert_eq!(churn_bp(2), BP_ONE);
        assert!(churn_bp(5) < BP_ONE);
        assert_eq!(churn_bp(100), CHURN_MINT_FLOOR_BP);
        let stable = score_mint(
            EconomyEvent::Heartbeat,
            FairnessSnapshot {
                mem_mib: 8192,
                continuous_online_secs: 86_400,
                disconnects_window: 0,
                ..Default::default()
            },
        );
        let flaky = score_mint(
            EconomyEvent::Heartbeat,
            FairnessSnapshot {
                mem_mib: 8192,
                continuous_online_secs: 86_400,
                disconnects_window: 12,
                ..Default::default()
            },
        );
        assert!(
            flaky.total_mj < stable.total_mj,
            "flaky={} stable={}",
            flaky.total_mj,
            stable.total_mj
        );
        assert!(flaky.churn_bp < stable.churn_bp);
    }

    #[test]
    fn split_donate_equitable_conserves_and_is_deterministic() {
        let rec = vec!["carol".into(), "alice".into(), "bob".into()];
        let a = split_donate_equitable(100, &rec);
        let b = split_donate_equitable(100, &rec);
        assert_eq!(a, b);
        assert_eq!(a.iter().map(|(_, s)| s).sum::<i64>(), 100);
        // sorted order
        assert_eq!(a[0].0, "alice");
        // remainder distributed
        let odd = split_donate_equitable(10, &["x".into(), "y".into(), "z".into()]);
        assert_eq!(odd.iter().map(|(_, s)| s).sum::<i64>(), 10);
    }

    #[test]
    fn zero_verified_mem_still_mints_floor_not_claim() {
        // Callers must pass verified mem; 0 is floored to 256 in mem_factor via max(256).
        let m = score_mint(
            EconomyEvent::Heartbeat,
            FairnessSnapshot {
                mem_mib: 0,
                ..Default::default()
            },
        );
        assert!(m.total_mj >= 1);
        // A fake 64 GiB claim would use mem_mib=65536; protocol must pass verified only.
        let fake = mem_factor_bp(65_536);
        let real0 = mem_factor_bp(0);
        assert!(fake > real0);
    }
}
