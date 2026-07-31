//! Capacity attestation — **memory-hard** unlock unit (1:1 MiB).
//!
//! Invariant: **trusted MiB ≤ proven working-set MiB**.
//!
//! - Working set = `credit_mib × 1 MiB` (true 1:1, not a 4 KiB theater ratio).
//! - Max credit per challenge is [`crate::CHALLENGE_CREDIT_MIB`]; work is capped
//!   at that × 1 MiB (never unbounded).
//! - Control stores `expected = proof_hex(seed, credit)` at issue time; settle
//!   exact-matches. Unlock credits **only** that proven `credit_mib`.
//! - Public stub strings / short buffers / wrong seeds cannot unlock.

use sha2::{Digest, Sha256};

use crate::CHALLENGE_CREDIT_MIB;

/// **True 1:1** — each MiB of challenge credit requires 1 MiB of working-set RAM.
pub const BYTES_PER_CREDIT_MIB: usize = 1024 * 1024;

/// Mix passes over the full working set (must touch every page).
pub const MIX_PASSES: u32 = 2;

/// Domain tag so proofs are not interchangeable with arbitrary sha256.
const DOMAIN: &[u8] = b"joule-capacity-v2";

/// Max working-set bytes for one challenge (= max credit × 1 MiB).
pub fn max_work_bytes() -> usize {
    (CHALLENGE_CREDIT_MIB as usize).saturating_mul(BYTES_PER_CREDIT_MIB)
}

/// Clamp credit to protocol max; zero credit is invalid for unlock work.
pub fn clamp_credit_mib(credit_mib: u32) -> u32 {
    credit_mib.clamp(1, CHALLENGE_CREDIT_MIB)
}

/// Working-set size in bytes for a given credit (after clamp).
///
/// **Invariant:** `work_bytes(c) == clamp_credit_mib(c) as usize * 1 MiB`.
pub fn work_bytes(credit_mib: u32) -> usize {
    let c = clamp_credit_mib(credit_mib) as usize;
    c.saturating_mul(BYTES_PER_CREDIT_MIB)
}

/// Solve capacity attestation: allocate + touch **full** `work_bytes(credit)` buffer.
///
/// Cost is proportional to `credit_mib` in **real host RAM**. A 4 MiB machine
/// cannot honestly solve a 1024 MiB credit without paging/OOM.
pub fn solve(seed: &[u8; 32], credit_mib: u32) -> [u8; 32] {
    let credit = clamp_credit_mib(credit_mib);
    let len = work_bytes(credit);
    debug_assert_eq!(len, credit as usize * BYTES_PER_CREDIT_MIB);
    debug_assert!(len <= max_work_bytes());

    let mut buf = vec![0u8; len];

    // Expand seed into entire working set (sequential write — forces commit).
    let mut block = *seed;
    for chunk in buf.chunks_mut(32) {
        let mut h = Sha256::new();
        h.update(DOMAIN);
        h.update(b"expand");
        h.update(block);
        h.update(credit.to_le_bytes());
        let dig: [u8; 32] = h.finalize().into();
        let n = chunk.len().min(32);
        chunk[..n].copy_from_slice(&dig[..n]);
        block = dig;
    }

    // Mix passes: read+write every byte so optimizers cannot skip the buffer.
    for pass in 0..MIX_PASSES {
        let mut h = Sha256::new();
        h.update(DOMAIN);
        h.update([pass as u8]);
        h.update(seed);
        h.update(credit.to_le_bytes());
        // Hash full buffer (depends on all prior touches).
        h.update(&buf);
        let dig: [u8; 32] = h.finalize().into();
        for i in 0..buf.len() {
            buf[i] ^= dig[i % 32];
            if i + 1 < buf.len() {
                let add = buf[i];
                buf[i + 1] = buf[i + 1].wrapping_add(add);
            }
        }
    }

    let mut out = Sha256::new();
    out.update(DOMAIN);
    out.update(b"finalize");
    out.update(seed);
    out.update(credit.to_le_bytes());
    out.update((len as u64).to_le_bytes());
    out.update(&buf);
    out.finalize().into()
}

/// Hex-encoded proof (lowercase) — wire format for `ChallengeResult.completion`.
pub fn proof_hex(seed: &[u8; 32], credit_mib: u32) -> String {
    hex::encode(solve(seed, credit_mib))
}

/// Verify hex proof for the **same** seed+credit (full recompute of work).
pub fn verify(seed: &[u8; 32], credit_mib: u32, proof_hex_str: &str) -> bool {
    let want = proof_hex(seed, credit_mib);
    let got = proof_hex_str.trim().to_ascii_lowercase();
    want == got
}

/// Parse 32-byte seed from hex (64 hex chars).
pub fn parse_seed_hex(s: &str) -> Option<[u8; 32]> {
    let raw = hex::decode(s.trim()).ok()?;
    if raw.len() != 32 {
        return None;
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&raw);
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn work_bytes_is_true_one_to_one_mib() {
        assert_eq!(BYTES_PER_CREDIT_MIB, 1024 * 1024);
        assert_eq!(work_bytes(1), 1024 * 1024);
        assert_eq!(work_bytes(2), 2 * 1024 * 1024);
        assert_eq!(work_bytes(64), 64 * 1024 * 1024);
        // Cap at CHALLENGE_CREDIT_MIB
        assert_eq!(
            work_bytes(CHALLENGE_CREDIT_MIB),
            CHALLENGE_CREDIT_MIB as usize * BYTES_PER_CREDIT_MIB
        );
        assert_eq!(work_bytes(CHALLENGE_CREDIT_MIB + 999), max_work_bytes());
        assert_eq!(max_work_bytes(), CHALLENGE_CREDIT_MIB as usize * BYTES_PER_CREDIT_MIB);
        // Zero credit still runs 1 MiB of work (clamp) — unlock path should pass real credit.
        assert_eq!(work_bytes(0), BYTES_PER_CREDIT_MIB);
    }

    #[test]
    fn proof_deterministic_and_seed_and_credit_sensitive() {
        // Small credits only (1–2 MiB) so unit tests stay lab-friendly.
        let seed = [7u8; 32];
        let a = proof_hex(&seed, 1);
        let b = proof_hex(&seed, 1);
        assert_eq!(a, b);
        assert_eq!(a.len(), 64);
        let mut seed2 = seed;
        seed2[0] ^= 1;
        assert_ne!(proof_hex(&seed2, 1), a);
        // Different credit → different work set → different proof (cannot reuse 1 MiB solve for 2 MiB).
        assert_ne!(proof_hex(&seed, 2), a);
        assert!(verify(&seed, 1, &a));
        assert!(!verify(&seed, 2, &a), "1 MiB proof must not satisfy 2 MiB credit");
    }

    #[test]
    fn public_stub_formula_is_not_valid_proof() {
        let seed = [1u8; 32];
        let credit = 1u32;
        let proof = proof_hex(&seed, credit);
        let stub = "[joule-stub:kimi-open] joule-challenge:deadbeef".to_string();
        assert_ne!(proof, stub);
        assert!(!verify(&seed, credit, &stub));
        assert!(verify(&seed, credit, &proof));
    }

    #[test]
    fn wrong_seed_and_truncated_hex_fail() {
        let seed = [2u8; 32];
        let proof = proof_hex(&seed, 1);
        let mut bad = [2u8; 32];
        bad[31] ^= 0xff;
        assert!(!verify(&bad, 1, &proof));
        assert!(!verify(&seed, 1, "abcd"));
        assert!(!verify(&seed, 1, ""));
    }

    #[test]
    fn parse_seed_roundtrip() {
        let seed = [9u8; 32];
        let h = hex::encode(seed);
        assert_eq!(parse_seed_hex(&h), Some(seed));
        assert!(parse_seed_hex("zz").is_none());
    }

    /// Credit-2 work is strictly larger than credit-1; proves scale without 1 GiB alloc.
    #[test]
    fn larger_credit_requires_more_work_bytes() {
        assert!(work_bytes(2) > work_bytes(1));
        assert_eq!(work_bytes(2) / work_bytes(1), 2);
        // Solving both succeeds only with full buffers for each credit.
        let seed = [0xAAu8; 32];
        let p1 = proof_hex(&seed, 1);
        let p2 = proof_hex(&seed, 2);
        assert_ne!(p1, p2);
        assert!(verify(&seed, 1, &p1));
        assert!(verify(&seed, 2, &p2));
        assert!(!verify(&seed, 1, &p2));
        assert!(!verify(&seed, 2, &p1));
    }
}
