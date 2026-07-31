//! Capacity attestation challenge — mem-bound pure unit (not a public format string).
//!
//! Control generates a random **seed** per challenge and stores
//! `expected = proof_hex(seed, credit_mib)`. The agent must run [`solve`] (real
//! working-set work scaled by `credit_mib`) and return the hex digest.
//!
//! [`crate::StubEngine`]-style `format!("[joule-stub:{model}] {prompt}")` is **not**
//! a valid proof and must never unlock [`crate::CHALLENGE_CREDIT_MIB`].
//!
//! Honest loaded and unloaded agents both pass by performing the same work unit
//! (independent of model infer text). Model quality is not the capacity oracle.

use sha2::{Digest, Sha256};

/// Working-set bytes per MiB of challenge credit (lab-friendly scale).
/// 1024 MiB credit → 4 MiB buffer × mix passes — not a one-line formula.
pub const BYTES_PER_CREDIT_MIB: usize = 4096;

/// Cap working set so control/agent never allocate unbounded RAM on bad inputs.
pub const MAX_WORK_BYTES: usize = 16 * 1024 * 1024;

/// Mix passes over the working set.
pub const MIX_PASSES: u32 = 3;

/// Domain tag so proofs are not interchangeable with arbitrary sha256.
const DOMAIN: &[u8] = b"joule-capacity-v1";

/// Solve capacity attestation for `seed` + `credit_mib`.
///
/// Cost scales with `credit_mib` (working set + mix). Offline string formatting
/// cannot produce this digest without running the same work.
pub fn solve(seed: &[u8; 32], credit_mib: u32) -> [u8; 32] {
    let mib = credit_mib.max(1) as usize;
    let len = mib
        .saturating_mul(BYTES_PER_CREDIT_MIB)
        .clamp(4096, MAX_WORK_BYTES);
    let mut buf = vec![0u8; len];

    // Expand seed into working set (sequential, memory-touching).
    let mut block = *seed;
    for chunk in buf.chunks_mut(32) {
        let mut h = Sha256::new();
        h.update(DOMAIN);
        h.update(b"expand");
        h.update(block);
        let dig: [u8; 32] = h.finalize().into();
        let n = chunk.len().min(32);
        chunk[..n].copy_from_slice(&dig[..n]);
        block = dig;
    }

    for pass in 0..MIX_PASSES {
        let mut h = Sha256::new();
        h.update(DOMAIN);
        h.update([pass as u8]);
        h.update(seed);
        h.update(credit_mib.to_le_bytes());
        h.update(&buf);
        let dig: [u8; 32] = h.finalize().into();
        // In-place mix (index loop avoids double-borrow).
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
    out.update(credit_mib.to_le_bytes());
    out.update(&buf);
    out.finalize().into()
}

/// Hex-encoded proof (lowercase) — wire format for `ChallengeResult.completion`.
pub fn proof_hex(seed: &[u8; 32], credit_mib: u32) -> String {
    hex::encode(solve(seed, credit_mib))
}

/// Constant-time-ish verify of hex proof (length + equality).
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
    fn proof_deterministic_and_seed_sensitive() {
        let seed = [7u8; 32];
        let a = proof_hex(&seed, 64);
        let b = proof_hex(&seed, 64);
        assert_eq!(a, b);
        assert_eq!(a.len(), 64);
        let mut seed2 = seed;
        seed2[0] ^= 1;
        assert_ne!(proof_hex(&seed2, 64), a);
        assert_ne!(proof_hex(&seed, 128), a);
    }

    #[test]
    fn public_stub_formula_is_not_valid_proof() {
        let seed = [1u8; 32];
        let credit = 1024u32;
        let proof = proof_hex(&seed, credit);
        let stub = "[joule-stub:kimi-open] joule-challenge:deadbeef".to_string();
        assert_ne!(proof, stub);
        assert!(!verify(&seed, credit, &stub));
        assert!(verify(&seed, credit, &proof));
    }

    #[test]
    fn parse_seed_roundtrip() {
        let seed = [9u8; 32];
        let h = hex::encode(seed);
        assert_eq!(parse_seed_hex(&h), Some(seed));
        assert!(parse_seed_hex("zz").is_none());
    }
}
