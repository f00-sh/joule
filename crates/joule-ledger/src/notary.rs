//! Checkpoint notary signatures (self-govern trust slice).
//!
//! Notaries co-attest a ledger head hash. Signatures are **outside** the sealed
//! entry hash (sidecar) so the chain remains append-only without chicken-egg.
//! Verification fails closed on bad signatures / wrong head.

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// One notary's attestation of a ledger head.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotaryAttestation {
    /// Notary node / account id (human label).
    pub notary_id: String,
    /// Ed25519 public key hex (64).
    pub pubkey_hex: String,
    /// Signature over `preimage(head_hash_hex)` as hex.
    pub sig_hex: String,
}

/// Domain-separated preimage for head attestation.
pub fn notary_preimage(head_hash_hex: &str) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(b"joule-notary-v0|");
    h.update(head_hash_hex.as_bytes());
    h.finalize().into()
}

pub fn sign_head(
    signing_key: &SigningKey,
    head_hash_hex: &str,
    notary_id: &str,
) -> NotaryAttestation {
    let msg = notary_preimage(head_hash_hex);
    let sig = signing_key.sign(&msg);
    let vk = signing_key.verifying_key();
    NotaryAttestation {
        notary_id: notary_id.into(),
        pubkey_hex: hex::encode(vk.to_bytes()),
        sig_hex: hex::encode(sig.to_bytes()),
    }
}

/// Verify a single attestation against its claimed pubkey and head.
pub fn verify_attestation(head_hash_hex: &str, att: &NotaryAttestation) -> Result<(), String> {
    let pk_bytes = hex::decode(att.pubkey_hex.trim()).map_err(|e| format!("pubkey hex: {e}"))?;
    if pk_bytes.len() != 32 {
        return Err("pubkey must be 32 bytes".into());
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&pk_bytes);
    let vk = VerifyingKey::from_bytes(&arr).map_err(|e| format!("pubkey: {e}"))?;
    let sig_bytes = hex::decode(att.sig_hex.trim()).map_err(|e| format!("sig hex: {e}"))?;
    if sig_bytes.len() != 64 {
        return Err("sig must be 64 bytes".into());
    }
    let mut sarr = [0u8; 64];
    sarr.copy_from_slice(&sig_bytes);
    let sig = Signature::from_bytes(&sarr);
    let msg = notary_preimage(head_hash_hex);
    vk.verify(&msg, &sig)
        .map_err(|_| "notary signature invalid".to_string())
}

/// Require `min_ok` valid distinct notary signatures on this head. Fail closed.
pub fn verify_quorum(
    head_hash_hex: &str,
    attestations: &[NotaryAttestation],
    min_ok: usize,
) -> Result<usize, String> {
    if min_ok == 0 {
        return Ok(0);
    }
    let mut ok = 0usize;
    let mut seen = std::collections::HashSet::new();
    for att in attestations {
        if !seen.insert(att.pubkey_hex.to_lowercase()) {
            continue; // one vote per key
        }
        match verify_attestation(head_hash_hex, att) {
            Ok(()) => ok += 1,
            Err(_) => continue,
        }
    }
    if ok < min_ok {
        return Err(format!(
            "notary quorum failed: {ok}/{min_ok} valid signatures"
        ));
    }
    Ok(ok)
}

/// Deterministic lab signing key from a seed string (tests / demo only).
pub fn lab_signing_key(seed: &str) -> SigningKey {
    let mut h = Sha256::new();
    h.update(b"joule-lab-notary|");
    h.update(seed.as_bytes());
    let bytes: [u8; 32] = h.finalize().into();
    SigningKey::from_bytes(&bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notary_sign_verify_roundtrip() {
        let sk = lab_signing_key("alice");
        let head = "deadbeefcafebabe";
        let att = sign_head(&sk, head, "notary-alice");
        verify_attestation(head, &att).unwrap();
        // Wrong head fails closed
        assert!(verify_attestation("00", &att).is_err());
        // Tampered sig fails
        let mut bad = att.clone();
        bad.sig_hex = "00".repeat(64);
        assert!(verify_attestation(head, &bad).is_err());
    }

    #[test]
    fn quorum_requires_min_valid() {
        let head = "aabbccdd";
        let a = sign_head(&lab_signing_key("a"), head, "a");
        let b = sign_head(&lab_signing_key("b"), head, "b");
        let mut evil = a.clone();
        evil.sig_hex = "11".repeat(64);
        assert_eq!(verify_quorum(head, &[a.clone(), b.clone()], 2).unwrap(), 2);
        assert!(verify_quorum(head, &[a, evil], 2).is_err());
        assert!(verify_quorum(head, &[], 1).is_err());
    }
}
