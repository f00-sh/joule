//! Cryptographic acceptance of anonymous joule accounts by the pool.
//!
//! Account id is a fingerprint of an ed25519 public key. Hellos must be signed
//! so the whole control/group accepts only holders of the matching private key
//! (derived from the user-facing recovery code / seed).

use anyhow::{bail, Context, Result};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use joule_proto::{hello_sign_preimage, NodeId, PROTOCOL_VERSION};
use sha2::{Digest, Sha256};

/// Account prefix for signed anonymous ids.
pub const ACCOUNT_PREFIX: &str = "j1";

/// How long a Hello signature is valid (ms).
pub const HELLO_SIG_MAX_SKEW_MS: u64 = 15 * 60 * 1000;

/// Public account id from ed25519 verifying key bytes: `j1` + 32 hex of sha256(pk).
pub fn account_id_from_pubkey(pubkey: &[u8; 32]) -> String {
    let h = Sha256::digest(pubkey);
    format!("{ACCOUNT_PREFIX}{}", hex::encode(&h[..16]))
}

pub fn account_id_from_verifying_key(vk: &VerifyingKey) -> String {
    account_id_from_pubkey(vk.as_bytes())
}

/// Expand a 16-byte recovery seed (UUID bytes) into a 32-byte ed25519 seed.
pub fn signing_seed_from_recovery(recovery16: &[u8; 16]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(b"joule-identity-v1");
    h.update(recovery16);
    h.finalize().into()
}

pub fn signing_key_from_recovery(recovery16: &[u8; 16]) -> SigningKey {
    SigningKey::from_bytes(&signing_seed_from_recovery(recovery16))
}

pub fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Sign a Hello for the pool.
pub fn sign_hello(
    sk: &SigningKey,
    account: &str,
    from: &NodeId,
    signed_at_unix_ms: u64,
) -> (String, String) {
    let vk = sk.verifying_key();
    let pubkey_hex = hex::encode(vk.as_bytes());
    let pre = hello_sign_preimage(account, from, &pubkey_hex, signed_at_unix_ms);
    let sig = sk.sign(pre.as_bytes());
    (pubkey_hex, hex::encode(sig.to_bytes()))
}

/// Verify signed Hello fields. Returns Ok(pubkey_hex) if accepted.
pub fn verify_hello(
    account: &str,
    from: &NodeId,
    pubkey_hex: &str,
    sig_hex: &str,
    signed_at_unix_ms: u64,
    now_ms: u64,
) -> Result<String> {
    let account = account.trim();
    let pubkey_hex = pubkey_hex.trim().to_ascii_lowercase();
    let sig_hex = sig_hex.trim().to_ascii_lowercase();

    if !account.starts_with(ACCOUNT_PREFIX) {
        bail!("signed hello requires j1… account id");
    }
    if pubkey_hex.len() != 64 || !pubkey_hex.chars().all(|c| c.is_ascii_hexdigit()) {
        bail!("invalid pubkey_hex");
    }
    if sig_hex.len() != 128 || !sig_hex.chars().all(|c| c.is_ascii_hexdigit()) {
        bail!("invalid sig_hex");
    }
    if signed_at_unix_ms == 0 {
        bail!("signed_at_unix_ms required");
    }
    let skew = now_ms.abs_diff(signed_at_unix_ms);
    if skew > HELLO_SIG_MAX_SKEW_MS {
        bail!("hello signature expired or clock skew too large ({skew} ms)");
    }

    let pk_bytes = hex::decode(&pubkey_hex).context("pubkey decode")?;
    let pk_arr: [u8; 32] = pk_bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("pubkey must be 32 bytes"))?;
    let expect_acct = account_id_from_pubkey(&pk_arr);
    if expect_acct != account {
        bail!("account id does not match pubkey fingerprint");
    }

    let vk = VerifyingKey::from_bytes(&pk_arr).context("pubkey")?;
    let sig_bytes = hex::decode(&sig_hex).context("sig decode")?;
    let sig_arr: [u8; 64] = sig_bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("sig must be 64 bytes"))?;
    let sig = Signature::from_bytes(&sig_arr);
    let pre = hello_sign_preimage(account, from, &pubkey_hex, signed_at_unix_ms);
    vk.verify(pre.as_bytes(), &sig)
        .map_err(|_| anyhow::anyhow!("hello signature invalid"))?;

    // Touch protocol constant so preimage stays versioned.
    let _ = PROTOCOL_VERSION;
    Ok(pubkey_hex)
}

/// Lab nicknames (not j1…) may join without a signature.
pub fn requires_signature(account: &str) -> bool {
    account.trim().starts_with(ACCOUNT_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use joule_proto::NodeId;

    #[test]
    fn sign_verify_roundtrip() {
        let recovery = [7u8; 16];
        let sk = signing_key_from_recovery(&recovery);
        let account = account_id_from_verifying_key(&sk.verifying_key());
        assert!(account.starts_with("j1"));
        assert_eq!(account.len(), 2 + 32);
        let from = NodeId::new();
        let ts = now_unix_ms();
        let (pk, sig) = sign_hello(&sk, &account, &from, ts);
        let got = verify_hello(&account, &from, &pk, &sig, ts, ts).unwrap();
        assert_eq!(got, pk);
    }

    #[test]
    fn wrong_account_rejected() {
        let sk = signing_key_from_recovery(&[1u8; 16]);
        let from = NodeId::new();
        let ts = now_unix_ms();
        let account = account_id_from_verifying_key(&sk.verifying_key());
        let (pk, sig) = sign_hello(&sk, &account, &from, ts);
        let bad = verify_hello("j1ffffffffffffffffffffffffffffffff", &from, &pk, &sig, ts, ts);
        assert!(bad.is_err());
    }

    #[test]
    fn forged_sig_rejected() {
        let sk = signing_key_from_recovery(&[2u8; 16]);
        let from = NodeId::new();
        let ts = now_unix_ms();
        let account = account_id_from_verifying_key(&sk.verifying_key());
        let (pk, _) = sign_hello(&sk, &account, &from, ts);
        let fake_sig = "ab".repeat(64);
        assert!(verify_hello(&account, &from, &pk, &fake_sig, ts, ts).is_err());
    }
}
