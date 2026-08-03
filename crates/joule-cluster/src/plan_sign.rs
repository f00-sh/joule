//! Ed25519 device signatures for PlanOffer / PlanAccept (admit bus authenticity).
//!
//! Content hashes (`plan_hash_hex` / `confirm_hex`) prove body integrity.
//! Signatures prove the **device key** behind `from` endorsed that material.

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use joule_proto::NodeId;
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub const DOMAIN_PLAN_OFFER_SIG: &[u8] = b"joule-plan-offer-sig-v1";
pub const DOMAIN_PLAN_ACCEPT_SIG: &[u8] = b"joule-plan-accept-sig-v1";

/// Preimage for PlanOffer signature.
pub fn plan_offer_sign_preimage(
    from: &NodeId,
    plan_id: Uuid,
    request_id: Uuid,
    plan_hash_hex: &str,
    signed_at_unix_ms: u64,
) -> String {
    format!(
        "joule-plan-offer-sig-v1|{from}|{plan_id}|{request_id}|{plan_hash}|{ts}",
        from = from,
        plan_id = plan_id,
        request_id = request_id,
        plan_hash = plan_hash_hex.trim().to_ascii_lowercase(),
        ts = signed_at_unix_ms,
    )
}

/// Preimage for PlanAccept signature.
pub fn plan_accept_sign_preimage(
    from: &NodeId,
    plan_id: Uuid,
    request_id: Uuid,
    accepted: bool,
    plan_hash_hex: &str,
    confirm_hex: &str,
    signed_at_unix_ms: u64,
) -> String {
    format!(
        "joule-plan-accept-sig-v1|{from}|{plan_id}|{request_id}|{acc}|{plan_hash}|{confirm}|{ts}",
        from = from,
        plan_id = plan_id,
        request_id = request_id,
        acc = u8::from(accepted),
        plan_hash = plan_hash_hex.trim().to_ascii_lowercase(),
        confirm = confirm_hex.trim().to_ascii_lowercase(),
        ts = signed_at_unix_ms,
    )
}

pub fn sign_preimage(sk: &SigningKey, preimage: &str) -> (String, String) {
    let vk = sk.verifying_key();
    let pubkey_hex = hex::encode(vk.as_bytes());
    let sig = sk.sign(preimage.as_bytes());
    (pubkey_hex, hex::encode(sig.to_bytes()))
}

pub fn verify_sig(pubkey_hex: &str, sig_hex: &str, preimage: &str) -> Result<(), String> {
    let pk = pubkey_hex.trim().to_ascii_lowercase();
    let sg = sig_hex.trim().to_ascii_lowercase();
    if pk.len() != 64 || !pk.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err("invalid plan signer pubkey_hex".into());
    }
    if sg.len() != 128 || !sg.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err("invalid plan sig_hex".into());
    }
    let pk_bytes: [u8; 32] = hex::decode(&pk)
        .map_err(|e| e.to_string())?
        .try_into()
        .map_err(|_| "pubkey len".to_string())?;
    let sig_bytes: [u8; 64] = hex::decode(&sg)
        .map_err(|e| e.to_string())?
        .try_into()
        .map_err(|_| "sig len".to_string())?;
    let vk = VerifyingKey::from_bytes(&pk_bytes).map_err(|e| e.to_string())?;
    let sig = Signature::from_bytes(&sig_bytes);
    vk.verify(preimage.as_bytes(), &sig)
        .map_err(|_| "plan signature verification failed".to_string())
}

/// Fail closed: missing or invalid PlanOffer signature.
pub fn verify_plan_offer_sig(
    from: &NodeId,
    plan_id: Uuid,
    request_id: Uuid,
    plan_hash_hex: &str,
    signer_pubkey_hex: &str,
    sig_hex: &str,
    signed_at_unix_ms: u64,
) -> Result<(), String> {
    if signer_pubkey_hex.is_empty() || sig_hex.is_empty() {
        return Err("missing plan offer signature".into());
    }
    let pre = plan_offer_sign_preimage(from, plan_id, request_id, plan_hash_hex, signed_at_unix_ms);
    verify_sig(signer_pubkey_hex, sig_hex, &pre)
}

/// Fail closed: missing or invalid PlanAccept signature.
#[allow(clippy::too_many_arguments)]
pub fn verify_plan_accept_sig(
    from: &NodeId,
    plan_id: Uuid,
    request_id: Uuid,
    accepted: bool,
    plan_hash_hex: &str,
    confirm_hex: &str,
    signer_pubkey_hex: &str,
    sig_hex: &str,
    signed_at_unix_ms: u64,
) -> Result<(), String> {
    if signer_pubkey_hex.is_empty() || sig_hex.is_empty() {
        return Err("missing plan accept signature".into());
    }
    let pre = plan_accept_sign_preimage(
        from,
        plan_id,
        request_id,
        accepted,
        plan_hash_hex,
        confirm_hex,
        signed_at_unix_ms,
    );
    verify_sig(signer_pubkey_hex, sig_hex, &pre)
}

/// Deterministic lab key from node id (tests / peer bus without persisted identity).
pub fn lab_signing_key_for_node(node: &NodeId) -> SigningKey {
    let mut h = Sha256::new();
    h.update(b"joule-lab-device-key-v1");
    h.update(node.0.as_bytes());
    let seed: [u8; 32] = h.finalize().into();
    SigningKey::from_bytes(&seed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_offer_accept_sign_roundtrip_and_tamper() {
        let from = NodeId::new();
        let sk = lab_signing_key_for_node(&from);
        let plan_id = Uuid::new_v4();
        let rid = Uuid::new_v4();
        let ph = "ab".repeat(32);
        let ts = 1_700_000_000_000u64;
        let pre = plan_offer_sign_preimage(&from, plan_id, rid, &ph, ts);
        let (pk, sig) = sign_preimage(&sk, &pre);
        assert!(verify_plan_offer_sig(&from, plan_id, rid, &ph, &pk, &sig, ts).is_ok());
        assert!(verify_plan_offer_sig(&from, plan_id, rid, &ph, &pk, &sig, ts + 1).is_err());
        assert!(verify_plan_offer_sig(&from, plan_id, rid, &ph, "", &sig, ts).is_err());
        assert!(verify_plan_offer_sig(&from, plan_id, rid, &ph, &pk, "00".repeat(64).as_str(), ts)
            .is_err());

        let confirm = "cd".repeat(32);
        let pre_a = plan_accept_sign_preimage(&from, plan_id, rid, true, &ph, &confirm, ts);
        let (pk2, sig2) = sign_preimage(&sk, &pre_a);
        assert!(verify_plan_accept_sig(
            &from, plan_id, rid, true, &ph, &confirm, &pk2, &sig2, ts
        )
        .is_ok());
        assert!(verify_plan_accept_sig(
            &from, plan_id, rid, false, &ph, &confirm, &pk2, &sig2, ts
        )
        .is_err());
    }
}
