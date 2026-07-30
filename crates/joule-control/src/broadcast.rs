//! Operator broadcast bus: verify signed envelopes, dedupe, flood agents.
//!
//! Authority is the **operator public key**, not the hostname of control.
//! See docs/design/broadcast-v0.md.

use joule_proto::{OperatorKind, SignedEnvelope};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

/// Preimage signed by the operator ed25519 key:
/// `sha256( id || "\n" || issued_at || "\n" || kind || "\n" || body_sha256 )`
pub fn operator_preimage(env: &SignedEnvelope) -> [u8; 32] {
    let kind = match env.kind {
        OperatorKind::Notice => "notice",
        OperatorKind::SoftwareUpdate => "software_update",
        OperatorKind::ModelUpdate => "model_update",
        OperatorKind::Policy => "policy",
        OperatorKind::PauseService => "pause_service",
        OperatorKind::ResumeService => "resume_service",
        OperatorKind::Revoke => "revoke",
        OperatorKind::Other => "other",
    };
    let mut h = Sha256::new();
    h.update(env.id.to_string().as_bytes());
    h.update(b"\n");
    h.update(env.issued_at_unix_ms.to_string().as_bytes());
    h.update(b"\n");
    h.update(kind.as_bytes());
    h.update(b"\n");
    h.update(env.body_sha256.as_bytes());
    h.finalize().into()
}

pub fn body_sha256_hex(body_json: &str) -> String {
    let mut h = Sha256::new();
    h.update(body_json.as_bytes());
    hex::encode(h.finalize())
}

/// In-memory dedupe + last-N log for joiners.
#[derive(Debug, Default)]
pub struct BroadcastLog {
    seen: HashSet<uuid::Uuid>,
    /// Envelope ids explicitly revoked by operator (never re-accept).
    revoked: HashSet<uuid::Uuid>,
    recent: Vec<SignedEnvelope>,
    max_recent: usize,
}

impl BroadcastLog {
    pub fn new(max_recent: usize) -> Self {
        Self {
            seen: HashSet::new(),
            revoked: HashSet::new(),
            recent: Vec::new(),
            max_recent: max_recent.max(16),
        }
    }

    pub fn recent(&self) -> &[SignedEnvelope] {
        &self.recent
    }

    pub fn revoked_count(&self) -> usize {
        self.revoked.len()
    }

    /// Record revoke targets from a verified revoke envelope body.
    pub fn apply_revoke_body(&mut self, body_json: &str) {
        #[derive(serde::Deserialize)]
        struct RevokeBody {
            #[serde(default)]
            ids: Vec<uuid::Uuid>,
            #[serde(default)]
            id: Option<uuid::Uuid>,
        }
        if let Ok(b) = serde_json::from_str::<RevokeBody>(body_json) {
            for id in b.ids {
                self.revoked.insert(id);
                self.seen.insert(id);
            }
            if let Some(id) = b.id {
                self.revoked.insert(id);
                self.seen.insert(id);
            }
        }
    }

    /// Returns true if this is a newly accepted envelope (caller should flood).
    pub fn accept(&mut self, env: SignedEnvelope, now_ms: u64) -> Result<bool, String> {
        if self.revoked.contains(&env.id) {
            return Err("envelope revoked".into());
        }
        if self.seen.contains(&env.id) {
            return Ok(false);
        }
        if let Some(exp) = env.expires_at_unix_ms {
            if now_ms > exp {
                return Err("envelope expired".into());
            }
        }
        let expect = body_sha256_hex(&env.body_json);
        if expect != env.body_sha256.to_lowercase() && expect != env.body_sha256 {
            // allow either case
            if expect != env.body_sha256.to_lowercase() {
                return Err("body_sha256 mismatch".into());
            }
        }
        // Always verify against official embed (or lab override if allowed).
        // See docs/design/master-key-trust-v0.md — no "unsigned open" mode.
        let pk = operator_pubkey_hex();
        verify_operator_sig(&env, &pk)?;
        self.seen.insert(env.id);
        if env.kind == OperatorKind::Revoke {
            self.apply_revoke_body(&env.body_json);
        }
        self.recent.push(env);
        if self.recent.len() > self.max_recent {
            let drop_n = self.recent.len() - self.max_recent;
            self.recent.drain(0..drop_n);
        }
        Ok(true)
    }
}

/// Public for CLI / inject / agents — always returns the effective verify key.
/// Official builds use the embedded pin; see `crate::pins`.
pub fn operator_pubkey_hex() -> String {
    crate::pins::effective_protocol_pubkey_hex()
}

pub fn verify_operator_sig(env: &SignedEnvelope, pubkey_hex: &str) -> Result<(), String> {
    use ed25519_dalek::{Signature, VerifyingKey};
    let pre = operator_preimage(env);
    let pk_bytes = hex::decode(pubkey_hex.trim()).map_err(|e| e.to_string())?;
    if pk_bytes.len() != 32 {
        return Err("operator pubkey must be 32 bytes hex".into());
    }
    let mut pk = [0u8; 32];
    pk.copy_from_slice(&pk_bytes);
    let vk = VerifyingKey::from_bytes(&pk).map_err(|e| e.to_string())?;
    let sig_bytes = hex::decode(env.sig_ed25519_hex.trim()).map_err(|e| e.to_string())?;
    let sig = Signature::from_slice(&sig_bytes).map_err(|e| e.to_string())?;
    vk.verify_strict(&pre, &sig)
        .map_err(|_| "operator signature invalid".to_string())?;
    Ok(())
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};
    use joule_proto::OperatorKind;
    use rand::rngs::OsRng;
    use uuid::Uuid;

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        crate::pins::test_env_lock()
    }

    #[test]
    fn sign_and_accept() {
        let _g = env_lock();
        let sk = SigningKey::generate(&mut OsRng);
        let pk = hex::encode(sk.verifying_key().to_bytes());
        std::env::set_var("JOULE_ALLOW_UNOFFICIAL_OPERATOR", "1");
        std::env::set_var("JOULE_OPERATOR_PUBKEY", &pk);

        let body = r#"{"title":"hello","body":"swarm"}"#;
        let body_hash = body_sha256_hex(body);
        let mut env = SignedEnvelope {
            id: Uuid::new_v4(),
            issued_at_unix_ms: now_ms(),
            expires_at_unix_ms: None,
            kind: OperatorKind::Notice,
            body_json: body.into(),
            body_sha256: body_hash,
            sig_ed25519_hex: String::new(),
            openpgp_sig: None,
        };
        let pre = operator_preimage(&env);
        env.sig_ed25519_hex = hex::encode(sk.sign(&pre).to_bytes());

        let mut log = BroadcastLog::new(32);
        assert!(log.accept(env.clone(), now_ms()).unwrap());
        assert!(!log.accept(env, now_ms()).unwrap()); // dedupe
        std::env::remove_var("JOULE_OPERATOR_PUBKEY");
        std::env::remove_var("JOULE_ALLOW_UNOFFICIAL_OPERATOR");
    }

    #[test]
    fn revoke_blocks_id() {
        let _g = env_lock();
        let sk = SigningKey::generate(&mut OsRng);
        let pk = hex::encode(sk.verifying_key().to_bytes());
        std::env::set_var("JOULE_ALLOW_UNOFFICIAL_OPERATOR", "1");
        std::env::set_var("JOULE_OPERATOR_PUBKEY", &pk);

        let victim_id = Uuid::new_v4();
        let rev_body = format!(r#"{{"ids":["{victim_id}"]}}"#);
        let mut rev = SignedEnvelope {
            id: Uuid::new_v4(),
            issued_at_unix_ms: now_ms(),
            expires_at_unix_ms: None,
            kind: OperatorKind::Revoke,
            body_json: rev_body.clone(),
            body_sha256: body_sha256_hex(&rev_body),
            sig_ed25519_hex: String::new(),
            openpgp_sig: None,
        };
        let pre = operator_preimage(&rev);
        rev.sig_ed25519_hex = hex::encode(sk.sign(&pre).to_bytes());
        let mut log = BroadcastLog::new(32);
        assert!(log.accept(rev, now_ms()).unwrap());
        assert_eq!(log.revoked_count(), 1);

        let body = r#"{"title":"nope"}"#;
        let mut bad = SignedEnvelope {
            id: victim_id,
            issued_at_unix_ms: now_ms(),
            expires_at_unix_ms: None,
            kind: OperatorKind::Notice,
            body_json: body.into(),
            body_sha256: body_sha256_hex(body),
            sig_ed25519_hex: String::new(),
            openpgp_sig: None,
        };
        let pre = operator_preimage(&bad);
        bad.sig_ed25519_hex = hex::encode(sk.sign(&pre).to_bytes());
        assert!(log.accept(bad, now_ms()).unwrap_err().contains("revoked"));
        std::env::remove_var("JOULE_OPERATOR_PUBKEY");
        std::env::remove_var("JOULE_ALLOW_UNOFFICIAL_OPERATOR");
    }

    #[test]
    fn official_pin_rejects_random_key_without_unofficial_flag() {
        let _g = env_lock();
        std::env::remove_var("JOULE_ALLOW_UNOFFICIAL_OPERATOR");
        std::env::remove_var("JOULE_OPERATOR_PUBKEY");
        let sk = SigningKey::generate(&mut OsRng);
        let body = r#"{"title":"hijack?"}"#;
        let mut env = SignedEnvelope {
            id: Uuid::new_v4(),
            issued_at_unix_ms: now_ms(),
            expires_at_unix_ms: None,
            kind: OperatorKind::Notice,
            body_json: body.into(),
            body_sha256: body_sha256_hex(body),
            sig_ed25519_hex: String::new(),
            openpgp_sig: None,
        };
        let pre = operator_preimage(&env);
        env.sig_ed25519_hex = hex::encode(sk.sign(&pre).to_bytes());
        let mut log = BroadcastLog::new(8);
        let err = log.accept(env, now_ms()).unwrap_err();
        assert!(
            err.contains("signature") || err.contains("invalid"),
            "{err}"
        );
    }
}
