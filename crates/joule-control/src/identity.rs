//! Pool operator identity: ed25519 keypair for signed public snapshots.
//!
//! Keys live under the control data dir (`pool.ed25519` = 32-byte seed, hex).
//! Anyone can verify with the published verifying key; the site multi-sources
//! signed feeds without trusting Cloudflare as authority.

use anyhow::{Context, Result};
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};

const KEY_FILE: &str = "pool.ed25519";

#[derive(Clone)]
pub struct PoolIdentity {
    pub pool_id: String,
    signing: SigningKey,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicKeyInfo {
    pub pool_id: String,
    /// Hex-encoded 32-byte ed25519 verifying key.
    pub verifying_key_hex: String,
    pub algorithm: &'static str,
}

impl PoolIdentity {
    pub fn load_or_create(data_dir: Option<&Path>) -> Result<Self> {
        let pool_id = std::env::var("JOULE_POOL_ID").unwrap_or_else(|_| "joule-default".into());
        let dir = data_dir
            .map(Path::to_path_buf)
            .or_else(default_identity_dir)
            .unwrap_or_else(|| PathBuf::from("./.joule-data"));
        fs::create_dir_all(&dir).with_context(|| format!("mkdir {}", dir.display()))?;
        let path = dir.join(KEY_FILE);
        let signing = if path.exists() {
            let hex = fs::read_to_string(&path).context("read pool.ed25519")?;
            let bytes = decode_hex32(hex.trim()).context("parse pool.ed25519")?;
            SigningKey::from_bytes(&bytes)
        } else {
            let sk = SigningKey::generate(&mut OsRng);
            let hex = hex::encode(sk.to_bytes());
            fs::write(&path, format!("{hex}\n")).context("write pool.ed25519")?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = fs::set_permissions(&path, fs::Permissions::from_mode(0o600));
            }
            tracing::info!(path = %path.display(), "generated new pool signing key");
            sk
        };
        Ok(Self { pool_id, signing })
    }

    pub fn public_info(&self) -> PublicKeyInfo {
        PublicKeyInfo {
            pool_id: self.pool_id.clone(),
            verifying_key_hex: hex::encode(self.signing.verifying_key().to_bytes()),
            algorithm: "ed25519",
        }
    }

    pub fn verifying_key(&self) -> VerifyingKey {
        self.signing.verifying_key()
    }

    /// Sign a preimage (raw bytes). Returns hex signature (64 bytes).
    pub fn sign_bytes(&self, msg: &[u8]) -> String {
        let sig: Signature = self.signing.sign(msg);
        hex::encode(sig.to_bytes())
    }
}

/// Verify a hex signature over a preimage with a hex verifying key.
pub fn verify_preimage(
    verifying_key_hex: &str,
    preimage: &[u8],
    signature_hex: &str,
) -> Result<bool> {
    let pk = decode_hex32(verifying_key_hex).context("verifying key")?;
    let sig_bytes = hex::decode(signature_hex).context("signature hex")?;
    anyhow::ensure!(sig_bytes.len() == 64, "signature must be 64 bytes");
    let vk = VerifyingKey::from_bytes(&pk).context("verifying key bytes")?;
    let sig = Signature::from_slice(&sig_bytes).context("signature bytes")?;
    // ed25519-dalek 2.x: verify_strict is inherent on VerifyingKey
    Ok(vk.verify_strict(preimage, &sig).is_ok())
}

/// Canonical preimage for snapshot signatures (must match browser / docs).
///
/// ```text
/// sha256( pool_id || "\n" || updated_unix_ms || "\n" || body_json )
/// ```
/// where `body_json` is compact JSON of `{capacity,readiness,scheduler,nodes}`
/// in that field order (serde struct order).
pub fn snapshot_preimage(pool_id: &str, updated_unix_ms: u64, body_json: &str) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(pool_id.as_bytes());
    h.update(b"\n");
    h.update(updated_unix_ms.to_string().as_bytes());
    h.update(b"\n");
    h.update(body_json.as_bytes());
    h.finalize().into()
}

/// Preimage for public source announce (no privileged token).
///
/// ```text
/// sha256( pool_id || "\n" || snapshot_url || "\n" || updated_unix_ms )
/// ```
pub fn announce_preimage(pool_id: &str, snapshot_url: &str, updated_unix_ms: u64) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(pool_id.as_bytes());
    h.update(b"\n");
    h.update(snapshot_url.as_bytes());
    h.update(b"\n");
    h.update(updated_unix_ms.to_string().as_bytes());
    h.finalize().into()
}

fn default_identity_dir() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/share/joule"))
}

fn decode_hex32(s: &str) -> Result<[u8; 32]> {
    let v = hex::decode(s).context("hex decode")?;
    anyhow::ensure!(v.len() == 32, "expected 32 bytes, got {}", v.len());
    let mut out = [0u8; 32];
    out.copy_from_slice(&v);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_roundtrip_preimage() {
        let id = PoolIdentity {
            pool_id: "test".into(),
            signing: SigningKey::generate(&mut OsRng),
        };
        let body = r#"{"capacity":{},"readiness":null,"scheduler":null,"nodes":[]}"#;
        let pre = snapshot_preimage("test", 123, body);
        let sig_hex = id.sign_bytes(&pre);
        let vk = hex::encode(id.verifying_key().to_bytes());
        assert!(verify_preimage(&vk, &pre, &sig_hex).unwrap());
        assert!(!verify_preimage(&vk, b"tampered", &sig_hex).unwrap());
    }
}
