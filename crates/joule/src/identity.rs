//! Anonymous multi-device **joule code** with pool-accepted signatures (no PII).
//!
//! - **Recovery code** (UUID): secret you type on each machine — expands to an ed25519 key.
//! - **Account id** (`j1…`): public fingerprint of the pubkey — ledger account.
//! - **Hello** is signed so the whole pool accepts only the real key holder.
//!
//! Install → run agent → code created automatically. Same code on N machines = one balance.

use anyhow::{bail, Context, Result};
use joule_control::{
    account_id_from_verifying_key, sign_hello, signing_key_from_recovery, ACCOUNT_PREFIX,
};
use joule_proto::{Message, NodeId};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// On-disk identity. The recovery code is the multi-machine secret.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Identity {
    /// User-facing recovery code (UUID). Same on every machine you own.
    pub recovery_code: String,
    /// Public ledger account = fingerprint of derived pubkey (`j1…`).
    pub account_id: String,
    /// Ed25519 public key hex (64 chars).
    pub pubkey_hex: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    #[serde(default)]
    pub created_unix_ms: u64,
    #[serde(default = "default_version")]
    pub version: u32,
}

fn default_version() -> u32 {
    2
}

impl Identity {
    /// Fresh random recovery code → keypair → account fingerprint.
    pub fn generate() -> Self {
        let recovery = Uuid::new_v4();
        Self::from_recovery_uuid(recovery)
    }

    pub fn from_recovery_uuid(recovery: Uuid) -> Self {
        let bytes = *recovery.as_bytes();
        let sk = signing_key_from_recovery(&bytes);
        let vk = sk.verifying_key();
        let account_id = account_id_from_verifying_key(&vk);
        let pubkey_hex = hex::encode(vk.as_bytes());
        let created_unix_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        Self {
            recovery_code: recovery.to_string(),
            account_id,
            pubkey_hex,
            api_key: None,
            created_unix_ms,
            version: 2,
        }
    }

    /// User-facing multi-machine code (secret).
    pub fn code(&self) -> &str {
        &self.recovery_code
    }

    pub fn signing_key(&self) -> Result<ed25519_dalek::SigningKey> {
        let u = Uuid::parse_str(&self.recovery_code).context("recovery_code uuid")?;
        Ok(signing_key_from_recovery(u.as_bytes()))
    }

    /// Build a signed Hello message for the pool.
    pub fn signed_hello(&self, from: &NodeId, caps: joule_proto::NodeCaps) -> Result<Message> {
        let sk = self.signing_key()?;
        let ts = joule_control::account_auth_now_ms();
        let (pubkey_hex, sig_hex) = sign_hello(&sk, &self.account_id, from, ts);
        Ok(Message::Hello {
            account: self.account_id.clone(),
            caps,
            pubkey_hex,
            sig_hex,
            signed_at_unix_ms: ts,
        })
    }

    #[allow(dead_code)]
    pub fn is_signed_account(s: &str) -> bool {
        s.trim().starts_with(ACCOUNT_PREFIX)
    }
}

/// Normalize recovery code (UUID) from user paste.
pub fn normalize_code(input: &str) -> Result<String> {
    let s = input.trim().to_ascii_lowercase().replace([' ', '\t', '\n', '\r'], "");
    if s.is_empty() {
        bail!("empty joule code");
    }
    if let Ok(u) = Uuid::parse_str(&s) {
        return Ok(u.to_string());
    }
    let hex_part = s.replace('-', "");
    if hex_part.len() == 32 && hex_part.chars().all(|c| c.is_ascii_hexdigit()) {
        let bytes = hex::decode(&hex_part).context("decode code hex")?;
        let arr: [u8; 16] = bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("code must be 16 bytes"))?;
        return Ok(Uuid::from_bytes(arr).to_string());
    }
    // legacy j_ + 32 hex was account id, not recovery — cannot re-derive key
    if s.starts_with("j_") || s.starts_with(ACCOUNT_PREFIX) {
        bail!("that looks like a public account id, not a recovery code — use the UUID code from your first machine");
    }
    bail!("invalid joule code (need UUID like 550e8400-e29b-41d4-a716-446655440000)");
}

pub fn default_path() -> PathBuf {
    if let Ok(p) = std::env::var("JOULE_IDENTITY") {
        return PathBuf::from(p);
    }
    if let Ok(cfg) = std::env::var("XDG_CONFIG_HOME") {
        return PathBuf::from(cfg).join("joule").join("identity.json");
    }
    if let Ok(home) = std::env::var("HOME") {
        return PathBuf::from(home)
            .join(".config")
            .join("joule")
            .join("identity.json");
    }
    if let Ok(appdata) = std::env::var("APPDATA") {
        return PathBuf::from(appdata).join("joule").join("identity.json");
    }
    PathBuf::from("joule-identity.json")
}

pub fn load(path: &Path) -> Result<Identity> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("read identity {}", path.display()))?;
    let v: serde_json::Value = serde_json::from_str(&raw).context("parse identity JSON")?;
    // v2: recovery_code + account_id + pubkey
    if v.get("recovery_code").is_some() {
        let id: Identity = serde_json::from_value(v).context("parse identity v2")?;
        if id.recovery_code.is_empty() || id.account_id.is_empty() {
            bail!("identity missing recovery_code or account_id");
        }
        return Ok(id);
    }
    // v1 migrate: account_id was UUID code — treat as recovery
    if let Some(aid) = v.get("account_id").and_then(|x| x.as_str()) {
        if let Ok(code) = normalize_code(aid) {
            let mut id = Identity::from_recovery_uuid(Uuid::parse_str(&code)?);
            if let Some(k) = v.get("api_key").and_then(|x| x.as_str()) {
                id.api_key = Some(k.to_string());
            }
            save(path, &id)?;
            return Ok(id);
        }
    }
    bail!("unrecognized identity file (run joule identity new --force)");
}

pub fn save(path: &Path, id: &Identity) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create {}", parent.display()))?;
    }
    let raw = serde_json::to_string_pretty(id).context("serialize identity")?;
    fs::write(path, format!("{raw}\n")).with_context(|| format!("write {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

pub fn load_or_init(path: &Path) -> Result<(Identity, bool)> {
    if path.is_file() {
        return Ok((load(path)?, false));
    }
    let id = Identity::generate();
    save(path, &id)?;
    Ok((id, true))
}

pub fn use_code(path: &Path, code: &str) -> Result<Identity> {
    let code = normalize_code(code)?;
    let u = Uuid::parse_str(&code)?;
    let id = Identity::from_recovery_uuid(u);
    save(path, &id)?;
    Ok(id)
}

pub fn resolve_account(
    code: Option<&str>,
    explicit_account: Option<&str>,
    identity_path: &Path,
) -> Result<(Identity, bool)> {
    if let Some(c) = code.map(str::trim).filter(|s| !s.is_empty()) {
        let id = use_code(identity_path, c)?;
        return Ok((id, false));
    }
    if let Some(a) = explicit_account.map(str::trim).filter(|s| !s.is_empty()) {
        // Lab nickname: synthetic identity without real key (unsigned hello).
        if normalize_code(a).is_err() && !a.starts_with(ACCOUNT_PREFIX) {
            let id = Identity {
                recovery_code: String::new(),
                account_id: a.to_string(),
                pubkey_hex: String::new(),
                api_key: None,
                created_unix_ms: 0,
                version: 2,
            };
            return Ok((id, false));
        }
        // Treat as recovery code if it parses.
        if normalize_code(a).is_ok() {
            let id = use_code(identity_path, a)?;
            return Ok((id, false));
        }
    }
    load_or_init(identity_path)
}

pub fn remember_api_key(path: &Path, api_key: &str) -> Result<()> {
    let mut id = if path.is_file() {
        load(path)?
    } else {
        return Ok(());
    };
    if id.api_key.as_deref() == Some(api_key) {
        return Ok(());
    }
    id.api_key = Some(api_key.to_string());
    save(path, &id)
}

pub fn print_code_banner(id: &Identity, path: &Path, fresh: bool) {
    println!();
    println!("╔══════════════════════════════════════════════════════════════╗");
    if fresh {
        println!("║  Your joule code was created automatically.                  ║");
    } else {
        println!("║  Your joule code (secret — same on every machine).           ║");
    }
    println!("║                                                              ║");
    println!("║  CODE  {}  ║", pad60(id.code()));
    println!("║  ACCT  {}  ║", pad60(&id.account_id));
    println!("║                                                              ║");
    println!("║  Other PC:  joule identity use <CODE>                        ║");
    println!("║  Pool accepts only signed Hellos for ACCT (ed25519).         ║");
    println!("║  Saved: {} ║", pad52(&path.display().to_string()));
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();
}

fn pad60(s: &str) -> String {
    let mut t = s.chars().take(60).collect::<String>();
    while t.chars().count() < 60 {
        t.push(' ');
    }
    t
}

fn pad52(s: &str) -> String {
    let mut t = s.chars().take(52).collect::<String>();
    while t.chars().count() < 52 {
        t.push(' ');
    }
    t
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn generate_has_j1_account_and_uuid_code() {
        let id = Identity::generate();
        assert!(Uuid::parse_str(id.code()).is_ok());
        assert!(id.account_id.starts_with("j1"));
        assert_eq!(id.pubkey_hex.len(), 64);
        // Same recovery → same account
        let u = Uuid::parse_str(id.code()).unwrap();
        let id2 = Identity::from_recovery_uuid(u);
        assert_eq!(id.account_id, id2.account_id);
        assert_eq!(id.pubkey_hex, id2.pubkey_hex);
    }

    #[test]
    fn signed_hello_verifies() {
        let id = Identity::generate();
        let from = NodeId::new();
        let msg = id
            .signed_hello(
                &from,
                joule_proto::NodeCaps::for_cluster(joule_proto::DeviceClass::Gpu, 8192, 10),
            )
            .unwrap();
        match msg {
            Message::Hello {
                account,
                pubkey_hex,
                sig_hex,
                signed_at_unix_ms,
                ..
            } => {
                assert_eq!(account, id.account_id);
                let now = joule_control::account_auth_now_ms();
                joule_control::verify_hello(
                    &account,
                    &from,
                    &pubkey_hex,
                    &sig_hex,
                    signed_at_unix_ms,
                    now,
                )
                .unwrap();
            }
            _ => panic!("hello"),
        }
    }

    #[test]
    fn use_code_roundtrip() {
        let dir = std::env::temp_dir().join(format!(
            "joule-sig-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("identity.json");
        let a = Identity::generate();
        let b = use_code(&path, a.code()).unwrap();
        assert_eq!(a.account_id, b.account_id);
        let _ = fs::remove_dir_all(&dir);
    }
}
