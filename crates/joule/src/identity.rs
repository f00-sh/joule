//! Anonymous multi-device account identity (no PII).
//!
//! One **account_id** = one millijoule ledger account on a pool.
//! Machines share the same identity file → same balance.
//!
//! - No names, emails, phones, or other PII.
//! - Identity is a random opaque id (`j_` + 32 hex chars from 16 random bytes).
//! - Multi-machine: `joule identity export` → copy file → `import` on other hosts.
//! - Optional cached `api_key` after first Welcome (convenience only).

use anyhow::{bail, Context, Result};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

/// On-disk identity (JSON). Treat as a secret (anyone with it can claim the account).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Identity {
    /// Anonymous ledger account id (not a human name).
    pub account_id: String,
    /// Optional API key cached from control Welcome (same key for all machines of this account).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    /// When this identity file was created (unix ms).
    #[serde(default)]
    pub created_unix_ms: u64,
    /// Schema marker.
    #[serde(default = "default_version")]
    pub version: u32,
}

fn default_version() -> u32 {
    1
}

impl Identity {
    /// Fresh anonymous identity (no network).
    pub fn generate() -> Self {
        let mut raw = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut raw);
        let account_id = format!("j_{}", hex::encode(raw));
        let created_unix_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        Self {
            account_id,
            api_key: None,
            created_unix_ms,
            version: 1,
        }
    }

    pub fn is_anonymous_id(s: &str) -> bool {
        let s = s.trim();
        if let Some(rest) = s.strip_prefix("j_") {
            rest.len() == 32 && rest.chars().all(|c| c.is_ascii_hexdigit())
        } else {
            false
        }
    }
}

/// Default path: `$JOULE_IDENTITY` or `~/.config/joule/identity.json`
/// (or `%APPDATA%/joule/identity.json` on Windows via dirs fallback).
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
    let id: Identity = serde_json::from_str(&raw).context("parse identity JSON")?;
    if id.account_id.trim().is_empty() {
        bail!("identity has empty account_id");
    }
    Ok(id)
}

pub fn save(path: &Path, id: &Identity) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create {}", parent.display()))?;
    }
    let raw = serde_json::to_string_pretty(id).context("serialize identity")?;
    fs::write(path, format!("{raw}\n")).with_context(|| format!("write {}", path.display()))?;
    // Best-effort private mode on unix.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

/// Load existing or create + save a new anonymous identity.
pub fn load_or_init(path: &Path) -> Result<Identity> {
    if path.is_file() {
        return load(path);
    }
    let id = Identity::generate();
    save(path, &id)?;
    Ok(id)
}

/// Resolve account string for the agent:
/// - explicit non-empty `--account` wins (lab / advanced)
/// - else identity file account_id (auto-init if missing)
pub fn resolve_account(explicit: Option<&str>, identity_path: &Path) -> Result<(String, PathBuf)> {
    if let Some(a) = explicit.map(str::trim).filter(|s| !s.is_empty()) {
        // Allow lab names; still prefer j_ ids for production.
        return Ok((a.to_string(), identity_path.to_path_buf()));
    }
    let id = load_or_init(identity_path)?;
    Ok((id.account_id, identity_path.to_path_buf()))
}

pub fn remember_api_key(path: &Path, api_key: &str) -> Result<()> {
    let mut id = if path.is_file() {
        load(path)?
    } else {
        return Ok(()); // nothing to update
    };
    if id.api_key.as_deref() == Some(api_key) {
        return Ok(());
    }
    id.api_key = Some(api_key.to_string());
    save(path, &id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn generate_is_anonymous_j_prefix() {
        let id = Identity::generate();
        assert!(Identity::is_anonymous_id(&id.account_id), "{}", id.account_id);
        assert!(id.api_key.is_none());
        assert_eq!(id.version, 1);
    }

    #[test]
    fn roundtrip_file() {
        let dir = std::env::temp_dir().join(format!(
            "joule-id-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("identity.json");
        let id = Identity::generate();
        save(&path, &id).unwrap();
        let loaded = load(&path).unwrap();
        assert_eq!(loaded.account_id, id.account_id);
        remember_api_key(&path, "joule_testkey").unwrap();
        let again = load(&path).unwrap();
        assert_eq!(again.api_key.as_deref(), Some("joule_testkey"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolve_prefers_explicit() {
        let dir = std::env::temp_dir().join(format!("joule-id-ex-{}", std::process::id()));
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("identity.json");
        let (acct, _) = resolve_account(Some("lab-alice"), &path).unwrap();
        assert_eq!(acct, "lab-alice");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolve_inits_anonymous() {
        let dir = std::env::temp_dir().join(format!(
            "joule-id-init-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("identity.json");
        let (acct, _) = resolve_account(None, &path).unwrap();
        assert!(Identity::is_anonymous_id(&acct));
        let (acct2, _) = resolve_account(None, &path).unwrap();
        assert_eq!(acct, acct2, "stable across loads");
        let _ = fs::remove_dir_all(&dir);
    }
}
