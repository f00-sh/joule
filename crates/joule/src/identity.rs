//! Anonymous multi-device **joule code** (no PII).
//!
//! User story (dummy easy):
//! 1. Install app → first run **auto-creates** a random code (UUID). You never pick it.
//! 2. All machines that enter the **same code** share one millijoule balance.
//! 3. No names, emails, or phones.
//!
//! Canonical account id is a lowercase UUID string, e.g.
//! `550e8400-e29b-41d4-a716-446655440000`.
//! Legacy `j_`+32-hex ids still accepted and normalized.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// On-disk identity. Treat the **code** as a secret (like a password).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Identity {
    /// Anonymous ledger account id (= joule code). UUID form preferred.
    pub account_id: String,
    /// Cached API key after first Welcome (optional).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    #[serde(default)]
    pub created_unix_ms: u64,
    #[serde(default = "default_version")]
    pub version: u32,
}

fn default_version() -> u32 {
    1
}

impl Identity {
    /// Fresh random code — user never chooses it.
    pub fn generate() -> Self {
        let account_id = Uuid::new_v4().to_string();
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

    /// Human-facing code (same as account_id; always normalized).
    pub fn code(&self) -> &str {
        &self.account_id
    }

    pub fn is_anonymous_id(s: &str) -> bool {
        normalize_code(s).is_ok()
    }
}

/// Normalize a pasted/typed code into canonical UUID account_id.
///
/// Accepts:
/// - `550e8400-e29b-41d4-a716-446655440000`
/// - `550e8400e29b41d4a716446655440000` (no dashes)
/// - `j_` + 32 hex (legacy)
pub fn normalize_code(input: &str) -> Result<String> {
    let s = input.trim().to_ascii_lowercase();
    let s = s.replace([' ', '\t', '\n', '\r'], "");
    if s.is_empty() {
        bail!("empty joule code");
    }

    // UUID with dashes
    if let Ok(u) = Uuid::parse_str(&s) {
        return Ok(u.to_string());
    }

    // j_ + 32 hex → UUID bytes
    let hex_part = s.strip_prefix("j_").unwrap_or(s.as_str());
    let hex_part = hex_part.replace('-', "");
    if hex_part.len() == 32 && hex_part.chars().all(|c| c.is_ascii_hexdigit()) {
        let bytes = hex::decode(&hex_part).context("decode code hex")?;
        let arr: [u8; 16] = bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("code must be 16 bytes"))?;
        return Ok(Uuid::from_bytes(arr).to_string());
    }

    bail!(
        "invalid joule code (need a UUID like 550e8400-e29b-41d4-a716-446655440000)"
    );
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
    let mut id: Identity = serde_json::from_str(&raw).context("parse identity JSON")?;
    if id.account_id.trim().is_empty() {
        bail!("identity has empty account_id");
    }
    // Migrate legacy j_ ids to UUID form on load (same bytes → same account).
    if let Ok(canon) = normalize_code(&id.account_id) {
        if canon != id.account_id {
            id.account_id = canon;
        }
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
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

/// Load or auto-create (user never picks the code).
pub fn load_or_init(path: &Path) -> Result<(Identity, bool)> {
    if path.is_file() {
        return Ok((load(path)?, false));
    }
    let id = Identity::generate();
    save(path, &id)?;
    Ok((id, true))
}

/// Set this machine to an existing code (multi-device link).
pub fn use_code(path: &Path, code: &str) -> Result<Identity> {
    let account_id = normalize_code(code)?;
    let mut id = if path.is_file() {
        load(path).unwrap_or_else(|_| Identity::generate())
    } else {
        Identity {
            account_id: account_id.clone(),
            api_key: None,
            created_unix_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            version: 1,
        }
    };
    id.account_id = account_id;
    // New code ⇒ old cached api_key is wrong account.
    id.api_key = None;
    save(path, &id)?;
    Ok(id)
}

/// Resolve ledger account for agent:
/// 1. `--code` → use_code (link)
/// 2. `--account` non-empty lab string (advanced)
/// 3. else load_or_init identity file
pub fn resolve_account(
    code: Option<&str>,
    explicit_account: Option<&str>,
    identity_path: &Path,
) -> Result<(String, PathBuf, bool /* newly_created */)> {
    if let Some(c) = code.map(str::trim).filter(|s| !s.is_empty()) {
        let id = use_code(identity_path, c)?;
        return Ok((id.account_id, identity_path.to_path_buf(), false));
    }
    if let Some(a) = explicit_account.map(str::trim).filter(|s| !s.is_empty()) {
        // If it looks like a code, normalize; else lab nickname.
        if let Ok(canon) = normalize_code(a) {
            let id = use_code(identity_path, &canon)?;
            return Ok((id.account_id, identity_path.to_path_buf(), false));
        }
        return Ok((a.to_string(), identity_path.to_path_buf(), false));
    }
    let (id, fresh) = load_or_init(identity_path)?;
    Ok((id.account_id, identity_path.to_path_buf(), fresh))
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

/// Banner lines for CLI (first run / show).
pub fn print_code_banner(code: &str, path: &Path, fresh: bool) {
    println!();
    println!("╔══════════════════════════════════════════════════════════════╗");
    if fresh {
        println!("║  Your joule code was created automatically (no name/email).  ║");
    } else {
        println!("║  Your joule code (same code = same millijoules).             ║");
    }
    println!("║                                                              ║");
    println!("║  {code:<60}  ║", code = code);
    println!("║                                                              ║");
    println!("║  Other computer? paste it:                                   ║");
    println!("║    joule identity use {code}");
    println!("║  Saved: {path:<52} ║", path = path.display());
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn generate_is_uuid() {
        let id = Identity::generate();
        assert!(Uuid::parse_str(&id.account_id).is_ok(), "{}", id.account_id);
        assert!(Identity::is_anonymous_id(&id.account_id));
    }

    #[test]
    fn normalize_accepts_uuid_and_hex_and_legacy_j() {
        let u = "550e8400-e29b-41d4-a716-446655440000";
        assert_eq!(normalize_code(u).unwrap(), u);
        assert_eq!(
            normalize_code("550e8400e29b41d4a716446655440000").unwrap(),
            u
        );
        assert_eq!(
            normalize_code("  550E8400-E29B-41D4-A716-446655440000  ").unwrap(),
            u
        );
        // legacy j_ + same 16 bytes
        let j = format!("j_{}", "550e8400e29b41d4a716446655440000");
        assert_eq!(normalize_code(&j).unwrap(), u);
    }

    #[test]
    fn use_code_links_machine() {
        let dir = std::env::temp_dir().join(format!(
            "joule-code-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("identity.json");
        let code = "550e8400-e29b-41d4-a716-446655440000";
        let id = use_code(&path, code).unwrap();
        assert_eq!(id.account_id, code);
        let (acct, _, fresh) = resolve_account(None, None, &path).unwrap();
        assert!(!fresh);
        assert_eq!(acct, code);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn auto_init_stable() {
        let dir = std::env::temp_dir().join(format!(
            "joule-auto-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("identity.json");
        let (a, _, f1) = resolve_account(None, None, &path).unwrap();
        assert!(f1);
        let (b, _, f2) = resolve_account(None, None, &path).unwrap();
        assert!(!f2);
        assert_eq!(a, b);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolve_code_flag() {
        let dir = std::env::temp_dir().join(format!("joule-rc-{}", std::process::id()));
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("identity.json");
        let (acct, _, _) =
            resolve_account(Some("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"), None, &path)
                .unwrap();
        assert_eq!(acct, "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee");
        let _ = fs::remove_dir_all(&dir);
    }
}
