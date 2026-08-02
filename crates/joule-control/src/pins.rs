//! Official trust pins for joule operator authority.
//!
//! Changing these constants produces a **fork**, not a hijack of the official
//! network. Official clients only accept envelopes signed by the embedded
//! protocol ed25519 key (certified by OpenPGP master tj@f00.sh).
//!
//! See docs/design/master-key-trust-v0.md.

/// OpenPGP master fingerprint (tj@f00.sh) — 40 hex chars, no spaces.
pub const MASTER_OPENPGP_FINGERPRINT: &str = "4B18FA65E246ACC61701B6AFCA4CB80ABF1AF878";

/// Full armored public key for the master (include for audit / offline verify).
pub const MASTER_OPENPGP_ASC: &str = include_str!("../../../docs/operator-keys/master.asc");

/// Protocol ed25519 verifying key (64 hex). Agents/control verify bus envelopes
/// against this key in stock builds.
pub const PROTOCOL_ED25519_PUBKEY_HEX: &str =
    "29d1fa05394673402c99ab39a6ea97d8fa02ed1b88667acbc348e4e06790e78a";

/// Official website locations (TLS). Fetched material must match the embed.
pub const OFFICIAL_MASTER_ASC_URL: &str = "https://joule.f00.sh/operator-keys/master.asc";
pub const OFFICIAL_PROTOCOL_PUB_URL: &str =
    "https://joule.f00.sh/operator-keys/protocol.ed25519.pub";

/// True only when lab/fork intentionally opts out of official authority.
pub fn unofficial_operator_allowed() -> bool {
    matches!(
        std::env::var("JOULE_ALLOW_UNOFFICIAL_OPERATOR").as_deref(),
        Ok("1") | Ok("true") | Ok("yes")
    )
}

/// The ed25519 hex used to verify operator envelopes.
///
/// Official path: always the embedded pin.
/// Lab path: optional env/file override when `JOULE_ALLOW_UNOFFICIAL_OPERATOR=1`.
pub fn effective_protocol_pubkey_hex() -> String {
    if unofficial_operator_allowed() {
        if let Ok(p) = std::env::var("JOULE_OPERATOR_PUBKEY") {
            let t = p.trim().to_string();
            if !t.is_empty() {
                return t.to_lowercase();
            }
        }
        for path in [
            std::env::var_os("HOME")
                .map(|h| std::path::PathBuf::from(h).join(".config/f00/joule/operator.pub")),
            Some(std::path::PathBuf::from("operator.pub")),
        ]
        .into_iter()
        .flatten()
        {
            if let Ok(s) = std::fs::read_to_string(&path) {
                if let Some(line) = s
                    .lines()
                    .find(|l| !l.trim().is_empty() && !l.starts_with('#'))
                {
                    let t = line.trim();
                    if t.len() == 64 {
                        return t.to_lowercase();
                    }
                }
            }
        }
    }
    PROTOCOL_ED25519_PUBKEY_HEX.to_lowercase()
}

/// Normalize fingerprint (strip spaces/colons).
pub fn normalize_fpr(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_ascii_hexdigit())
        .collect::<String>()
        .to_uppercase()
}

/// Extract OpenPGP fingerprint from armored ASC using a simple scan of
/// `gpg --with-fingerprint` style is external; here we pin the known fpr and
/// ensure the ASC blob contains the expected fingerprint comment or packet hex.
pub fn master_asc_contains_pin(asc: &str) -> bool {
    let pin = normalize_fpr(MASTER_OPENPGP_FINGERPRINT);
    // Armored keys don't always echo fingerprint as text; require ASC non-empty
    // and that our include_str pin matches constant (compile-time identity).
    if pin.len() != 40 {
        return false;
    }
    // Soft check: exported ASC from our key includes last 16 of keyid in packets
    // when re-exported with comments — also accept exact include_str equality.
    if asc.trim() == MASTER_OPENPGP_ASC.trim() {
        return true;
    }
    // Fingerprint as spaced groups sometimes appears in comments after import tools.
    let compact = normalize_fpr(asc);
    compact.contains(&pin) || asc.contains(MASTER_OPENPGP_FINGERPRINT)
}

/// Parse first 64-hex line from a protocol.pub file body.
pub fn parse_protocol_pub_file(body: &str) -> Option<String> {
    body.lines()
        .map(str::trim)
        .find(|l| !l.is_empty() && !l.starts_with('#') && l.len() == 64)
        .filter(|l| l.chars().all(|c| c.is_ascii_hexdigit()))
        .map(|l| l.to_lowercase())
}

/// Compare website-fetched protocol pub to embed.
pub fn website_protocol_matches_embed(body: &str) -> bool {
    parse_protocol_pub_file(body)
        .map(|h| h == PROTOCOL_ED25519_PUBKEY_HEX.to_lowercase())
        .unwrap_or(false)
}

/// Serialize tests that mutate operator-related env vars (shared with broadcast tests).
#[cfg(test)]
pub fn test_env_lock() -> std::sync::MutexGuard<'static, ()> {
    use std::sync::{Mutex, OnceLock};
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pins_are_well_formed() {
        assert_eq!(normalize_fpr(MASTER_OPENPGP_FINGERPRINT).len(), 40);
        assert_eq!(PROTOCOL_ED25519_PUBKEY_HEX.len(), 64);
        assert!(MASTER_OPENPGP_ASC.contains("BEGIN PGP PUBLIC KEY BLOCK"));
        assert!(master_asc_contains_pin(MASTER_OPENPGP_ASC));
    }

    /// Shipped public key files under docs/operator-keys must match embed pins
    /// (drives real include_str + parse_protocol_pub_file, not a reimplementation).
    #[test]
    fn operator_key_files_match_embed_pins() {
        let pub_file = include_str!("../../../docs/operator-keys/protocol.ed25519.pub");
        let from_file = parse_protocol_pub_file(pub_file).expect("protocol pub hex line");
        assert_eq!(from_file, PROTOCOL_ED25519_PUBKEY_HEX.to_lowercase());
        let asc_file = include_str!("../../../docs/operator-keys/master.asc");
        assert_eq!(asc_file.trim(), MASTER_OPENPGP_ASC.trim());
        assert!(asc_file.contains("BEGIN PGP PUBLIC KEY BLOCK"));
        // Fingerprint pin is the stable human identifier (UID is armored/base64).
        assert_eq!(
            normalize_fpr(MASTER_OPENPGP_FINGERPRINT),
            "4B18FA65E246ACC61701B6AFCA4CB80ABF1AF878"
        );
    }

    #[test]
    fn effective_defaults_to_official() {
        let _g = test_env_lock();
        std::env::remove_var("JOULE_ALLOW_UNOFFICIAL_OPERATOR");
        std::env::remove_var("JOULE_OPERATOR_PUBKEY");
        assert_eq!(
            effective_protocol_pubkey_hex(),
            PROTOCOL_ED25519_PUBKEY_HEX.to_lowercase()
        );
    }

    #[test]
    fn unofficial_override_only_when_allowed() {
        let _g = test_env_lock();
        std::env::set_var("JOULE_OPERATOR_PUBKEY", "aa".repeat(32));
        std::env::remove_var("JOULE_ALLOW_UNOFFICIAL_OPERATOR");
        assert_eq!(
            effective_protocol_pubkey_hex(),
            PROTOCOL_ED25519_PUBKEY_HEX.to_lowercase()
        );
        std::env::set_var("JOULE_ALLOW_UNOFFICIAL_OPERATOR", "1");
        assert_eq!(effective_protocol_pubkey_hex(), "aa".repeat(32));
        std::env::remove_var("JOULE_ALLOW_UNOFFICIAL_OPERATOR");
        std::env::remove_var("JOULE_OPERATOR_PUBKEY");
    }

    /// Dual-pin: mismatched website protocol material fails closed (not accepted as pin).
    #[test]
    fn website_protocol_mismatch_fails_closed() {
        let evil = "ff".repeat(32);
        assert!(!website_protocol_matches_embed(&evil));
        assert!(!website_protocol_matches_embed("not-a-key\n"));
        // Exact embed body matches
        let good = format!("{}\n", PROTOCOL_ED25519_PUBKEY_HEX);
        assert!(website_protocol_matches_embed(&good));
        // Without lab flag, effective key never becomes the evil hex
        let _g = test_env_lock();
        std::env::remove_var("JOULE_ALLOW_UNOFFICIAL_OPERATOR");
        std::env::set_var("JOULE_OPERATOR_PUBKEY", &evil);
        assert_ne!(effective_protocol_pubkey_hex(), evil.to_lowercase());
        assert_eq!(
            effective_protocol_pubkey_hex(),
            PROTOCOL_ED25519_PUBKEY_HEX.to_lowercase()
        );
        std::env::remove_var("JOULE_OPERATOR_PUBKEY");
    }
}
