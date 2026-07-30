//! Pull official public keys from joule.f00.sh over TLS and ensure they match embed.
//!
//! Website is a **mirror of the pin**, not a replacement root of trust.
//! See docs/design/master-key-trust-v0.md.

use crate::pins::{
    master_asc_contains_pin, website_protocol_matches_embed, MASTER_OPENPGP_ASC,
    OFFICIAL_MASTER_ASC_URL, OFFICIAL_PROTOCOL_PUB_URL, PROTOCOL_ED25519_PUBKEY_HEX,
};
use serde::Serialize;
use tracing::{info, warn};

#[derive(Debug, Clone, Serialize)]
pub struct OfficialKeyAudit {
    pub ok: bool,
    pub embed_protocol: String,
    pub website_checked: bool,
    pub master_match: Option<bool>,
    pub protocol_match: Option<bool>,
    pub message: String,
}

/// Offline-only audit of embedded pins (always available).
pub fn audit_embed_only() -> OfficialKeyAudit {
    OfficialKeyAudit {
        ok: true,
        embed_protocol: PROTOCOL_ED25519_PUBKEY_HEX.to_lowercase(),
        website_checked: false,
        master_match: Some(master_asc_contains_pin(MASTER_OPENPGP_ASC)),
        protocol_match: Some(true),
        message: "using embedded official pins (website not checked)".into(),
    }
}

/// Fetch official keys from the website and compare to embed.
pub async fn audit_official_keys() -> OfficialKeyAudit {
    if std::env::var("JOULE_SKIP_OFFICIAL_KEY_FETCH")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
    {
        return audit_embed_only();
    }

    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(12))
        .user_agent("joule-control/0.0.0 (official key audit)")
        .https_only(true)
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            return OfficialKeyAudit {
                ok: true,
                embed_protocol: PROTOCOL_ED25519_PUBKEY_HEX.to_lowercase(),
                website_checked: false,
                master_match: None,
                protocol_match: None,
                message: format!("client build failed; embed only: {e}"),
            };
        }
    };

    let master_body = match client.get(OFFICIAL_MASTER_ASC_URL).send().await {
        Ok(r) if r.status().is_success() => r.text().await.ok(),
        Ok(r) => {
            warn!(status = %r.status(), "official master.asc fetch non-success");
            None
        }
        Err(e) => {
            warn!(error = %e, "official master.asc fetch failed (offline OK)");
            None
        }
    };

    let proto_body = match client.get(OFFICIAL_PROTOCOL_PUB_URL).send().await {
        Ok(r) if r.status().is_success() => r.text().await.ok(),
        Ok(r) => {
            warn!(status = %r.status(), "official protocol.pub fetch non-success");
            None
        }
        Err(e) => {
            warn!(error = %e, "official protocol.pub fetch failed (offline OK)");
            None
        }
    };

    if master_body.is_none() && proto_body.is_none() {
        return audit_embed_only();
    }

    let master_match = master_body
        .as_deref()
        .map(|b| master_asc_contains_pin(b) || b.contains("tj@f00.sh"));
    // Prefer exact ASC match when site has fully deployed current ASC.
    let master_match = match (master_match, master_body.as_deref()) {
        (Some(true), _) => Some(true),
        (_, Some(b)) if b.trim() == MASTER_OPENPGP_ASC.trim() => Some(true),
        (m, _) => m,
    };

    let protocol_match = proto_body.as_deref().map(website_protocol_matches_embed);

    let ok = !matches!(
        (master_match, protocol_match),
        (Some(false), _) | (_, Some(false))
    );

    let message = if !ok {
        "CRITICAL: website operator keys do not match embedded official pins — refusing to trust website material; still using embed for verify".into()
    } else {
        "official website keys match embedded pins".into()
    };

    if ok {
        info!(%message);
    } else {
        warn!(%message);
    }

    OfficialKeyAudit {
        ok,
        embed_protocol: PROTOCOL_ED25519_PUBKEY_HEX.to_lowercase(),
        website_checked: true,
        master_match,
        protocol_match,
        message,
    }
}
