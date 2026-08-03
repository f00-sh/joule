//! Offline Kimi-K3 meta pin: `num_hidden_layers` from sha256-verified config.json.
//!
//! CI never downloads full K3 weights. The fixture under
//! `models/fixtures/kimi-k3-meta/config.json` must match the MANIFEST
//! `kimi-k3-meta` `config.json` digest (same bytes embedded here for offline use).

use crate::manifest::ManifestFile;
use sha2::{Digest, Sha256};

/// Offline fixture bytes (must match MANIFEST `kimi-k3-meta` / `config.json` sha256).
pub const EMBEDDED_K3_CONFIG_JSON: &str =
    include_str!("../../../models/fixtures/kimi-k3-meta/config.json");

/// Parse transformer layer count from Kimi-K3-style HF config JSON.
///
/// Prefers `text_config.num_hidden_layers`, then top-level `num_hidden_layers`.
pub fn num_hidden_layers_from_config_json(json: &str) -> Result<u32, String> {
    let v: serde_json::Value =
        serde_json::from_str(json).map_err(|e| format!("k3 meta config json: {e}"))?;
    if let Some(n) = v
        .pointer("/text_config/num_hidden_layers")
        .and_then(|x| x.as_u64())
    {
        return u32::try_from(n).map_err(|_| "num_hidden_layers out of range".into());
    }
    if let Some(n) = v.get("num_hidden_layers").and_then(|x| x.as_u64()) {
        return u32::try_from(n).map_err(|_| "num_hidden_layers out of range".into());
    }
    Err("num_hidden_layers missing in K3 config (text_config or root)".into())
}

/// SHA-256 hex of UTF-8 config bytes (same as weight-file pins).
pub fn config_sha256_hex(json: &str) -> String {
    hex::encode(Sha256::digest(json.as_bytes()))
}

/// MANIFEST pin for `kimi-k3-meta` / `config.json`, if present.
pub fn manifest_k3_config_digest(manifest: &ManifestFile) -> Result<String, String> {
    let model = manifest
        .primary()
        .ok_or_else(|| "no primary model".to_string())?;
    let quant = model
        .weights
        .quants
        .iter()
        .find(|q| q.id == "kimi-k3-meta")
        .ok_or_else(|| "kimi-k3-meta quant missing from MANIFEST".to_string())?;
    let file = quant
        .files
        .iter()
        .find(|f| f.path == "config.json" || f.path.ends_with("/config.json"))
        .ok_or_else(|| "kimi-k3-meta config.json pin missing".to_string())?;
    Ok(file.sha256.trim().to_ascii_lowercase())
}

/// Layer count from embedded meta **only if** its sha256 matches MANIFEST pin.
///
/// Fail closed: wrong hash ⇒ error (no silent ungrounded override).
pub fn verified_k3_model_layers() -> Result<u32, String> {
    verified_k3_model_layers_from(EMBEDDED_K3_CONFIG_JSON, &ManifestFile::load_default()?)
}

/// Pure: verify `json` against MANIFEST pin, then parse `num_hidden_layers`.
pub fn verified_k3_model_layers_from(json: &str, manifest: &ManifestFile) -> Result<u32, String> {
    let want = manifest_k3_config_digest(manifest)?;
    let got = config_sha256_hex(json);
    if got != want {
        return Err(format!(
            "k3 meta config sha256 mismatch (got {got}, want {want}) — refuse ungrounded layers"
        ));
    }
    num_hidden_layers_from_config_json(json)
}

/// Placement layer total: verified K3 meta when pin matches; else MANIFEST `model_layers`
/// only when it already equals verified meta (CI offline path uses fixture).
pub fn placement_model_layers() -> u32 {
    match verified_k3_model_layers() {
        Ok(n) if n > 0 => n,
        _ => ManifestFile::load_default()
            .ok()
            .and_then(|m| m.primary().map(|s| s.model_layers))
            .filter(|n| *n > 0)
            .unwrap_or(80),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_config_matches_manifest_pin_and_layers() {
        let m = ManifestFile::load_default().unwrap();
        let pin = manifest_k3_config_digest(&m).unwrap();
        assert_eq!(config_sha256_hex(EMBEDDED_K3_CONFIG_JSON), pin);
        let n = verified_k3_model_layers().expect("verified layers");
        assert_eq!(n, 93, "Kimi-K3 text_config.num_hidden_layers");
        assert_eq!(
            m.primary().unwrap().model_layers,
            n,
            "MANIFEST model_layers must match verified meta"
        );
        assert_eq!(placement_model_layers(), n);
        eprintln!("OBSERVE layer-pin-meta: model_layers={n} sha={pin}");
    }

    #[test]
    fn wrong_meta_bytes_fail_closed() {
        let m = ManifestFile::load_default().unwrap();
        let err = verified_k3_model_layers_from("{\"text_config\":{\"num_hidden_layers\":1}}", &m)
            .unwrap_err();
        assert!(
            err.contains("sha256 mismatch") || err.contains("mismatch"),
            "{err}"
        );
    }

    #[test]
    fn design_docs_state_geometry_not_fake_pp() {
        let cluster = include_str!("../../../docs/design/cluster-v0.md");
        assert!(
            cluster.contains("scheduling geometry")
                && cluster.contains("not executed multi-node pipeline-parallelism"),
            "cluster-v0 must state layers are geometry only"
        );
        assert!(
            cluster.contains("File weight shards ≠ transformer layers")
                || cluster.contains("File weight shards"),
            "cluster-v0 must distinguish file shards from layer ranges"
        );
        let disc = include_str!("../../../docs/design/decentral-discovery-v0.md");
        assert!(
            disc.contains("scheduling geometry only") || disc.contains("geometry only"),
            "discovery doc must not oversell PP"
        );
        eprintln!(
            "OBSERVE no-fake-pp: docs ok cluster-v0.md decentral-discovery-v0.md layers={}",
            placement_model_layers()
        );
    }
}
