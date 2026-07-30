//! Software update staging: peer-seeded binaries (never from f00 CDN).
//!
//! Flow:
//! 1. Operator signs `software_update` with target digests
//! 2. Agent fetches matching sha256 from swarm → blob store
//! 3. `stage_blob` copies verified bytes into software stage dir
//! 4. Operator/user restarts process with staged binary (`apply_staged` / CLI)
//!
//! See docs/design/distribution-v0.md and broadcast-v0.md.

use crate::weights::WeightsStore;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SoftwareTarget {
    pub os: String,
    pub arch: String,
    pub sha256: String,
    #[serde(default)]
    pub size: u64,
    #[serde(default = "default_name")]
    pub name: String,
}

fn default_name() -> String {
    "joule".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SoftwareUpdateBody {
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub targets: Vec<SoftwareTarget>,
    #[serde(default)]
    pub notes: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageStatus {
    pub staged: bool,
    pub version: String,
    pub sha256: String,
    pub path: String,
    pub message: String,
}

pub fn software_root() -> PathBuf {
    if let Ok(p) = env::var("JOULE_SOFTWARE_DIR") {
        return PathBuf::from(p);
    }
    env::var_os("HOME")
        .map(|h| PathBuf::from(h).join(".local/share/joule/software"))
        .unwrap_or_else(|| PathBuf::from("./.joule-software"))
}

pub fn stage_dir() -> PathBuf {
    software_root().join("stage")
}

pub fn current_os() -> &'static str {
    env::consts::OS
}

pub fn current_arch() -> &'static str {
    env::consts::ARCH
}

/// Pick target matching this host (os + arch, case-insensitive).
pub fn match_target(body: &SoftwareUpdateBody) -> Option<&SoftwareTarget> {
    let os = current_os();
    let arch = current_arch();
    body.targets.iter().find(|t| {
        t.os.eq_ignore_ascii_case(os)
            && (t.arch.eq_ignore_ascii_case(arch)
                || (arch == "x86_64" && t.arch.eq_ignore_ascii_case("amd64"))
                || (arch == "aarch64" && t.arch.eq_ignore_ascii_case("arm64")))
    })
}

pub fn parse_software_update(body_json: &str) -> Result<SoftwareUpdateBody, String> {
    serde_json::from_str(body_json).map_err(|e| format!("software_update json: {e}"))
}

/// Stage a verified blob as the next binary for this host.
pub fn stage_blob(version: &str, target: &SoftwareTarget) -> Result<StageStatus, String> {
    let hash = target.sha256.to_lowercase();
    let data = WeightsStore::read_blob(&hash)?;
    if target.size > 0 && data.len() as u64 != target.size {
        return Err(format!(
            "size mismatch: blob {} bytes, target listed {}",
            data.len(),
            target.size
        ));
    }
    let dir = stage_dir();
    fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    let bin_name = if target.name.is_empty() {
        "joule".into()
    } else {
        target.name.clone()
    };
    let dest = dir.join(&bin_name);
    let tmp = dest.with_extension("partial");
    fs::write(&tmp, &data).map_err(|e| e.to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(&tmp, fs::Permissions::from_mode(0o755));
    }
    fs::rename(&tmp, &dest).map_err(|e| e.to_string())?;

    let meta = StageMeta {
        version: version.to_string(),
        sha256: hash.clone(),
        name: bin_name,
        os: target.os.clone(),
        arch: target.arch.clone(),
        staged_at_unix: unix_now(),
        size: data.len() as u64,
    };
    let meta_path = dir.join("stage.json");
    let mut f = fs::File::create(&meta_path).map_err(|e| e.to_string())?;
    writeln!(
        f,
        "{}",
        serde_json::to_string_pretty(&meta).map_err(|e| e.to_string())?
    )
    .map_err(|e| e.to_string())?;

    Ok(StageStatus {
        staged: true,
        version: version.to_string(),
        sha256: hash,
        path: dest.display().to_string(),
        message: format!(
            "staged {} v{} — restart with `joule software apply` (or replace binary manually)",
            meta.name, version
        ),
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StageMeta {
    version: String,
    sha256: String,
    name: String,
    os: String,
    arch: String,
    staged_at_unix: u64,
    size: u64,
}

pub fn read_stage() -> Option<StageStatus> {
    let dir = stage_dir();
    let meta_path = dir.join("stage.json");
    let raw = fs::read_to_string(meta_path).ok()?;
    let meta: StageMeta = serde_json::from_str(&raw).ok()?;
    let path = dir.join(&meta.name);
    Some(StageStatus {
        staged: path.is_file(),
        version: meta.version,
        sha256: meta.sha256,
        path: path.display().to_string(),
        message: if path.is_file() {
            "staged binary ready to apply".into()
        } else {
            "stage metadata present but binary missing".into()
        },
    })
}

/// Copy staged binary over `dest` (usually current exe). Atomic-ish via temp + rename.
pub fn apply_staged(dest: &Path) -> Result<StageStatus, String> {
    let st = read_stage().ok_or_else(|| "nothing staged".to_string())?;
    if !st.staged {
        return Err(st.message);
    }
    let src = PathBuf::from(&st.path);
    if !src.is_file() {
        return Err("staged binary missing".into());
    }
    // Re-verify hash before apply.
    let data = fs::read(&src).map_err(|e| e.to_string())?;
    let got = {
        use sha2::{Digest, Sha256};
        hex::encode(Sha256::digest(&data))
    };
    if got != st.sha256.to_lowercase() {
        return Err(format!("staged hash mismatch: want {}, got {got}", st.sha256));
    }
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let tmp = dest.with_extension("joule-new");
    fs::write(&tmp, &data).map_err(|e| e.to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(&tmp, fs::Permissions::from_mode(0o755));
    }
    // On Windows rename over existing may fail; remove first best-effort.
    let _ = fs::remove_file(dest);
    fs::rename(&tmp, dest).map_err(|e| e.to_string())?;
    Ok(StageStatus {
        staged: true,
        version: st.version.clone(),
        sha256: st.sha256,
        path: dest.display().to_string(),
        message: format!("applied staged binary v{} → {}", st.version, dest.display()),
    })
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    #[test]
    fn stage_roundtrip() {
        let _env = crate::weights::test_env::lock();
        let dir = std::env::temp_dir().join(format!("joule-sw-{}", std::process::id()));
        let blobs = dir.join("blobs");
        let soft = dir.join("soft");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&blobs).unwrap();
        fs::create_dir_all(&soft).unwrap();
        std::env::set_var("JOULE_BLOBS_DIR", &blobs);
        std::env::set_var("JOULE_SOFTWARE_DIR", &soft);

        let payload = b"#!/bin/sh\necho joule-test\n";
        let hash = hex::encode(Sha256::digest(payload));
        WeightsStore::store_blob(&hash, payload).unwrap();
        let target = SoftwareTarget {
            os: current_os().into(),
            arch: current_arch().into(),
            sha256: hash.clone(),
            size: payload.len() as u64,
            name: "joule".into(),
        };
        let body = SoftwareUpdateBody {
            version: "0.0.1-test".into(),
            targets: vec![target.clone()],
            notes: "unit".into(),
        };
        assert!(match_target(&body).is_some());
        let st = stage_blob("0.0.1-test", &target).unwrap();
        assert!(st.staged);
        assert!(Path::new(&st.path).is_file());
        let apply_to = soft.join("installed-joule");
        let applied = apply_staged(&apply_to).unwrap();
        assert!(applied.message.contains("applied"));
        assert_eq!(fs::read(&apply_to).unwrap(), payload);

        std::env::remove_var("JOULE_BLOBS_DIR");
        std::env::remove_var("JOULE_SOFTWARE_DIR");
        let _ = fs::remove_dir_all(&dir);
    }
}
