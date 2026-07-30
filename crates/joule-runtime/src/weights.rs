//! On-disk weight cache: arm, HTTP(S) download, sha256 verify, repo fixtures.

use crate::manifest::{ModelSpec, QuantSpec, WeightFile};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareStatus {
    pub model: String,
    pub quant: String,
    pub armed: bool,
    pub files_complete: bool,
    pub cache_dir: String,
    pub message: String,
}

pub struct WeightsStore {
    root: PathBuf,
}

impl WeightsStore {
    pub fn default_root() -> PathBuf {
        if let Ok(p) = std::env::var("JOULE_WEIGHTS_DIR") {
            return PathBuf::from(p);
        }
        std::env::var_os("HOME")
            .map(|h| PathBuf::from(h).join(".local/share/joule/weights"))
            .unwrap_or_else(|| PathBuf::from("./.joule-weights"))
    }

    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn model_dir(&self, model: &str, quant: &str) -> PathBuf {
        self.root.join(model).join(quant)
    }

    pub fn armed_marker(&self, model: &str, quant: &str) -> PathBuf {
        self.model_dir(model, quant).join(".armed")
    }

    pub fn is_armed(&self, model: &str, quant: &str) -> bool {
        self.armed_marker(model, quant).is_file()
    }

    pub fn files_complete(&self, model: &str, quant: &QuantSpec) -> bool {
        if quant.files.is_empty() {
            return self.is_armed(model, &quant.id);
        }
        let dir = self.model_dir(model, &quant.id);
        quant.files.iter().all(|f| {
            let p = dir.join(&f.path);
            p.is_file() && file_sha256(&p).ok().as_deref() == Some(f.sha256.as_str())
        })
    }

    /// Prepare local cache for a quant.
    /// - unpublished / empty files → arm marker only
    /// - published + files → download (or copy `repo://`) + sha256 verify
    pub fn prepare(&self, spec: &ModelSpec, quant: &QuantSpec) -> Result<PrepareStatus, String> {
        let dir = self.model_dir(&spec.id, &quant.id);
        fs::create_dir_all(&dir).map_err(|e| e.to_string())?;

        if !spec.weights.published || quant.files.is_empty() {
            return self.arm_only(spec, quant, &dir);
        }

        for file in &quant.files {
            self.ensure_file(&dir, file)?;
        }

        // Arm after successful verify
        let marker = self.armed_marker(&spec.id, &quant.id);
        let mut f = fs::File::create(&marker).map_err(|e| e.to_string())?;
        writeln!(
            f,
            "model={}\nquant={}\npublished=true\nfiles={}\narmed_at_unix={}",
            spec.id,
            quant.id,
            quant.files.len(),
            unix_now(),
        )
        .map_err(|e| e.to_string())?;

        Ok(PrepareStatus {
            model: spec.id.clone(),
            quant: quant.id.clone(),
            armed: true,
            files_complete: true,
            cache_dir: dir.display().to_string(),
            message: format!(
                "weights ready ({} files verified) for {}/{}",
                quant.files.len(),
                spec.id,
                quant.id
            ),
        })
    }

    fn arm_only(
        &self,
        spec: &ModelSpec,
        quant: &QuantSpec,
        dir: &Path,
    ) -> Result<PrepareStatus, String> {
        let marker = self.armed_marker(&spec.id, &quant.id);
        let mut f = fs::File::create(&marker).map_err(|e| e.to_string())?;
        writeln!(
            f,
            "model={}\nquant={}\npublished={}\narmed_at_unix={}\nnote={}",
            spec.id,
            quant.id,
            spec.weights.published,
            unix_now(),
            spec.weights.note
        )
        .map_err(|e| e.to_string())?;
        Ok(PrepareStatus {
            model: spec.id.clone(),
            quant: quant.id.clone(),
            armed: true,
            files_complete: false,
            cache_dir: dir.display().to_string(),
            message: if spec.weights.published {
                "armed (no files listed for this quant — stub until shards pinned)".into()
            } else {
                "pool gate passed — cache armed; weights not published (still stub inference)"
                    .into()
            },
        })
    }

    fn ensure_file(&self, dir: &Path, file: &WeightFile) -> Result<(), String> {
        let dest = dir.join(&file.path);
        if dest.is_file() {
            let h = file_sha256(&dest)?;
            if h == file.sha256 {
                return Ok(());
            }
            // Corrupt / wrong version — re-fetch
            let _ = fs::remove_file(&dest);
        }
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }

        if let Some(rel) = file.url.strip_prefix("repo://") {
            copy_from_repo(rel, &dest)?;
        } else if file.url.starts_with("http://") || file.url.starts_with("https://") {
            download_http(&file.url, &dest)?;
        } else {
            return Err(format!("unsupported weight url scheme: {}", file.url));
        }

        let h = file_sha256(&dest)?;
        if h != file.sha256 {
            let _ = fs::remove_file(&dest);
            return Err(format!(
                "sha256 mismatch for {}: got {h}, want {}",
                file.path, file.sha256
            ));
        }
        if file.size_bytes > 0 {
            let meta = fs::metadata(&dest).map_err(|e| e.to_string())?;
            if meta.len() != file.size_bytes {
                // size is advisory if sha matches — warn only
                tracing_log(&format!(
                    "size note for {}: {} bytes (manifest {})",
                    file.path,
                    meta.len(),
                    file.size_bytes
                ));
            }
        }
        Ok(())
    }

    pub fn status(&self, spec: &ModelSpec, quant: &QuantSpec) -> PrepareStatus {
        let dir = self.model_dir(&spec.id, &quant.id);
        let armed = self.is_armed(&spec.id, &quant.id);
        let files_complete = self.files_complete(&spec.id, quant);
        PrepareStatus {
            model: spec.id.clone(),
            quant: quant.id.clone(),
            armed,
            files_complete,
            cache_dir: dir.display().to_string(),
            message: if files_complete {
                "weights ready".into()
            } else if armed {
                "armed, awaiting published weight files".into()
            } else {
                "not prepared".into()
            },
        }
    }

    pub fn ensure_root(&self) -> Result<(), String> {
        fs::create_dir_all(&self.root).map_err(|e| e.to_string())
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn file_sha256(path: &Path) -> Result<String, String> {
    let mut f = fs::File::open(path).map_err(|e| e.to_string())?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = f.read(&mut buf).map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn download_http(url: &str, dest: &Path) -> Result<(), String> {
    let tmp = dest.with_extension("part");
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .user_agent("joule-weights/0.0.0")
        .build()
        .map_err(|e| e.to_string())?;
    let mut resp = client
        .get(url)
        .send()
        .map_err(|e| format!("GET {url}: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("GET {url}: HTTP {}", resp.status()));
    }
    let mut out = fs::File::create(&tmp).map_err(|e| e.to_string())?;
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = resp.read(&mut buf).map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        out.write_all(&buf[..n]).map_err(|e| e.to_string())?;
    }
    out.flush().map_err(|e| e.to_string())?;
    fs::rename(&tmp, dest).map_err(|e| e.to_string())?;
    Ok(())
}

/// Resolve `repo://path` against workspace root (CARGO_MANIFEST_DIR/../../ or cwd).
fn copy_from_repo(rel: &str, dest: &Path) -> Result<(), String> {
    let candidates = [
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join(rel),
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(rel),
        PathBuf::from(rel),
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(rel),
    ];
    for src in &candidates {
        if src.is_file() {
            fs::copy(src, dest).map_err(|e| e.to_string())?;
            return Ok(());
        }
    }
    Err(format!(
        "repo:// file not found: {rel} (looked relative to crate/workspace/cwd)"
    ))
}

fn tracing_log(msg: &str) {
    let _ = msg;
    // keep pure — optional tracing later
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::ManifestFile;

    #[test]
    fn arm_or_download_lab_tiny() {
        let dir = std::env::temp_dir().join(format!("joule-w-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let store = WeightsStore::new(&dir);
        let m = ManifestFile::load_default().unwrap();
        let spec = m.model("kimi-open").unwrap();
        let quant = spec
            .weights
            .quants
            .iter()
            .find(|q| q.id == "lab-tiny")
            .expect("lab-tiny");
        let st = store.prepare(spec, quant).unwrap();
        assert!(st.armed);
        assert!(st.files_complete, "{}", st.message);
        assert!(store.files_complete("kimi-open", quant));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn pick_quant_prefers_files() {
        let m = ManifestFile::load_default().unwrap();
        let spec = m.model("kimi-open").unwrap();
        let q = spec.pick_quant(8192).unwrap();
        assert!(!q.files.is_empty(), "should pick quant with files, got {}", q.id);
    }
}
