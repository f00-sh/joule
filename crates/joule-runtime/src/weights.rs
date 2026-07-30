//! On-disk weight cache / arming for agents.
//!
//! Until weights are published in the manifest, "prepare" only writes a marker
//! that this node is armed for the model once the pool is ready.

use crate::manifest::{ModelSpec, QuantSpec};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
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
        quant.files.iter().all(|f| dir.join(&f.path).is_file())
    }

    /// Prepare local cache for a quant. Downloads when files are listed + published;
    /// otherwise writes an arm marker only.
    pub fn prepare(&self, spec: &ModelSpec, quant: &QuantSpec) -> Result<PrepareStatus, String> {
        let dir = self.model_dir(&spec.id, &quant.id);
        fs::create_dir_all(&dir).map_err(|e| e.to_string())?;

        if !spec.weights.published || quant.files.is_empty() {
            let marker = self.armed_marker(&spec.id, &quant.id);
            let mut f = fs::File::create(&marker).map_err(|e| e.to_string())?;
            writeln!(
                f,
                "model={}\nquant={}\npublished={}\narmed_at_unix={}\nnote={}",
                spec.id,
                quant.id,
                spec.weights.published,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0),
                spec.weights.note
            )
            .map_err(|e| e.to_string())?;
            return Ok(PrepareStatus {
                model: spec.id.clone(),
                quant: quant.id.clone(),
                armed: true,
                files_complete: false,
                cache_dir: dir.display().to_string(),
                message: if spec.weights.published {
                    "armed (no files listed in manifest yet)".into()
                } else {
                    "pool gate passed — cache armed; weights not published (still stub inference)"
                        .into()
                },
            });
        }

        // Future: HTTP download + sha256 verify for each file.
        for file in &quant.files {
            let dest = dir.join(&file.path);
            if dest.is_file() {
                continue;
            }
            return Err(format!(
                "weight download not implemented yet for {} (would fetch {})",
                file.path, file.url
            ));
        }

        Ok(PrepareStatus {
            model: spec.id.clone(),
            quant: quant.id.clone(),
            armed: true,
            files_complete: true,
            cache_dir: dir.display().to_string(),
            message: "weights present on disk".into(),
        })
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
                "armed, awaiting published weights".into()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::ManifestFile;

    #[test]
    fn arm_without_download() {
        let dir = std::env::temp_dir().join(format!("joule-w-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let store = WeightsStore::new(&dir);
        let m = ManifestFile::load_default().unwrap();
        let spec = m.model("kimi-open").unwrap();
        let quant = spec.pick_quant(8192).unwrap();
        let st = store.prepare(spec, quant).unwrap();
        assert!(st.armed);
        assert!(!st.files_complete);
        assert!(store.is_armed("kimi-open", &quant.id));
        let _ = fs::remove_dir_all(&dir);
    }
}
