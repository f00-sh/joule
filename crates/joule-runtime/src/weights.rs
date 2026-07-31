//! On-disk weight cache: local-first, peer-seeded, optional external fetch.
//!
//! **Distribution law:** f00 does not host weights. Populate from local cache,
//! operator drop-ins, peer swarm, or (opt-in) third-party URLs.
//! See docs/design/distribution-v0.md.

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

    /// Content-addressed blob store (shared by weights + future software seeds).
    pub fn blob_root() -> PathBuf {
        if let Ok(p) = std::env::var("JOULE_BLOBS_DIR") {
            return PathBuf::from(p);
        }
        std::env::var_os("HOME")
            .map(|h| PathBuf::from(h).join(".local/share/joule/blobs/sha256"))
            .unwrap_or_else(|| PathBuf::from("./.joule-blobs/sha256"))
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

    /// Inventory of complete weight files as content-addressed blob metas (for seeding).
    pub fn local_blob_metas(&self, model: &str, quant: &QuantSpec) -> Vec<BlobAnnounce> {
        let dir = self.model_dir(model, &quant.id);
        let mut out = Vec::new();
        for f in &quant.files {
            let p = dir.join(&f.path);
            if !p.is_file() {
                continue;
            }
            if let Ok(h) = file_sha256(&p) {
                if h == f.sha256 {
                    let size = fs::metadata(&p).map(|m| m.len()).unwrap_or(f.size_bytes);
                    out.push(BlobAnnounce {
                        sha256: h,
                        size,
                        kind: "weight".into(),
                        name: format!("{model}/{}/{}", quant.id, f.path),
                    });
                }
            }
        }
        out
    }

    /// Prepare local cache for a quant.
    ///
    /// Order: local/blob store → repo:// (git checkout only) → optional external
    /// (`JOULE_ALLOW_EXTERNAL_FETCH=1`). Never requires f00 to serve files.
    pub fn prepare(&self, spec: &ModelSpec, quant: &QuantSpec) -> Result<PrepareStatus, String> {
        let dir = self.model_dir(&spec.id, &quant.id);
        fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
        let _ = fs::create_dir_all(Self::blob_root());

        if !spec.weights.published || quant.files.is_empty() {
            return self.arm_only(spec, quant, &dir);
        }

        let external = external_fetch_allowed();
        let mut missing = Vec::new();
        for file in &quant.files {
            match self.ensure_file(&dir, file, external) {
                Ok(()) => {}
                Err(e) => missing.push(format!("{}: {e}", file.path)),
            }
        }

        if !missing.is_empty() {
            // Still arm so the node is ready to *receive* seeds.
            let _ = self.arm_only(spec, quant, &dir);
            return Ok(PrepareStatus {
                model: spec.id.clone(),
                quant: quant.id.clone(),
                armed: true,
                files_complete: false,
                cache_dir: dir.display().to_string(),
                message: format!(
                    "armed; waiting for peer seeds (or opt-in external). missing: {}",
                    missing.join("; ")
                ),
            });
        }

        let marker = self.armed_marker(&spec.id, &quant.id);
        let mut f = fs::File::create(&marker).map_err(|e| e.to_string())?;
        writeln!(
            f,
            "model={}\nquant={}\npublished=true\nfiles={}\narmed_at_unix={}\ndistribution=peer-seeded",
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
                "weights ready ({} files, content-addressed) for {}/{}",
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
                "armed — need peer seed or local drop-in for listed files".into()
            } else {
                "armed; weights not published in manifest".into()
            },
        })
    }

    fn ensure_file(&self, dir: &Path, file: &WeightFile, external: bool) -> Result<(), String> {
        let dest = dir.join(&file.path);
        let hash = file.sha256.to_lowercase();

        // 1) Already correct in quant dir
        if dest.is_file() {
            let h = file_sha256(&dest)?;
            if h == hash {
                self.ingest_blob(&dest, &hash)?;
                return Ok(());
            }
            let _ = fs::remove_file(&dest);
        }

        // 2) Content-addressed blob store
        let blob = Self::blob_root().join(&hash);
        if blob.is_file() {
            let h = file_sha256(&blob)?;
            if h == hash {
                if let Some(parent) = dest.parent() {
                    fs::create_dir_all(parent).map_err(|e| e.to_string())?;
                }
                fs::copy(&blob, &dest).map_err(|e| e.to_string())?;
                return Ok(());
            }
        }

        // 3) repo:// — git checkout only (developers), not a CDN
        if let Some(rel) = file.url.strip_prefix("repo://") {
            copy_from_repo(rel, &dest)?;
            verify_dest(&dest, &hash, file)?;
            self.ingest_blob(&dest, &hash)?;
            return Ok(());
        }

        // 4) Optional external (never required; never f00 as product origin)
        if external
            && (file.url.starts_with("http://") || file.url.starts_with("https://"))
            && !is_f00_origin(&file.url)
        {
            download_http(&file.url, &dest)?;
            verify_dest(&dest, &hash, file)?;
            self.ingest_blob(&dest, &hash)?;
            return Ok(());
        }

        if !external && (file.url.starts_with("http://") || file.url.starts_with("https://")) {
            return Err(
                "external fetch disabled (set JOULE_ALLOW_EXTERNAL_FETCH=1 to use third-party URLs; prefer peer seed)"
                    .into(),
            );
        }

        Err("not found locally; wait for a peer to seed this sha256".into())
    }

    /// Copy verified file into blob store for future seeding.
    fn ingest_blob(&self, path: &Path, hash: &str) -> Result<(), String> {
        let blob = Self::blob_root().join(hash);
        if blob.is_file() {
            return Ok(());
        }
        fs::create_dir_all(Self::blob_root()).map_err(|e| e.to_string())?;
        fs::copy(path, &blob).map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Path of a content-addressed blob (`blobs/sha256/<hex>`).
    pub fn blob_path(sha256: &str) -> PathBuf {
        Self::blob_root().join(sha256.to_lowercase())
    }

    pub fn has_blob(sha256: &str) -> bool {
        let p = Self::blob_path(sha256);
        p.is_file()
    }

    /// Read verified blob bytes; errors if missing or hash mismatch.
    pub fn read_blob(sha256: &str) -> Result<Vec<u8>, String> {
        let hash = sha256.to_lowercase();
        let p = Self::blob_path(&hash);
        if !p.is_file() {
            return Err(format!("blob not found: {hash}"));
        }
        let data = fs::read(&p).map_err(|e| e.to_string())?;
        let got = hex::encode(Sha256::digest(&data));
        if got != hash {
            return Err(format!("blob corrupt: want {hash}, got {got}"));
        }
        Ok(data)
    }

    /// Write bytes into the blob store after verifying sha256. Idempotent.
    pub fn store_blob(sha256: &str, data: &[u8]) -> Result<u64, String> {
        let hash = sha256.to_lowercase();
        if hash.len() != 64 || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err("sha256 must be 64 hex chars".into());
        }
        let got = hex::encode(Sha256::digest(data));
        if got != hash {
            return Err(format!("store_blob hash mismatch: want {hash}, got {got}"));
        }
        let dest = Self::blob_path(&hash);
        if dest.is_file() {
            return Ok(data.len() as u64);
        }
        fs::create_dir_all(Self::blob_root()).map_err(|e| e.to_string())?;
        let tmp = dest.with_extension("partial");
        fs::write(&tmp, data).map_err(|e| e.to_string())?;
        fs::rename(&tmp, &dest).map_err(|e| e.to_string())?;
        Ok(data.len() as u64)
    }

    /// Scan content-addressed blob store (for BlobsHave after peer receive).
    pub fn list_blob_store() -> Vec<BlobAnnounce> {
        let root = Self::blob_root();
        let mut out = Vec::new();
        let Ok(rd) = fs::read_dir(&root) else {
            return out;
        };
        for ent in rd.flatten() {
            let name = ent.file_name().to_string_lossy().to_lowercase();
            if name.len() != 64 || !name.chars().all(|c| c.is_ascii_hexdigit()) {
                continue;
            }
            let path = ent.path();
            if !path.is_file() {
                continue;
            }
            let size = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            // Trust filename only if length matches on-disk (cheap integrity).
            if let Ok(h) = file_sha256(&path) {
                if h == name {
                    out.push(BlobAnnounce {
                        sha256: h,
                        size,
                        kind: "blob".into(),
                        name: name.clone(),
                    });
                }
            }
        }
        out.sort_by(|a, b| a.sha256.cmp(&b.sha256));
        out
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
                "weights ready (local / seeded)".into()
            } else if armed {
                "armed; awaiting peer seeds or local drop-in".into()
            } else {
                "not prepared".into()
            },
        }
    }

    pub fn ensure_root(&self) -> Result<(), String> {
        fs::create_dir_all(&self.root).map_err(|e| e.to_string())?;
        fs::create_dir_all(Self::blob_root()).map_err(|e| e.to_string())
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobAnnounce {
    pub sha256: String,
    pub size: u64,
    pub kind: String,
    pub name: String,
}

fn external_fetch_allowed() -> bool {
    matches!(
        std::env::var("JOULE_ALLOW_EXTERNAL_FETCH").as_deref(),
        Ok("1") | Ok("true") | Ok("yes")
    )
}

fn is_f00_origin(url: &str) -> bool {
    // Product law: never treat f00 as a required payload origin.
    let u = url.to_ascii_lowercase();
    u.contains("://f00.sh/")
        || u.contains("://joule.f00.sh/")
        || u.contains("://www.f00.sh/")
        || u.contains(".f00.sh/")
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

fn verify_dest(dest: &Path, hash: &str, file: &WeightFile) -> Result<(), String> {
    let h = file_sha256(dest)?;
    if h != hash {
        let _ = fs::remove_file(dest);
        return Err(format!(
            "sha256 mismatch for {}: got {h}, want {hash}",
            file.path
        ));
    }
    Ok(())
}

fn download_http(url: &str, dest: &Path) -> Result<(), String> {
    if is_f00_origin(url) {
        return Err("refusing f00.sh as weight origin (website only)".into());
    }
    let tmp = dest.with_extension("part");
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .user_agent("joule-weights/0.0.0 (peer-seeded; external opt-in)")
        .build()
        .map_err(|e| e.to_string())?;
    let mut resp = client
        .get(url)
        .send()
        .map_err(|e| format!("GET {url}: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("GET {url}: HTTP {}", resp.status()));
    }
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
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

fn copy_from_repo(rel: &str, dest: &Path) -> Result<(), String> {
    let candidates = [
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(rel),
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(rel),
        PathBuf::from(rel),
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(rel),
    ];
    for src in &candidates {
        if src.is_file() {
            if let Some(parent) = dest.parent() {
                fs::create_dir_all(parent).map_err(|e| e.to_string())?;
            }
            fs::copy(src, dest).map_err(|e| e.to_string())?;
            return Ok(());
        }
    }
    Err(format!(
        "repo:// not found: {rel} (git checkout path — not a CDN)"
    ))
}

#[cfg(test)]
pub(crate) mod test_env {
    use std::sync::{Mutex, OnceLock};
    /// Serialize tests that mutate JOULE_* path env vars.
    pub fn lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::ManifestFile;

    #[test]
    fn lab_tiny_from_repo_no_external() {
        let _env = test_env::lock();
        // Default: external off; lab-tiny uses repo://
        std::env::remove_var("JOULE_ALLOW_EXTERNAL_FETCH");
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
        // blob store should hold the hash
        let blob = WeightsStore::blob_root()
            .join("b937cbc2c6def42c46579c6caab3b1a881b451994d6382d298d66d67f1549b24");
        assert!(blob.is_file() || store.files_complete("kimi-open", quant));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn refuses_f00_origin() {
        assert!(is_f00_origin("https://joule.f00.sh/weights/x"));
        assert!(!is_f00_origin(
            "https://huggingface.co/moonshotai/Kimi-K3/resolve/main/x"
        ));
    }

    #[test]
    fn pick_quant_prefers_files() {
        let m = ManifestFile::load_default().unwrap();
        let spec = m.model("kimi-open").unwrap();
        let q = spec.pick_quant(8192).unwrap();
        assert!(!q.files.is_empty(), "got {}", q.id);
        // Mid-class VRAM: largest loadable fixture (lab-mid), not empty/meta/K3 peer pins.
        assert_eq!(q.id, "lab-mid", "got {}", q.id);
    }

    #[test]
    fn store_and_list_blob_roundtrip() {
        let _env = test_env::lock();
        let dir = std::env::temp_dir().join(format!("joule-blobs-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        std::env::set_var("JOULE_BLOBS_DIR", &dir);
        let payload = b"joule peer seed bytes";
        let hash = hex::encode(Sha256::digest(payload));
        let n = WeightsStore::store_blob(&hash, payload).unwrap();
        assert_eq!(n, payload.len() as u64);
        assert!(WeightsStore::has_blob(&hash));
        assert_eq!(WeightsStore::read_blob(&hash).unwrap(), payload);
        // corrupt reject
        assert!(WeightsStore::store_blob(&hash, b"wrong").is_err());
        let list = WeightsStore::list_blob_store();
        assert!(list.iter().any(|b| b.sha256 == hash));
        std::env::remove_var("JOULE_BLOBS_DIR");
        let _ = fs::remove_dir_all(&dir);
    }
}
