//! Actual model weight loading into process memory.
//!
//! Supports:
//! - **safetensors** shards (pure Rust via `safetensors` + `memmap2`)
//! - **raw binary** blobs listed in the quant file list
//! - **arm marker** only (no tensors yet)
//!
//! This is the load path for the logical device. When Kimi files are published
//! into the manifest, the same code loads them. Decode/generation still needs
//! architecture-specific kernels; loading means weights are **resident**.

use crate::manifest::{ModelSpec, QuantSpec};
use crate::weights::WeightsStore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TensorInfo {
    pub name: String,
    pub dtype: String,
    pub shape: Vec<usize>,
    pub nbytes: u64,
}

/// Weights loaded into the process for one quant.
#[derive(Debug)]
pub struct LoadedModel {
    pub model_id: String,
    pub quant: String,
    pub source_dir: PathBuf,
    /// Tensor name → raw bytes (host RAM).
    pub tensors: HashMap<String, Vec<u8>>,
    pub tensor_info: Vec<TensorInfo>,
    pub bytes_resident: u64,
    pub loaded_at_unix: u64,
    /// Basenames of weight files that contributed tensors (band gate).
    pub loaded_file_basenames: Vec<String>,
    /// Tensor name → source weight file basename (for band-scoped stage select).
    pub tensor_sources: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadReport {
    pub model: String,
    pub quant: String,
    pub tensors: usize,
    pub bytes_resident: u64,
    pub bytes_resident_gib: f64,
    pub source_dir: String,
    pub message: String,
}

#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("not prepared: {0}")]
    NotPrepared(String),
    #[error("io: {0}")]
    Io(String),
    #[error("format: {0}")]
    Format(String),
    #[error("pool/model gate: {0}")]
    Gate(String),
}

impl LoadedModel {
    pub fn report(&self) -> LoadReport {
        LoadReport {
            model: self.model_id.clone(),
            quant: self.quant.clone(),
            tensors: self.tensors.len(),
            bytes_resident: self.bytes_resident,
            bytes_resident_gib: self.bytes_resident as f64 / (1024.0 * 1024.0 * 1024.0),
            source_dir: self.source_dir.display().to_string(),
            message: format!(
                "loaded {} tensors ({:.2} GiB) for {}/{}",
                self.tensors.len(),
                self.bytes_resident as f64 / (1024.0 * 1024.0 * 1024.0),
                self.model_id,
                self.quant
            ),
        }
    }
}

/// Load model weights from the agent cache directory into RAM.
pub fn load_model(
    store: &WeightsStore,
    spec: &ModelSpec,
    quant: &QuantSpec,
) -> Result<LoadedModel, LoadError> {
    let dir = store.model_dir(&spec.id, &quant.id);
    if !dir.is_dir() {
        return Err(LoadError::NotPrepared(format!(
            "missing cache dir {}",
            dir.display()
        )));
    }

    let mut tensors = HashMap::new();
    let mut tensor_info = Vec::new();
    let mut bytes_resident = 0u64;
    let mut loaded_file_basenames = Vec::new();
    let mut tensor_sources = HashMap::new();

    // 1) Explicit manifest files (raw or safetensors by extension).
    for file in &quant.files {
        let path = dir.join(&file.path);
        if !path.is_file() {
            return Err(LoadError::NotPrepared(format!(
                "missing weight file {}",
                path.display()
            )));
        }
        let base = path_basename(&file.path);
        if file.path.ends_with(".safetensors") {
            let (t, info, n) = load_safetensors_file(&path)?;
            bytes_resident = bytes_resident.saturating_add(n);
            for (k, v) in t {
                tensor_sources.insert(k.clone(), base.clone());
                tensors.insert(k, v);
            }
            tensor_info.extend(info);
        } else {
            let data = fs::read(&path).map_err(|e| LoadError::Io(e.to_string()))?;
            let n = data.len() as u64;
            bytes_resident = bytes_resident.saturating_add(n);
            let name = file.path.clone();
            tensor_info.push(TensorInfo {
                name: name.clone(),
                dtype: "bytes".into(),
                shape: vec![data.len()],
                nbytes: n,
            });
            tensor_sources.insert(name.clone(), base.clone());
            tensors.insert(name, data);
        }
        loaded_file_basenames.push(base);
    }

    // 2) Auto-discover safetensors in the quant dir (published drop layout).
    if quant.files.is_empty() {
        if let Ok(rd) = fs::read_dir(&dir) {
            for ent in rd.flatten() {
                let path = ent.path();
                if path.extension().and_then(|e| e.to_str()) == Some("safetensors") {
                    let base = path
                        .file_name()
                        .and_then(|s| s.to_str())
                        .unwrap_or("unknown.safetensors")
                        .to_string();
                    let (t, info, n) = load_safetensors_file(&path)?;
                    bytes_resident = bytes_resident.saturating_add(n);
                    for (k, v) in t {
                        tensor_sources.insert(k.clone(), base.clone());
                        tensors.insert(k, v);
                    }
                    tensor_info.extend(info);
                    loaded_file_basenames.push(base);
                }
            }
        }
    }

    if tensors.is_empty() {
        // Armed-only: still "load" a zero-length resident marker so the pipeline is real.
        if store.is_armed(&spec.id, &quant.id) {
            let marker = b"JOULE_ARMED_NO_TENSORS".to_vec();
            bytes_resident = marker.len() as u64;
            tensor_info.push(TensorInfo {
                name: "__joule_armed__".into(),
                dtype: "marker".into(),
                shape: vec![marker.len()],
                nbytes: bytes_resident,
            });
            tensors.insert("__joule_armed__".into(), marker);
            tensor_sources.insert("__joule_armed__".into(), ".armed".into());
        } else {
            return Err(LoadError::NotPrepared(
                "no tensors on disk and cache not armed".into(),
            ));
        }
    }

    Ok(LoadedModel {
        model_id: spec.id.clone(),
        quant: quant.id.clone(),
        source_dir: dir,
        tensors,
        tensor_info,
        bytes_resident,
        loaded_at_unix: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        loaded_file_basenames,
        tensor_sources,
    })
}

/// Load only weight files required for layer band `[layer_start, layer_end]`.
///
/// Uses the file↔layer map + quant intersection (see
/// [`WeightsStore::required_weight_files_for_band`]). Fails closed if any
/// required file is missing from the quant list or disk.
pub fn load_model_for_band(
    store: &WeightsStore,
    spec: &ModelSpec,
    quant: &QuantSpec,
    layer_start: u32,
    layer_end: u32,
) -> Result<LoadedModel, LoadError> {
    store
        .band_files_ready(&spec.id, quant, layer_start, layer_end)
        .map_err(LoadError::NotPrepared)?;
    let required = WeightsStore::required_weight_files_for_band(quant, layer_start, layer_end)
        .map_err(LoadError::NotPrepared)?;
    let dir = store.model_dir(&spec.id, &quant.id);
    let mut tensors = HashMap::new();
    let mut tensor_info = Vec::new();
    let mut bytes_resident = 0u64;
    let mut loaded_file_basenames = Vec::new();
    let mut tensor_sources = HashMap::new();

    for base in &required {
        let file = quant
            .files
            .iter()
            .find(|f| path_basename(&f.path) == *base)
            .ok_or_else(|| {
                LoadError::NotPrepared(format!("band load: quant missing file {base}"))
            })?;
        let path = dir.join(&file.path);
        if !path.is_file() {
            return Err(LoadError::NotPrepared(format!(
                "band load: missing {}",
                path.display()
            )));
        }
        if file.path.ends_with(".safetensors") {
            let (t, info, n) = load_safetensors_file(&path)?;
            bytes_resident = bytes_resident.saturating_add(n);
            for (k, v) in t {
                tensor_sources.insert(k.clone(), base.clone());
                tensors.insert(k, v);
            }
            tensor_info.extend(info);
        } else {
            let data = fs::read(&path).map_err(|e| LoadError::Io(e.to_string()))?;
            let n = data.len() as u64;
            bytes_resident = bytes_resident.saturating_add(n);
            let name = file.path.clone();
            tensor_info.push(TensorInfo {
                name: name.clone(),
                dtype: "bytes".into(),
                shape: vec![data.len()],
                nbytes: n,
            });
            tensor_sources.insert(name.clone(), base.clone());
            tensors.insert(name, data);
        }
        loaded_file_basenames.push(base.clone());
    }

    if tensors.is_empty() && loaded_file_basenames.is_empty() {
        return Err(LoadError::NotPrepared(
            "band load: no weight files for layer band".into(),
        ));
    }

    Ok(LoadedModel {
        model_id: spec.id.clone(),
        quant: quant.id.clone(),
        source_dir: dir,
        tensors,
        tensor_info,
        bytes_resident,
        loaded_at_unix: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        loaded_file_basenames,
        tensor_sources,
    })
}

fn path_basename(path: &str) -> String {
    path.rsplit('/').next().unwrap_or(path).to_string()
}

type SafetensorsLoad = (HashMap<String, Vec<u8>>, Vec<TensorInfo>, u64);

fn load_safetensors_file(path: &Path) -> Result<SafetensorsLoad, LoadError> {
    let file = fs::File::open(path).map_err(|e| LoadError::Io(e.to_string()))?;
    let mmap = unsafe { memmap2::Mmap::map(&file) }.map_err(|e| LoadError::Io(e.to_string()))?;
    let st = safetensors::SafeTensors::deserialize(&mmap)
        .map_err(|e| LoadError::Format(e.to_string()))?;

    let mut tensors = HashMap::new();
    let mut info = Vec::new();
    let mut total = 0u64;

    for name in st.names() {
        let t = st
            .tensor(name)
            .map_err(|e| LoadError::Format(e.to_string()))?;
        let data = t.data().to_vec();
        let nbytes = data.len() as u64;
        total = total.saturating_add(nbytes);
        info.push(TensorInfo {
            name: name.to_string(),
            dtype: format!("{:?}", t.dtype()),
            shape: t.shape().to_vec(),
            nbytes,
        });
        tensors.insert(name.to_string(), data);
    }

    Ok((tensors, info, total))
}

/// Write a tiny safetensors fixture for tests / CI load path.
#[cfg(test)]
pub(crate) fn write_tiny_safetensors_fixture(path: &Path) -> Result<(), LoadError> {
    use safetensors::tensor::{serialize, TensorView};
    use safetensors::Dtype;
    use std::collections::BTreeMap;

    let data: Vec<u8> = vec![0u8; 64]; // 16 x f32
    let tensor = TensorView::new(Dtype::F32, vec![4, 4], &data)
        .map_err(|e| LoadError::Format(e.to_string()))?;
    let mut map: BTreeMap<String, TensorView<'_>> = BTreeMap::new();
    map.insert("demo.weight".into(), tensor);
    let bytes = serialize(&map, &None).map_err(|e| LoadError::Format(e.to_string()))?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| LoadError::Io(e.to_string()))?;
    }
    fs::write(path, bytes).map_err(|e| LoadError::Io(e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::ManifestFile;
    use crate::weights::WeightsStore;

    #[test]
    fn load_tiny_safetensors() {
        let dir = std::env::temp_dir().join(format!("joule-load-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let store = WeightsStore::new(&dir);
        let m = ManifestFile::load_default().unwrap();
        let spec = m.model("kimi-open").unwrap();
        let quant = spec
            .weights
            .quants
            .iter()
            .find(|q| q.id == "lab-tiny")
            .unwrap();
        store.prepare(spec, quant).unwrap();
        // Overwrite fixture with a known demo tensor name for this unit test.
        let st_path = store
            .model_dir(&spec.id, &quant.id)
            .join("model.safetensors");
        write_tiny_safetensors_fixture(&st_path).unwrap();
        let loaded = load_model(&store, spec, quant).unwrap();
        assert!(
            loaded.tensors.contains_key("demo.weight")
                || loaded.tensors.contains_key("tok_embeddings.weight"),
            "keys={:?}",
            loaded.tensors.keys().collect::<Vec<_>>()
        );
        assert!(loaded.bytes_resident >= 64);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_model_for_band_standin_and_fail_closed() {
        use crate::manifest::{QuantSpec, WeightFile};
        use sha2::{Digest, Sha256};
        use uuid::Uuid;

        let dir = std::env::temp_dir().join(format!("joule-band-load-{}", Uuid::new_v4()));
        let _ = fs::remove_dir_all(&dir);
        let store = WeightsStore::new(&dir);
        let m = ManifestFile::load_default().unwrap();
        let spec = m.model("kimi-open").unwrap();

        let model_dir = store.model_dir(&spec.id, "kimi-k3-band-test");
        fs::create_dir_all(&model_dir).unwrap();
        let f1 = model_dir.join("model-00001-of-00016.safetensors");
        let f2 = model_dir.join("model-00002-of-00016.safetensors");
        write_tiny_safetensors_fixture(&f1).unwrap();
        write_tiny_safetensors_fixture(&f2).unwrap();
        let h1 = {
            let mut hasher = Sha256::new();
            hasher.update(fs::read(&f1).unwrap());
            hex::encode(hasher.finalize())
        };
        let h2 = {
            let mut hasher = Sha256::new();
            hasher.update(fs::read(&f2).unwrap());
            hex::encode(hasher.finalize())
        };
        let quant = QuantSpec {
            id: "kimi-k3-band-test".into(),
            min_node_vram_mib: 256,
            approx_file_mib: 1,
            files: vec![
                WeightFile {
                    path: "model-00001-of-00016.safetensors".into(),
                    sha256: h1,
                    url: "peer://k3/1".into(),
                    size_bytes: fs::metadata(&f1).unwrap().len(),
                },
                WeightFile {
                    path: "model-00002-of-00016.safetensors".into(),
                    sha256: h2,
                    url: "peer://k3/2".into(),
                    size_bytes: fs::metadata(&f2).unwrap().len(),
                },
            ],
        };
        // Remove file 2 → band 6-11 fails; 0-5 (file 1) ready.
        fs::remove_file(&f2).unwrap();
        assert!(store.band_files_ready(&spec.id, &quant, 0, 5).is_ok());
        assert!(store.band_files_ready(&spec.id, &quant, 6, 11).is_err());
        let loaded = load_model_for_band(&store, spec, &quant, 0, 5).unwrap();
        assert_eq!(
            loaded.loaded_file_basenames,
            vec!["model-00001-of-00016.safetensors"]
        );
        assert!(load_model_for_band(&store, spec, &quant, 6, 11).is_err());
        eprintln!(
            "OBSERVE band-load: ready_0_5 basenames={:?} bytes={}",
            loaded.loaded_file_basenames, loaded.bytes_resident
        );
        let _ = fs::remove_dir_all(&dir);
    }
}
