//! Executable Kimi-K3 file index ↔ transformer layer map (design k3-file-layer-map-v0).
//!
//! Production pin: **96** weight files (`model-NNNNN-of-000096.safetensors`) from
//! moonshotai/Kimi-K3, **93** transformer layers. Pure math — no I/O.
//!
//! Files 1..=93 map 1:1 to layers 0..=92. Files 94..=96 are residual/global shards
//! (embeddings / lm_head / multimodal packing) preferred for every band.

/// Design constants from offline K3 meta pin + MANIFEST shard count.
pub const K3_MODEL_LAYERS: u32 = 93;
pub const K3_FILE_COUNT: u32 = 96;

/// Inclusive layer range for a 1-based file index (1..=96).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LayerRange {
    pub start: u32,
    pub end: u32,
}

impl LayerRange {
    pub fn contains(&self, layer: u32) -> bool {
        layer >= self.start && layer <= self.end
    }

    pub fn intersects(&self, other: &LayerRange) -> bool {
        self.start <= other.end && other.start <= self.end
    }

    pub fn span(&self) -> u32 {
        self.end.saturating_sub(self.start).saturating_add(1)
    }
}

/// Layers covered by file index `i` (1-based). Err if out of range.
///
/// - Files `1..=93`: single layer `i-1`
/// - Files `94..=96`: residual global weights attached to last layer for fetch preference
pub fn layers_for_file(file_index_1based: u32) -> Result<LayerRange, String> {
    if file_index_1based == 0 || file_index_1based > K3_FILE_COUNT {
        return Err(format!(
            "file index {file_index_1based} out of 1..={K3_FILE_COUNT}"
        ));
    }
    if file_index_1based <= K3_MODEL_LAYERS {
        let l = file_index_1based - 1;
        Ok(LayerRange { start: l, end: l })
    } else {
        Ok(LayerRange {
            start: K3_MODEL_LAYERS - 1,
            end: K3_MODEL_LAYERS - 1,
        })
    }
}

/// True when this file is residual/global (always preferred for any band).
pub fn is_global_weight_file(file_index_1based: u32) -> bool {
    file_index_1based > K3_MODEL_LAYERS && file_index_1based <= K3_FILE_COUNT
}

/// 1-based file indices whose layer ranges intersect `[layer_start, layer_end]`.
/// Residual files 94..=96 are always included (global weights).
pub fn files_intersecting_layers(layer_start: u32, layer_end: u32) -> Result<Vec<u32>, String> {
    if layer_end < layer_start {
        return Err("layer_end < layer_start".into());
    }
    if layer_end >= K3_MODEL_LAYERS {
        return Err(format!("layer_end {layer_end} >= {K3_MODEL_LAYERS}"));
    }
    let want = LayerRange {
        start: layer_start,
        end: layer_end,
    };
    let mut out = Vec::new();
    for i in 1..=K3_FILE_COUNT {
        if is_global_weight_file(i) {
            out.push(i);
            continue;
        }
        let r = layers_for_file(i)?;
        if r.intersects(&want) {
            out.push(i);
        }
    }
    Ok(out)
}

/// Weight file basenames preferred for a donor owning layer band `[Ls, Le]`.
pub fn preferred_weight_files(layer_start: u32, layer_end: u32) -> Result<Vec<String>, String> {
    let idx = files_intersecting_layers(layer_start, layer_end)?;
    Ok(idx
        .into_iter()
        // HF Kimi-K3 names: model-00001-of-000096.safetensors (5-digit index, 6-digit total).
        .map(|i| format!("model-{i:05}-of-{K3_FILE_COUNT:06}.safetensors"))
        .collect())
}

/// Order `(path, sha256)` pairs for fetch: paths intersecting `[Ls,Le]` first.
pub fn order_digests_for_layer_fetch(
    layer_start: u32,
    layer_end: u32,
    path_and_digest: &[(String, String)],
) -> Result<Vec<String>, String> {
    let preferred = preferred_weight_files(layer_start, layer_end)?;
    let mut preferred_digests = Vec::new();
    let mut rest = Vec::new();
    for (path, digest) in path_and_digest {
        let base = path.rsplit('/').next().unwrap_or(path.as_str());
        if preferred.iter().any(|p| p == base || path.ends_with(p)) {
            preferred_digests.push(digest.to_lowercase());
        } else {
            rest.push(digest.to_lowercase());
        }
    }
    preferred_digests.extend(rest);
    let mut seen = std::collections::HashSet::new();
    preferred_digests.retain(|d| seen.insert(d.clone()));
    Ok(preferred_digests)
}

/// Infer K3 layer range from a weight file basename (`model-NNNNN-of-000096.safetensors`).
pub fn layers_for_weight_path(path: &str) -> Option<LayerRange> {
    let base = path.rsplit('/').next().unwrap_or(path);
    let rest = base.strip_prefix("model-")?;
    let idx_str = rest.split('-').next()?;
    let idx: u32 = idx_str.parse().ok()?;
    layers_for_file(idx).ok()
}

/// Layer files 1..=93 cover 0..92 exactly once; residual 94..=96 may re-touch last layer.
pub fn map_covers_all_layers() -> bool {
    let mut covered = [false; K3_MODEL_LAYERS as usize];
    for i in 1..=K3_MODEL_LAYERS {
        let Ok(r) = layers_for_file(i) else {
            return false;
        };
        for l in r.start..=r.end {
            if l >= K3_MODEL_LAYERS {
                return false;
            }
            if covered[l as usize] {
                return false;
            }
            covered[l as usize] = true;
        }
    }
    covered.iter().all(|&c| c)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn design_table_file_1_and_93_and_residuals() {
        let f1 = layers_for_file(1).unwrap();
        assert_eq!(f1, LayerRange { start: 0, end: 0 });
        let f93 = layers_for_file(93).unwrap();
        assert_eq!(f93, LayerRange { start: 92, end: 92 });
        assert!(map_covers_all_layers());
        assert!(is_global_weight_file(96));
        assert!(!is_global_weight_file(1));
        assert_eq!(K3_FILE_COUNT, 96);
        let mut total = 0u32;
        for i in 1..=93 {
            total += layers_for_file(i).unwrap().span();
        }
        assert_eq!(total, 93);
        eprintln!("OBSERVE file-layer-map: files=96 layers=93 residual=94..96");
    }

    #[test]
    fn band_40_50_intersects_expected_files() {
        let files = files_intersecting_layers(40, 50).unwrap();
        // layer files 41..=51 (1-based) + residual 94..96
        assert!(files.contains(&41)); // layer 40
        assert!(files.contains(&51)); // layer 50
        assert!(files.contains(&96));
        let prefs = preferred_weight_files(40, 50).unwrap();
        assert!(prefs.iter().any(|p| p.contains("00041")));
        assert!(prefs.iter().any(|p| p.contains("00096")));
        eprintln!(
            "OBSERVE file-layer-map: layers 40-50 → n={} prefs_head={}",
            files.len(),
            prefs[0]
        );
    }

    #[test]
    fn fetch_preference_orders_intersecting_files_first() {
        let pairs = vec![
            ("model-00001-of-000096.safetensors".into(), "aa".repeat(32)),
            ("model-00041-of-000096.safetensors".into(), "bb".repeat(32)),
            ("model-00051-of-000096.safetensors".into(), "cc".repeat(32)),
            ("model-00002-of-000096.safetensors".into(), "dd".repeat(32)),
        ];
        // Donor owns layers 40-50 → prefer file 41 and 51 among these.
        let ordered = order_digests_for_layer_fetch(40, 50, &pairs).unwrap();
        assert_eq!(ordered.len(), 4);
        assert_eq!(ordered[0], "bb".repeat(32));
        assert_eq!(ordered[1], "cc".repeat(32));
        let r = layers_for_weight_path("model-00001-of-000096.safetensors").unwrap();
        assert_eq!(r, LayerRange { start: 0, end: 0 });
        eprintln!("OBSERVE file-layer-map fetch preference: ordered={ordered:?}");
    }
}
