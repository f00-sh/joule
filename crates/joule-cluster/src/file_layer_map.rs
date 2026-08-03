//! Executable Kimi-K3 file index ↔ transformer layer map (design k3-file-layer-map-v0).
//!
//! 16 weight files, 93 layers. Pure math — no I/O.

/// Design constants from offline K3 meta pin + MANIFEST shard count.
pub const K3_MODEL_LAYERS: u32 = 93;
pub const K3_FILE_COUNT: u32 = 16;

/// Inclusive layer range for a 1-based file index (1..=16).
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
/// First 15 files span 6 layers each; file 16 spans 3 (15*6+3=93).
pub fn layers_for_file(file_index_1based: u32) -> Result<LayerRange, String> {
    if file_index_1based == 0 || file_index_1based > K3_FILE_COUNT {
        return Err(format!(
            "file index {file_index_1based} out of 1..={K3_FILE_COUNT}"
        ));
    }
    if file_index_1based <= 15 {
        let start = (file_index_1based - 1) * 6;
        Ok(LayerRange {
            start,
            end: start + 5,
        })
    } else {
        Ok(LayerRange { start: 90, end: 92 })
    }
}

/// 1-based file indices whose layer ranges intersect `[layer_start, layer_end]`.
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
        let r = layers_for_file(i)?;
        if r.intersects(&want) {
            out.push(i);
        }
    }
    Ok(out)
}

/// Weight file basenames preferred for a donor owning layer band `[Ls, Le]`.
///
/// **Fetch preference call site:** agents / model_update order BlobWant digests
/// with these paths first so a shard with layers `[Ls,Le]` pulls intersecting
/// weight files before the rest of the quant.
pub fn preferred_weight_files(layer_start: u32, layer_end: u32) -> Result<Vec<String>, String> {
    let idx = files_intersecting_layers(layer_start, layer_end)?;
    Ok(idx
        .into_iter()
        .map(|i| format!("model-{i:05}-of-{K3_FILE_COUNT:05}.safetensors"))
        .collect())
}

/// Order `(path, sha256)` pairs for fetch: paths intersecting `[Ls,Le]` first.
///
/// Used by placement-aware fetch preference (model_update FetchDigests order).
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
    // stable dedup
    let mut seen = std::collections::HashSet::new();
    preferred_digests.retain(|d| seen.insert(d.clone()));
    Ok(preferred_digests)
}

/// Infer K3 layer range from a weight file basename (`model-NNNNN-of-00016.safetensors`).
pub fn layers_for_weight_path(path: &str) -> Option<LayerRange> {
    let base = path.rsplit('/').next().unwrap_or(path);
    // model-00008-of-00016.safetensors
    let rest = base.strip_prefix("model-")?;
    let idx_str = rest.split('-').next()?;
    let idx: u32 = idx_str.parse().ok()?;
    layers_for_file(idx).ok()
}

/// Total layer coverage check: union of all files is 0..92 contiguous, no gaps.
pub fn map_covers_all_layers() -> bool {
    let mut covered = [false; K3_MODEL_LAYERS as usize];
    for i in 1..=K3_FILE_COUNT {
        let Ok(r) = layers_for_file(i) else {
            return false;
        };
        for l in r.start..=r.end {
            if l >= K3_MODEL_LAYERS {
                return false;
            }
            if covered[l as usize] {
                return false; // overlap
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
    fn design_table_file_1_and_16() {
        let f1 = layers_for_file(1).unwrap();
        assert_eq!(f1, LayerRange { start: 0, end: 5 });
        assert_eq!(f1.span(), 6);
        let f16 = layers_for_file(16).unwrap();
        assert_eq!(f16, LayerRange { start: 90, end: 92 });
        assert_eq!(f16.span(), 3);
        assert!(map_covers_all_layers());
        let mut total = 0u32;
        for i in 1..=16 {
            total += layers_for_file(i).unwrap().span();
        }
        assert_eq!(total, 93);
        eprintln!("OBSERVE file-layer-map: file1=0-5 file16=90-92 total_span=93");
    }

    #[test]
    fn band_40_50_intersects_expected_files() {
        // 40-45 → file 8 (42-47), 46-47 file 8, 48-50 file 9 (48-53)
        let files = files_intersecting_layers(40, 50).unwrap();
        assert!(files.contains(&7) || files.contains(&8)); // 36-41 and 42-47
        assert!(files.contains(&8));
        assert!(files.contains(&9));
        let prefs = preferred_weight_files(40, 50).unwrap();
        assert!(prefs.iter().any(|p| p.contains("00008")));
        assert!(prefs.iter().any(|p| p.contains("00009")));
        eprintln!("OBSERVE file-layer-map: layers 40-50 → files {files:?} prefs={prefs:?}");
    }

    #[test]
    fn fetch_preference_orders_intersecting_files_first() {
        let pairs = vec![
            ("model-00001-of-00016.safetensors".into(), "aa".repeat(32)),
            ("model-00008-of-00016.safetensors".into(), "bb".repeat(32)),
            ("model-00009-of-00016.safetensors".into(), "cc".repeat(32)),
            ("model-00016-of-00016.safetensors".into(), "dd".repeat(32)),
        ];
        // Donor owns layers 40-50 → prefer files 7/8/9; 8 and 9 must lead.
        let ordered = order_digests_for_layer_fetch(40, 50, &pairs).unwrap();
        assert_eq!(ordered.len(), 4);
        assert_eq!(ordered[0], "bb".repeat(32));
        assert_eq!(ordered[1], "cc".repeat(32));
        assert!(ordered[2..].contains(&"aa".repeat(32)));
        assert!(ordered[2..].contains(&"dd".repeat(32)));
        let r = layers_for_weight_path("model-00001-of-00016.safetensors").unwrap();
        assert_eq!(r, LayerRange { start: 0, end: 5 });
        eprintln!("OBSERVE file-layer-map fetch preference: ordered={ordered:?}");
    }
}
