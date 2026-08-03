# K3 file shards ↔ transformer layer ranges (v0)

**Status:** design table (not wired as automatic placement driver)  
**Related:** `cluster-v0.md` (placement geometry), `MANIFEST.json` (`kimi-k3-shards`), `pipeline.rs` activation handoff

## Problem

Two different axes:

| Axis | Meaning | Count (Kimi-K3 pin) |
|------|---------|---------------------|
| **Transformer layers** | Placement `layer_start`/`layer_end` (scheduling geometry) | **93** (`text_config.num_hidden_layers`) |
| **Weight files** | Content-addressed safetensors shards | **16** × ~20 GiB (`model-0000N-of-00016`) |

They are **not** 1:1. Operators and code must not assume file *N* = layer *N*.

## Explicit map (v0 proposal)

Even partition of 93 layers across 16 files (last file absorbs remainder):

| File index (1-based) | Path | Layers (inclusive) | Span |
|----------------------|------|--------------------|------|
| 1 | `model-00001-of-00016.safetensors` | 0–5 | 6 |
| 2 | `model-00002-of-00016.safetensors` | 6–11 | 6 |
| 3 | `model-00003-of-00016.safetensors` | 12–17 | 6 |
| 4 | `model-00004-of-00016.safetensors` | 18–23 | 6 |
| 5 | `model-00005-of-00016.safetensors` | 24–29 | 6 |
| 6 | `model-00006-of-00016.safetensors` | 30–35 | 6 |
| 7 | `model-00007-of-00016.safetensors` | 36–41 | 6 |
| 8 | `model-00008-of-00016.safetensors` | 42–47 | 6 |
| 9 | `model-00009-of-00016.safetensors` | 48–53 | 6 |
| 10 | `model-00010-of-00016.safetensors` | 54–59 | 6 |
| 11 | `model-00011-of-00016.safetensors` | 60–65 | 6 |
| 12 | `model-00012-of-00016.safetensors` | 66–71 | 6 |
| 13 | `model-00013-of-00016.safetensors` | 72–77 | 6 |
| 14 | `model-00014-of-00016.safetensors` | 78–83 | 6 |
| 15 | `model-00015-of-00016.safetensors` | 84–89 | 6 |
| 16 | `model-00016-of-00016.safetensors` | 90–92 | 3 |

**Formula (v0):**  
`base = floor(93/16) = 5`, remainder distributed so first 15 bands span 6 layers and last spans 3 — total 15×6+3 = 93.  
**Shipped helpers:** `joule_cluster::{layers_for_file, files_intersecting_layers, preferred_weight_files, order_digests_for_layer_fetch}` and runtime `WeightsStore::{required_weight_files_for_band, band_files_ready}` + `load_model_for_band`.

## Placement vs fetch

- **Placement** still assigns donors by **verified VRAM** to continuous layer bands (may cross file boundaries).  
- **Fetch preference:** a donor owning layers `[Ls, Le]` prefers files whose map intersects that interval (`preferred_weight_files` / `order_digests_for_layer_fetch` on model_update).  
- **Band weight gate:** `stage_layers` with `require_band_weights` fails closed unless preferred files for `[Ls, Le]` are staged/loaded (`load_model_for_band` / ClusterEngine). Lab/stub paths may leave the gate off.  
- **Weight-backed stage:** ClusterEngine with a `LoadedModel` emits **JST2** activations that hash/sample resident tensor bytes (`weight_material_from_tensors` + `stage_activation_with_weights`). Different staged weights change the activation. Stub/lab without load stays **JST1**.

## Real content path

1. Obtain real safetensors offline (peer, operator, never f00 CDN).  
2. Run `scripts/pin-k3-shards.sh DIR` → rewrite MANIFEST digests + sizes with **real** sha256.  
3. Synthetic `a100…` placeholders never unlock digests; real digests + staged bytes may.

## Non-goals (v0)

- Automatic re-slice of safetensors to exact layer boundaries.  
- Claiming true pipeline-parallel matmul across donors solely from this table.
