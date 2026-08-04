# K3 file shards ↔ transformer layer ranges (v0)

**Status:** design table + shipped helpers (`joule_cluster::file_layer_map`)  
**Related:** `cluster-v0.md` (placement geometry), `MANIFEST.json` (`kimi-k3-shards`), pipeline activation handoff

## Problem

Two different axes:

| Axis | Meaning | Count (production Kimi-K3 pin) |
|------|---------|--------------------------------|
| **Transformer layers** | Placement `layer_start`/`layer_end` (scheduling geometry) | **93** (`text_config.num_hidden_layers`) |
| **Weight files** | Content-addressed safetensors shards | **96** (`model-NNNNN-of-000096` from moonshotai/Kimi-K3) |

They are **not** 1:1 MoE packing. Operators and code must not assume file *N* = layer *N* for every tensor inside a shard.

## Explicit map (production)

- Files **1..=93**: layer `i-1` only (1:1 for fetch preference).
- Files **94..=96**: residual/global weights (embeddings / head / multimodal packing) — **always preferred** for any band.
- Basename format: `model-00001-of-000096.safetensors` (5-digit index, 6-digit total).

**Shipped helpers:** `joule_cluster::{layers_for_file, files_intersecting_layers, preferred_weight_files, order_digests_for_layer_fetch, is_global_weight_file}` and runtime `WeightsStore::{required_weight_files_for_band, band_files_ready}` + `load_model_for_band`.

## Placement vs fetch

- **Placement** assigns donors by **verified VRAM** to continuous layer bands.
- **Fetch preference:** a donor owning layers `[Ls, Le]` prefers intersecting files + global residuals.
- **Band weight gate:** `stage_layers` with `require_band_weights` fails closed unless preferred files for `[Ls, Le]` that the quant lists are staged/loaded.
- **Production engine:** ADR 0003 `ProductionEngine` (CUDA driver FFI) for production agents; digests must be real LFS content hashes (not `a100…` placeholders).

## Real content path

1. Obtain real safetensors offline (peer, operator, never f00 CDN). HF optional via `JOULE_ALLOW_EXTERNAL_FETCH=1`.
2. Pins are published in `models/MANIFEST.json` / `models/kimi-k3-shards.pins.json` (Git LFS content oid = sha256).
3. Or re-pin from local dir: `scripts/pin-k3-shards.sh DIR`.
4. Unlock only when staged bytes match real digests; placeholders never unlock.

## Fleet honesty

Full K3 service-live requires multi-backend + high aggregate VRAM (`≥64 GiB` and `≥3` backends in MANIFEST milestones / `full_k3_service_fleet_ok`). A single ~8 GiB card alone cannot honestly claim production service-live for full K3.

**Not every donor downloads all 96 files.** Band prepare + preferred file map stage only intersecting shards (+ global residuals). Full multi‑TB per box would defeat the product; peer seeders spread content.

## Non-goals

- Automatic re-slice of safetensors to exact layer boundaries inside a shard.
- Claiming full multi-TB residency on a single developer host without the bytes.
- Requiring every client to download the full production quant.
