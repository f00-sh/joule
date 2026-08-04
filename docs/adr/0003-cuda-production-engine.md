# ADR 0003: CUDA driver FFI production Engine

- Status: **Accepted**
- Date: 2026-08-04
- Deciders: product (f00-sh/joule)
- Supersedes: nothing (extends [0001-gpu-ffi-engine.md](0001-gpu-ffi-engine.md); production path beyond [0002-pure-rust-alpha-default.md](0002-pure-rust-alpha-default.md))

## Context

ADR 0001/0002 kept pure-Rust lab engines (`StubEngine`, `ClusterEngine`) as the
alpha default. Production Kimi-class service requires a named GPU/FFI backend so
donors with NVIDIA devices can run **real-weight** prepare → stage → infer on the
mesh, not only toy MATMUL_DIM geometry.

Full multi-TB Kimi-K3 residency still needs fleet storage and multi-backend VRAM
gates; this ADR names the engine path and purity exception so production agents
select a non-lab backend when weights and digests allow.

## Decision

1. **Backend:** NVIDIA **CUDA driver API** via dynamic load of `libcuda.so`
   (`cuInit`, `cuDeviceGetCount`). No static link to proprietary SDK at build
   time; no f00-hosted proprietary blobs.
2. **Rust surface:** `joule_runtime::ProductionEngine` implements `Engine`.
   It wraps weight residency (`ClusterEngine` load/install) and requires
   **content-proof digests** for the production `kimi-k3-shards` quant before
   production stage/infer claims.
3. **License / threat model:**
   - CUDA driver is vendor-provided on the host; joule only `dlopen`s it.
   - Weights remain content-addressed peer seed (product law 11).
   - Failure modes fail closed: missing `libcuda`, zero devices, or missing
     digests/weights → no production service claim.
4. **Lab CI:** pure-Rust `ClusterEngine` / lab fixtures remain for protocol tests.
   Production agents construct `ProductionEngine` (ADR path).
5. **AGENTS.md** documents this purity exception under Language / purity.

## Consequences

### Positive

- Explicit, reviewable FFI boundary; no stealth GPU.
- Agents can probe real GPU presence for production path selection.
- Digest/weight gates stay unit-testable without full TB downloads.

### Negative / risks

- Full MoE Kimi-K3 forward pass quality still depends on resident shards + fleet
  capacity; this ADR does not claim single-box TB residency.
- Hosts without NVIDIA drivers use fail-closed production GPU claims (lab path
  may still serve fixtures).

## Alternatives considered

1. **Stay pure-Rust forever** — rejected for production Kimi throughput goals.
2. **Shell out to external Python/vLLM without Engine trait** — rejected (ADR 0001).
3. **Static link CUDA toolkit in CI** — rejected (build/driver matrix, no toolkit required for pin/digest CI).

## Notes

- Pin source: `moonshotai/Kimi-K3` LFS content digests in `models/MANIFEST.json`
  / `models/kimi-k3-shards.pins.json`.
- Related: `docs/design/cluster-v0.md`, `docs/design/k3-file-layer-map-v0.md`.
