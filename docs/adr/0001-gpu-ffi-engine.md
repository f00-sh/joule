# ADR 0001: GPU / FFI inference engine gate

- Status: Accepted
- Date: 2026-08-03
- Deciders: product (f00-sh/joule)

## Context

joule’s declared language is pure Rust for first-party protocol, cluster, ledger,
and CLI. Inference is the path most likely to need GPU kernels or vendor runtimes
(CUDA, ROCm, Metal, or an external engine crate with C FFI).

Shipping a non-Rust engine without a written decision would break the language /
purity product law in `AGENTS.md` and invite silent FFI sprawl.

The pure-Rust path today (ClusterEngine, lab-tiny decode, JST3 band matmul) is
intentionally **toy geometry** — enough to prove pipeline stages, weight gates,
and peer PP handoff — not full multi-hundred-GB Kimi matmul performance.

## Decision

1. **Default:** pure-Rust `joule_runtime::Engine` implementations only
   (`StubEngine`, `ClusterEngine`, stage/decode modules on crates.io Rust deps).
2. **Any FFI / non-Rust runtime** (GPU drivers, vendor SDKs, foreign process
   engines) **requires**:
   - An ADR under `docs/adr/` (this document is the gate; new backends get a
     follow-on ADR with scope, threat model, and purity exception),
   - An explicit note in `AGENTS.md` under Language / purity before merge,
   - No f00 hosting of proprietary blobs; content-addressed peer seed still applies
     for weights/software.
3. **Do not** implement a GPU/FFI backend in the same change as this ADR unless
   a separate goal authorizes it. This ADR is the **gate**, not the engine.

## Consequences

### Positive

- Clear bar for purity exceptions; reviewers have a checklist.
- Pure-Rust track can continue (band load, PP, decode) without pretending to be
  production Kimi FLOPs.
- When pure-Rust is too slow for public alpha, the ADR path is already open.

### Negative / risks

- Full open-weight throughput may lag until an ADR’d GPU path lands.
- Temporary dual maintenance if pure-Rust toy path and GPU path coexist.

### Neutral

- Lab fixtures and CI stay pure Rust forever where possible.

## Alternatives considered

1. **Allow ad-hoc FFI in runtime without ADR** — rejected (product law).
2. **Mandate GPU now** — rejected; no multi-GB weights or driver matrix in CI.
3. **Shell out to external llama.cpp / etc. without Engine trait** — rejected;
   breaks single Engine abstraction and contribution accounting.

## Notes

- Related: `docs/design/cluster-v0.md` §7 Runtime strategy; `AGENTS.md` Language / purity.
- Follow-on: if/when pure-Rust is too slow for alpha, open ADR 0002+ naming the
  concrete backend, license, and purity exception.
