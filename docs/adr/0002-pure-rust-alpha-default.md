# ADR 0002: Pure-Rust remains the alpha inference default

- Status: Accepted
- Date: 2026-08-03
- Deciders: product (f00-sh/joule)
- Supersedes: nothing (extends [0001-gpu-ffi-engine.md](0001-gpu-ffi-engine.md))

## Context

ADR 0001 established that any GPU/FFI engine requires an explicit ADR and
`AGENTS.md` purity note. After shipping lab-mid / lab-large tensor-backed
decode (weight- and activation-sensitive pure-Rust generate) plus multi-donor
pipeline stages, we re-evaluated whether pure-Rust is “too slow for public
alpha.”

Observations on this track:

- Lab-mid and lab-large fixtures load multi-file safetensors and produce
  deterministic tensor-backed tokens in CI (sub-second).
- Measured tokens/s on the control capacity path is derived from real
  completion wall time, not inventing FLOPs from `throughput_class_sum`.
- Full multi-hundred-GB Kimi matmul is **still** out of scope for pure-Rust
  toy geometry (`MATMUL_DIM`, lab fixtures). That is a quality gap, not an
  immediate alpha blocker for protocol, capacity, and cluster PP honesty.

## Decision

1. **For alpha, pure-Rust remains the default** inference path
   (`ClusterEngine`, stage/decode, lab fixtures). No GPU/FFI backend ships
   under this ADR.
2. **ADR 0001 gate stays in force.** A future CUDA/ROCm/Metal/vendor engine
   needs **ADR 0003+** with concrete backend, license, threat model, and an
   `AGENTS.md` Language/purity update **before** merge.
3. **Do not** stealth-introduce FFI crates, `cc` builds of kernels, or
   shell-outs to external engines without that follow-on ADR.
4. Revisit only if public alpha cannot meet a documented minimum measured
   tokens/s on target hardware with lab-large (or real K3 pin) — open 0003
   then, not before.

## Consequences

### Positive

- Clear, recorded answer to the “only if pure-Rust too slow” product branch.
- Protocol/cluster work continues without waiting on driver matrices in CI.
- Capacity dashboard can show honest measured tokens/s as it grows.

### Negative / risks

- Moonshot-class latency/throughput remains unavailable until a later GPU ADR.
- Operators must not market pure-Rust lab decode as full Kimi quality.

### Neutral

- Offline real K3 pin (`pin-k3-shards`) stays independent of engine choice.

## Alternatives considered

1. **Ship GPU now without 0003** — rejected (violates 0001 + product law).
2. **Declare pure-Rust permanently sufficient** — rejected; leave 0003 open
   when measured need exists.
3. **Disable multi-file lab path until GPU** — rejected; lab-mid/large prove
   load→decode without multi-GB K3.

## Notes

- Related: ADR 0001, `docs/design/cluster-v0.md` open questions, CHANGELOG
  Unreleased capacity `tokens_per_sec`.
