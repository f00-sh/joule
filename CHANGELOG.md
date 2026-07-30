# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed

- Product language: **distributed internet-wide cluster** (not mesh/LAN-first)
- Crate rename: `joule-mesh` → `joule-cluster`; `MeshPlan` → `ClusterPlan`
- Design SoT: `docs/design/cluster-v0.md` (replaces mesh-v0)

### Added

- `ClusterCapacity` aggregate + `Cluster::capacity()` for live dashboard feed
- CLI: `joule capacity [--peers N] [--json]`
- Capacity message type on the wire protocol
- Dashboard requirement: always show live distributed compute

### Prior scaffold

- Rust workspace: `joule`, `joule-proto`, `joule-cluster`, `joule-runtime`, `joule-ledger`
- CLI: `joule version`, `joule lab`, `joule credits`, `joule agent` (stub)
- Placement: replica + pipeline plans
- Stub inference engine for CI/lab
- Millijoule ledger mint/burn
- CI workflow (fmt, clippy, test, release build)

## [0.0.0] - 2026-07-30

### Added

- Initial repository scaffold (house kit)
