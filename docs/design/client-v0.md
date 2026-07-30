# joule — client / tray / service install v0

**Status:** active  
**Crate:** `joule-client` · **CLI:** `joule status|monitor|tray|service`

## Platforms

| OS | CLI status | Monitor dash | Auto-start |
|----|------------|--------------|------------|
| Linux | yes | yes | systemd **user** unit (optional system unit for headless agent) |
| macOS | yes | yes | LaunchAgents plist |
| Windows | yes | yes | Task Scheduler XML (logon) |

**Yes — macOS and Windows get the CLI** (`joule status`, `monitor`, etc.). Same binary surface.

## System service vs tray

A **root system** service cannot own a user systray icon. v0 uses **OS-managed auto-start in the user session** (systemd `--user`, LaunchAgents, interactive scheduled task) so agent + tray can share the GPU session with low friction.

## Status fields (shared)

`ClientStatus` (one assembly path for CLI + tray):

- connection (connected / degraded / disconnected)
- api base, account, api key hint, donating
- millijoule balance + window contribute/consume
- tokens used (prompt + completion + total) from control account API
- pool backends, VRAM GiB, agents, stream slots, service_live, inference mode
- compact `cards[]` for monitor dash

## Commands

```text
joule status --api http://127.0.0.1:7700 --key joule_… [--dash|--json]
joule monitor --api … --key … --interval-secs 3
joule tray --api … --key …          # same poller; tray GUI can wrap later
joule service generate --platform linux --kind agent --out ~/.config/systemd/user/joule-agent.service
joule service install-help --platform macos
```
