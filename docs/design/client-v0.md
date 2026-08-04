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
joule get-started                 # dumb-user checklist
joule start                       # one-shot local control + agent
joule service install             # write+enable control + agent + tray (user session)
joule service install --dry-run
joule status --api http://127.0.0.1:7700 --key joule_… [--dash|--json]
joule monitor --api … --key … --interval-secs 3
joule tray --api … --key …
joule service generate --platform linux --kind control|agent|tray --out …
joule service install-help --platform macos
```

**GUI:** default `joule` opens the dashboard. **★ DO EVERYTHING (local pool)** starts
control + agent; first open auto-starts if nothing is on `:7700` (`JOULE_GUI_NO_AUTO=1` skips).
