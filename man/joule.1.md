# joule(1) — mesh supercomputer agent and lab CLI

## NAME

joule — donate idle GPU compute to an open mesh; earn millijoules for AI API access

## SYNOPSIS

```text
joule version
joule lab [--model NAME] [--prompt TEXT] [--pipeline] [--stages N] [--peers N]
joule credits [--account NAME]
joule agent [--model NAME] [--config PATH]
joule -h | --help
joule -V | --version
```

## DESCRIPTION

**joule** is the command-line tool for the joule mesh supercomputer.

Donors run a native agent that joins a peer mesh. The mesh places open-weight
model inference across nodes (pipeline when enough peers exist; replica otherwise).
Users spend **millijoules** on API access. Credits are minted from verified
contribution only. The public pool does not sell cash bypasses.

Version **0.0.0** ships a local lab and ledger demo. Network agent transport
and real model engines land in later milestones (see docs/design/mesh-v0.md).

Primary user: operator or donor who contributes GPU time.
Primary job: run lab experiments now; run agent and serve mesh later.

## OPTIONS

### joule version

Print package version, protocol version, and build posture.

### joule lab

Run an in-process mesh lab with synthetic peers, a placement plan, stub
inference, and a ledger mint/burn demo.

```text
--model NAME
    Model tag to plan and load (default: kimi-open-q4).

--prompt TEXT
    Prompt for stub completion.

--pipeline / --no-pipeline
    Prefer pipeline placement when enough peers exist (default: true).

--stages N
    Pipeline stage count (default: 2).

--peers N
    Number of synthetic GPU peers (default: 3).
```

### joule credits

Demonstrate millijoule mint and burn for an account.

```text
--account NAME
    Account id (default: donor).
```

### joule agent

Start a donor agent. **Not implemented in 0.0.0** (exits with error pointing at the design doc).

```text
--model NAME
    Model tag this node can host.

--config PATH
    Optional config file path (reserved).
```

```text
-h, --help
    Show help and exit.

-V, --version
    Show version and exit.
```

## EXIT STATUS

| Code | Meaning |
|---|---|
| 0 | Success |
| non-zero | Failure (plan error, runtime error, or unimplemented agent) |

## ENVIRONMENT

| Variable | Purpose |
|---|---|
| `RUST_LOG` | Tracing filter (e.g. `joule=debug`) |

## FILES

| Path | Purpose |
|---|---|
| `docs/design/mesh-v0.md` | Architecture and phase plan |
| `file_id.diz` | Release scene card (repository root; GitHub Release asset) |

All supported install methods install this manual page.

## EXAMPLES

```text
# Build identity
joule version

# Three-peer pipeline lab
joule lab --peers 3 --stages 2 --model kimi-open-q4

# Ledger demo
joule credits --account alice
```

## SEE ALSO

- [README.md](../README.md)
- [docs/design/mesh-v0.md](../docs/design/mesh-v0.md)
- Project site under [docs/](../docs/)
- [CHANGELOG.md](../CHANGELOG.md)
- [file_id.diz](../file_id.diz)

## BUGS

Report issues in the project tracker. Do not file security issues in public
trackers; see [SECURITY.md](../SECURITY.md).
