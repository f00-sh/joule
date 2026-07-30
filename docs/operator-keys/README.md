# Operator keys (public only)

**Private keys never go in this directory or git.**

## Master OpenPGP — `tj@f00.sh`

| File | Role |
|------|------|
| [master.asc](master.asc) | Full public key (also on the site) |
| Fingerprint | `4B18FA65E246ACC61701B6AFCA4CB80ABF1AF878` |

This is the **master** human/release authority for joule.

## Protocol ed25519 (agent bus)

| File | Role |
|------|------|
| [protocol.ed25519.pub](protocol.ed25519.pub) | 64-hex verify key (embedded in binaries) |
| [protocol.ed25519.pub.asc](protocol.ed25519.pub.asc) | Detached GPG signature by master |

Agents verify bus envelopes with the **protocol** key (fast, pure Rust).  
Humans verify this file with:

```text
gpg --import master.asc
gpg --verify protocol.ed25519.pub.asc protocol.ed25519.pub
```

## Website

After Pages deploy:

- https://joule.f00.sh/operator-keys/master.asc  
- https://joule.f00.sh/operator-keys/protocol.ed25519.pub  

Clients may fetch these over **TLS**, but only **accept if they match the embedded pins**.  
Website alone cannot replace the master key.

## Secrets (this machine / yours)

```text
~/.config/f00/joule/master.gpg.sec.asc
~/.config/f00/joule/master.gpg.pass
~/.config/f00/joule/protocol.ed25519.sec
~/.config/f00/joule/gnupg-joule-master/
```

## Full design

**[docs/design/master-key-trust-v0.md](../design/master-key-trust-v0.md)** — how hijacks are blocked, what embed + website do, what they cannot do.
