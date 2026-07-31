# joule — anonymous multi-device identity v0

**Goal:** one millijoule **account** across many machines, **no PII**.

## Laws

1. **No names, emails, phones, or government IDs** in the protocol.  
2. **Account id is opaque:** `j_` + 32 hex chars (16 random bytes). Not derived from hardware serials (those leak PII-adjacent fingerprinting).  
3. **One account_id ⇒ one sealed-ledger balance** on a given pool control.  
4. **Multi-machine:** same identity file on each host.  
5. **Identity file is the secret** — treat like a password; anyone with it can donate/spend as that account.  
6. **API key** (`joule_…`) is a bearer token for HTTP chat/status; control returns the same key for the same account_id on every Welcome.

## CLI

```text
joule identity init          # create ~/.config/joule/identity.json
joule identity show
joule identity export --out stick.json
joule identity import --from stick.json

joule agent --control HOST:7701   # uses identity by default (no --account)
```

Lab override: `joule agent --account lab-alice` (not recommended for real use).

Env: `JOULE_IDENTITY=/path/to/identity.json`

## What we never collect

| Field | Stored? |
|-------|---------|
| Email / phone / real name | **No** |
| IP in ledger | **No** (may appear in operator logs of control host — ops hygiene) |
| Hardware serial / MAC as account | **No** |
| Account id | Yes (opaque) |
| API key | Optional cache on identity file after first join |

## Trust notes

- This is **pseudonymous**, not mathematically unlinkable traffic analysis.  
- Same account on many GPUs is intentional (your home + laptop share mJ).  
- Sybil farms can still make many identity files — economy uses verified capacity + leecher rules, not KYC.  
- Fully decentralized multi-pool federation of balances is **out of scope** for v0 (each control has its own sealed ledger).
