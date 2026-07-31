# joule — signed anonymous identity v0

**Goal:** install → run → **done**. One millijoule account on many machines. **No PII.**  
**Group acceptance:** the pool only accepts **cryptographically signed** Hellos for real accounts.

## Two strings (don’t confuse them)

| Name | What | Share? |
|------|------|--------|
| **CODE** (recovery) | UUID, e.g. `550e8400-e29b-41d4-a716-446655440000` | **Secret** — type this on other PCs |
| **ACCT** (account id) | `j1` + 32 hex = fingerprint of ed25519 pubkey | Public — ledger key, not enough to steal funds |

## How crypto works

```text
CODE (16 random bytes as UUID)
   │
   ▼  SHA-256("joule-identity-v1" || code_bytes)
ed25519 signing key
   │
   ▼
public key ──► ACCT = "j1" || hex(sha256(pubkey)[0..16])
   │
   ▼
Hello { account: ACCT, pubkey, sig(preimage), … }
   │
   ▼
Control verifies sig + fingerprint ──► whole pool accepts account
```

Preimage (stable, versioned):

```text
joule-hello-v1|{account}|{node_id}|{pubkey_hex}|{signed_at_ms}|{protocol}
```

- Wrong code ⇒ wrong key ⇒ signature fails ⇒ **rejected**.  
- Lab nicknames (`mesh-alice`) may still join **unsigned** (tests / local).  
- First signed Hello **binds** pubkey to ACCT; a different key for the same ACCT is rejected.

## User story

```text
# machine 1
joule agent --control HOST:7701
# shows CODE + ACCT automatically

# machine 2
joule identity use 550e8400-e29b-41d4-a716-446655440000
joule agent --control HOST:7701
```

Same CODE ⇒ same key ⇒ same ACCT ⇒ **same millijoules**.

## What we never collect

No email, phone, real name, government ID, or hardware serial as identity.

## Threat notes

| Attack | Mitigation |
|--------|------------|
| Type someone else’s **ACCT** without CODE | Signature fails |
| Steal CODE | Full account control (like a seed phrase) — protect it |
| Replay old Hello | Timestamp skew window (15 min) |
| Sybil many CODEs | Economy (verified capacity, leecher rules), not KYC |

Fully trustless multi-pool federation of balances remains out of scope for v0 (per-pool sealed ledger).
