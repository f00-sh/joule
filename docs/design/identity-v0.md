# joule — anonymous multi-device identity v0

**Goal:** install app → **done**. One millijoule account, many machines, **no PII**.

## User story (dummy easy)

1. Install joule.  
2. Run agent (or open tray). A **joule code** (UUID) is created **automatically** — you never pick it.  
3. On another computer: type/paste  
   `joule identity use 550e8400-e29b-41d4-a716-446655440000`  
   (or `joule agent --code …`). Same code ⇒ same millijoules.

## Laws

1. **No names, emails, phones, or government IDs.**  
2. **Code is a random UUID** (auto-generated). Not hardware serials.  
3. **One code ⇒ one sealed-ledger balance** on a pool.  
4. **Multi-machine = same code** (type it or paste it).  
5. **The code is the secret** — anyone with it is “you” on that pool.  
6. **API key** (`joule_…`) is for HTTP chat; same account always gets the same key from control.

## CLI

```text
joule agent --control HOST:7701     # auto code on first run; prints banner
joule identity show                 # print your code again
joule identity use <UUID>           # link this machine to an existing code

# also fine:
joule agent --code 550e8400-e29b-41d4-a716-446655440000 --control HOST:7701
```

Env: `JOULE_IDENTITY=/path/to/identity.json` (stores the code locally).

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
