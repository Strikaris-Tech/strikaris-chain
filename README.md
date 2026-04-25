# strikaris-chain

A minimal hash-chained ledger for tamper-evident record keeping.

Four files. No external databases. No blockchain runtime. Auditors verify with `sha256sum`.

---

## Components

| File | Role |
|---|---|
| `chain.py` | Chain server: SQLite ledger, hash chaining, HTTP API |
| `heartbeat.py` | Liveness ticker: keeps the chain extending between author writs |
| `auditor.py` | Integrity auditor: verifies hashes, checks liveness, writes anchors |
| `author.py` | Author relay: pushes local writs to a remote chain node via SSH |

Each component is independent and configurable via environment variables or CLI flags.

---

## Quick Start

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# Start the chain node
uvicorn chain:app --host 127.0.0.1 --port 7333

# In separate terminals:
python heartbeat.py --interval 60
python auditor.py --interval 300
```

---

## API

```
POST /writ                          seal a writ (verb + body)
GET  /chain/status                  height, last_ts, mode (live | failover)
GET  /chain/block/{query}           query = height (int) or 64-char hash
GET  /chain/verify/{from_h}/{to_h}  batch integrity check
POST /writ/batch                    receive bundled writs from author node
```

### Verbs

| Verb | Description |
|---|---|
| `Tick` | Heartbeat tick (liveness proof) |
| `Log` | Human-authored freeform entry |
| `Audit` | Chain audit summary from auditor |
| `Alert` | Anomaly flag from auditor |
| `Witness` | External event observation |
| `Goal` | Scheduled goal marker |
| `Consent` | Consent grant between agents |
| `Stewardship` | Stewardship declaration |
| `Transform` | Transformation record |
| `Sense` | Sensor reading |

---

## Hash Chaining

Every block stores the SHA-256 hash of the previous block in its `details.prev_hash` field.
The hash of any block is computed as:

```python
import hashlib, json
entry = {k: block[k] for k in ["id", "timestamp", "agent", "status", "action", "details"]}
hash  = hashlib.sha256(json.dumps(entry, sort_keys=True).encode()).hexdigest()
```

This uses only Python's standard library. No external dependencies required for verification.

---

## Storage Backends

The hash chain is storage-agnostic. SQLite is the default because it is portable,
queryable, and trivially copied with `cp` or `docker cp`. The algorithm does not
depend on it.

Any append-only store works:

| Backend | Notes |
|---|---|
| **SQLite** (default) | Portable single file, easy to inspect and copy |
| **Append-only JSON log** | One JSON object per line, human-readable, grep-friendly |
| **Postgres** | Suited for high write volume or multi-writer deployments |
| **S3 / object storage** | Immutable object per block, natural off-site backup |
| **Flat binary log** | Minimal overhead, suited for embedded or constrained environments |
| **Kafka topic** | Ordered, replicated, retention-controlled; hash chain provides tamper evidence on top |

To swap backends, replace the DB read/write calls in `chain.py` with your store of
choice. The `_row_hash()` function and the `prev_hash` field in each record's details
are the only parts that define the chain -- everything else is I/O.

---

## Verifying a Block Locally

```bash
# Fetch the block
curl https://your-chain-node/chain/block/42 > block.json

# Recompute the hash
python3 -c "
import json, hashlib
b = json.load(open('block.json'))
entry = {k: b[k] for k in ['id','timestamp','agent','status','action','details']}
print(hashlib.sha256(json.dumps(entry, sort_keys=True).encode()).hexdigest())
"

# Compare to block.json's 'hash' field -- they must match.
```

---

## Two-Node Topology

Run `chain.py` on a remote node (EC2, VPS) as the public mirror.
Run `author.py` on the author machine to relay writs to the remote node.
The remote chain interleaves author writs with local heartbeat ticks, all hash-chained.

```
Author machine (private)          Remote node (public)
  local forge                       chain.py
  author.py  ──── SSH relay ──────► /relay/batch ──► chain.db
                                    heartbeat.py ──► chain.db (ticks)
                                    auditor.py   ──► checks + anchors
```

---

## Production Deployment

See `services/` for systemd unit files. Recommended layout:

```
/opt/chain/
  chain.py
  heartbeat.py
  auditor.py
  author.py         (author machine only)
  venv/
  data/
    chain.db
    chain_anchor.log
```

```bash
# Install
sudo cp services/*.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable chain heartbeat auditor
sudo systemctl start chain heartbeat auditor
```

---

## Live Example

[verify.strikaris.com](https://verify.strikaris.com) runs this stack.
Source for the verification UI: [strikaris-site](https://github.com/Strikaris-Tech/strikaris-site)
