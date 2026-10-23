"""
mirror_node.py — Strikaris EC2 mirror node

Receives pulses from local merlin (heartbeat ticks) and from the Pi relay
(author writs). Maintains a local hash-chained SQLite ledger. Exposes a
read-only API for verify.strikaris.com.

Hash algorithm: SHA-256 of the full row dict (id, timestamp, agent, status,
action, details) — identical to the Pi forge. Auditors can recompute offline.

Routes:
  POST /pulse                     receive tick or relayed writ (localhost only)
  GET  /chain/status              height, last_ts, mode (live | failover)
  GET  /chain/block/{query}       query = height (int) or 64-char hash
  GET  /chain/verify/{from_h}/{to_h}  batch integrity check

Usage:
  /opt/chain/venv/bin/uvicorn mirror_node:app --host 127.0.0.1 --port 7333
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# ── Config ────────────────────────────────────────────────────────────────────

CHAIN_DB   = Path(os.getenv("CHAIN_DB", "/opt/chain/data/chain.db"))
FAILOVER_S = int(os.getenv("FAILOVER_AFTER_SEC", 900))   # 15 min = amber badge

app = FastAPI(title="mirror-node", docs_url=None, redoc_url=None)

# ── DB setup ──────────────────────────────────────────────────────────────────

def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(CHAIN_DB))
    conn.row_factory = sqlite3.Row
    return conn


def _init_db() -> None:
    conn = _conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS log (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            agent     TEXT NOT NULL,
            status    TEXT NOT NULL,
            action    TEXT NOT NULL,
            details   TEXT NOT NULL
        )
    """)
    # Tracks when the Pi relay last delivered a writ (for green/amber badge).
    conn.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()

# ── Hash chaining ─────────────────────────────────────────────────────────────

def _row_hash(row: dict) -> str:
    """SHA-256 of a log row dict — same algorithm as Pi forge."""
    entry = {
        "id":        row["id"],
        "timestamp": row["timestamp"],
        "agent":     row["agent"],
        "status":    row["status"],
        "action":    row["action"],
        "details":   json.loads(row["details"]) if isinstance(row["details"], str) else row["details"],
    }
    return hashlib.sha256(
        json.dumps(entry, sort_keys=True, default=str).encode()
    ).hexdigest()


def _head_hash() -> str:
    """Hash of the last row, or genesis sentinel."""
    conn = _conn()
    row = conn.execute(
        "SELECT id, timestamp, agent, status, action, details FROM log ORDER BY id DESC LIMIT 1"
    ).fetchone()
    conn.close()
    if not row:
        return "0" * 64
    return _row_hash(dict(row))

# ── Pulse intake ──────────────────────────────────────────────────────────────

class PulseRequest(BaseModel):
    mirror_id: str = "merlin"
    verb:      str
    body:      dict[str, Any] = {}
    # Pi relay sets this so mirror_node knows the origin agent
    origin_agent: str | None = None


def _to_atom(v: Any) -> str:
    if isinstance(v, str):
        return f"'{v}'"
    return str(v)


def _pulse_to_fact(verb: str, mirror_id: str, body: dict) -> str:
    verb_l  = verb.lower()
    body_kv = ", ".join(f"{k}={_to_atom(v)}" for k, v in body.items() if not k.startswith("_"))
    return f"{verb_l}({mirror_id}, {body_kv})" if body_kv else f"{verb_l}({mirror_id})"


@app.post("/pulse")
async def receive_pulse(req: PulseRequest):
    ts      = datetime.now(tz=timezone.utc).isoformat()
    agent   = req.origin_agent or req.mirror_id
    fact    = _pulse_to_fact(req.verb, req.mirror_id, req.body)
    action  = f"assert({fact})"
    details = json.dumps({"verb": req.verb, "body": req.body, "prev_hash": _head_hash()})

    conn = _conn()
    cur  = conn.execute(
        "INSERT INTO log (timestamp, agent, status, action, details) VALUES (?, ?, ?, ?, ?)",
        (ts, agent, "ASSERTED", action, details),
    )
    row_id = cur.lastrowid
    conn.commit()

    # Track last Pi relay timestamp for badge logic.
    if req.origin_agent and req.origin_agent != "merlin":
        conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES ('last_pi_relay_ts', ?)",
            (ts,),
        )
        conn.commit()

    conn.close()
    return {"status": "ASSERTED", "id": row_id, "action": action}

# ── Read-only API ─────────────────────────────────────────────────────────────

def _mode() -> str:
    """live = Pi relay recent; failover = relay silent > FAILOVER_S."""
    conn  = _conn()
    row   = conn.execute("SELECT value FROM meta WHERE key='last_pi_relay_ts'").fetchone()
    conn.close()
    if not row:
        return "failover"
    last  = datetime.fromisoformat(row["value"])
    age_s = (datetime.now(tz=timezone.utc) - last).total_seconds()
    return "live" if age_s < FAILOVER_S else "failover"


@app.get("/chain/status")
async def chain_status():
    conn   = _conn()
    height = conn.execute("SELECT COUNT(*) FROM log").fetchone()[0]
    last   = conn.execute(
        "SELECT timestamp, agent, action FROM log ORDER BY id DESC LIMIT 1"
    ).fetchone()
    conn.close()
    return {
        "height":    height,
        "last_ts":   last["timestamp"] if last else None,
        "last_agent": last["agent"] if last else None,
        "head_hash": _head_hash(),
        "mode":      _mode(),
    }


@app.get("/chain/block/{query}")
async def chain_block(query: str):
    conn = _conn()

    # Height lookup (integer).
    if query.isdigit():
        offset = int(query) - 1
        row = conn.execute(
            "SELECT * FROM log ORDER BY id LIMIT 1 OFFSET ?", (offset,)
        ).fetchone()
    # Hash lookup (64 hex chars).
    elif len(query) == 64:
        rows = conn.execute(
            "SELECT * FROM log ORDER BY id"
        ).fetchall()
        row = next(
            (r for r in rows if _row_hash(dict(r)) == query),
            None,
        )
    else:
        conn.close()
        raise HTTPException(status_code=400, detail="query must be a block height (int) or 64-char hash")

    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="block not found")

    d = dict(row)
    return {
        "id":        d["id"],
        "timestamp": d["timestamp"],
        "agent":     d["agent"],
        "status":    d["status"],
        "action":    d["action"],
        "details":   json.loads(d["details"]),
        "hash":      _row_hash(d),
        "type":      "tick" if d["agent"] == "merlin" else "assert",
        "signed_by": "merlin-tick" if d["agent"] == "merlin" else "pi-architect",
    }


@app.get("/chain/verify/{from_h}/{to_h}")
async def chain_verify(from_h: int, to_h: int):
    if from_h < 1 or to_h < from_h:
        raise HTTPException(status_code=400, detail="invalid range")

    conn = _conn()
    rows = conn.execute(
        "SELECT * FROM log ORDER BY id LIMIT ? OFFSET ?",
        (to_h - from_h + 1, from_h - 1),
    ).fetchall()
    conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail="range not found")

    results = []
    for r in rows:
        d       = dict(r)
        details = json.loads(d["details"])
        prev    = details.get("prev_hash", "unknown")
        h       = _row_hash(d)
        results.append({
            "height":    d["id"],
            "hash":      h,
            "prev_hash": prev,
            "agent":     d["agent"],
            "timestamp": d["timestamp"],
        })

    return {"range": [from_h, to_h], "blocks": results, "count": len(results)}


# ── Relay intake (Pi → EC2, localhost only) ───────────────────────────────────

class RelayRow(BaseModel):
    timestamp: str
    agent:     str
    status:    str
    action:    str
    details:   str   # raw JSON string from Pi log row


@app.post("/relay/batch")
async def relay_batch(rows: list[RelayRow]):
    """
    Accepts a batch of raw log rows from the Pi architect_node relay.
    Inserts each into the local chain with EC2 hash chaining.
    Updates last_pi_relay_ts so the status badge reflects Pi liveness.
    Called via: ssh strikaris-prime 'curl ... http://127.0.0.1:7333/relay/batch'
    """
    if not rows:
        return {"relayed": 0}

    conn = _conn()
    relayed = 0
    for row in rows:
        try:
            details = json.loads(row.details)
        except Exception:
            details = {"raw": row.details}
        details["prev_hash"]     = _head_hash()
        details["relayed_from"]  = "pi-architect"

        conn.execute(
            "INSERT INTO log (timestamp, agent, status, action, details) VALUES (?, ?, ?, ?, ?)",
            (row.timestamp, row.agent, row.status, row.action, json.dumps(details)),
        )
        conn.commit()
        relayed += 1

    now = datetime.now(tz=timezone.utc).isoformat()
    conn.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('last_pi_relay_ts', ?)",
        (now,),
    )
    conn.commit()
    conn.close()
    return {"relayed": relayed, "ts": now}


# ── Startup ───────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    CHAIN_DB.parent.mkdir(parents=True, exist_ok=True)
    _init_db()
    height = _conn().execute("SELECT COUNT(*) FROM log").fetchone()[0]
    print(f"mirror-node  db={CHAIN_DB}  height={height}  mode={_mode()}")
