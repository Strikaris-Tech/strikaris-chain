"""
chain.py -- Strikaris Chain server.

Hash-chained SQLite ledger with a minimal HTTP API.
Accepts writs from local keepers and bundles from an author node.
Exposes read-only routes for public verification.

Routes:
  POST /writ                          seal a writ (localhost only in prod, via Nginx)
  GET  /chain/status                  height, last_ts, mode (live | failover)
  GET  /chain/block/{query}           query = height (int) or 64-char hash
  GET  /chain/verify/{from_h}/{to_h}  batch integrity check
  POST /writ/batch                    receive bundled writs from author node (localhost only)

Usage:
  uvicorn chain:app --host 127.0.0.1 --port 7333

Environment:
  CHAIN_DB             path to SQLite file (default: ./data/chain.db)
  FAILOVER_AFTER_SEC   seconds before mode flips to failover (default: 900)
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# ── Config ────────────────────────────────────────────────────────────────────

CHAIN_DB      = Path(os.getenv("CHAIN_DB", "./data/chain.db"))
FAILOVER_S    = int(os.getenv("FAILOVER_AFTER_SEC", 900))

app = FastAPI(title="strikaris-chain", docs_url=None, redoc_url=None)

# ── DB ────────────────────────────────────────────────────────────────────────

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
    conn = _conn()
    row = conn.execute(
        "SELECT id, timestamp, agent, status, action, details FROM log ORDER BY id DESC LIMIT 1"
    ).fetchone()
    conn.close()
    if not row:
        return "0" * 64
    return _row_hash(dict(row))

# ── Action encoding ───────────────────────────────────────────────────────────

def _atom(v: Any) -> str:
    if isinstance(v, (int, float)):
        return str(v)
    s = re.sub(r"[^a-z0-9_]", "_", str(v).lower()).strip("_") or "unknown"
    return f"'{s}'" if s[0].isdigit() else s


def _to_action(verb: str, agent: str, body: dict) -> str:
    """Encode a writ as a Prolog-style fact string stored in the action field."""
    if "_fact" in body:
        return body["_fact"]

    mid = _atom(agent)

    handlers = {
        "Tick":        lambda: f"tick({mid}, {int(body.get('seq', 0))})",
        "Audit":       lambda: f"audit({mid}, {body.get('range', [0,0])[0]}, {body.get('range', [0,0])[-1]}, {_atom(body.get('dest', 'local'))})",
        "Alert":       lambda: f"alert({mid}, {_atom(body.get('level', 'amber'))}, {_atom(body.get('summary', 'unknown'))}, {body.get('value', 0)})",
        "Log":         lambda: f"log({mid}, {_atom(str(body.get('note', ''))[:256])}, {_atom(','.join(body.get('tags', [])[:8]) if isinstance(body.get('tags'), list) else str(body.get('tags', '')))})",
        "Witness":     lambda: f"witness({mid}, {_atom(body.get('source', 'unknown'))}, {_atom(body.get('channel', 'unknown'))}, {_atom(body.get('sender', 'unknown'))}, {_atom(str(body.get('content_hash', 'unknown'))[:12])})",
        "Goal":        lambda: f"goal({mid}, {_atom(body.get('target', 'unknown'))}, {body.get('value', 0)}, {_atom(body.get('window', 'unknown'))})",
        "Sense":       lambda: f"sense({mid}, {_atom(body.get('sensor', 'unknown'))}, {_atom(body.get('field', 'unknown'))}, {body.get('reading', 0)})",
        "Consent":     lambda: f"consent({_atom(body.get('actor', mid))}, {_atom(body.get('to', 'unknown'))}, {_atom(body.get('action', 'unknown'))})",
        "Stewardship": lambda: f"stewardship({_atom(body.get('owner', mid))}, {_atom(body.get('steward', 'unknown'))}, {_atom(body.get('asset', 'unknown'))})",
        "Transform":   lambda: f"transform({mid}, {_atom(body.get('src', 'unknown'))}, {_atom(body.get('dst', 'unknown'))})",
    }

    fn = handlers.get(verb)
    return fn() if fn else f"writ({mid}, {_atom(verb)})"

# ── Writ endpoint ─────────────────────────────────────────────────────────────

ALLOWED_VERBS = {
    "Tick", "Log", "Audit", "Alert", "Witness",
    "Goal", "Sense", "Consent", "Stewardship", "Transform",
}


class WritRequest(BaseModel):
    mirror_id:    str = "heartbeat"
    verb:         str
    body:         dict[str, Any] = {}
    origin_agent: str | None = None


@app.post("/writ")
async def receive_writ(req: WritRequest):
    if req.verb not in ALLOWED_VERBS:
        raise HTTPException(status_code=422, detail=f"Unknown verb '{req.verb}'. Allowed: {sorted(ALLOWED_VERBS)}. See POST /writ for usage.")

    ts     = datetime.now(tz=timezone.utc).isoformat()
    agent  = req.origin_agent or req.mirror_id
    action = f"assert({_to_action(req.verb, agent, req.body)})"
    details = json.dumps({"verb": req.verb, "body": req.body, "prev_hash": _head_hash()})

    conn   = _conn()
    cur    = conn.execute(
        "INSERT INTO log (timestamp, agent, status, action, details) VALUES (?, ?, ?, ?, ?)",
        (ts, agent, "ASSERTED", action, details),
    )
    row_id = cur.lastrowid
    conn.commit()

    if req.origin_agent and req.origin_agent not in ("heartbeat",):
        conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES ('last_author_relay_ts', ?)", (ts,)
        )
        conn.commit()

    conn.close()
    return {"status": "ASSERTED", "id": row_id, "action": action}

# ── Read API ──────────────────────────────────────────────────────────────────

def _mode() -> str:
    conn = _conn()
    row  = conn.execute("SELECT value FROM meta WHERE key='last_author_relay_ts'").fetchone()
    conn.close()
    if not row:
        return "failover"
    age = (datetime.now(tz=timezone.utc) - datetime.fromisoformat(row["value"])).total_seconds()
    return "live" if age < FAILOVER_S else "failover"


@app.get("/chain/status")
async def chain_status():
    conn   = _conn()
    height = conn.execute("SELECT COUNT(*) FROM log").fetchone()[0]
    last   = conn.execute(
        "SELECT timestamp, agent, action FROM log ORDER BY id DESC LIMIT 1"
    ).fetchone()
    conn.close()
    return {
        "height":     height,
        "last_ts":    last["timestamp"] if last else None,
        "last_agent": last["agent"] if last else None,
        "head_hash":  _head_hash(),
        "mode":       _mode(),
    }


@app.get("/chain/block/{query}")
async def chain_block(query: str):
    conn = _conn()
    if query.isdigit():
        row = conn.execute(
            "SELECT * FROM log ORDER BY id LIMIT 1 OFFSET ?", (int(query) - 1,)
        ).fetchone()
    elif len(query) == 64:
        rows = conn.execute("SELECT * FROM log ORDER BY id").fetchall()
        row  = next((r for r in rows if _row_hash(dict(r)) == query), None)
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
        "type":      "tick" if d["agent"] == "heartbeat" else "assert",
        "signed_by": f"{d['agent']}-tick" if d["agent"] == "heartbeat" else "author",
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
        results.append({
            "height":    d["id"],
            "hash":      _row_hash(d),
            "prev_hash": details.get("prev_hash", "unknown"),
            "agent":     d["agent"],
            "timestamp": d["timestamp"],
        })

    return {"range": [from_h, to_h], "blocks": results, "count": len(results)}

# ── Bundle intake (author relay) ──────────────────────────────────────────────

class BundleRow(BaseModel):
    timestamp: str
    agent:     str
    status:    str
    action:    str
    details:   str


@app.post("/writ/batch")
async def writ_batch(rows: list[BundleRow]):
    """Accepts a bundle of raw log rows from the author node relay."""
    if not rows:
        return {"relayed": 0}

    conn    = _conn()
    relayed = 0
    for row in rows:
        try:
            details = json.loads(row.details)
        except Exception:
            details = {"raw": row.details}
        details["prev_hash"]    = _head_hash()
        details["relayed_from"] = "author-node"

        conn.execute(
            "INSERT INTO log (timestamp, agent, status, action, details) VALUES (?, ?, ?, ?, ?)",
            (row.timestamp, row.agent, row.status, row.action, json.dumps(details)),
        )
        conn.commit()
        relayed += 1

    ts = datetime.now(tz=timezone.utc).isoformat()
    conn.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('last_author_relay_ts', ?)", (ts,)
    )
    conn.commit()
    conn.close()
    return {"relayed": relayed, "ts": ts}

# ── Startup ───────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    CHAIN_DB.parent.mkdir(parents=True, exist_ok=True)
    _init_db()
    height = _conn().execute("SELECT COUNT(*) FROM log").fetchone()[0]
    print(f"chain  db={CHAIN_DB}  height={height}  mode={_mode()}")
