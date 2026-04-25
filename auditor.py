"""
auditor.py -- Chain integrity auditor for the Strikaris Chain.

Polls the chain DB on a fixed interval, verifies row-level hash integrity,
checks heartbeat liveness, emits an Audit writ, and writes a timestamped
anchor line to an anchor log.

Usage:
  python auditor.py [--chain URL] [--db PATH] [--interval SECONDS]

Environment:
  CHAIN_URL         Chain server URL (default: http://localhost:7333)
  CHAIN_DB          Path to chain SQLite DB (default: ./data/chain.db)
  AUDITOR_INTERVAL  Seconds between audit cycles (default: 300)
  ANCHOR_LOG        Path to anchor log file (default: ./data/chain_anchor.log)
  AUDITOR_AGENT     Agent ID for audit writs (default: auditor)
  SILENCE_LIMIT     Seconds of heartbeat silence before alert (default: 600)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sqlite3
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

DEFAULT_CHAIN_URL = "http://localhost:7333"
DEFAULT_DB        = Path("./data/chain.db")
DEFAULT_INTERVAL  = 300.0
DEFAULT_ANCHOR    = Path("./data/chain_anchor.log")
DEFAULT_AGENT     = "auditor"
DEFAULT_SILENCE   = 600

STATE_FILE = Path(__file__).parent / ".auditor_state.json"

# ── DB helpers ────────────────────────────────────────────────────────────────

def _db_conn(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _rows_since(db_path: Path, last_id: int) -> list[dict]:
    if not db_path.exists():
        return []
    try:
        conn = _db_conn(db_path)
        rows = conn.execute(
            "SELECT id, timestamp, agent, status, action, details "
            "FROM log WHERE id > ? ORDER BY id ASC",
            (last_id,),
        ).fetchall()
        conn.close()
        return [
            {"id": r[0], "timestamp": r[1], "agent": r[2],
             "status": r[3], "action": r[4],
             "details": json.loads(r[5]) if r[5] else {}}
            for r in rows
        ]
    except Exception as e:
        logger.error(f"DB read failed: {e}")
        return []


def _max_id(db_path: Path) -> int:
    if not db_path.exists():
        return 0
    try:
        conn = _db_conn(db_path)
        row  = conn.execute("SELECT MAX(id) FROM log").fetchone()
        conn.close()
        return row[0] or 0
    except Exception:
        return 0

# ── Chain verification ────────────────────────────────────────────────────────

def _row_hash(row: dict) -> str:
    entry = {k: row[k] for k in ("id", "timestamp", "agent", "status", "action", "details")}
    return hashlib.sha256(
        json.dumps(entry, sort_keys=True, default=str).encode()
    ).hexdigest()


def _check_hashes(rows: list[dict]) -> list[dict]:
    """Verify each row's prev_hash matches the computed hash of the row before it."""
    gaps = []
    for i, row in enumerate(rows):
        if i == 0:
            continue
        prev        = rows[i - 1]
        expected    = _row_hash(prev)
        actual      = row.get("details", {}).get("prev_hash", "")
        if actual and actual != expected:
            gaps.append({"index": row["id"], "expected": expected[:12], "actual": actual[:12]})
    return gaps


def _check_heartbeat(rows: list[dict], silence_limit: int) -> list[dict]:
    """Flag if no heartbeat tick has been seen within silence_limit seconds."""
    now   = datetime.now(timezone.utc)
    ticks = [r for r in rows if r.get("agent") == "heartbeat"]
    if not ticks:
        return [{"type": "no_ticks_in_window"}]
    last_ts = ticks[-1]["timestamp"].rstrip("Z").replace(" ", "T")
    if "+" not in last_ts:
        last_ts += "+00:00"
    try:
        age = (now - datetime.fromisoformat(last_ts)).total_seconds()
        if age > silence_limit:
            return [{"type": "heartbeat_silent", "age_seconds": round(age)}]
    except ValueError:
        pass
    return []

# ── Anchor ────────────────────────────────────────────────────────────────────

def _rolling_head(rows: list[dict], prev_head: str) -> str:
    h = hashlib.sha256(prev_head.encode())
    for r in rows:
        h.update(f"{r['id']}:{r['status']}:{r['agent']}:{r['timestamp']}".encode())
    return h.hexdigest()


def _write_anchor(anchor_log: Path, head: str, audit_count: int, end_id: int) -> None:
    ts   = datetime.now(timezone.utc).isoformat()
    line = f"{ts}  audit={audit_count}  end_id={end_id}  head={head}\n"
    try:
        anchor_log.parent.mkdir(parents=True, exist_ok=True)
        with open(anchor_log, "a") as f:
            f.write(line)
    except Exception as e:
        logger.warning(f"Anchor write failed: {e}")

# ── State ─────────────────────────────────────────────────────────────────────

def _load_state() -> dict:
    try:
        return json.loads(STATE_FILE.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {"last_db_id": 0, "audit_count": 0, "chain_head": "genesis"}


def _save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))

# ── Forge post ────────────────────────────────────────────────────────────────

def _post_writ(chain_url: str, agent: str, verb: str, body: dict) -> dict:
    payload = json.dumps({"mirror_id": agent, "verb": verb, "body": body}).encode()
    req = urllib.request.Request(
        f"{chain_url}/writ",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except Exception as e:
        return {"status": "ERROR", "error": str(e)}

# ── Audit cycle ───────────────────────────────────────────────────────────────

def _audit(chain_url: str, db_path: Path, anchor_log: Path,
           agent: str, silence_limit: int, state: dict) -> None:
    window = _rows_since(db_path, state["last_db_id"])
    if not window:
        return

    start_id = state["last_db_id"]
    end_id   = window[-1]["id"]
    alerts   = []

    hash_gaps = _check_hashes(window)
    hb_gaps   = _check_heartbeat(window, silence_limit)

    if hash_gaps:
        alerts.append({"level": "red", "summary": "hash_gap",
                        "value": len(hash_gaps),
                        "detail": f"first at id {hash_gaps[0].get('index')}"})
    if hb_gaps:
        alerts.append({"level": "amber", "summary": "heartbeat_gap",
                        "value": len(hb_gaps)})

    for a in alerts:
        _post_writ(chain_url, agent, "Alert", a)
        logger.info(f"  Alert {a['level']}  {a['summary']}")

    new_head = _rolling_head(window, state.get("chain_head", "genesis"))
    _write_anchor(anchor_log, new_head, state["audit_count"] + 1, end_id)

    result = _post_writ(chain_url, agent, "Audit", {
        "range":       [start_id, end_id],
        "dest":        "local",
        "status":      "anomalies" if alerts else "clean",
        "alert_count": len(alerts),
        "chain_head":  new_head,
    })

    state["last_db_id"]  = end_id
    state["audit_count"] += 1
    state["chain_head"]  = new_head
    _save_state(state)

    ts = time.strftime("%Y-%m-%dT%H:%M:%S")
    audit_status = "anomalies" if alerts else "clean"
    logger.info(
        f"[{ts}]  Audit #{state['audit_count']}  "
        f"range=[{start_id},{end_id}]  {audit_status}  "
        f"alerts={len(alerts)}  head={new_head[:12]}  {result.get('status', '?')}"
    )

# ── Main loop ─────────────────────────────────────────────────────────────────

def run(chain_url: str, db_path: Path, anchor_log: Path,
        interval: float, agent: str, silence_limit: int) -> None:
    state = _load_state()
    if "last_db_id" not in state:
        state["last_db_id"] = _max_id(db_path)

    logger.info(f"auditor  chain={chain_url}  db={db_path}  interval={interval}s")
    logger.info(f"         audits={state['audit_count']}  last_db_id={state['last_db_id']}")
    logger.info("-" * 60)

    _audit(chain_url, db_path, anchor_log, agent, silence_limit, state)

    while True:
        time.sleep(interval)
        _audit(chain_url, db_path, anchor_log, agent, silence_limit, state)


def main() -> None:
    parser = argparse.ArgumentParser(description="auditor.py -- Strikaris Chain integrity auditor")
    parser.add_argument("--chain",    default=os.getenv("CHAIN_URL",        DEFAULT_CHAIN_URL))
    parser.add_argument("--db",       default=os.getenv("CHAIN_DB",         str(DEFAULT_DB)))
    parser.add_argument("--interval", type=float, default=float(os.getenv("AUDITOR_INTERVAL", DEFAULT_INTERVAL)))
    parser.add_argument("--anchor",   default=os.getenv("ANCHOR_LOG",       str(DEFAULT_ANCHOR)))
    parser.add_argument("--agent",    default=os.getenv("AUDITOR_AGENT",    DEFAULT_AGENT))
    parser.add_argument("--silence",  type=int,   default=int(os.getenv("SILENCE_LIMIT",  DEFAULT_SILENCE)))
    args = parser.parse_args()

    try:
        run(
            chain_url=args.chain,
            db_path=Path(args.db),
            anchor_log=Path(args.anchor),
            interval=args.interval,
            agent=args.agent,
            silence_limit=args.silence,
        )
    except KeyboardInterrupt:
        logger.info("\nauditor stopped.")


if __name__ == "__main__":
    main()
