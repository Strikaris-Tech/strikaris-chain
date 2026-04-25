"""
author.py -- Author node relay: pushes writs from a local chain DB to a
remote chain node via SSH.

Runs as a daemon on the author machine (Pi, workstation, etc.).
Every RELAY_INTERVAL seconds:
  1. Copy the source DB out of the forge container (docker cp)
  2. Read new rows since last_relayed_id
  3. Filter out personal entries (SKIP_AGENTS)
  4. POST batch to remote chain node via SSH tunnel

The relay path:
  author.py
    → docker cp <container>:<db_path> /tmp/relay_src.db
    → SELECT rows WHERE id > last_relayed_id AND agent NOT IN (SKIP_AGENTS)
    → ssh <remote> "curl -s POST http://127.0.0.1:7333/writ/batch -d @-"

Usage:
  python author.py [--remote HOST] [--interval SECONDS] [--container NAME]

Environment:
  RELAY_REMOTE       SSH host alias for remote chain node (required)
  RELAY_INTERVAL     Relay interval in seconds (default: 300)
  RELAY_SKIP_AGENTS  Comma-separated agent names to exclude from relay (default: none)
  FORGE_CONTAINER    Docker container name holding source DB (default: mirroros-forge-1)
  CONTAINER_DB       Path to DB inside container (default: /app/mrs/memory/reasoning.db)
  TEMP_DB            Local temp path for docker cp output (default: /tmp/relay_src.db)
  RELAY_STATE        Path to state file (default: .author_state.json)
  MIRROR_RELAY_URL   Remote relay endpoint (default: http://127.0.0.1:7333/relay/batch)
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

_remote_env = os.getenv("RELAY_REMOTE", "")
if not _remote_env:
    print("ERROR: RELAY_REMOTE environment variable is required (SSH host alias for the remote chain node)")
    sys.exit(1)
DEFAULT_REMOTE      = _remote_env
DEFAULT_INTERVAL    = int(os.getenv("RELAY_INTERVAL", 300))
DEFAULT_SKIP        = os.getenv("RELAY_SKIP_AGENTS",  "")
DEFAULT_CONTAINER   = os.getenv("FORGE_CONTAINER",    "mirroros-forge-1")
DEFAULT_CONTAIN_DB  = os.getenv("CONTAINER_DB",       "/app/mrs/memory/reasoning.db")
DEFAULT_TEMP_DB     = os.getenv("TEMP_DB",            "/tmp/relay_src.db")
STATE_FILE          = Path(os.getenv("RELAY_STATE",   str(Path(__file__).parent / ".author_state.json")))
MIRROR_RELAY_URL    = os.getenv("MIRROR_RELAY_URL",   "http://127.0.0.1:7333/writ/batch")

# ── State ─────────────────────────────────────────────────────────────────────

def _load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {"last_relayed_id": 0, "total_relayed": 0}


def _save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))

# ── DB copy ───────────────────────────────────────────────────────────────────

def _copy_db(container: str, container_path: str, temp_path: str) -> bool:
    result = subprocess.run(
        ["docker", "cp", f"{container}:{container_path}", temp_path],
        capture_output=True, timeout=30,
    )
    if result.returncode != 0:
        print(f"[author] docker cp failed: {result.stderr.decode().strip()}", flush=True)
        return False
    return True

# ── Row fetch ─────────────────────────────────────────────────────────────────

def _fetch_rows(db_path: str, since_id: int, skip_agents: set[str]) -> list[dict]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, timestamp, agent, status, action, details "
        "FROM log WHERE id > ? ORDER BY id ASC",
        (since_id,),
    ).fetchall()
    conn.close()
    return [
        dict(r) for r in rows
        if r["agent"] not in skip_agents
    ]

# ── Relay ─────────────────────────────────────────────────────────────────────

def _relay(rows: list[dict], remote: str) -> tuple[bool, int]:
    payload = json.dumps([
        {
            "timestamp": r["timestamp"],
            "agent":     r["agent"],
            "status":    r["status"],
            "action":    r["action"],
            "details":   r["details"],
        }
        for r in rows
    ]).encode()

    cmd = [
        "ssh", remote,
        f"curl -s -X POST {MIRROR_RELAY_URL} "
        f"-H 'Content-Type: application/json' -d @-",
    ]
    result = subprocess.run(cmd, input=payload, capture_output=True, timeout=60)

    if result.returncode != 0:
        print(f"[author] SSH error: {result.stderr.decode().strip()}", flush=True)
        return False, 0

    try:
        resp = json.loads(result.stdout)
        return True, resp.get("relayed", 0)
    except Exception as e:
        print(f"[author] Bad response: {result.stdout.decode().strip()} -- {e}", flush=True)
        return False, 0

# ── Main loop ─────────────────────────────────────────────────────────────────

def run(remote: str, interval: int, container: str,
        container_db: str, temp_db: str, skip_agents: set[str]) -> None:
    state = _load_state()
    print(
        f"author  remote={remote}  interval={interval}s  "
        f"skip={sorted(skip_agents)}  last_id={state['last_relayed_id']}",
        flush=True,
    )
    print("-" * 60, flush=True)

    while True:
        ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        if not _copy_db(container, container_db, temp_db):
            print(f"[{ts}]  copy failed -- retry in {interval}s", flush=True)
            time.sleep(interval)
            continue

        rows = _fetch_rows(temp_db, state["last_relayed_id"], skip_agents)
        if not rows:
            print(f"[{ts}]  no new rows (head id={state['last_relayed_id']})", flush=True)
            time.sleep(interval)
            continue

        # Track the highest id fetched (including skipped), so we don't re-fetch them.
        # Re-read without filter just to get the true last id.
        conn = sqlite3.connect(temp_db)
        all_new = conn.execute(
            "SELECT MAX(id) FROM log WHERE id > ?", (state["last_relayed_id"],)
        ).fetchone()[0] or state["last_relayed_id"]
        conn.close()

        print(f"[{ts}]  fetched {len(rows)} row(s) to relay", flush=True)

        ok, count = _relay(rows, remote)
        if ok:
            state["last_relayed_id"] = all_new
            state["total_relayed"]  += count
            _save_state(state)
            print(f"[{ts}]  relayed {count}  total={state['total_relayed']}", flush=True)
        else:
            print(f"[{ts}]  relay failed -- state unchanged", flush=True)

        time.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser(description="author.py -- Strikaris Chain author relay")
    parser.add_argument("--remote",    default=DEFAULT_REMOTE)
    parser.add_argument("--interval",  type=int, default=DEFAULT_INTERVAL)
    parser.add_argument("--container", default=DEFAULT_CONTAINER)
    parser.add_argument("--skip",      default=DEFAULT_SKIP,
                        help="Comma-separated agent names to exclude from relay")
    args = parser.parse_args()

    skip_agents = {a.strip() for a in args.skip.split(",") if a.strip()}

    try:
        run(
            remote=args.remote,
            interval=args.interval,
            container=args.container,
            container_db=DEFAULT_CONTAIN_DB,
            temp_db=DEFAULT_TEMP_DB,
            skip_agents=skip_agents,
        )
    except KeyboardInterrupt:
        print("\nauthor stopped.", flush=True)
        sys.exit(0)


if __name__ == "__main__":
    main()
