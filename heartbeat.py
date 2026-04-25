"""
heartbeat.py -- Liveness ticker for the Strikaris Chain.

Emits a Tick writ at a fixed interval to keep the chain extending
during periods when no author writs are flowing.

Usage:
  python heartbeat.py [--chain URL] [--interval SECONDS] [--agent AGENT_ID]

Environment:
  CHAIN_URL           Chain server URL (default: http://localhost:7333)
  HEARTBEAT_INTERVAL  Tick interval in seconds (default: 60)
  HEARTBEAT_AGENT     Agent ID written into each tick (default: heartbeat)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_CHAIN_URL = "http://localhost:7333"
DEFAULT_INTERVAL  = 60
DEFAULT_AGENT     = "heartbeat"

STATE_FILE = Path(__file__).parent / ".heartbeat_state.json"


def _load_seq() -> int:
    try:
        return json.loads(STATE_FILE.read_text()).get("seq", 0)
    except (FileNotFoundError, json.JSONDecodeError):
        return 0


def _save_seq(seq: int) -> None:
    STATE_FILE.write_text(json.dumps({"seq": seq}))


def _tick(chain_url: str, agent: str, seq: int, ts: str) -> dict:
    payload = json.dumps({
        "mirror_id": agent,
        "verb":      "Tick",
        "body":      {"seq": seq, "ts_utc": ts},
    }).encode()
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


def run(chain_url: str, interval: float, agent: str) -> None:
    seq = _load_seq()
    print(f"heartbeat  chain={chain_url}  interval={interval}s  agent={agent}  seq_start={seq}")
    print("-" * 60)

    while True:
        seq += 1
        ts     = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        result = _tick(chain_url, agent, seq, ts)
        status = result.get("status", "?")
        print(f"[{ts}]  Tick  seq={seq:<6}  {status}", flush=True)
        _save_seq(seq)
        time.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser(description="heartbeat.py -- Strikaris Chain liveness ticker")
    parser.add_argument("--chain",    default=os.getenv("CHAIN_URL",          DEFAULT_CHAIN_URL))
    parser.add_argument("--interval", type=float, default=float(os.getenv("HEARTBEAT_INTERVAL", DEFAULT_INTERVAL)))
    parser.add_argument("--agent",    default=os.getenv("HEARTBEAT_AGENT",    DEFAULT_AGENT))
    args = parser.parse_args()

    try:
        run(args.chain, args.interval, args.agent)
    except KeyboardInterrupt:
        print("\nheartbeat stopped.")
        sys.exit(0)


if __name__ == "__main__":
    main()
