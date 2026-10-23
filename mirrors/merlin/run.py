"""
Merlin — the metronome mirror.

Emits a Tick pulse at a fixed interval.  On each tick, evaluates three
schedule types and emits wake-triggered pulses:

  schedule/3      tick-based interval  — seq % period == 0
  time_window/5   wall-clock window    — emits start/stop on transitions
  daily_at/3      one-shot per day     — fires once when clock reaches hour

Drift guard: compares wall-clock time to expected tick time on each cycle.
If abs(drift) exceeds max_drift seconds, emits an Alert pulse (once per
drifting run — suppressed until drift clears).

Phase 1: all three schedule types evaluated in Python from this file's
         SCHEDULE / TIME_WINDOWS / DAILY_AT tables. Keep in sync with
         prolog/tempo.pl facts.
Phase 2: wire wake/2, in_window/2, daily_at_due/2 queries via MRS bridge.

Usage:
    python mrs/mirrors/merlin/run.py [--interval 10] [--forge http://localhost:8765]

Environment:
    MERLIN_INTERVAL   tick interval in seconds (default: 10 in dev)
    FORGE_URL         Forge API base URL (default: http://localhost:8765)
    MERLIN_MAX_DRIFT  drift threshold in seconds before Alert fires (default: 5)
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────

DEFAULT_FORGE_URL = "http://localhost:8765"
DEFAULT_INTERVAL  = 10    # seconds — 10 s in dev, 60 s in prod
DEFAULT_MAX_DRIFT = 5     # seconds before drift Alert fires

STATE_FILE = Path(__file__).parent / "merlin_state.json"

# ── Schedule tables ───────────────────────────────────────────────────────────
# Mirror prolog/tempo.pl facts — keep in sync.

# schedule/3 — tick-based interval
# { period (ticks), mirror_id, job_id, verb, body }
SCHEDULE: list[dict] = [
    {
        "job_id":    "sixbeat",
        "period":    6,
        "mirror_id": "merlin",
        "verb":      "Goal",
        "body": {
            "target": "tempo_mark",
            "value":  1,
            "window": "1d",
            "guard":  {},
        },
    },
]

# time_window/5 — wall-clock window (start/stop transitions)
# { mirror_id, job_id, days, start_hour, end_hour, verb, body }
# days: "all" | "weekdays" | "weekends"
# start_hour inclusive, end_hour exclusive (24h local time)
TIME_WINDOWS: list[dict] = [
    {
        "mirror_id":  "zeek",
        "job_id":     "slack_holder",
        "days":       "weekdays",
        "start_hour": 8,
        "end_hour":   17,
        "verb":       "Goal",
        "body":       {"target": "slack_holder"},
    },
]

# daily_at/3 — one-shot per day at a specific local hour
# { mirror_id, job_id, hour, verb, body }
DAILY_AT: list[dict] = [
    # Example (uncomment to enable):
    # {
    #     "mirror_id": "cindrell",
    #     "job_id":    "bill_check",
    #     "hour":      8,
    #     "verb":      "Goal",
    #     "body":      {"target": "bill_check"},
    # },
]

# ── HTTP helper ───────────────────────────────────────────────────────────────

def post_pulse(forge_url: str, mirror_id: str, verb: str, body: dict) -> dict:
    payload = json.dumps({"mirror_id": mirror_id, "verb": verb, "body": body}).encode()
    req = urllib.request.Request(
        f"{forge_url}/pulse",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        try:
            detail = json.loads(e.read()).get("detail", str(e))
        except Exception:
            detail = str(e)
        return {"status": "ERROR", "error": detail}
    except Exception as e:
        return {"status": "ERROR", "error": str(e)}

# ── State persistence ─────────────────────────────────────────────────────────

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {
        "seq":           0,
        "prev_ts":       None,
        "drift_alerted": False,
        "window_state":  {},   # { "mirror:job": bool } — currently active windows
        "daily_fired":   {},   # { "mirror:job": "YYYY-MM-DD" } — last fired date
    }


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))

# ── Drift guard ───────────────────────────────────────────────────────────────

def check_drift(
    forge_url: str,
    now_ts: float,
    prev_ts: float | None,
    interval: float,
    max_drift: float,
    drift_alerted: bool,
) -> bool:
    if prev_ts is None:
        return False

    expected = prev_ts + interval
    drift = abs(now_ts - expected)

    if drift > max_drift:
        if not drift_alerted:
            result = post_pulse(forge_url, "merlin", "Alert", {
                "level":   "amber",
                "subject": "tick_drift",
                "value":   round(drift, 2),
                "guard":   {"max_drift": max_drift},
            })
            print(f"  ⚠ Drift  {drift:.2f}s > {max_drift}s  Alert {result.get('status', '?')}")
        return True
    return False

# ── Window evaluation ─────────────────────────────────────────────────────────

def _in_day_set(days: str) -> bool:
    """Return True if today matches the day-set atom."""
    weekday = datetime.now().weekday()   # 0=Mon … 6=Sun
    if days == "all":
        return True
    if days == "weekdays":
        return weekday < 5
    if days == "weekends":
        return weekday >= 5
    return False


def evaluate_windows(
    forge_url: str,
    seq: int,
    window_state: dict,
) -> dict:
    """
    For each time_window entry, detect open/close transitions and emit
    Goal pulses with action="start" or action="stop".

    Args:
        forge_url:    Forge API base URL
        seq:          Current tick sequence number
        window_state: Mutable dict { "mirror:job": bool } tracking active windows

    Returns:
        Updated window_state dict
    """
    now = datetime.now()
    hour = now.hour

    for job in TIME_WINDOWS:
        key = f"{job['mirror_id']}:{job['job_id']}"
        currently_active = window_state.get(key, False)
        should_be_active = (
            job["start_hour"] <= hour < job["end_hour"]
            and _in_day_set(job["days"])
        )

        if should_be_active and not currently_active:
            body = {**job["body"], "action": "start", "_wake_seq": seq}
            result = post_pulse(forge_url, job["mirror_id"], job["verb"], body)
            print(
                f"  └ Window {job['job_id']:<16}  start  "
                f"{result.get('status', '?')}  {result.get('action_id', '')}"
            )
            window_state[key] = True

        elif not should_be_active and currently_active:
            body = {**job["body"], "action": "stop", "_wake_seq": seq}
            result = post_pulse(forge_url, job["mirror_id"], job["verb"], body)
            print(
                f"  └ Window {job['job_id']:<16}  stop   "
                f"{result.get('status', '?')}  {result.get('action_id', '')}"
            )
            window_state[key] = False

    return window_state

# ── Daily-at evaluation ───────────────────────────────────────────────────────

def evaluate_daily_at(
    forge_url: str,
    seq: int,
    daily_fired: dict,
) -> dict:
    """
    For each daily_at entry, fire a Goal pulse once per day when the
    current local hour matches the scheduled hour.

    Args:
        forge_url:   Forge API base URL
        seq:         Current tick sequence number
        daily_fired: Mutable dict { "mirror:job": "YYYY-MM-DD" } tracking fired dates

    Returns:
        Updated daily_fired dict
    """
    now = datetime.now()
    today = now.strftime("%Y-%m-%d")
    hour = now.hour

    for job in DAILY_AT:
        key = f"{job['mirror_id']}:{job['job_id']}"
        if hour == job["hour"] and daily_fired.get(key) != today:
            body = {**job["body"], "_wake_seq": seq, "_job": job["job_id"]}
            result = post_pulse(forge_url, job["mirror_id"], job["verb"], body)
            print(
                f"  └ Daily  {job['job_id']:<16}  "
                f"{result.get('status', '?')}  {result.get('action_id', '')}"
            )
            daily_fired[key] = today

    return daily_fired

# ── Main loop ─────────────────────────────────────────────────────────────────

def run(forge_url: str, interval: float, max_drift: float) -> None:
    state        = load_state()
    seq          = state.get("seq", 0)
    prev_ts      = state.get("prev_ts")
    drift_alerted= state.get("drift_alerted", False)
    window_state = state.get("window_state", {})
    daily_fired  = state.get("daily_fired", {})

    print(f"Merlin  forge={forge_url}  interval={interval}s  seq_start={seq}  max_drift={max_drift}s")
    print("─" * 60)

    while True:
        seq += 1
        now_ts = time.time()
        ts = datetime.fromtimestamp(now_ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        # ── Drift guard ───────────────────────────────────────────────────────
        drift_alerted = check_drift(
            forge_url, now_ts, prev_ts, interval, max_drift, drift_alerted
        )

        # ── Tick ──────────────────────────────────────────────────────────────
        tick_result = post_pulse(forge_url, "merlin", "Tick", {
            "seq":    seq,
            "ts_utc": ts,
        })
        status = tick_result.get("status", "?")
        action = tick_result.get("action_id", "")
        print(f"[{ts}]  Tick  seq={seq:<6}  {status}  {action}")

        # ── Tick-based wake evaluation ─────────────────────────────────────
        for job in SCHEDULE:
            if seq % job["period"] == 0:
                body = {**job["body"], "_wake_seq": seq, "_job": job["job_id"]}
                wake_result = post_pulse(forge_url, job["mirror_id"], job["verb"], body)
                print(
                    f"  └ Wake  {job['job_id']:<16}"
                    f"  {job['verb']:<12}"
                    f"  {wake_result.get('status', '?')}  {wake_result.get('action_id', '')}"
                )

        # ── Window-based evaluation ────────────────────────────────────────
        window_state = evaluate_windows(forge_url, seq, window_state)

        # ── Daily one-shot evaluation ──────────────────────────────────────
        daily_fired = evaluate_daily_at(forge_url, seq, daily_fired)

        # ── Persist state ──────────────────────────────────────────────────
        save_state({
            "seq":           seq,
            "prev_ts":       now_ts,
            "drift_alerted": drift_alerted,
            "window_state":  window_state,
            "daily_fired":   daily_fired,
        })
        time.sleep(interval)


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    import os

    parser = argparse.ArgumentParser(
        description="Merlin — MirrorOS metronome mirror",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python run.py                        # 10 s dev tick\n"
            "  python run.py --interval 60          # 1 min prod tick\n"
            "  python run.py --forge http://srv:8765\n"
        ),
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=float(os.getenv("MERLIN_INTERVAL", DEFAULT_INTERVAL)),
        help="Tick interval in seconds (default: 10)",
    )
    parser.add_argument(
        "--forge",
        default=os.getenv("FORGE_URL", DEFAULT_FORGE_URL),
        help="Forge API base URL (default: http://localhost:8765)",
    )
    parser.add_argument(
        "--max-drift",
        type=float,
        default=float(os.getenv("MERLIN_MAX_DRIFT", DEFAULT_MAX_DRIFT)),
        help="Drift threshold in seconds before Alert fires (default: 5)",
    )
    args = parser.parse_args()

    try:
        run(forge_url=args.forge, interval=args.interval, max_drift=args.max_drift)
    except KeyboardInterrupt:
        print("\nMerlin stopped.")
        sys.exit(0)


if __name__ == "__main__":
    main()
