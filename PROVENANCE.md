# Provenance — server-deployed programs backfilled 2026-10-23

These two programs run in production on strikaris-prime (EC2, us-east-1) but were
never committed to any repository. They were captured read-only on 2026-10-23 via
AWS Systems Manager Run Command (gzip + base64 transfer) and verified
byte-identical against the live files by SHA-256 before commit. The live files
remain the running copies; from this commit forward, this repository is the
canonical source (repo-first rule: change here, then deploy).

## mirror_node.py
- Deployed path: /opt/chain/mirror_node.py (file dated Apr 24, 2026)
- SHA-256: 2a35dff8bb76adf86d06e5edd458f113befe3155b4d942e7fc21d52b120772e9 (10,610 bytes)
- Role: the strikaris-chain mirror node behind verify.strikaris.com —
  POST /pulse, GET /chain/status, GET /chain/block/{query},
  GET /chain/verify/{from}/{to}, POST /relay/batch (localhost-only Pi intake).
- Author: Brandon Lee Zeek ("TheArchitect"). Deployed directly to the server;
  no .git checkout existed on the server, and org-wide + GitHub-global
  fingerprint searches on 2026-10-23 found this code in no repository.

## mirrors/merlin/run.py
- Deployed path: /opt/chain/mrs/mirrors/merlin/run.py (Apr 2026)
- SHA-256: e55f73c9167abe496b9266ef9279198bdef8900db9fc707628bd16ef6ca7cb8c (14,070 bytes)
- Role: "Merlin — the metronome mirror," the heartbeat agent that posts pulses
  to a Forge API (default http://localhost:8765, configurable).
- Author: Brandon Lee Zeek. Same deployment history as mirror_node.py.

Neither file contains credentials (scanned before commit, 2026-10-23).
