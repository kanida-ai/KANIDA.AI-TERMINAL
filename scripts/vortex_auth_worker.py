"""Standalone Vortex auth worker — fresh short-lived process (mirrors auth_worker.py).
Task Scheduler runs this every 30 min (self-gates to IST window). Each run:
  1. load config/.env   2. skip if token already valid   3. else one Playwright login →
  exchange → store   4. exit (0 healthy, 1 failed).
"""
from __future__ import annotations
import asyncio, os, sys, logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_PROJECT = _HERE.parent
_BACKEND = _PROJECT / "backend"
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))
IST = timezone(timedelta(hours=5, minutes=30))
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s", stream=sys.stdout)
log = logging.getLogger("kanida.vortex_auth_worker")


def _load_env(p: Path):
    if not p.exists(): return
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1); os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def main() -> int:
    _load_env(_PROJECT / "config" / ".env")
    # PLAYWRIGHT browsers must live in the machine-wide path (Task Scheduler can't see user AppData)
    os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", r"C:\ProgramData\ms-playwright")
    now = datetime.now(IST); hm = now.hour * 60 + now.minute
    # self-gate: weekday 06:00-15:30 IST (token needed by 09:15; keep refreshing if it drops)
    if now.weekday() >= 5 or hm < 6 * 60 or hm > 15 * 60 + 30:
        log.info("vortex_auth: outside IST window (%s) — exit", now.strftime("%Y-%m-%d %H:%M"))
        return 0
    from services.vortex_auto_auth import run_auth_attempt, token_is_valid
    if token_is_valid():
        log.info("vortex_auth: token already valid — skip")
        return 0
    res = asyncio.run(run_auth_attempt())
    log.info("vortex_auth: status=%s stage=%s code=%s preview=%s elapsed=%dms detail=%s",
             res.status, res.stage, res.error_code, res.token_preview, res.elapsed_ms, res.error_detail or "")
    return 0 if res.status == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
