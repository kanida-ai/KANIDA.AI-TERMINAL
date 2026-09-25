"""Capture watchdog (launchd: com.kanida.capture-watchdog, every 10 minutes).

During the NSE session (Mon-Fri 09:30-15:40 IST, holidays not known here) it checks that the two live stores are
still receiving data and, when either falls more than 35 minutes behind, shows a macOS notification and writes a
line to logs/capture_watchdog.log. Read-only on the databases; it never restarts or mints anything.
  * db/derivatives.db  snapshots.captured_at   (F&O capture, 15-minute marks)
  * db/market15.db     candles_15m.bar_end     (NIFTY 500 15-minute candles)
State is kept in logs/capture_watchdog.state so a stale store alerts once per hour, not every run.
"""
from __future__ import annotations
import json, sqlite3, subprocess, sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[2]
IST = ZoneInfo('Asia/Kolkata')
LOG = ROOT / 'logs' / 'capture_watchdog.log'
STATE = ROOT / 'logs' / 'capture_watchdog.state'
STORES = [('F&O capture', ROOT / 'db' / 'derivatives.db', 'select max(captured_at) from snapshots'),
          ('Stock 15-min candles', ROOT / 'db' / 'market15.db', 'select max(bar_end) from candles_15m')]
LAG = timedelta(minutes=35)


def newest(db: Path, sql: str):
    try:
        c = sqlite3.connect(f'file:{db}?mode=ro', uri=True, timeout=10)
        v = c.execute(sql).fetchone()[0]; c.close()
    except sqlite3.Error as e:
        return None, f'unreadable ({type(e).__name__})'
    if not v:
        return None, 'empty'
    return datetime.fromisoformat(str(v)[:19].replace('T', ' ')), None


def notify(title: str, msg: str) -> None:
    safe = msg.replace('"', "'")
    subprocess.run(['osascript', '-e', f'display notification "{safe}" with title "{title}" sound name "Basso"'], check=False)


def main() -> int:
    now = datetime.now(IST).replace(tzinfo=None)
    LOG.parent.mkdir(parents=True, exist_ok=True)
    in_session = now.weekday() < 5 and (9, 30) <= (now.hour, now.minute) <= (15, 40)
    state = json.loads(STATE.read_text()) if STATE.exists() else {}
    lines = []
    for name, db, sql in STORES:
        t, err = newest(db, sql)
        lag = (now - t) if t else None
        stale = in_session and (err is not None or lag > LAG)
        lines.append(f"{now:%Y-%m-%d %H:%M} {name}: newest={t} lag={str(lag).split('.')[0] if lag else err} "
                     f"{'STALE' if stale else 'ok' if in_session else 'market closed'}")
        last = state.get(name)
        if stale and (not last or now - datetime.fromisoformat(last) > timedelta(hours=1)):
            notify('KANIDA capture stopped', f"{name}: newest data {t or err} ({str(lag).split('.')[0] if lag else ''} behind). "
                   'Check: bash migration/mac/4_services.sh status')
            state[name] = now.isoformat()
        elif not stale:
            state.pop(name, None)
    STATE.write_text(json.dumps(state))
    with LOG.open('a') as f:
        f.write('\n'.join(lines) + '\n')
    return 0


if __name__ == '__main__':
    sys.exit(main())
