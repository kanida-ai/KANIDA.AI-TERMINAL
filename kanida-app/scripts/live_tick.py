"""One audit tick for one reading: capture timing, store recomputation (live_audit_mark), snapshot + S/N worker."""
import re, sqlite3, subprocess, sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
mark = sys.argv[1]
hhmm = mark[11:16]
log = (ROOT / 'logs' / 'derivatives_capture_service.log').read_text(encoding='utf-8', errors='replace').splitlines()
cap = next((l for l in reversed(log) if f'{mark} [' in l), None)
print('CAPTURE', (re.search(r'(ok|error|failed).*?(\d+ rows).*?([\d.]+s wall)', cap).groups() if cap else 'NOT FOUND'),
      cap[:23] if cap else '')
mlog = (ROOT / 'logs' / 'metrics_loop_service.log').read_text(encoding='utf-8', errors='replace').splitlines()
print('METRICS', next((l[:60] for l in reversed(mlog) if f'{mark} ->' in l), 'NOT YET'))
out = subprocess.run([str(ROOT / 'market_scanner/.venv/Scripts/python.exe'), str(ROOT / 'kanida-app/scripts/live_audit_mark.py'),
                      mark, 'NIFTY', 'BANKNIFTY', 'RELIANCE', 'HDFCBANK'], capture_output=True, text=True).stdout
for l in out.splitlines():
    if any(k in l for k in ('coverage', 'underlyings:', 'frozen', 'absent', 'mismatches', 'MISMATCH', 'FAIL')):
        print('STORE', l.strip()[:170])
d = sqlite3.connect(ROOT / 'kanida-app/var/intelligence.db')
ev = d.execute('select max(engine_version) from reading_snapshots').fetchone()[0]
print('SNAPSHOTS', d.execute("select count(*),sum(status='ok'),min(created_at) from reading_snapshots where reading_at=? and engine_version=?", (mark, ev)).fetchone(), ev)
r = d.execute("select chained from reading_snapshots where reading_at=? and underlying='NIFTY' and engine_version=?", (mark, ev)).fetchone()
if r and r[0]:
    import json; s = json.loads(r[0])
    print('NIFTY', s['state'], '|', s['plain_language_read'], '|', (s.get('persistence_since') or '')[11:16], '|', s.get('plain_language_evidence', '')[:150])
print('S/N', d.execute('select count(*) from insight_records').fetchone()[0], 'records;',
      d.execute('select final,count(*) from insight_verdicts group by final').fetchall())
