#!/usr/bin/env bash
# Step 5: prove the Mac copy matches Windows before anything is switched over.
#   bash 5_verify.sh            (uses the manifest copied off the SSD in step 2)
# Exit code 0 = every check passed.
set -uo pipefail
source "$(cd "$(dirname "$0")" && pwd)/config.sh"
fail=0; ok() { printf '   \033[32mok\033[0m   %s\n' "$*"; }; bad() { printf '   \033[31mFAIL\033[0m %s\n' "$*"; fail=1; }

say "Databases vs the Windows manifest (size + max(rowid) of every table)"
python3 - "$MIG/manifest/dbs.jsonl" <<'PY' || fail=1
import json, os, sqlite3, sys
from pathlib import Path
K = Path.home() / "Kanida"
ROOTS = {"files/Kanida_Falcon": K / "Kanida_Falcon", "files/engine": K / "engine",
         "files/KANIDA.AI_TERMINAL": K / "KANIDA.AI_TERMINAL", "files/_kanida_deploy": K / "_kanida_deploy"}
PERSONAL = set(os.environ.get("PERSONAL", "").split("|"))
def where(root):                 # mirrors route() in config.sh
    if root in ROOTS: return ROOTS[root]
    if not root.startswith("files/archive/"): return None
    parts = root[len("files/archive/"):].split("/")
    if parts[0] == "Downloads": return Path.home() / "Downloads" / "From Windows" / Path(*parts[1:]) if parts[1:] else Path.home() / "Downloads" / "From Windows"
    if parts[0] in ("Desktop", "Documents") and len(parts) > 1 and parts[1] in PERSONAL:
        return Path.home() / Path(*parts)
    return K / "archive" / Path(*parts)
latest = {}                      # a re-snapshot at cutover appends; the last record wins
for line in open(sys.argv[1], encoding="utf-8"):
    r = json.loads(line)
    if Path(r["path"]).name.startswith("._"):   # macOS AppleDouble junk; the restore skips these on purpose
        continue
    latest[(r.get("root"), r["path"])] = r
n = bad = 0
for r in latest.values():
    n += 1
    base = where(r.get("root", ""))
    p = base / r["path"] if base else None
    if not p or not p.exists():
        print(f"   FAIL missing {r.get('root')}/{r['path']}"); bad += 1; continue
    if p.stat().st_size != r["bytes"]:
        print(f"   FAIL size {p} {p.stat().st_size} != {r['bytes']}"); bad += 1; continue
    if r["kind"] == "sqlite":
        con = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
        for t, want in r["tables"].items():
            try: got = con.execute(f'SELECT max(rowid) FROM "{t}"').fetchone()[0]
            except sqlite3.Error: got = None
            if got != want:
                print(f"   FAIL {p.name}.{t}: max(rowid) {got} != {want}"); bad += 1
        con.close()
print(f"   {n} database files checked, {bad} problems")
sys.exit(1 if bad else 0)
PY

say "Integrity check on the live-path databases (quick_check)"
for db in "$FALCON/db/derivatives.db" "$FALCON/db/market15.db" "$FALCON/kanida-app/var/intelligence.db" "$ENGINE/data/db/kanida_universe.db"; do
  [ -f "$db" ] || { bad "missing $db"; continue; }
  r=$(sqlite3 "file:$db?mode=ro" 'PRAGMA quick_check;' 2>&1 | head -1); [ "$r" = "ok" ] && ok "$(basename "$db")" || bad "$(basename "$db"): $r"
done

say "Secrets present (names only)"
for f in "$FALCON/market_data/.env" "$FALCON/kanida-app/.env.pilot" "$FALCON/kanida-app/var/pilot.key" "$ENGINE/config/.env" "$TERMINAL/backend/.env"; do
  [ -s "$f" ] && ok "${f#$K/}" || bad "missing ${f#$K/}"
done
grep -l 'C:\\' "$FALCON/market_data/.env" "$FALCON/kanida-app/.env.pilot" "$ENGINE/config/.env" 2>/dev/null | sed 's/^/   CHECK Windows path left in /'

say "Code state"
for d in "$FALCON" "$ENGINE" "$WT"/*; do [ -e "$d/.git" ] && ok "$(basename "$d"): $(git -C "$d" branch --show-current), $(git -C "$d" status --porcelain | wc -l | tr -d ' ') uncommitted"; done

say "Tests"
t() { local label="$1" dir="$2"; shift 2; local out
  if out=$(cd "$dir" && "$@" 2>&1); then ok "$label: $(tail -1 <<<"$out")"; else bad "$label: $(tail -1 <<<"$out")"; fi; }
t market_data "$FALCON" market_scanner/.venv/bin/python -m pytest market_data/tests -q -k "not live"
t "kanida-app server" "$FALCON/kanida-app" env PYTHONPATH=server .pilot-venv/bin/python -m pytest server/tests -q
(cd "$FALCON/kanida-app" && npx tsc --noEmit -p . >/dev/null 2>&1) && ok "kanida-app typecheck" || bad "kanida-app typecheck (run: cd kanida-app && npx tsc --noEmit)"

say "Services can import (no broker calls, nothing started)"
(cd "$FALCON" && market_scanner/.venv/bin/python -c "import market_data.live.cli, market_data.derivatives.cli, market_data.kite_provider as k; print(k.ENGINE_ROOT, k.PLAYWRIGHT_BROWSERS_PATH)") \
  && ok "falcon capture modules" || bad "falcon capture modules"
(cd "$ENGINE/backend" && ../.venv/bin/python -m py_compile main.py) && ok "engine backend compiles" || bad "engine backend"
"$ENGINE/.venv/bin/python" -c "from playwright.sync_api import sync_playwright as s; p=s().start(); b=p.chromium.launch(); b.close(); p.stop()" \
  && ok "playwright chromium launches" || bad "playwright chromium"

[ "$fail" = 0 ] && say "ALL CHECKS PASSED" || { warn "some checks failed — fix before step 6 (cutover)"; exit 1; }
