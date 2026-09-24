#!/usr/bin/env bash
# Step 3: Python venvs + node modules + Playwright, rebuilt natively for the Mac.
# The Windows venvs (and anaconda) can't be copied; this rebuilds them from the repo's
# requirement files, then fills gaps from the exact Windows package lists (env-freeze/).
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/config.sh"
FREEZE="$MIG/env-freeze"
WIN_ONLY='^(pywin32|pywin32-ctypes|pywinpty|pypiwin32|windows-curses|wmi|comtypes|winshell|win32-setctime|conda|conda-.*|anaconda-.*|menuinst|navigator-.*|clyent|ruamel-yaml-conda|libmambapy)=='

# <venv> <python-version> <freeze-name> <source dirs to scan for imports...> -- <requirement files...>
build() {
  local venv="$1" pyv="$2" freeze="$3"; shift 3
  local scan=(); while [ "$1" != "--" ]; do scan+=("$1"); shift; done; shift
  say "venv $venv (python $pyv)"
  [ -x "$venv/bin/python" ] || uv venv --python "$pyv" "$venv"
  local r; for r in "$@"; do
    [ -f "$r" ] || continue
    grep -viE "$WIN_ONLY" "$r" > "$MIG/req.tmp" || true
    uv pip install --python "$venv/bin/python" -r "$MIG/req.tmp" || warn "some of $r failed — see above"
  done
  # anything the code imports that is still missing: take the exact version Windows had
  if [ -f "$FREEZE/$freeze.txt" ]; then
    python3 "$FALCON/migration/mac/check_imports.py" "$venv/bin/python" "$FREEZE/$freeze.txt" "${scan[@]}" > "$MIG/missing-$freeze.txt" || true
    if [ -s "$MIG/missing-$freeze.txt" ]; then
      while read -r pin; do uv pip install --python "$venv/bin/python" "$pin" >/dev/null 2>&1 && echo "   + $pin" || warn "could not install $pin"; done < "$MIG/missing-$freeze.txt"
    fi
    python3 "$FALCON/migration/mac/check_imports.py" "$venv/bin/python" "$FREEZE/$freeze.txt" "${scan[@]}" --report || true
  fi
}

build "$FALCON/market_scanner/.venv" 3.12 falcon-market_scanner \
  "$FALCON/market_data" "$FALCON/market_scanner" "$FALCON/scripts" -- \
  "$FALCON/market_scanner/requirements.txt" "$FALCON/market_scanner/requirements-backtest.txt" \
  "$FALCON/market_scanner/pattern_research/requirements.txt"
uv pip install --python "$FALCON/market_scanner/.venv/bin/python" pytest >/dev/null

build "$FALCON/kanida-app/.pilot-venv" 3.12 falcon-pilot \
  "$FALCON/kanida-app/server" -- "$FALCON/kanida-app/server/requirements.lock.txt"

build "$ENGINE/.venv" 3.13 engine-anaconda \
  "$ENGINE/backend" "$ENGINE/scripts" -- "$ENGINE/requirements.txt" "$ENGINE/backend/requirements.txt"
uv pip install --python "$ENGINE/.venv/bin/python" uvicorn >/dev/null

build "$TERMINAL/.venv" 3.13 terminal-miniconda \
  "$TERMINAL/backend" -- "$TERMINAL/backend/requirements.txt"

say "Playwright Chromium (broker auth workers)"
"$ENGINE/.venv/bin/python" -m playwright install chromium || warn "playwright install failed — auth workers will not log in"

say "Node modules (kanida-app)"
(cd "$FALCON/kanida-app" && npm ci --no-audit --no-fund)

say "Step 3 done. Next:  bash $FALCON/migration/mac/5_verify.sh /Volumes/<SSD>"
