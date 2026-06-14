# Production / Dev separation — READ BEFORE EDITING ANYTHING

**Why this exists:** the live backend serves code **from the production working tree's files**, and Task Scheduler restarts it **daily ~14:30 IST**. So *any* file edited in the production tree goes LIVE on the next restart — committed or not, regardless of branch. On 2026‑06‑14 an in‑progress paywall change went live this way and broke the Power User portal. This split prevents that.

## The two trees

| | Path | Branch | Role |
|---|---|---|---|
| **PRODUCTION** | `C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine` | `prod` | Serves the live site. Holds the live DB (`data/db/*.db`), `config/.env`, and the Task Scheduler jobs (backend, auth, pipeline). **FROZEN.** |
| **DEV** | `C:\Users\SPS\Desktop\kanida-dev` | `feat/power-user-portal` (+ feature branches) | All ongoing work (autotrade, launch, etc.). Inert — nothing auto-runs it. |

The live DB, `config/.env`, logs, and the 14 GB research DB are **untracked** and exist **only in the production tree**. They are NOT in the dev worktree, so dev work cannot touch live data.

## The rules (do not break these)

1. **NEVER edit files in the production tree** (`…\Quant Intelligence Engine\`). All edits happen in `kanida-dev`.
2. **Do dev work in `C:\Users\SPS\Desktop\kanida-dev`** on a feature branch (e.g. `git -C C:\Users\SPS\Desktop\kanida-dev checkout -b feat/autotrade-xyz`).
3. **The production tree only changes via a deliberate "ship" step** (below). It is not a place to type code.

## Shipping dev → production (deliberate, controlled)

```bash
# 1) in DEV: commit + push your feature branch, merge it into feat/power-user-portal (or your release branch)
# 2) in PROD tree: bring the reviewed code onto prod
cd "C:/Users/SPS/Desktop/Kanida.ai Terminal Quant Intelligence Engine"
git checkout prod
git merge <reviewed-branch>          # fast-forward or reviewed merge only
# 3) restart the backend so the new code loads (no --reload in prod)
#    Stop-Process the uvicorn/python PIDs, then run scripts\start_backend.bat (detached)
# 4) the authenticated warmer (scripts/warm_cache.py) runs on start; verify:
curl -s -o /dev/null -w "%{http_code}\n" "http://localhost:8001/api/power/today/falcon-top-20?universe=all500"
#    expect 200, fast on the second call
```

## Restart procedure (prod)

```powershell
Stop-Process -Id <uvicorn pid>,<python pid> -Force
Start-Process cmd -ArgumentList '/c','"...\scripts\start_backend.bat"' -WindowStyle Hidden
# poll http://127.0.0.1:8001/openapi.json until 200; warmer warms the cache (~60-70s cold)
```

## Notes
- The persona/top‑20 cache is **in‑memory (24h TTL), wiped on restart** → `scripts/warm_cache.py` (authenticated) re‑warms on every start. Do not revert it to the old unauthenticated `curl`.
- The paywall (`current_paid_user_required`) is **defined but NOT wired** to live endpoints; it gets re‑wired only when the full launch (frontend + billing) ships together.
- Task Scheduler still points at the production tree's `scripts\start_backend.bat` — unchanged.
