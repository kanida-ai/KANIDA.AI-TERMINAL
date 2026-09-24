# Moving Kanida from the Windows laptop to the Mac

Written 23 Sep 2026 from an inventory of the Windows laptop. Scripts are in this folder; every
one is re-runnable.

## What moves, and where it lands

| Windows | Mac | How |
|---|---|---|
| `Documents\Kanida_Falcon` | `~/Kanida/Kanida_Falcon` | git clone of branch `wip/mac-move-2026-09-23` + data from the SSD |
| `Desktop\Kanida.ai Terminal Quant Intelligence Engine` | `~/Kanida/engine` | git worktree of `feat/cloud-rupeezy-token-sync`, uncommitted work restored as uncommitted |
| `Desktop\koptions`, `kanida-dev`, `_kanida_autotrade`, `_kanida_persona` | `~/Kanida/worktrees/<name>` | same |
| other worktrees (`kagents`, `kbuilder`, `_kanida_*`) | not recreated | branches are on GitHub; `git worktree add` when needed |
| `Desktop\KANIDA.AI_TERMINAL`, `Desktop\_kanida_deploy` | `~/Kanida/<same>` | SSD |
| everything else on Desktop / Documents | `~/Kanida/archive/Desktop/*`, `.../Documents/*` | SSD, as-is (includes local-only repos AIScreener, App for Kids, Kanida Ai) |
| ~265 GB of SQLite (`db\kanida.db` 150 GB, `market15.db` 19 GB, `outcomes.sqlite3` 37 GB …) | same relative paths | SSD, **consistent online snapshots** + per-table rowid manifest |
| `.env` files, `pilot.key`, `~\.aws`, `~\.cloudflared`, `Password Manager.txt`, key files | same relative paths, `chmod 600` | one AES-256 bundle `secrets.tar.enc`, passphrase only in your head |
| anaconda / miniconda / codex-runtime Python | `uv` venvs: Falcon 3.12, engine 3.13, terminal 3.13 | rebuilt from requirement files + exact Windows versions for anything missing |
| 13 Task Scheduler jobs + the `Cloudflared` service | 17 `launchd` agents `com.kanida.*` | `4_services.sh` |
| `~\.claude` rules, agents, skills, project memory | `~/.claude`, memory re-keyed to the new paths | SSD |

`~/Kanida` is deliberately outside `~/Documents` and `~/Desktop`: if iCloud "Desktop & Documents"
is on, macOS would try to upload the 265 GB of databases.

Already done from Windows (23 Sep): every uncommitted change and all 87 never-pushed commits are
on GitHub under `wip/mac-move-2026-09-23` and `wip/mac-move-terminal/*`. Secrets and files over
5 MB were kept out of git; they travel on the SSD.

## Timing: market hours are the constraint

Windows runs on Pacific time; NSE trades 09:15–15:30 IST = **20:45–03:00 Pacific** (PDT).
All the heavy steps below happen in the **Pacific daytime (03:00–20:45)**, when nothing is
capturing. Windows keeps capturing through steps 1–3; nothing on it is modified.

## Step 1 — Windows: export to the SSD (≈2–4 h, mostly kanida.db)

Drive: ≥ 350 GB free. exFAT or NTFS both work: Windows writes it, and the Mac only reads it
(macOS reads NTFS natively). An NTFS volume must be **clean**: macOS won't mount one flagged dirty,
so run `chkdsk X: /f` first if Windows reports it.

```powershell
powershell -ExecutionPolicy Bypass -File C:\Users\SPS\Documents\Kanida_Falcon\migration\windows\export_to_ssd.ps1 -Ssd E:
```
At the end it asks for a passphrase for the secrets bundle. Pick a strong one and keep it
somewhere safe that isn't the SSD.

## Step 2 — Mac: tools and code (≈20 min)

Plug in the SSD and run the setup straight off it (the export copied this folder):
```bash
bash "/Volumes/<SSD>/KanidaMove/files/Kanida_Falcon/migration/mac/1_setup_mac.sh"
```
It installs Xcode tools, Homebrew, git/gh/aws/uv/node 24/cloudflared, and signs you into GitHub
in the browser. Then it clones the repo into `~/Kanida` and restores the worktrees with their
uncommitted work. From here on, every script runs from the clone.

## Step 3 — Mac: data, environments, verification

```bash
bash ~/Kanida/Kanida_Falcon/migration/mac/2_restore_from_ssd.sh /Volumes/<SSD>   # ≈1–2 h; asks for the passphrase
bash ~/Kanida/Kanida_Falcon/migration/mac/3_build_envs.sh                        # ≈10 min
bash ~/Kanida/Kanida_Falcon/migration/mac/5_verify.sh                            # must end "ALL CHECKS PASSED"
```
`5_verify` compares every database with the Windows manifest (size and `max(rowid)` of every
table), runs `quick_check` on the live databases, confirms the secrets are in place, runs the
test suites and the typecheck, and launches Playwright's Chromium. It starts nothing and calls no
broker.

From here the Mac is a full **development** machine. Install the Claude desktop app, open
`~/Kanida/Kanida_Falcon`, and your rules, agents and project memory are already there. The pilot
runs with `bash migration/mac/start-pilot.sh 8082 --build`.

## Step 4 — cutover of the live services (one Pacific afternoon)

Only one machine may run capture, broker auth and the API tunnel at a time. Two auth workers
minting Kite/Vortex tokens can invalidate each other. Two tunnel connectors would split
`api.kanida.ai` traffic between a live backend and a dead one.

1. **Windows — stop everything** (admin PowerShell, after 03:00 PT):
   ```powershell
   Get-ScheduledTask | ? TaskName -match 'kanida' | Disable-ScheduledTask
   Get-ScheduledTask | ? { $_.TaskName -match 'kanida' -and $_.State -eq 'Running' } | Stop-ScheduledTask
   $p = (Get-NetTCPConnection -LocalPort 8001 -State Listen -EA SilentlyContinue).OwningProcess; if ($p) { Stop-Process -Id $p -Force }
   Stop-Service Cloudflared; Set-Service Cloudflared -StartupType Manual
   ```
   `api.kanida.ai` is down from here until item 4 below. Keep this window short.
2. **Windows — delta export:** re-run the step 1 command. Only the databases written since the
   first run are re-copied, which takes minutes, not hours.
3. **Mac — take the delta and check it:**
   ```bash
   bash ~/Kanida/Kanida_Falcon/migration/mac/2_restore_from_ssd.sh /Volumes/<SSD> --delta
   bash ~/Kanida/Kanida_Falcon/migration/mac/5_verify.sh
   ```
4. **Mac — power settings, then start the services:**
   ```bash
   sudo pmset -c sleep 0 disksleep 0      # never sleep on AC power
   bash ~/Kanida/Kanida_Falcon/migration/mac/4_services.sh plan
   bash ~/Kanida/Kanida_Falcon/migration/mac/4_services.sh install
   curl -s https://api.kanida.ai/openapi.json | head -c 80      # portal API back up
   ```
   A MacBook still sleeps when its **lid is closed** unless an external display is connected.
   Keep the lid open, or use `sudo pmset -a disablesleep 1`.
5. **Next IST session (from 20:45 PT):** `4_services.sh status` shows every job with exit 0, and
   the logs keep advancing:
   `4_services.sh logs fno-capture`, `logs equity-prices`, `~/Kanida/engine/logs/auth_worker.log`.
   The Falcon data-status panel should show fresh 5-minute F&O snapshots.

**Rollback** (any time): `4_services.sh uninstall` on the Mac, then on Windows
`Get-ScheduledTask | ? TaskName -match 'kanida' | Enable-ScheduledTask`, `Start-Service Cloudflared`
and reboot. It picks up exactly where it stopped. Keep Windows intact for at least a week.

## Service map

| launchd agent | Windows task | when |
|---|---|---|
| equity-prices, fno-capture, fno-metrics | KANIDA Equity prices / F&O capture / F&O metrics | always on |
| backend (+ cache warm-up) | KanidaBackendAutoStart + start_backend.bat | always on, :8001 |
| api-tunnel | Cloudflared service | always on |
| keep-awake | none (Windows didn't sleep) | always on |
| zerodha-auth, vortex-auth, mkt-poller, mkt-tick | same names | every 30 min, self-gating to IST hours |
| workflow-watchdog | KanidaWorkflowWatchdog | every 15 min |
| flow-eod / mkt-backfill | KanidaFlowEODReport / KanidaMktBackfill | 15:45 / 16:30 IST daily |
| backend-restart | KanidaBackendDailyRestart | 03:00 IST daily |
| weekly-learner | KanidaTierWeeklyLearner | Mon 18:30 IST |
| nse-nightly / us-nightly | KANIDA_NSE_Nightly / KANIDA_US_Nightly | Mon–Fri 18:30 IST / 17:00 Pacific |
| not migrated | KanidaSignalTierDeploy, KanidaAutoTradeHardeningDeploy | one-shot deploys that already ran; deploys are human-gated |

Fixed-time jobs are pinned to IST by `run_at.sh`. The Windows triggers were in Pacific time, so
they drifted an hour against the market every November; that is fixed here. A job missed while
the Mac slept still runs, up to 3 hours late.

## Follow-ups (not blocking the move)

- **~115 research scripts hard-code `C:\Users\SPS\...`** (`scripts/`, `arena/`, `SELVI/`, `engine/`,
  `kanida_engine/`). The live path is fixed (`market_data/kite_provider.py`); these will fail on
  the Mac until switched to repo-relative paths.
- Windows-only helpers in `kanida-app/scripts/*.ps1`: `start-pilot` has a Mac port here; the others
  (`start-screener-dev`, `register-screener-task`) don't yet.
- **Secrets hygiene:** the engine repo has a plaintext `Password Manager.txt`, and
  `KANIDA.AI_TERMINAL` has a file named `set ANTHROPIC_API_KEY= sk-ant-api03.txt`. Both travel
  encrypted, but they should go into a real password manager, and that Anthropic key should be
  rotated.
- The `wip/mac-move-*` branches are safety snapshots. Fold them into real branches and delete them
  once the Mac is settled.
- The cloud move (`kanida-app/docs/CLOUD_MOVE_CHECKLIST.md`) would take the laptop, any laptop,
  out of the live path.
