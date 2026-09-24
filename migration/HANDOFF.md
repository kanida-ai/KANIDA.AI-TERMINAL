# Move handoff: where things stand (written from the Windows session, 23 Sep 2026)

Read this first in any new Claude session on the Mac. It carries the state of the Windows
session that did the move.

## Done
- Every uncommitted change and every never-pushed commit is on GitHub:
  - Falcon: `wip/mac-move-2026-09-23`
  - Terminal engine: `wip/mac-move-terminal/*`
- Drive `My Passport` holds ~365 GB, verified on Windows:
  - all 149 databases, with sizes and per-table `max(rowid)` in `manifest/dbs.jsonl`
  - Downloads
  - the encrypted `secrets.tar.enc` (75 files)
- Mac step 1 (`1_setup_mac.sh`) done: tools installed, repos cloned, and the worktrees restored
  (the engine's 913 uncommitted files match the Windows snapshot exactly).
- Mac step 2 (`2_restore_from_ssd.sh`): the owner reports the copy completed.

## Next, in order
1. `bash ~/Kanida/Kanida_Falcon/migration/mac/3_build_envs.sh`: venvs, Playwright, npm.
2. `bash ~/Kanida/Kanida_Falcon/migration/mac/5_verify.sh`: it must print ALL CHECKS PASSED.
3. Cutover (MAC_MIGRATION.md step 4), not yet started. **The Windows laptop is still the live
   machine.** It runs capture, broker auth, backend :8001, and the Cloudflare tunnel for
   api.kanida.ai (the Vercel portal's API). Only one machine may run these. Cutover:
   - stop the Windows tasks and the Cloudflared service
   - re-run the Windows export (delta: only changed databases)
   - on the Mac: `2_restore_from_ssd.sh --delta`, then `5_verify.sh`, then `4_services.sh install`

## Things to know
- Claude context on the Mac: rules, agents and skills are in `~/.claude`. Project memory was
  re-keyed to `~/Kanida/Kanida_Falcon` (and the engine / koptions paths). The full Windows
  transcripts, for reference only, are in `~/Kanida/_migration/claude-projects-windows/`.
- Open follow-ups:
  - ~115 research scripts still hard-code `C:\Users\SPS` paths (the live path is already fixed)
  - Rotate the Anthropic key in `KANIDA.AI_TERMINAL/set ANTHROPIC_API_KEY= sk-ant-api03.txt`
  - Move `Password Manager.txt` (engine) and `Password Manager 2.txt` (Desktop/BG for retail
    Traders) into a real password manager
- The owner accidentally pasted a password into the Windows chat on 23 Sep and was told to
  change it.
