# _archive

Code removed from the live app during the UX-fix wave 2 dead-code pass (task 4.1, 2026-09-13).
Nothing here is imported, type-checked (`tsconfig.json` excludes `_archive`) or bundled. Expo Router only
bundles `app/` routes and what they import. Most of `src/` is untracked by git, so these files are
the only copy. Restore a file by moving it back to its original path.

| File | Original path | Last modified (local) | Why archived |
|---|---|---|---|
| `src/StrategyFlow.tsx` | `src/StrategyFlow.tsx` | 2026-09-13 09:32:33 | Not imported anywhere. **Looks like unfinished work** from another tool: it has a syntax error at line 24 (TS1109 "Expression expected", cols 532-533), which was the only typecheck failure in the project. Preserved as is. |
| `src/StrategyEvidence.tsx` | `src/StrategyEvidence.tsx` | 2026-09-13 09:34:00 | Not imported anywhere. Edited this morning alongside StrategyFlow, so it **looks like unfinished work** too. It imports `EquityPlot`, `HistoryChart` and `Metric` from `src/ReplayStudio.tsx`, so check that those still exist before restoring it. Preserved as is. |
| `src/Screens.dead.tsx` | snapshot of `src/Screens.tsx` | 2026-09-12 08:44:24 | A **full, unmodified snapshot** of the original file. Only `Connect` (plus `Page`/`Heading`) is live, via `app/connect.tsx`. `Discover`, `Portfolios`, `AutoTrade`, `Agent`, `SetupCard`, `BriefCard`, `Orb` and `Avatar` had no importers and were removed from the live file. Note: `BriefCard`'s "Open my briefing" button links to `/agent`, which now redirects to `/`. |
| `src/TraderShell.dead.tsx` | snapshot of `src/TraderShell.tsx` | 2026-09-12 19:49:54 | A **full, unmodified snapshot**. The `TraderShell` layout component was never mounted (`app/_layout.tsx` uses another shell). Only `AgentMark` is live (used by PilotShell, TraderDesk and Welcome) and it remains in `src/TraderShell.tsx`. |
| `src/Chart.tsx` | `src/Chart.tsx` | 2026-09-12 09:25:21 | Moved in the final step (F2). Its only importer was `src/Sheets.tsx` (`CandleChart`, `CashCurve`), which never used them; that import was removed. A grep of `src/`, `app/`, `scripts/`, `qa/` and root `*.ts` found no other importer. Setup charts use `PatternCanvas`. `README.md` (project root, line ~65) still describes this file. |
| `index.ts` | `index.ts` (project root) | 2026-09-12 08:25:29 | Moved in the final step (F2), together with `App.tsx`. Expo starter boilerplate: `registerRootComponent(App)`. Not the entry, because `package.json` `main` is `expo-router/entry`. Nothing in `scripts/`, `eas.json`, `app.json`, `app.config.ts` or `Dockerfile` references it. It was type-checked only because tsconfig includes `*.ts`. |
| `App.tsx` | `App.tsx` (project root) | 2026-09-12 08:25:29 | Moved together with `index.ts`, its only importer. Expo starter placeholder screen ("Open up App.tsx to start working on your app!"). |

The `.dead.tsx` snapshots import siblings with `./` paths that no longer resolve from this folder. They are
text records, not buildable modules. The same applies to `src/Chart.tsx` (`./model`) and `index.ts`/`App.tsx`: restore `index.ts` and `App.tsx` together.
