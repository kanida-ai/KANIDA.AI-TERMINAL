# Cloud move — what today's changes require (22 Sep 2026)

Written after the 21 Sep live session. Checked against `Dockerfile` and `infra/task-definition.template.json`
as they stand; nothing here has been applied.

## 1. The server now runs Node at runtime — the image has none

Immutable snapshots are written by the SAME TypeScript the browser runs (`src/derivative/signal.ts`), executed by
`server/engine/run_engine.cjs` under Node. The runtime stage is `python:3.13-slim` and copies only `kanida_pilot/`.
Without the four lines below the pilot boots, the snapshot worker logs `intelligence engine exited`, the snapshots
route serves nothing new, and the panes fall back to their live calculation.

Add to the runtime stage (after `COPY kanida-app/server/kanida_pilot/ ...`):

```dockerfile
COPY --from=web /usr/local/bin/node /usr/local/bin/node
COPY --from=web /build/node_modules/typescript ./node_modules/typescript
COPY kanida-app/server/engine/ ./server/engine/
COPY kanida-app/src/derivative/ ./src/derivative/
```

`run_engine.cjs` resolves everything from `/app/kanida-app` (its `../..`), so these paths are the ones it expects.
Python 3.13 satisfies the report code (it needs ≥ 3.12).

**Verify in the built image:** `echo '{"id":1,"op":"version"}' | node server/engine/run_engine.cjs`
must print `pane/3.3 …` and `signal/2`.

## 2. `intelligence.db` is the immutable history — it needs durable, writable storage

Default path is `kanida-app/var/intelligence.db`. In the container that is `/app/kanida-app/var`, which (a) the
non-root user `10001` cannot create, and (b) disappears with the task. It holds the snapshots and the
signal-to-noise records that are, by design, never rewritten — losing it loses the audit trail.

- Mount a persistent volume and set `PILOT_INTELLIGENCE_DATABASE=/data/intelligence.db` (the setting exists).
- Make the mount writable by uid 10001.
- **Run exactly one task** (`desiredCount: 1`). The snapshot worker runs inside the pilot process; two tasks would
  be two writers. `INSERT OR IGNORE` prevents duplicate snapshots, but SQLite on shared network storage is not a
  safe multi-writer setup. If more than one task is ever needed, move these tables to the Postgres already
  configured in `PILOT_DATABASE_URL`, or run the worker as its own single task and set `PILOT_SNAPSHOTS=off` on
  the web tasks.

## 3. Where does `derivatives.db` live in the cloud? — undecided, and it gates everything

Today capture, metrics and the equity loop run on this Windows machine and write `db/derivatives.db` locally. The
pilot only reads it (`PILOT_DERIVATIVES_DATABASE`). Moving the pilot alone leaves it reading nothing. Decide one:

- move capture + metrics + equity loop to the cloud too (they need the Kite token and the rate limiter there), or
- keep them local and ship the store (hard to do safely for a 800 MB SQLite file written every 15 minutes).

This is the real cut-over decision; the two items above are mechanical once it is made.

## 4. Today's restarts taught two operational rules

- **A restart parks every open browser tab on "Reconnect to KANIDA"** and it does not recover by itself. In the
  cloud, a rolling deploy will do this to every trader. Deploy outside market hours, or fix the shell to retry.
- **The pattern scanner (8765) is not a service, and started plainly it reads the frozen research store**
  (`Data 31 Jul · STALE`). It must start with `SCANNER_CANDLE_SOURCE=market15`, or `"candle_source": "market15"` in
  `market_scanner/config.json` — an owner decision still open.

## 5. Nothing from 21 Sep is committed

All of today's changes are in the working tree only. Commit before building the image, or the image will not
contain them.
