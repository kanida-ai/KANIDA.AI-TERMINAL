# module: efs (persistent SQLite volume for the single Fargate task)

**Purpose.** Give the ephemeral Fargate task a **persistent** place to keep the
production SQLite DB so it survives restarts and the app's A2 preflight passes.
Closes the gap documented in `modules/compute/README.md` item 1.

## What it creates

| Resource | Why |
|---|---|
| `aws_efs_file_system` | Encrypted (stack KMS key), `generalPurpose` (low latency), one writer. |
| `aws_security_group` (efs) | NFS **2049 from the app SG only** — data plane unreachable except from the task. |
| `aws_efs_mount_target` × N | One per **private subnet / AZ** (a task can only mount EFS in its own AZ). `count`, not `for_each`, so `plan` stays clean when subnet IDs are unknown pre-apply. |
| `aws_efs_access_point` | Rooted at `/kanida-db`, **POSIX-squashed to uid/gid 10001** (the Dockerfile `appuser`). The task mounts THIS. |

**Outputs:** `file_system_id`, `access_point_id`, `security_group_id`.

## The load-bearing detail: POSIX uid/gid

`deploy/Dockerfile` runs the app as **uid 10001** (`useradd --system --uid 10001 ...`
+ `USER appuser`; the gid is pinned to 10001 too). The access point's
`posix_user { uid = 10001, gid = 10001 }` **squashes every file operation** to
10001:10001 regardless of the NFS client, and `creation_info` makes the DB
directory owned 10001:10001 with `0755`. So:

- the app process (uid 10001) **matches the file owner** → owner `rwx` applies,
- A2's `R_OK` read passes, and
- SQLite can write its `-wal` / `-shm` siblings.

This is robust even if the image's supplementary GID ever drifts, because the
access point forces the identity.

## Mount contract with `modules/compute`

The compute task-def declares an EFS `volume` (transit encryption ON,
`authorization_config.access_point_id = <this access_point_id>`) and a
`mountPoint` at **`/data/db`** — matching `FALCON_DB_PATH` /
`POWER_DB_PATH = /data/db/kanida_universe.db` from the Dockerfile. The DB file
therefore lives at `<access-point-root>/kanida_universe.db`, i.e. mounted as
`/data/db/kanida_universe.db`.

## Honest caveats

1. **SQLite-on-EFS needs a single writer.** `desired_count` stays **1** (see
   compute README + `docs/design/SCHEDULER_EXTERNALIZATION_DESIGN.md`). Two tasks
   = two writers = corruption + double-fired schedulers. Do not scale out on EFS.
2. **EFS is the bridge, not the end-state.** The real target is RDS Postgres
   (`modules/rds`, stood up empty). EFS-SQLite gets "one cloud server off the
   laptop" working now; the Postgres migration removes the single-writer ceiling
   later.
3. **The empty-EFS bootstrap problem.** A fresh EFS has no DB, so A2 would
   (correctly) refuse to boot. The prod DB must be **seeded onto EFS once**
   before first app boot — see `deploy/PHASE2_3_RUNBOOK.md` Phase 2 step "Seed
   the prod DB onto EFS".

**Status.** Authored, UNVALIDATED — never init/validate/plan/applied.
