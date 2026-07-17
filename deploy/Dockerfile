# ============================================================================
# deploy/Dockerfile — Kanida.AI backend, PRODUCTION container image
# ----------------------------------------------------------------------------
# Cloud-Migration PHASE 0 — AUTHORED, UNVERIFIED.
#   Docker is NOT installed on the authoring machine, so this image has NEVER
#   been built or run. Treat every line as a proposal to validate once Docker
#   is available (see deploy/PHASE0_README.md for the exact build/run steps).
#
# DESIGN CONTRACT (grounded in the actual code at prod a0dbe19):
#   * NO `COPY . .`  — we copy ONLY backend/ + requirements.txt. The old root
#     Dockerfile did `COPY . .` (pulls the 3.4 GB .git, the 652 MB prod DB, the
#     36 GB R&D DB, docs, xlsx). This image's build context is MB, not GB —
#     enforced by deploy/.dockerignore.
#   * BAKE NO DATABASE. The old Dockerfile `cp`'d the SQLite DBs into the image
#     as a "seed". We do the opposite: the prod DB is a MOUNTED VOLUME. The A2
#     startup preflight (backend/falcon/config.py::verify_falcon_db +
#     power_user/config.py::verify_power_db, called from main.py lifespan)
#     REFUSES TO BOOT if FALCON_DB_PATH / POWER_DB_PATH do not resolve to a
#     real, readable file. So a mis-mounted volume fails LOUD here — exactly
#     the intended behaviour. A DB baked into the image would defeat that gate.
#   * BAKE NO SECRETS. config/.env is NOT copied. main.py's env loader is
#     best-effort: if backend/../config/.env is absent it silently skips and
#     reads os.environ instead (verified: main.py lines 20-36). All gating env
#     vars therefore arrive at RUNTIME from the container environment
#     (docker-compose env_file locally; AWS Secrets Manager in prod). See
#     deploy/SECRETS_MAP.md.
#   * LAUNCH CONTRACT preserved: uvicorn main:app on port 8001, run from
#     /app/backend (matches scripts/start_backend.bat and the WORKDIR the old
#     image used).
# ============================================================================


# ─────────────────────────────────────────────────────────────────────────────
# STAGE 1 — builder: compile/install Python deps into an isolated venv.
# gcc/g++ live ONLY here. numpy/pandas/psycopg2-binary ship manylinux wheels so
# a compiler is usually not exercised, but kiteconnect/cryptography transitive
# deps occasionally need one — keeping it in the throwaway builder stage means
# the toolchain never reaches the final image (smaller, smaller attack surface).
# ─────────────────────────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

# Build-only system deps. Removed implicitly when this stage is discarded.
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        g++ \
    && rm -rf /var/lib/apt/lists/*

# Self-contained venv we can copy wholesale into the runtime stage.
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install deps first (own layer) so app-code edits don't bust the pip cache.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /tmp/requirements.txt


# ─────────────────────────────────────────────────────────────────────────────
# STAGE 2 — runtime: slim image with the venv + Chromium + app code only.
# ─────────────────────────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

# --- Environment -------------------------------------------------------------
# PYTHONUNBUFFERED: logs flush immediately (matters for CloudWatch/ECS log tail).
# PYTHONDONTWRITEBYTECODE: no .pyc litter in the read-only image layers.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:$PATH"

# DB PATHS → mount points, NOT baked files. These are the load-bearing lines:
#   * FALCON_DB_PATH + POWER_DB_PATH point at the mounted prod DB. The A2
#     preflight (main.py lifespan) passes ONLY when this volume is mounted and
#     readable; otherwise the container exits with the FATAL "production
#     database not found" banner. That is the desired fail-loud.
#   * KANIDA_DB_PATH is the LEGACY quant DB (kanida_quant.db) used by the legacy
#     pipeline. main.py defaults it to backend/../data/db/kanida_quant.db if
#     unset; we pin it to the mount so legacy code finds it if that DB is also
#     mounted. (A2 does NOT gate on this one — only falcon + power.)
#   * NOTE: we deliberately DO NOT set DATABASE_URL. backend/db.py flips
#     IS_POSTGRES the moment DATABASE_URL is a postgres:// URL. Phase 0 keeps
#     the app on SQLite-via-mounted-volume; the parallel Postgres in
#     docker-compose stands up EMPTY and UNUSED. Wiring the app onto Postgres is
#     a LATER phase, not Phase 0.
ENV FALCON_DB_PATH=/data/db/kanida_universe.db \
    POWER_DB_PATH=/data/db/kanida_universe.db \
    KANIDA_DB_PATH=/data/db/kanida_quant.db \
    PORT=8001

# Playwright browser cache: install to a machine-wide, session-agnostic path and
# point the runtime at the same path. This mirrors the laptop fix in
# scripts/start_backend.bat (PLAYWRIGHT_BROWSERS_PATH=C:\ProgramData\ms-playwright)
# that solved the recurring BROWSER_LAUNCH_FAILED. Chromium is required by the
# Zerodha headless token refresh (scripts/auth_worker.py + services/
# zerodha_auto_auth.py). See requirements.txt notes on playwright/pyotp.
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# --- Python deps from builder ------------------------------------------------
COPY --from=builder /opt/venv /opt/venv

# --- Chromium + its OS libraries ---------------------------------------------
# `playwright install --with-deps chromium` needs the playwright package (now in
# the venv) AND apt to pull Chromium's shared libraries. This is the one place
# the runtime stage touches apt; the lib set MUST persist (unlike gcc), so it
# lives here, not in the builder. Without it the token refresh fails with
# BROWSER_LAUNCH_FAILED (verified rationale in requirements.txt + old Dockerfile).
RUN playwright install --with-deps chromium \
    && rm -rf /var/lib/apt/lists/*

# --- Application code (NARROW copy — never `COPY . .`) ------------------------
# Only what uvicorn needs to import and run. requirements.txt is copied for
# provenance/debuggability (deps are already installed above). scripts/ is
# included because backend code shells out to sibling scripts at ROOT/scripts
# (e.g. main.py PIPELINE_STEPS, auth_worker.py). If a leaner image is desired
# later, prune scripts/ to just the auth + pipeline entrypoints actually invoked.
WORKDIR /app
COPY requirements.txt /app/requirements.txt
COPY backend/ /app/backend/
COPY scripts/ /app/scripts/

# --- Non-root runtime user ---------------------------------------------------
# Trading backend should not run as root. The mounted /data volume must be
# readable (A2 needs R_OK) AND writable (SQLite writes -wal/-shm siblings). On
# Docker Desktop bind mounts this is transparent; on Linux/ECS ensure the volume
# is owned by (or group-shared with) UID 10001. See PHASE0_README validation notes.
# We pin BOTH uid and gid to 10001 (explicit groupadd) so the identity is
# DETERMINISTIC and matches the EFS access point's posix_user (modules/efs sets
# uid/gid = 10001). Without the explicit group, useradd's gid is distro-dependent
# and could drift. On ECS the EFS access point squashes all I/O to 10001:10001
# regardless, but pinning the container identity keeps the two provably in sync
# and makes local docker-compose bind-mounts behave too.
RUN groupadd --system --gid 10001 appuser \
    && useradd --system --uid 10001 --gid 10001 --create-home appuser \
    && mkdir -p /data/db \
    && chown -R appuser:appuser /app /ms-playwright /data
USER appuser

# --- Healthcheck -------------------------------------------------------------
# GET / returns the app JSON (main.py root()). Uses stdlib urllib so we don't
# have to install curl. ECS/compose use this to know the app actually booted
# (i.e. the A2 preflight passed) vs. crash-looped on a missing DB.
HEALTHCHECK --interval=30s --timeout=5s --start-period=90s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0) if urllib.request.urlopen('http://127.0.0.1:8001/',timeout=4).status==200 else sys.exit(1)" || exit 1

# --- Launch ------------------------------------------------------------------
# WORKDIR must be /app/backend so `main:app` imports (main.py sits in backend/).
# Bind 0.0.0.0 inside the container (the laptop bat binds 127.0.0.1 because a
# local tunnel fronts it; in a container the orchestrator/ALB is the front door).
WORKDIR /app/backend
EXPOSE 8001
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8001}"]
