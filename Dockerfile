FROM python:3.11-slim

WORKDIR /app

# System dependencies (gcc/g++ for sklearn/numpy compile)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Headless Chromium for Zerodha token refresh (scripts/auth_worker.py).
# --with-deps pulls the Linux shared libraries Chromium needs. Without this the
# host auth refresh fails with BROWSER_LAUNCH_FAILED. See docs/launch/RUNBOOK_deploy.md §6.
RUN playwright install --with-deps chromium

# Copy app
COPY . .

# Bundle BOTH DBs as seeds for fresh Railway volumes
#   - kanida_quant.db   → legacy engine
#   - kanida_universe.db → Falcon engine (V7.1)
# Falcon DB lives at universe_engine/data/db/ in the repo; we copy to data/db/
# so production paths are uniform.
RUN mkdir -p /app/data/db /app/data/db_bundle && \
    cp /app/data/db/kanida_quant.db /app/data/db_bundle/kanida_quant.db 2>/dev/null || true && \
    cp /app/universe_engine/data/db/kanida_universe.db /app/data/db/kanida_universe.db 2>/dev/null || true && \
    cp /app/data/db/kanida_universe.db /app/data/db_bundle/kanida_universe.db 2>/dev/null || true

# Entrypoint handles volume init then starts uvicorn (or cron)
COPY entrypoint.sh /entrypoint.sh
COPY entrypoint_cron.sh /entrypoint_cron.sh
RUN chmod +x /entrypoint.sh /entrypoint_cron.sh

WORKDIR /app/backend
CMD ["/entrypoint.sh"]
