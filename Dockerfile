FROM python:3.11-slim

WORKDIR /app

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY . .

# Bundle the DB as the seed for fresh Railway volumes
RUN mkdir -p /app/data/db_bundle && \
    cp /app/data/db/kanida_quant.db /app/data/db_bundle/kanida_quant.db 2>/dev/null || true

# Entrypoint handles volume init then starts uvicorn
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

WORKDIR /app/backend
CMD ["/entrypoint.sh"]
