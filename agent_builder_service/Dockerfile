# Standalone image for the Agent Builder (or fold agent_builder/ into the main backend image).
FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends gcc g++ && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
# Data via env: AGENT_DATA_URI=s3://bucket/kanida/daily/  (or a local dir);  WALLET_DB=... (shared power_user DB)
EXPOSE 8010
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8010"]
