# Build from the Falcon root with the accompanying .dockerignore.
FROM node:22-slim AS web
WORKDIR /build
COPY kanida-app/package.json kanida-app/package-lock.json ./
RUN npm ci
COPY kanida-app/ ./
ARG PUBLIC_API_URL
ENV EXPO_PUBLIC_API_URL=${PUBLIC_API_URL}
RUN npx expo export --platform web --output-dir dist-pilot

FROM python:3.13-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app/kanida-app/server PILOT_BIND=0.0.0.0 PILOT_PORT=8080 PILOT_ENVIRONMENT=staging PILOT_WEB_DIRECTORY=/app/kanida-app/dist PILOT_LIVE_ENABLED=false
WORKDIR /app/kanida-app
COPY kanida-app/server/requirements.lock.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && useradd --uid 10001 --create-home pilot
COPY kanida-app/server/kanida_pilot/ ./server/kanida_pilot/
COPY market_scanner/*.py /app/market_scanner/
COPY --from=web /build/dist-pilot/ ./dist/
USER 10001
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/health',timeout=4)"
CMD ["python", "-m", "kanida_pilot"]
