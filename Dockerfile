FROM python:3.11-slim

LABEL org.opencontainers.image.title="alertmanager-telegram-bridge"
LABEL org.opencontainers.image.description="Lightweight Alertmanager webhook receiver for Telegram"
LABEL org.opencontainers.image.source="https://github.com/bibigon14/alertmanager-telegram-bridge"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bridge.py .

# Config is mounted at runtime
ENV BRIDGE_CONFIG=/config/config.yaml

EXPOSE 9119

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:9119/healthz')"

USER nobody

CMD ["python", "bridge.py"]
