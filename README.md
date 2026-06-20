# alertmanager-telegram-bridge

Lightweight [Alertmanager](https://prometheus.io/docs/alerting/latest/alertmanager/) webhook receiver that forwards alerts to Telegram.

No external dependencies beyond PyYAML. Zero bloat. Runs on a Raspberry Pi (on k3s).

```
Alertmanager (systemd, on host) ──POST /webhook──► bridge.py (k3s pod) ──► Telegram Bot API
```

## Features

- **Quiet hours** — suppress non-critical alerts at night; critical always gets through
- **Throttle / deduplication** — repeated firing alerts are silenced for a configurable interval
- **Routing** — send different alerts to different chats based on labels (severity, job, instance…)
- **Regex routing** — `match_re` for flexible label matching
- **`resolved` support** — fires a green ✅ message when Alertmanager clears the alert
- **`/healthz`** endpoint for uptime monitors and Kubernetes liveness/readiness probes
- **Runs as a Kubernetes Deployment** on a single-node k3s cluster (see [homelab-k3s](https://github.com/bibigon14/homelab-k3s)), exposed via NodePort so the host's systemd-managed Alertmanager can reach it
- Zero external HTTP libraries — uses only Python stdlib + PyYAML

## Alert format

```
🔥 NodeHighCPU [WARNING] 🟡
📍 homebridge:9100

CPU usage above 90% for more than 5 minutes
<i>Current value: 94.3%</i>

🏷 job=node  env=homelab

🕐 Fired: 2026-06-01 03:15:22 UTC
```

## Quick start (k3s)

This bridge runs as a Kubernetes Deployment, with config delivered via a Secret. Manifest lives in [homelab-k3s/apps/bridge](https://github.com/bibigon14/homelab-k3s/tree/main/apps/bridge).

```bash
# 1. Clone and configure
git clone https://github.com/bibigon14/alertmanager-telegram-bridge
cd alertmanager-telegram-bridge
cp config.yaml.example config.yaml
$EDITOR config.yaml   # fill in token + chat_id

# 2. Create the Secret from the config file
kubectl create secret generic bridge-config \
  --from-file=config.yaml=config.yaml -n homelab

# 3. Build and import the image (no registry — single-node cluster)
docker build -t alertmanager-telegram-bridge:latest .
docker save alertmanager-telegram-bridge:latest | sudo k3s ctr images import -

# 4. Deploy
kubectl apply -f /path/to/homelab-k3s/apps/bridge/deployment.yaml

# 5. Check logs
kubectl logs -n homelab deploy/alertmanager-telegram-bridge -f
```

The Service is exposed as `NodePort 30119`, so on the host, the bridge is reachable at `http://localhost:30119/webhook` — same port Alertmanager (running via systemd on the host, not in k3s) is configured to send to.

## Alternative: systemd / Docker

For a non-Kubernetes setup, the bridge also runs fine as a plain systemd service or standalone Docker container.

### systemd

```bash
python3 -m venv venv
venv/bin/pip install -r requirements.txt
sudo cp systemd/alertmanager-telegram-bridge.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now alertmanager-telegram-bridge
journalctl -u alertmanager-telegram-bridge -f
```

### Docker

```bash
docker compose up -d
```

## Configuration

```yaml
server:
  host: "0.0.0.0"
  port: 9119

telegram:
  token: "7123456789:AAF..."
  default_chat_id: "85....."

throttle:
  repeat_interval: 300   # seconds between repeated firing alerts

quiet_hours:
  enabled: true
  start: "23:00"
  end: "07:00"
  timezone: "America/Los_Angeles"
  # critical severity always bypasses quiet hours

routes:
  - match:
      severity: critical
    chat_id: "YOUR_CRITICAL_CHAT_ID"
    continue: false

  - match:
      severity: warning
    chat_id: "YOUR_WARNING_CHAT_ID"
    continue: false

  # regex example
  # - match_re:
  #     instance: ".*:9090"
  #   chat_id: "YOUR_PROMETHEUS_CHAT_ID"
```

### Routing logic

Rules are evaluated **top-down**. First match wins unless `continue: true` is set, in which case evaluation continues to the next rule. If no rule matches, the alert goes to `telegram.default_chat_id`.

## Alertmanager integration

On the host's `alertmanager.yml` (Alertmanager itself stays on the host, not in k3s):

```yaml
receivers:
  - name: telegram
    webhook_configs:
      - url: "http://localhost:30119/webhook"   # NodePort, if bridge runs in k3s
        # url: "http://localhost:9119/webhook"  # if running standalone (systemd/Docker)
        send_resolved: true

route:
  receiver: telegram
```

See [`alertmanager-example.yml`](alertmanager-example.yml) for a full example with inhibition rules.

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `BRIDGE_CONFIG` | `config.yaml` | Path to config file |

## Running tests

```bash
python -m pytest tests/ -v
```

## Healthcheck

```bash
curl http://localhost:30119/healthz   # k3s (NodePort)
curl http://localhost:9119/healthz    # standalone (systemd/Docker)
# → ok
```

## Tested on

- Raspberry Pi 5 (Raspberry Pi OS Lite 64-bit)
- Python 3.11
- Alertmanager 0.27
- k3s v1.35

## License

MIT
