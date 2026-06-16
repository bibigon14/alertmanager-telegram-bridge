# alertmanager-telegram-bridge

Lightweight [Alertmanager](https://prometheus.io/docs/alerting/latest/alertmanager/) webhook receiver that forwards alerts to Telegram.

No external dependencies beyond PyYAML. Zero bloat. Runs on a Raspberry Pi.

```
Alertmanager ──POST /webhook──► bridge.py ──► Telegram Bot API
```

## Features

- **Quiet hours** — suppress non-critical alerts at night; critical always gets through
- **Throttle / deduplication** — repeated firing alerts are silenced for a configurable interval
- **Routing** — send different alerts to different chats based on labels (severity, job, instance…)
- **Regex routing** — `match_re` for flexible label matching
- **`resolved` support** — fires a green ✅ message when Alertmanager clears the alert
- **`/healthz`** endpoint for uptime monitors
- **systemd** + **Docker** deployment options
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

## Quick start

### systemd (recommended for Pi)

```bash
# 1. Clone
git clone https://github.com/bibigon14/alertmanager-telegram-bridge
cd alertmanager-telegram-bridge

# 2. Install
python3 -m venv venv
venv/bin/pip install -r requirements.txt

# 3. Configure
cp config.yaml.example config.yaml
$EDITOR config.yaml   # fill in token + chat_id

# 4. Install service
sudo cp systemd/alertmanager-telegram-bridge.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now alertmanager-telegram-bridge

# 5. Check logs
journalctl -u alertmanager-telegram-bridge -f
```

### Docker

```bash
cp config.yaml.example config.yaml
$EDITOR config.yaml
docker compose up -d
```

## Configuration

```yaml
server:
  host: "0.0.0.0"
  port: 9119

telegram:
  token: "7123456789:AAF..."
  default_chat_id: "85698759"

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

Add to your `alertmanager.yml`:

```yaml
receivers:
  - name: telegram
    webhook_configs:
      - url: "http://localhost:9119/webhook"
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
curl http://localhost:9119/healthz
# → ok
```

## Tested on

- Raspberry Pi 5 (Raspberry Pi OS Lite 64-bit)
- Python 3.11
- Alertmanager 0.27

## License

MIT
