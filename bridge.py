#!/usr/bin/env python3
"""
alertmanager-telegram-bridge
Lightweight Alertmanager webhook receiver that forwards alerts to Telegram.
Includes a Telegram bot command interface (/status, /mute, /unmute, /help).
"""

import json
import logging
import os
import time
import threading
from datetime import datetime, time as dtime
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.request import urlopen, Request
from urllib.error import URLError
from zoneinfo import ZoneInfo

import yaml

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("bridge")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CONFIG_PATH = os.environ.get("BRIDGE_CONFIG", "config.yaml")


def load_config(path: str) -> dict:
    with open(path) as f:
        cfg = yaml.safe_load(f)
    log.info("Config loaded from %s", path)
    return cfg


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------
class Stats:
    def __init__(self):
        self.started_at = time.time()
        self.sent = 0
        self.throttled = 0
        self.suppressed = 0   # quiet hours
        self.failed = 0
        self._lock = threading.Lock()

    def inc(self, field: str):
        with self._lock:
            setattr(self, field, getattr(self, field) + 1)

    def uptime(self) -> str:
        secs = int(time.time() - self.started_at)
        h, m = divmod(secs // 60, 60)
        d, h = divmod(h, 24)
        parts = []
        if d: parts.append(f"{d}d")
        if h: parts.append(f"{h}h")
        parts.append(f"{m}m")
        return " ".join(parts)


# ---------------------------------------------------------------------------
# Throttle store
# ---------------------------------------------------------------------------
class ThrottleStore:
    """In-memory store: fingerprint → last_sent_ts"""

    def __init__(self, repeat_interval: int = 300):
        self._store: dict[str, float] = {}
        self._lock = threading.Lock()
        self.repeat_interval = repeat_interval

    def should_send(self, fingerprint: str) -> bool:
        now = time.time()
        with self._lock:
            last = self._store.get(fingerprint, 0)
            if now - last >= self.repeat_interval:
                self._store[fingerprint] = now
                return True
        return False

    def clear_resolved(self, fingerprint: str):
        with self._lock:
            self._store.pop(fingerprint, None)

    def active_count(self) -> int:
        with self._lock:
            return len(self._store)


# ---------------------------------------------------------------------------
# Manual mute (runtime override of quiet hours)
# ---------------------------------------------------------------------------
class MuteControl:
    def __init__(self):
        self._muted = False
        self._lock = threading.Lock()

    def mute(self):
        with self._lock:
            self._muted = True

    def unmute(self):
        with self._lock:
            self._muted = False

    def is_muted(self) -> bool:
        with self._lock:
            return self._muted


# ---------------------------------------------------------------------------
# Quiet hours
# ---------------------------------------------------------------------------
def is_quiet_hours(cfg: dict, mute: MuteControl) -> bool:
    if mute.is_muted():
        return True

    qh = cfg.get("quiet_hours", {})
    if not qh.get("enabled", False):
        return False

    tz = ZoneInfo(qh.get("timezone", "UTC"))
    now = datetime.now(tz).time()
    start = dtime.fromisoformat(qh["start"])
    end = dtime.fromisoformat(qh["end"])

    if start <= end:
        return start <= now < end
    else:  # wraps midnight
        return now >= start or now < end


# ---------------------------------------------------------------------------
# Telegram API helpers
# ---------------------------------------------------------------------------
def tg_request(token: str, method: str, payload: dict) -> dict | None:
    url = f"https://api.telegram.org/bot{token}/{method}"
    data = json.dumps(payload).encode()
    req = Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urlopen(req, timeout=10) as resp:
            return json.load(resp)
    except URLError as e:
        log.error("Telegram API %s error: %s", method, e)
        return None


def send_telegram(token: str, chat_id: str, text: str) -> bool:
    result = tg_request(token, "sendMessage", {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
    })
    if not result or not result.get("ok"):
        log.error("sendMessage failed: %s", result)
        return False
    return True


def get_updates(token: str, offset: int, timeout: int = 30) -> list[dict]:
    result = tg_request(token, "getUpdates", {
        "offset": offset,
        "timeout": timeout,
        "allowed_updates": ["message"],
    })
    if result and result.get("ok"):
        return result.get("result", [])
    return []


def set_bot_commands(token: str):
    commands = [
        {"command": "status",  "description": "Bridge status & stats"},
        {"command": "mute",    "description": "Mute all non-critical alerts"},
        {"command": "unmute",  "description": "Unmute alerts"},
        {"command": "help",    "description": "Show available commands"},
    ]
    tg_request(token, "setMyCommands", {"commands": commands})
    log.info("Bot commands registered")


# ---------------------------------------------------------------------------
# Alert formatter
# ---------------------------------------------------------------------------
SEVERITY_EMOJI = {"critical": "🔴", "warning": "🟡", "info": "🔵"}
STATUS_EMOJI   = {"firing": "🔥", "resolved": "✅"}


def format_alert(alert: dict) -> str:
    labels      = alert.get("labels", {})
    annotations = alert.get("annotations", {})
    status      = alert.get("status", "firing")
    severity    = labels.get("severity", "info").lower()
    alertname   = labels.get("alertname", "Unknown")
    instance    = labels.get("instance", labels.get("job", ""))
    summary     = annotations.get("summary", "")
    description = annotations.get("description", "")

    lines = [
        f"{STATUS_EMOJI.get(status, '❓')} <b>{alertname}</b> "
        f"[{severity.upper()}] {SEVERITY_EMOJI.get(severity, '⚪')}",
    ]
    if instance:
        lines.append(f"📍 <code>{instance}</code>")
    if summary:
        lines.append(f"\n{summary}")
    if description:
        lines.append(f"<i>{description}</i>")

    skip = {"alertname", "severity", "instance", "job"}
    extra = {k: v for k, v in labels.items() if k not in skip}
    if extra:
        lines.append("\n🏷 " + "  ".join(f"<code>{k}={v}</code>" for k, v in extra.items()))

    if status == "firing" and alert.get("startsAt"):
        lines.append(f"\n🕐 Fired: {alert['startsAt'][:19].replace('T', ' ')} UTC")
    elif status == "resolved" and alert.get("endsAt"):
        lines.append(f"\n🕐 Resolved: {alert['endsAt'][:19].replace('T', ' ')} UTC")

    return "\n".join(lines)


def alert_fingerprint(alert: dict) -> str:
    labels = alert.get("labels", {})
    return "|".join(f"{k}={v}" for k, v in sorted(labels.items()))


# ---------------------------------------------------------------------------
# Route resolution
# ---------------------------------------------------------------------------
def resolve_routes(cfg: dict, alert: dict) -> list[dict]:
    import re as _re
    labels = alert.get("labels", {})
    targets = []

    for route in cfg.get("routes", []):
        match = all(labels.get(k) == v for k, v in route.get("match", {}).items())
        if match:
            match = all(
                _re.fullmatch(v, labels.get(k, ""))
                for k, v in route.get("match_re", {}).items()
            )
        if match:
            targets.append({
                "chat_id": route["chat_id"],
                "token": route.get("token", cfg["telegram"]["token"]),
            })
            if not route.get("continue", False):
                return targets

    default_chat = cfg["telegram"].get("default_chat_id")
    if default_chat and not targets:
        targets.append({"chat_id": default_chat, "token": cfg["telegram"]["token"]})

    return targets


# ---------------------------------------------------------------------------
# Bot command handler
# ---------------------------------------------------------------------------
class BotCommandHandler:
    def __init__(self, cfg: dict, throttle: ThrottleStore,
                 stats: Stats, mute: MuteControl):
        self.cfg      = cfg
        self.throttle = throttle
        self.stats    = stats
        self.mute     = mute
        self.token    = cfg["telegram"]["token"]

        # Allowed chat IDs (only these can send commands)
        allowed = set()
        default = cfg["telegram"].get("default_chat_id")
        if default:
            allowed.add(str(default))
        for route in cfg.get("routes", []):
            allowed.add(str(route.get("chat_id", "")))
        self.allowed_chats = allowed

    def handle(self, update: dict):
        msg = update.get("message", {})
        chat_id = str(msg.get("chat", {}).get("id", ""))
        text    = msg.get("text", "").strip()

        if not text.startswith("/"):
            return
        if chat_id not in self.allowed_chats:
            log.warning("Command from unauthorized chat_id=%s, ignoring", chat_id)
            return

        cmd = text.split()[0].lstrip("/").split("@")[0].lower()
        log.info("Bot command /%s from chat_id=%s", cmd, chat_id)

        handlers = {
            "status": self._status,
            "mute":   self._mute,
            "unmute": self._unmute,
            "help":   self._help,
        }
        fn = handlers.get(cmd)
        if fn:
            reply = fn()
            send_telegram(self.token, chat_id, reply)
        else:
            send_telegram(self.token, chat_id,
                          f"Unknown command: <code>/{cmd}</code>\nTry /help")

    def _status(self) -> str:
        cfg = self.cfg
        qh  = cfg.get("quiet_hours", {})
        tz  = ZoneInfo(qh.get("timezone", "UTC"))
        now = datetime.now(tz).strftime("%H:%M %Z")

        quiet_status = "🔇 manually muted" if self.mute.is_muted() \
            else ("🌙 active" if is_quiet_hours(cfg, self.mute) else "☀️ inactive")

        return (
            f"<b>alertmanager-telegram-bridge</b>\n"
            f"\n"
            f"⏱ Uptime: <code>{self.stats.uptime()}</code>\n"
            f"🕐 Time:   <code>{now}</code>\n"
            f"\n"
            f"📊 <b>Alerts</b>\n"
            f"  ✅ Sent:       <code>{self.stats.sent}</code>\n"
            f"  ⏸ Throttled:  <code>{self.stats.throttled}</code>\n"
            f"  🌙 Suppressed: <code>{self.stats.suppressed}</code>\n"
            f"  ❌ Failed:     <code>{self.stats.failed}</code>\n"
            f"\n"
            f"🔔 <b>State</b>\n"
            f"  Quiet hours: {quiet_status}\n"
            f"  Window: <code>{qh.get('start', 'n/a')}–{qh.get('end', 'n/a')}</code>\n"
            f"  Active fingerprints: <code>{self.throttle.active_count()}</code>\n"
            f"  Repeat interval: <code>{self.throttle.repeat_interval}s</code>"
        )

    def _mute(self) -> str:
        self.mute.mute()
        log.info("Alerts manually muted via bot command")
        return "🔇 Alerts muted. Non-critical alerts will be suppressed.\nUse /unmute to restore."

    def _unmute(self) -> str:
        self.mute.unmute()
        log.info("Alerts manually unmuted via bot command")
        return "🔔 Alerts unmuted. Normal delivery resumed."

    def _help(self) -> str:
        return (
            "<b>Available commands</b>\n\n"
            "/status — bridge uptime, alert stats, quiet hours state\n"
            "/mute   — suppress all non-critical alerts until /unmute\n"
            "/unmute — resume normal alert delivery\n"
            "/help   — this message"
        )


# ---------------------------------------------------------------------------
# Telegram polling loop (runs in background thread)
# ---------------------------------------------------------------------------
def poll_loop(handler: BotCommandHandler, token: str):
    log.info("Bot polling started")
    offset = 0
    while True:
        try:
            updates = get_updates(token, offset, timeout=30)
            for upd in updates:
                offset = upd["update_id"] + 1
                handler.handle(upd)
        except Exception as e:
            log.error("Polling error: %s", e)
            time.sleep(5)


# ---------------------------------------------------------------------------
# HTTP handler
# ---------------------------------------------------------------------------
class WebhookHandler(BaseHTTPRequestHandler):
    cfg:      dict         = {}
    throttle: ThrottleStore = None
    stats:    Stats         = None
    mute:     MuteControl   = None

    def log_message(self, fmt, *args):
        log.info("HTTP %s", fmt % args)

    def do_GET(self):
        if self.path == "/healthz":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path != "/webhook":
            self.send_response(404)
            self.end_headers()
            return

        length = int(self.headers.get("Content-Length", 0))
        body   = self.rfile.read(length)

        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            log.error("Invalid JSON payload")
            self.send_response(400)
            self.end_headers()
            return

        self._process(payload)
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def _process(self, payload: dict):
        cfg    = self.cfg
        alerts = payload.get("alerts", [])
        log.info("Received %d alert(s), status=%s", len(alerts), payload.get("status"))

        quiet = is_quiet_hours(cfg, self.mute)

        for alert in alerts:
            status   = alert.get("status", "firing")
            labels   = alert.get("labels", {})
            severity = labels.get("severity", "info").lower()
            fp       = alert_fingerprint(alert)
            is_crit  = severity == "critical"

            if quiet and not is_crit:
                log.info("Suppressed (quiet hours): %s", fp)
                self.stats.inc("suppressed")
                continue

            if status == "firing":
                if not self.throttle.should_send(fp):
                    log.info("Throttled: %s", fp)
                    self.stats.inc("throttled")
                    continue
            elif status == "resolved":
                self.throttle.clear_resolved(fp)

            text    = format_alert(alert)
            targets = resolve_routes(cfg, alert)

            if not targets:
                log.warning("No targets for alert: %s", fp)
                continue

            for target in targets:
                ok = send_telegram(target["token"], target["chat_id"], text)
                if ok:
                    self.stats.inc("sent")
                else:
                    self.stats.inc("failed")
                log.info("Alert %s → %s: %s",
                         labels.get("alertname", fp),
                         target["chat_id"],
                         "sent" if ok else "FAILED")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    cfg      = load_config(CONFIG_PATH)
    throttle = ThrottleStore(cfg.get("throttle", {}).get("repeat_interval", 300))
    stats    = Stats()
    mute     = MuteControl()
    token    = cfg["telegram"]["token"]

    # Register bot commands in Telegram menu
    set_bot_commands(token)

    # Start polling thread
    bot_handler = BotCommandHandler(cfg, throttle, stats, mute)
    poll_thread = threading.Thread(
        target=poll_loop, args=(bot_handler, token), daemon=True
    )
    poll_thread.start()

    # Inject shared state into HTTP handler
    WebhookHandler.cfg      = cfg
    WebhookHandler.throttle = throttle
    WebhookHandler.stats    = stats
    WebhookHandler.mute     = mute

    host = cfg.get("server", {}).get("host", "0.0.0.0")
    port = cfg.get("server", {}).get("port", 9119)

    server = HTTPServer((host, port), WebhookHandler)
    log.info("alertmanager-telegram-bridge listening on %s:%d", host, port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Shutting down")


if __name__ == "__main__":
    main()
