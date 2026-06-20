"""
Tests for alertmanager-telegram-bridge
Run: python -m pytest tests/ -v
"""

import sys
import os
import time
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bridge import (
    ThrottleStore,
    MuteControl,
    Stats,
    is_quiet_hours,
    format_alert,
    alert_fingerprint,
    resolve_routes,
    BotCommandHandler,
    WebhookHandler,
)


# ---------------------------------------------------------------------------
# ThrottleStore
# ---------------------------------------------------------------------------
class TestThrottleStore(unittest.TestCase):

    def test_first_alert_passes(self):
        store = ThrottleStore(repeat_interval=300)
        self.assertTrue(store.should_send("fp1"))

    def test_immediate_repeat_suppressed(self):
        store = ThrottleStore(repeat_interval=300)
        store.should_send("fp1")
        self.assertFalse(store.should_send("fp1"))

    def test_different_fingerprints_independent(self):
        store = ThrottleStore(repeat_interval=300)
        self.assertTrue(store.should_send("fp1"))
        self.assertTrue(store.should_send("fp2"))

    def test_clear_resolved_allows_resend(self):
        store = ThrottleStore(repeat_interval=300)
        store.should_send("fp1")
        store.clear_resolved("fp1")
        self.assertTrue(store.should_send("fp1"))

    def test_repeat_after_interval(self):
        store = ThrottleStore(repeat_interval=1)
        store.should_send("fp1")
        time.sleep(1.1)
        self.assertTrue(store.should_send("fp1"))

    def test_active_count(self):
        store = ThrottleStore(repeat_interval=300)
        self.assertEqual(store.active_count(), 0)
        store.should_send("fp1")
        store.should_send("fp2")
        self.assertEqual(store.active_count(), 2)
        store.clear_resolved("fp1")
        self.assertEqual(store.active_count(), 1)


# ---------------------------------------------------------------------------
# MuteControl
# ---------------------------------------------------------------------------
class TestMuteControl(unittest.TestCase):

    def test_initially_unmuted(self):
        m = MuteControl()
        self.assertFalse(m.is_muted())

    def test_mute_unmute(self):
        m = MuteControl()
        m.mute()
        self.assertTrue(m.is_muted())
        m.unmute()
        self.assertFalse(m.is_muted())


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------
class TestStats(unittest.TestCase):

    def test_initial_zero(self):
        s = Stats()
        self.assertEqual(s.sent, 0)
        self.assertEqual(s.throttled, 0)
        self.assertEqual(s.suppressed, 0)
        self.assertEqual(s.failed, 0)

    def test_inc(self):
        s = Stats()
        s.inc("sent")
        s.inc("sent")
        s.inc("failed")
        self.assertEqual(s.sent, 2)
        self.assertEqual(s.failed, 1)

    def test_uptime_format(self):
        s = Stats()
        uptime = s.uptime()
        self.assertIn("m", uptime)


# ---------------------------------------------------------------------------
# Quiet hours
# ---------------------------------------------------------------------------
class TestQuietHours(unittest.TestCase):

    def _cfg(self, start, end, enabled=True, tz="UTC"):
        return {"quiet_hours": {"enabled": enabled, "start": start, "end": end, "timezone": tz}}

    def _mute(self, muted=False):
        m = MuteControl()
        if muted:
            m.mute()
        return m

    def test_disabled(self):
        cfg = self._cfg("23:00", "07:00", enabled=False)
        self.assertFalse(is_quiet_hours(cfg, self._mute()))

    def test_manual_mute_overrides(self):
        cfg = self._cfg("23:00", "07:00", enabled=False)
        self.assertTrue(is_quiet_hours(cfg, self._mute(muted=True)))

    def test_within_quiet_hours_wraps_midnight(self):
        cfg = self._cfg("23:00", "07:00", tz="UTC")
        with patch("bridge.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 1, 1, 2, 0, tzinfo=ZoneInfo("UTC"))
            self.assertTrue(is_quiet_hours(cfg, self._mute()))

    def test_outside_quiet_hours(self):
        cfg = self._cfg("23:00", "07:00", tz="UTC")
        with patch("bridge.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 1, 1, 12, 0, tzinfo=ZoneInfo("UTC"))
            self.assertFalse(is_quiet_hours(cfg, self._mute()))

    def test_within_non_wrapping_window(self):
        cfg = self._cfg("09:00", "18:00", tz="UTC")
        with patch("bridge.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 1, 1, 14, 0, tzinfo=ZoneInfo("UTC"))
            self.assertTrue(is_quiet_hours(cfg, self._mute()))


# ---------------------------------------------------------------------------
# Alert formatter
# ---------------------------------------------------------------------------
class TestFormatAlert(unittest.TestCase):

    def _alert(self, name="TestAlert", severity="warning", status="firing",
                instance="host:9090", summary="Something is wrong",
                description="More detail here"):
        return {
            "status": status,
            "labels": {"alertname": name, "severity": severity, "instance": instance},
            "annotations": {"summary": summary, "description": description},
            "startsAt": "2026-06-01T10:00:00Z",
            "endsAt":   "2026-06-01T10:05:00Z",
        }

    def test_firing_contains_alertname(self):
        self.assertIn("TestAlert", format_alert(self._alert()))

    def test_firing_emoji(self):
        self.assertIn("🔥", format_alert(self._alert(status="firing")))

    def test_resolved_emoji(self):
        self.assertIn("✅", format_alert(self._alert(status="resolved")))

    def test_critical_emoji(self):
        self.assertIn("🔴", format_alert(self._alert(severity="critical")))

    def test_instance_present(self):
        self.assertIn("myhost:9100", format_alert(self._alert(instance="myhost:9100")))

    def test_summary_present(self):
        self.assertIn("Disk full", format_alert(self._alert(summary="Disk full")))


# ---------------------------------------------------------------------------
# Fingerprint
# ---------------------------------------------------------------------------
class TestFingerprint(unittest.TestCase):

    def test_same_labels_same_fp(self):
        a1 = {"labels": {"alertname": "X", "severity": "warning"}}
        a2 = {"labels": {"severity": "warning", "alertname": "X"}}
        self.assertEqual(alert_fingerprint(a1), alert_fingerprint(a2))

    def test_different_labels_different_fp(self):
        a1 = {"labels": {"alertname": "X"}}
        a2 = {"labels": {"alertname": "Y"}}
        self.assertNotEqual(alert_fingerprint(a1), alert_fingerprint(a2))


# ---------------------------------------------------------------------------
# Routing
# ---------------------------------------------------------------------------
class TestResolveRoutes(unittest.TestCase):

    def _cfg(self, routes=None):
        return {
            "telegram": {"token": "TOKEN", "default_chat_id": "DEFAULT"},
            "routes": routes or [],
        }

    def _alert(self, severity="warning", job="node"):
        return {"labels": {"severity": severity, "job": job}}

    def test_fallback_to_default(self):
        targets = resolve_routes(self._cfg(), self._alert())
        self.assertEqual(targets[0]["chat_id"], "DEFAULT")

    def test_route_match(self):
        cfg = self._cfg([{"match": {"severity": "critical"}, "chat_id": "CRIT", "continue": False}])
        targets = resolve_routes(cfg, self._alert(severity="critical"))
        self.assertEqual(targets[0]["chat_id"], "CRIT")

    def test_no_match_falls_back(self):
        cfg = self._cfg([{"match": {"severity": "critical"}, "chat_id": "CRIT", "continue": False}])
        targets = resolve_routes(cfg, self._alert(severity="warning"))
        self.assertEqual(targets[0]["chat_id"], "DEFAULT")

    def test_continue_flag(self):
        cfg = self._cfg([
            {"match": {"severity": "warning"}, "chat_id": "WARN", "continue": True},
            {"match": {"job": "node"},          "chat_id": "NODE", "continue": False},
        ])
        chat_ids = [t["chat_id"] for t in resolve_routes(cfg, self._alert())]
        self.assertIn("WARN", chat_ids)
        self.assertIn("NODE", chat_ids)


# ---------------------------------------------------------------------------
# BotCommandHandler
# ---------------------------------------------------------------------------
class TestBotCommandHandler(unittest.TestCase):

    def _handler(self):
        cfg = {
            "telegram": {"token": "TOKEN", "default_chat_id": "123"},
            "quiet_hours": {"enabled": True, "start": "23:00", "end": "07:00", "timezone": "UTC"},
            "routes": [],
            "throttle": {},
        }
        throttle = ThrottleStore()
        stats    = Stats()
        mute     = MuteControl()
        return BotCommandHandler(cfg, throttle, stats, mute), mute, stats

    def test_status_contains_uptime(self):
        h, _, _ = self._handler()
        reply = h._status()
        self.assertIn("Uptime", reply)
        self.assertIn("Sent", reply)

    def test_mute_command(self):
        h, mute, _ = self._handler()
        self.assertFalse(mute.is_muted())
        h._mute()
        self.assertTrue(mute.is_muted())

    def test_unmute_command(self):
        h, mute, _ = self._handler()
        mute.mute()
        h._unmute()
        self.assertFalse(mute.is_muted())

    def test_help_lists_commands(self):
        h, _, _ = self._handler()
        reply = h._help()
        for cmd in ["/status", "/mute", "/unmute", "/help"]:
            self.assertIn(cmd, reply)

    def test_unauthorized_chat_ignored(self):
        h, _, _ = self._handler()
        update = {"update_id": 1, "message": {
            "chat": {"id": 999999},
            "text": "/status",
        }}
        with patch("bridge.send_telegram") as mock_send:
            h.handle(update)
            mock_send.assert_not_called()

    def test_authorized_chat_gets_reply(self):
        h, _, _ = self._handler()
        update = {"update_id": 1, "message": {
            "chat": {"id": 123},
            "text": "/status",
        }}
        with patch("bridge.send_telegram") as mock_send:
            h.handle(update)
            mock_send.assert_called_once()


# ---------------------------------------------------------------------------
# WebhookHandler._process — quiet hours vs. resolved alerts
# ---------------------------------------------------------------------------
class TestProcessQuietHoursResolved(unittest.TestCase):
    """Regression coverage for: resolved alerts must always be delivered,
    even for non-critical severity during quiet hours. Quiet hours should
    only ever suppress *new* (firing) non-critical alerts."""

    def _handler(self, quiet_hours_enabled=True):
        cfg = {
            "telegram": {"token": "TOKEN", "default_chat_id": "DEFAULT"},
            "quiet_hours": {
                "enabled": quiet_hours_enabled,
                "start": "23:00", "end": "07:00", "timezone": "UTC",
            },
            "routes": [],
            "throttle": {"repeat_interval": 300},
        }
        h = WebhookHandler.__new__(WebhookHandler)
        h.cfg      = cfg
        h.throttle = ThrottleStore(300)
        h.stats    = Stats()
        h.mute     = MuteControl()
        return h

    def _alert(self, status="firing", severity="warning", alertname="KubeJobFailed"):
        return {
            "status": status,
            "labels": {"alertname": alertname, "severity": severity},
            "annotations": {},
            "startsAt": "2026-06-20T05:00:00Z",
            "endsAt": "2026-06-20T06:00:00Z",
        }

    def test_warning_resolved_sent_during_quiet_hours(self):
        h = self._handler()
        with patch("bridge.is_quiet_hours", return_value=True), \
             patch("bridge.send_telegram", return_value=True) as mock_send:
            h._process({"alerts": [self._alert(status="resolved", severity="warning")]})
            mock_send.assert_called_once()
        self.assertEqual(h.stats.sent, 1)
        self.assertEqual(h.stats.suppressed, 0)

    def test_warning_firing_still_suppressed_during_quiet_hours(self):
        h = self._handler()
        with patch("bridge.is_quiet_hours", return_value=True), \
             patch("bridge.send_telegram", return_value=True) as mock_send:
            h._process({"alerts": [self._alert(status="firing", severity="warning")]})
            mock_send.assert_not_called()
        self.assertEqual(h.stats.suppressed, 1)

    def test_critical_firing_sent_during_quiet_hours(self):
        h = self._handler()
        with patch("bridge.is_quiet_hours", return_value=True), \
             patch("bridge.send_telegram", return_value=True) as mock_send:
            h._process({"alerts": [self._alert(status="firing", severity="critical")]})
            mock_send.assert_called_once()
        self.assertEqual(h.stats.sent, 1)

    def test_resolved_clears_throttle_store(self):
        h = self._handler()
        alert = self._alert(status="resolved", severity="warning")
        fp = alert_fingerprint(alert)
        h.throttle._store[fp] = time.time()
        with patch("bridge.is_quiet_hours", return_value=True), \
             patch("bridge.send_telegram", return_value=True):
            h._process({"alerts": [alert]})
        self.assertNotIn(fp, h.throttle._store)


if __name__ == "__main__":
    unittest.main()
