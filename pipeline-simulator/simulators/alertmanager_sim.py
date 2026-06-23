import random
import asyncio
import logging

from opentelemetry import trace
from opentelemetry.trace import SpanKind, StatusCode

log = logging.getLogger("alertmanager_sim")


class AlertManagerSimulator:
    def __init__(self, tracer_provider, log_emitter):
        self.tracer = tracer_provider.get_tracer("alertmanager", "0.27.0")
        self.log_emitter = log_emitter

    async def send_alert(self, parent_context, alert_name, severity, source_service):
        from opentelemetry import context

        ctx_token = context.attach(parent_context)
        try:
            with self.tracer.start_as_current_span(
                "notify.slack",
                kind=SpanKind.CLIENT,
                attributes={
                    "alert.name": alert_name,
                    "alert.severity": severity,
                    "alert.source_service": source_service,
                    "alert.channel": "slack",
                    "alert.recipient": "#data-alerts",
                    "http.method": "POST",
                    "http.url": "https://hooks.slack.com/services/T00000/B00000/XXXX",
                    "server.address": "hooks.slack.com",
                },
            ) as slack_span:
                delivery_ms = random.uniform(200, 500)
                await asyncio.sleep(min(delivery_ms / 1000, 0.3))

                if random.random() < 0.05:
                    slack_span.set_status(StatusCode.ERROR, "Slack webhook timeout")
                    slack_span.set_attribute("error", True)
                    slack_span.set_attribute("http.response.status_code", 504)
                    self.log_emitter("alertmanager", {
                        "event": "notification_failed", "channel": "slack",
                        "recipient": "#data-alerts", "alert_name": alert_name,
                        "error": "webhook timeout", "delivery_ms": round(delivery_ms),
                        "level": "error",
                    })
                else:
                    slack_span.set_status(StatusCode.OK)
                    slack_span.set_attribute("http.response.status_code", 200)
                    slack_span.set_attribute("alert.delivery_ms", delivery_ms)
                    self.log_emitter("alertmanager", {
                        "event": "notification_sent", "channel": "slack",
                        "recipient": "#data-alerts", "alert_name": alert_name,
                        "delivery_ms": round(delivery_ms), "status": "delivered",
                    })

            if severity == "critical":
                with self.tracer.start_as_current_span(
                    "notify.pagerduty",
                    kind=SpanKind.CLIENT,
                    attributes={
                        "alert.name": alert_name,
                        "alert.severity": severity,
                        "alert.channel": "pagerduty",
                        "alert.recipient": "data-platform-oncall",
                        "http.method": "POST",
                        "http.url": "https://events.pagerduty.com/v2/enqueue",
                        "server.address": "events.pagerduty.com",
                    },
                ) as pd_span:
                    pd_delivery_ms = random.uniform(100, 300)
                    await asyncio.sleep(min(pd_delivery_ms / 1000, 0.2))

                    if random.random() < 0.02:
                        pd_span.set_status(StatusCode.ERROR, "PagerDuty rate limited")
                        pd_span.set_attribute("error", True)
                        pd_span.set_attribute("http.response.status_code", 429)
                        self.log_emitter("alertmanager", {
                            "event": "notification_failed", "channel": "pagerduty",
                            "recipient": "data-platform-oncall", "alert_name": alert_name,
                            "error": "rate_limited (429)", "delivery_ms": round(pd_delivery_ms),
                            "level": "error",
                        })
                    else:
                        pd_span.set_status(StatusCode.OK)
                        pd_span.set_attribute("http.response.status_code", 202)
                        pd_span.set_attribute("alert.delivery_ms", pd_delivery_ms)
                        self.log_emitter("alertmanager", {
                            "event": "notification_sent", "channel": "pagerduty",
                            "recipient": "data-platform-oncall", "alert_name": alert_name,
                            "delivery_ms": round(pd_delivery_ms), "status": "delivered",
                        })
        finally:
            from opentelemetry import context
            context.detach(ctx_token)
