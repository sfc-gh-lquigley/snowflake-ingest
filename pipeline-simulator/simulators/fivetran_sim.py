import time
import random
import uuid
import asyncio
import logging

from opentelemetry import trace
from opentelemetry.trace import SpanKind, StatusCode

from config import FIVETRAN, SNOWFLAKE

log = logging.getLogger("fivetran_sim")


def _jitter(base, variance=0.4):
    return base * random.uniform(1 - variance, 1 + variance)


class FivetranSimulator:
    def __init__(self, tracer_provider, meter_provider, log_emitter, snowflake_tp=None):
        self.tracer = tracer_provider.get_tracer("fivetran-connector", "1.0.0")
        self.snowflake_tracer = snowflake_tp.get_tracer("snowflake", "8.40.0") if snowflake_tp else None
        self.meter = meter_provider.get_meter("fivetran-connector")
        self.log_emitter = log_emitter

        self.sync_duration_hist = self.meter.create_histogram(
            "fivetran.sync_duration_seconds",
            description="Fivetran connector sync duration",
            unit="s",
        )
        self.rows_synced_counter = self.meter.create_counter(
            "fivetran.rows_synced",
            description="Total rows synced by Fivetran",
        )

    async def run_forever(self):
        tasks = []
        for connector in FIVETRAN["connectors"]:
            tasks.append(asyncio.create_task(self._connector_loop(connector)))
        await asyncio.gather(*tasks)

    async def _connector_loop(self, connector):
        await asyncio.sleep(random.uniform(0, connector["schedule_seconds"] * 0.1))
        while True:
            try:
                await self._execute_sync(connector)
            except Exception as e:
                log.error("Fivetran sim error [%s]: %s", connector["id"], e)
            await asyncio.sleep(connector["schedule_seconds"])

    async def _execute_sync(self, connector):
        sync_id = str(uuid.uuid4())[:12]
        should_fail = random.random() < connector["failure_rate"]
        duration = _jitter(connector["avg_duration_seconds"])
        rows = int(_jitter(connector["avg_rows"]))

        self.log_emitter("fivetran-connector", {
            "event": "sync_start",
            "connector_id": connector["id"],
            "connector_type": connector["type"],
            "schema": connector["schema"],
            "sync_id": sync_id,
        })

        with self.tracer.start_as_current_span(
            f"sync.{connector['type']}",
            kind=SpanKind.SERVER,
            attributes={
                "fivetran.connector_id": connector["id"],
                "fivetran.connector_type": connector["type"],
                "fivetran.schema": connector["schema"],
                "fivetran.sync_id": sync_id,
                "fivetran.destination": "snowflake",
                "snowflake.database": SNOWFLAKE["database"],
            },
        ) as sync_span:
            with self.tracer.start_as_current_span(
                "extracting",
                kind=SpanKind.INTERNAL,
                attributes={"fivetran.phase": "extracting", "fivetran.connector_id": connector["id"]},
            ):
                await asyncio.sleep(min(duration * 0.4, 2))

            if should_fail:
                sync_span.set_status(StatusCode.ERROR, connector["failure_type"])
                sync_span.set_attribute("error", True)
                sync_span.set_attribute("fivetran.error_type", connector["failure_type"])
                sync_span.set_attribute("fivetran.sync_status", "FAILURE")

                if connector["failure_type"] == "credential_expired":
                    sync_span.add_event("auth_failure", attributes={
                        "error.message": "OAuth token expired. Re-authentication required.",
                        "http.status_code": 401,
                    })
                elif connector["failure_type"] == "rate_limited":
                    sync_span.add_event("rate_limit", attributes={
                        "error.message": "API rate limit exceeded. Retry after 60s.",
                        "http.status_code": 429,
                        "retry_after_seconds": 60,
                    })
                elif connector["failure_type"] == "schema_drift":
                    sync_span.add_event("schema_change", attributes={
                        "warning": "New column detected: marketing_attribution_source",
                        "table": f"{connector['schema']}.contacts",
                        "action": "column_added",
                    })
                    sync_span.set_status(StatusCode.OK)
                    sync_span.set_attribute("fivetran.sync_status", "SUCCESS_WITH_WARNINGS")
                    sync_span.set_attribute("fivetran.schema_changes", 1)

                self.log_emitter("fivetran-connector", {
                    "event": "sync_end",
                    "connector_id": connector["id"],
                    "connector_type": connector["type"],
                    "sync_id": sync_id,
                    "status": "FAILURE" if connector["failure_type"] != "schema_drift" else "SUCCESS_WITH_WARNINGS",
                    "error": connector["failure_type"],
                    "sync_duration_seconds": round(duration * 0.4, 1),
                })

                self.sync_duration_hist.record(
                    duration * 0.4,
                    {"connector_id": connector["id"], "status": "failed"},
                )
                return False

            with self.tracer.start_as_current_span(
                "loading",
                kind=SpanKind.INTERNAL,
                attributes={
                    "fivetran.phase": "loading",
                    "fivetran.connector_id": connector["id"],
                    "fivetran.rows": rows,
                },
            ):
                with self.tracer.start_as_current_span(
                    "snowflake.write",
                    kind=SpanKind.CLIENT,
                    attributes={
                        "db.system": "snowflake",
                        "db.operation": "INSERT",
                        "peer.service": "snowflake",
                        "snowflake.database": SNOWFLAKE["database"],
                        "snowflake.schema": connector["schema"],
                        "fivetran.rows": rows,
                    },
                ):
                    if self.snowflake_tracer:
                        with self.snowflake_tracer.start_as_current_span(
                            "fivetran.ingest",
                            kind=SpanKind.SERVER,
                            attributes={
                                "snowflake.database": SNOWFLAKE["database"],
                                "snowflake.schema": connector["schema"],
                                "snowflake.rows_produced": rows,
                            },
                        ):
                            await asyncio.sleep(min(duration * 0.6, 2))
                    else:
                        await asyncio.sleep(min(duration * 0.6, 2))

            sync_span.set_attribute("fivetran.rows_synced", rows)
            sync_span.set_attribute("fivetran.sync_duration_seconds", duration)
            sync_span.set_attribute("fivetran.sync_status", "SUCCESSFUL")
            sync_span.set_attribute("fivetran.tables_synced", random.randint(3, 12))
            sync_span.set_status(StatusCode.OK)

            self.sync_duration_hist.record(
                duration,
                {"connector_id": connector["id"], "status": "success"},
            )
            self.rows_synced_counter.add(
                rows,
                {"connector_id": connector["id"]},
            )

            self.log_emitter("fivetran-connector", {
                "event": "sync_end",
                "connector_id": connector["id"],
                "connector_type": connector["type"],
                "sync_id": sync_id,
                "status": "SUCCESSFUL",
                "rows_updated": rows,
                "sync_duration_seconds": round(duration, 1),
                "tables_synced": random.randint(3, 12),
            })

            return True
