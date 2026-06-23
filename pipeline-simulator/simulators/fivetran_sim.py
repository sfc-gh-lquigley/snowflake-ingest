import random
import uuid
import asyncio
import logging

from config import FIVETRAN, SNOWFLAKE

log = logging.getLogger("fivetran_sim")


def _jitter(base, variance=0.4):
    return base * random.uniform(1 - variance, 1 + variance)


class FivetranSimulator:
    def __init__(self, meter_provider, log_emitter):
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

        # Extraction phase
        await asyncio.sleep(min(duration * 0.4, 2))

        if should_fail:
            failure_type = connector["failure_type"]
            if failure_type == "schema_drift":
                self.log_emitter("fivetran-connector", {
                    "event": "sync_end",
                    "connector_id": connector["id"],
                    "connector_type": connector["type"],
                    "sync_id": sync_id,
                    "status": "SUCCESS_WITH_WARNINGS",
                    "warning": "schema_drift",
                    "schema_change": "column_added",
                    "new_column": "marketing_attribution_source",
                    "table": f"{connector['schema']}.contacts",
                    "sync_duration_seconds": round(duration * 0.4, 1),
                })
                self.sync_duration_hist.record(
                    duration * 0.4,
                    {"connector_id": connector["id"], "status": "success_with_warnings"},
                )
            else:
                error_detail = {
                    "credential_expired": "OAuth token expired. Re-authentication required.",
                    "rate_limited": "API rate limit exceeded. Retry after 60s.",
                }.get(failure_type, failure_type)

                self.log_emitter("fivetran-connector", {
                    "event": "sync_end",
                    "connector_id": connector["id"],
                    "connector_type": connector["type"],
                    "sync_id": sync_id,
                    "status": "FAILURE",
                    "error": failure_type,
                    "error_detail": error_detail,
                    "sync_duration_seconds": round(duration * 0.4, 1),
                    "level": "error",
                })
                self.sync_duration_hist.record(
                    duration * 0.4,
                    {"connector_id": connector["id"], "status": "failed"},
                )
            return False

        # Loading phase
        await asyncio.sleep(min(duration * 0.6, 2))

        tables_synced = random.randint(3, 12)
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
            "tables_synced": tables_synced,
        })

        return True
