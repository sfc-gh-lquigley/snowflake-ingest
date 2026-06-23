import time
import random
import uuid
import asyncio
import logging
from datetime import datetime, timezone

from opentelemetry import trace
from opentelemetry.trace import SpanKind, StatusCode

from config import SNOWPIPE, SNOWFLAKE

log = logging.getLogger("snowpipe_sim")


def _jitter(base, variance=0.4):
    return base * random.uniform(1 - variance, 1 + variance)


class SnowpipeSimulator:
    def __init__(self, snowpipe_tp, snowflake_tp, meter_provider, log_emitter):
        self.tracer = snowpipe_tp.get_tracer("snowpipe-ingest", "1.0.0")
        self.wh_tracer = snowflake_tp.get_tracer("snowflake", "8.40.0")
        self.meter = meter_provider.get_meter("snowpipe-ingest")
        self.log_emitter = log_emitter

        self.files_loaded_counter = self.meter.create_counter(
            "snowpipe.files_loaded",
            description="Total files loaded by Snowpipe",
        )
        self.bytes_loaded_counter = self.meter.create_counter(
            "snowpipe.bytes_loaded",
            description="Total bytes loaded by Snowpipe",
            unit="By",
        )
        self.load_latency_hist = self.meter.create_histogram(
            "snowpipe.load_latency_ms",
            description="Snowpipe file load latency",
            unit="ms",
        )
        self.files_queued_gauge = self.meter.create_up_down_counter(
            "snowpipe.files_queued",
            description="Files currently queued for Snowpipe ingest",
        )

        self._queued = 0

    async def run_forever(self):
        while True:
            try:
                await self._process_file()
            except Exception as e:
                log.error("Snowpipe sim error: %s", e)
            interval = random.uniform(*SNOWPIPE["file_interval_seconds"])
            await asyncio.sleep(interval)

    async def _process_file(self):
        now = datetime.now(timezone.utc)
        file_num = random.randint(1, 9999)
        file_path = (
            f"s3://{SNOWPIPE['bucket']}/{SNOWPIPE['prefix']}/"
            f"dt={now.strftime('%Y-%m-%d')}/part-{file_num:05d}.parquet"
        )
        file_size_mb = _jitter(SNOWPIPE["avg_file_size_mb"])
        file_size_bytes = int(file_size_mb * 1024 * 1024)
        rows_in_file = int(file_size_mb * 8000)

        should_fail_format = random.random() < SNOWPIPE["format_error_rate"]
        should_stall = random.random() < SNOWPIPE["stall_rate"]

        self._queued += 1
        self.files_queued_gauge.add(1, {"pipe_name": SNOWPIPE["pipe_name"]})

        with self.tracer.start_as_current_span(
            f"pipe.{SNOWPIPE['pipe_name']}",
            kind=SpanKind.SERVER,
            attributes={
                "snowpipe.pipe_name": SNOWPIPE["pipe_name"],
                "snowpipe.file_path": file_path,
                "snowpipe.file_size_bytes": file_size_bytes,
                "snowflake.database": SNOWFLAKE["database"],
                "snowflake.schema": SNOWFLAKE["schema"],
                "snowflake.warehouse": SNOWFLAKE["warehouse"],
                "messaging.system": "snowpipe",
                "messaging.destination": SNOWPIPE["pipe_name"],
            },
        ) as pipe_span:
            with self.tracer.start_as_current_span(
                "file_notification",
                kind=SpanKind.CONSUMER,
                attributes={
                    "snowpipe.event_type": "s3:ObjectCreated:Put",
                    "snowpipe.bucket": SNOWPIPE["bucket"],
                    "snowpipe.key": file_path.replace(f"s3://{SNOWPIPE['bucket']}/", ""),
                },
            ):
                await asyncio.sleep(random.uniform(0.5, 2))

            if should_stall:
                stall_duration = random.uniform(10, 30)
                pipe_span.add_event("queue_stall", attributes={
                    "snowpipe.stall_duration_seconds": stall_duration,
                    "snowpipe.queue_depth": self._queued + random.randint(5, 15),
                })
                self.log_emitter("snowpipe-ingest", {
                    "event": "queue_stall", "pipe_name": SNOWPIPE["pipe_name"],
                    "file": file_path, "queue_depth": self._queued + random.randint(5, 15),
                    "level": "warning",
                })
                await asyncio.sleep(min(stall_duration, 3))

            if should_fail_format:
                pipe_span.set_status(StatusCode.ERROR, "File format error")
                pipe_span.set_attribute("error", True)
                pipe_span.set_attribute("snowpipe.error", "LOAD_FAILED")
                pipe_span.set_attribute("snowpipe.error_message",
                                       f"File format error: invalid parquet magic bytes in {file_path}")
                self.log_emitter("snowpipe-ingest", {
                    "event": "load_failed", "pipe_name": SNOWPIPE["pipe_name"],
                    "file": file_path, "error": "File format error: invalid parquet magic bytes",
                    "level": "error",
                })
            else:
                load_duration = _jitter(SNOWPIPE["avg_load_seconds"])
                query_id = str(uuid.uuid4()).replace("-", "")[:16].upper()

                with self.tracer.start_as_current_span(
                    "snowflake.copy_into",
                    kind=SpanKind.CLIENT,
                    attributes={
                        "db.system": "snowflake",
                        "db.operation": "COPY INTO",
                        "peer.service": "snowflake",
                        "snowflake.warehouse": SNOWFLAKE["warehouse"],
                        "snowflake.database": SNOWFLAKE["database"],
                    },
                ):
                    with self.wh_tracer.start_as_current_span(
                        "COPY_INTO",
                        kind=SpanKind.SERVER,
                        attributes={
                            "snowflake.query_id": query_id,
                            "snowflake.warehouse": SNOWFLAKE["warehouse"],
                            "snowflake.statement_type": "COPY",
                            "snowflake.rows_produced": rows_in_file,
                            "snowflake.bytes_scanned": file_size_bytes,
                            "db.system": "snowflake",
                            "db.operation": "COPY INTO",
                        },
                    ):
                        await asyncio.sleep(min(load_duration, 2))

                with self.tracer.start_as_current_span(
                    "pipe_status_update",
                    kind=SpanKind.INTERNAL,
                    attributes={"snowpipe.status": "LOADED", "snowpipe.rows_loaded": rows_in_file},
                ):
                    await asyncio.sleep(random.uniform(0.1, 0.5))

                pipe_span.set_status(StatusCode.OK)
                pipe_span.set_attribute("snowpipe.rows_loaded", rows_in_file)
                pipe_span.set_attribute("snowpipe.load_duration_seconds", load_duration)
                pipe_span.set_attribute("snowflake.query_id", query_id)

                self.files_loaded_counter.add(1, {"pipe_name": SNOWPIPE["pipe_name"]})
                self.bytes_loaded_counter.add(file_size_bytes, {"pipe_name": SNOWPIPE["pipe_name"]})
                self.load_latency_hist.record(
                    load_duration * 1000,
                    {"pipe_name": SNOWPIPE["pipe_name"]},
                )

                self.log_emitter("snowpipe-ingest", {
                    "event": "file_loaded", "pipe_name": SNOWPIPE["pipe_name"],
                    "file": file_path, "rows_loaded": rows_in_file,
                    "load_duration_seconds": round(load_duration, 2),
                    "query_id": query_id,
                })

        self._queued -= 1
        self.files_queued_gauge.add(-1, {"pipe_name": SNOWPIPE["pipe_name"]})
