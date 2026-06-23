import random
import uuid
import asyncio
import logging
from datetime import datetime, timezone

from config import SNOWPIPE, SNOWFLAKE

log = logging.getLogger("snowpipe_sim")


def _jitter(base, variance=0.4):
    return base * random.uniform(1 - variance, 1 + variance)


class SnowpipeSimulator:
    def __init__(self, meter_provider, log_emitter):
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

        # Simulate S3 event notification delay
        await asyncio.sleep(random.uniform(0.5, 2))

        if should_stall:
            stall_duration = random.uniform(10, 30)
            queue_depth = self._queued + random.randint(5, 15)
            self.log_emitter("snowpipe-ingest", {
                "event": "queue_stall",
                "pipe_name": SNOWPIPE["pipe_name"],
                "file": file_path,
                "queue_depth": queue_depth,
                "stall_duration_seconds": round(stall_duration, 1),
                "level": "warning",
            })
            await asyncio.sleep(min(stall_duration, 3))

        if should_fail_format:
            self.log_emitter("snowpipe-ingest", {
                "event": "load_failed",
                "pipe_name": SNOWPIPE["pipe_name"],
                "file": file_path,
                "error": "LOAD_FAILED",
                "error_message": f"File format error: invalid parquet magic bytes in {file_path}",
                "level": "error",
            })
        else:
            load_duration = _jitter(SNOWPIPE["avg_load_seconds"])
            query_id = str(uuid.uuid4()).replace("-", "")[:16].upper()

            await asyncio.sleep(min(load_duration, 2))

            self.files_loaded_counter.add(1, {"pipe_name": SNOWPIPE["pipe_name"]})
            self.bytes_loaded_counter.add(file_size_bytes, {"pipe_name": SNOWPIPE["pipe_name"]})
            self.load_latency_hist.record(
                load_duration * 1000,
                {"pipe_name": SNOWPIPE["pipe_name"]},
            )

            self.log_emitter("snowpipe-ingest", {
                "event": "file_loaded",
                "pipe_name": SNOWPIPE["pipe_name"],
                "file": file_path,
                "rows_loaded": rows_in_file,
                "bytes_loaded": file_size_bytes,
                "load_duration_seconds": round(load_duration, 2),
                "query_id": query_id,
            })

        self._queued -= 1
        self.files_queued_gauge.add(-1, {"pipe_name": SNOWPIPE["pipe_name"]})
