import random
import asyncio
import logging
import math

from opentelemetry.metrics import Observation

from config import WAREHOUSE_METRICS, SNOWFLAKE

log = logging.getLogger("warehouse_metrics_sim")


class WarehouseMetricsSimulator:
    def __init__(self, meter_provider):
        self.meter = meter_provider.get_meter("snowflake-warehouse")

        self.credits_counter = self.meter.create_counter(
            "snowflake.warehouse_credits_used",
            description="Warehouse credits consumed",
            unit="credits",
        )
        self.queue_depth_gauge = self.meter.create_observable_gauge(
            "snowflake.warehouse_queue_depth",
            callbacks=[self._observe_queue_depth],
            description="Queries queued waiting for warehouse resources",
        )
        self.active_queries_gauge = self.meter.create_observable_gauge(
            "snowflake.active_queries",
            callbacks=[self._observe_active_queries],
            description="Currently executing queries",
        )
        self.warehouse_load_gauge = self.meter.create_observable_gauge(
            "snowflake.warehouse_load_percent",
            callbacks=[self._observe_load],
            description="Warehouse compute utilization percentage",
        )

        self._queue_depth = 0
        self._active_queries = WAREHOUSE_METRICS["base_active_queries"]
        self._load_percent = 30.0
        self._is_dag_running = False

    def set_dag_running(self, running: bool):
        self._is_dag_running = running

    def _observe_queue_depth(self, options):
        yield Observation(
            self._queue_depth,
            {"warehouse_name": SNOWFLAKE["warehouse"]},
        )

    def _observe_active_queries(self, options):
        yield Observation(
            self._active_queries,
            {"warehouse_name": SNOWFLAKE["warehouse"]},
        )

    def _observe_load(self, options):
        yield Observation(
            self._load_percent,
            {"warehouse_name": SNOWFLAKE["warehouse"]},
        )

    async def run_forever(self):
        interval = WAREHOUSE_METRICS["poll_interval_seconds"]
        tick = 0
        while True:
            tick += 1
            base_credits = WAREHOUSE_METRICS["base_credits_per_hour"] / (3600 / interval)

            if self._is_dag_running:
                credit_multiplier = random.uniform(2.0, 4.0)
                self._active_queries = random.randint(4, 12)
                self._queue_depth = random.randint(0, 5)
                self._load_percent = random.uniform(60, 95)
            else:
                diurnal = 1.0 + 0.3 * math.sin(tick * 2 * math.pi / (3600 / interval * 24))
                credit_multiplier = diurnal + random.uniform(-0.1, 0.1)
                self._active_queries = random.randint(0, 3)
                self._queue_depth = 0
                self._load_percent = random.uniform(10, 40)

            credits_this_tick = base_credits * credit_multiplier
            self.credits_counter.add(
                credits_this_tick,
                {"warehouse_name": SNOWFLAKE["warehouse"]},
            )

            await asyncio.sleep(interval)
