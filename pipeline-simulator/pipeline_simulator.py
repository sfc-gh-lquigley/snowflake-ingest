#!/usr/bin/env python3
"""
Snowflake Ingest Pipeline Telemetry Simulator

Emits realistic OTel traces, logs, and metrics mimicking a production
Snowflake data platform with Airflow, dbt, Fivetran, and Snowpipe.
"""
import os
import sys
import json
import asyncio
import logging
import time
from datetime import datetime, timezone

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import OTEL_ENDPOINT, DEPLOYMENT_ENV, SERVICE_NAMESPACE, AIRFLOW
from simulators.airflow_sim import AirflowSimulator
from simulators.dbt_sim import DbtSimulator
from simulators.fivetran_sim import FivetranSimulator
from simulators.snowpipe_sim import SnowpipeSimulator
from simulators.warehouse_metrics import WarehouseMetricsSimulator
from simulators.alertmanager_sim import AlertManagerSimulator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("pipeline_simulator")


def create_service_telemetry(service_name):
    """Create an isolated OTel stack (TracerProvider, MeterProvider, LoggerProvider)
    for a single logical service, with service.name set in the Resource."""
    resource = Resource.create({
        "service.name": service_name,
        "service.namespace": SERVICE_NAMESPACE,
        "deployment.environment": DEPLOYMENT_ENV,
        "service.version": "1.0.0",
    })

    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{OTEL_ENDPOINT}/v1/traces"))
    )

    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[PeriodicExportingMetricReader(
            OTLPMetricExporter(endpoint=f"{OTEL_ENDPOINT}/v1/metrics"),
            export_interval_millis=30000,
        )],
    )

    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(
        BatchLogRecordProcessor(OTLPLogExporter(endpoint=f"{OTEL_ENDPOINT}/v1/logs"))
    )

    return tracer_provider, meter_provider, logger_provider


class StructuredLogEmitter:
    """Routes log records to the LoggerProvider for the correct service so that
    the OTel resource carries the right service.name."""

    def __init__(self, logger_providers: dict):
        # {service_name: LoggerProvider}
        self._providers = logger_providers
        self._loggers = {}

    def _get_logger(self, service_name):
        if service_name not in self._loggers:
            provider = self._providers.get(service_name)
            if provider is None:
                raise ValueError(f"No LoggerProvider registered for service: {service_name}")
            self._loggers[service_name] = provider.get_logger(service_name)
        return self._loggers[service_name]

    def emit(self, service_name, data):
        from opentelemetry._logs import SeverityNumber

        level = data.get("level", "info").upper()
        severity_map = {
            "DEBUG": SeverityNumber.DEBUG,
            "INFO": SeverityNumber.INFO,
            "WARNING": SeverityNumber.WARN,
            "ERROR": SeverityNumber.ERROR,
            "CRITICAL": SeverityNumber.FATAL,
        }

        body = json.dumps({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "service": service_name,
            **data,
        })

        logger = self._get_logger(service_name)
        logger.emit(
            timestamp=int(time.time_ns()),
            observed_timestamp=int(time.time_ns()),
            severity_number=severity_map.get(level, SeverityNumber.INFO),
            severity_text=level,
            body=body,
            attributes={
                "log.source": service_name,
                "event.domain": "data-platform",
                "event.name": data.get("event", "unknown"),
            },
        )


async def run_airflow_loop(airflow_sim, dbt_sim, alert_sim, warehouse_metrics):
    airflow_sim.dbt_sim = dbt_sim
    interval = AIRFLOW["schedule_interval_seconds"]
    while True:
        try:
            warehouse_metrics.set_dag_running(True)
            result = await airflow_sim._execute_dag_run()

            if result and not result["success"]:
                await alert_sim.send_alert(
                    result["dag_ctx"],
                    alert_name="airflow_dag_failure",
                    severity="critical",
                    source_service="airflow-worker",
                )
            warehouse_metrics.set_dag_running(False)
        except Exception as e:
            log.error("Airflow loop error: %s", e)
            warehouse_metrics.set_dag_running(False)

        await asyncio.sleep(interval)


async def main():
    log.info("Starting Snowflake Ingest Pipeline Simulator")
    log.info("OTel endpoint: %s", OTEL_ENDPOINT)

    # --- Per-service telemetry stacks ---
    airflow_sched_tp, airflow_sched_mp, airflow_sched_lp = create_service_telemetry("airflow-scheduler")
    airflow_worker_tp, _,               airflow_worker_lp = create_service_telemetry("airflow-worker")
    dbt_tp,           dbt_mp,           dbt_lp            = create_service_telemetry("dbt-cloud")
    fivetran_tp,      fivetran_mp,      fivetran_lp       = create_service_telemetry("fivetran-connector")
    snowpipe_tp,      snowpipe_mp,      snowpipe_lp       = create_service_telemetry("snowpipe-ingest")
    snowflake_tp,     snowflake_mp,     _                 = create_service_telemetry("snowflake")
    alertmgr_tp,      _,                alertmgr_lp       = create_service_telemetry("alertmanager")

    log_emitter = StructuredLogEmitter({
        "airflow-scheduler":  airflow_sched_lp,
        "airflow-worker":     airflow_worker_lp,
        "dbt-cloud":          dbt_lp,
        "fivetran-connector": fivetran_lp,
        "snowpipe-ingest":    snowpipe_lp,
        "alertmanager":       alertmgr_lp,
    })

    airflow_sim   = AirflowSimulator(airflow_sched_tp, airflow_worker_tp, airflow_sched_mp, log_emitter.emit)
    dbt_sim       = DbtSimulator(dbt_tp, snowflake_tp, dbt_mp, log_emitter.emit)
    fivetran_sim  = FivetranSimulator(fivetran_tp, fivetran_mp, log_emitter.emit, snowflake_tp)
    snowpipe_sim  = SnowpipeSimulator(snowpipe_tp, snowflake_tp, snowpipe_mp, log_emitter.emit)
    warehouse_metrics = WarehouseMetricsSimulator(snowflake_mp)
    alert_sim     = AlertManagerSimulator(alertmgr_tp, log_emitter.emit)

    log.info("All simulators initialized. Starting event loops...")

    await asyncio.gather(
        run_airflow_loop(airflow_sim, dbt_sim, alert_sim, warehouse_metrics),
        fivetran_sim.run_forever(),
        snowpipe_sim.run_forever(),
        warehouse_metrics.run_forever(),
    )


if __name__ == "__main__":
    asyncio.run(main())
