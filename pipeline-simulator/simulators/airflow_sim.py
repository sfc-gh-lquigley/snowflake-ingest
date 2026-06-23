import time
import random
import uuid
import asyncio
import logging
from datetime import datetime, timezone

from opentelemetry import trace, context
from opentelemetry.trace import SpanKind, StatusCode

from config import AIRFLOW, SNOWFLAKE

log = logging.getLogger("airflow_sim")


def _run_id():
    now = datetime.now(timezone.utc)
    return f"scheduled__{now.strftime('%Y-%m-%dT%H:%M:%S+00:00')}"


def _jitter(base, variance=0.4):
    return base * random.uniform(1 - variance, 1 + variance)


class AirflowSimulator:
    def __init__(self, scheduler_tp, worker_tp, meter_provider, log_emitter):
        self.scheduler_tracer = scheduler_tp.get_tracer("airflow-scheduler", "2.8.1")
        self.worker_tracer = worker_tp.get_tracer("airflow-worker", "2.8.1")
        self.meter = meter_provider.get_meter("airflow-scheduler")
        self.log_emitter = log_emitter

        self.dag_duration_hist = self.meter.create_histogram(
            "airflow.dag_duration_seconds",
            description="Total DAG run duration",
            unit="s",
        )
        self.task_duration_hist = self.meter.create_histogram(
            "airflow.task_duration_seconds",
            description="Individual task duration",
            unit="s",
        )

    async def run_forever(self):
        interval = AIRFLOW["schedule_interval_seconds"]
        while True:
            try:
                await self._execute_dag_run()
            except Exception as e:
                log.error("Airflow sim error: %s", e)
            await asyncio.sleep(interval)

    async def _execute_dag_run(self):
        run_id = _run_id()
        dag_id = AIRFLOW["dag_id"]
        dag_start = time.time()

        with self.scheduler_tracer.start_as_current_span(
            "dag_run",
            kind=SpanKind.SERVER,
            attributes={
                "airflow.dag_id": dag_id,
                "airflow.run_id": run_id,
                "airflow.run_type": "scheduled",
                "airflow.executor": "CeleryExecutor",
            },
        ) as dag_span:
            self.log_emitter(
                "airflow-scheduler",
                {"dag_id": dag_id, "run_id": run_id, "state": "running", "event": "dag_started"},
            )

            dag_ctx = context.get_current()
            tasks_results = []

            task_result = await self._dispatch_task(
                dag_ctx, dag_id, run_id, "extract_from_s3",
                base_duration=10, operator="S3ToSnowflakeOperator",
                failure_type="S3 throttling: SlowDown",
            )
            tasks_results.append(task_result)
            if not task_result["success"]:
                dag_span.set_status(StatusCode.ERROR, "Task extract_from_s3 failed")

            task_result = await self._dispatch_task(
                dag_ctx, dag_id, run_id, "wait_for_fivetran",
                base_duration=60, operator="FivetranSensor",
                failure_type="Sensor timeout: fivetran sync not complete",
            )
            tasks_results.append(task_result)

            task_result = await self._dispatch_task(
                dag_ctx, dag_id, run_id, "run_dbt",
                base_duration=120, operator="BashOperator",
                failure_type="dbt run failed: compilation error",
                is_dbt_trigger=True,
            )
            tasks_results.append(task_result)

            task_result = await self._dispatch_task(
                dag_ctx, dag_id, run_id, "validate_data_quality",
                base_duration=20, operator="PythonOperator",
                failure_type="Great Expectations checkpoint failed: 2 expectations unmet",
            )
            tasks_results.append(task_result)

            dag_duration = time.time() - dag_start
            any_failed = any(not r["success"] for r in tasks_results)

            dag_span.set_attribute("airflow.duration_seconds", dag_duration)
            dag_span.set_attribute("airflow.dag_run_status", "failed" if any_failed else "success")

            if dag_duration > AIRFLOW["sla_seconds"]:
                dag_span.add_event("sla_miss", attributes={
                    "airflow.sla_seconds": AIRFLOW["sla_seconds"],
                    "airflow.actual_seconds": dag_duration,
                })
                self.log_emitter("airflow-scheduler", {
                    "dag_id": dag_id, "run_id": run_id, "event": "sla_miss",
                    "sla_seconds": AIRFLOW["sla_seconds"], "actual_seconds": round(dag_duration, 1),
                })

            if any_failed:
                dag_span.set_status(StatusCode.ERROR)

            self.dag_duration_hist.record(
                dag_duration,
                {"dag_id": dag_id, "status": "failed" if any_failed else "success"},
            )

            self.log_emitter("airflow-scheduler", {
                "dag_id": dag_id, "run_id": run_id,
                "state": "failed" if any_failed else "success",
                "event": "dag_finished", "duration_seconds": round(dag_duration, 1),
            })

            return {"success": not any_failed, "dag_ctx": dag_ctx, "run_id": run_id}

    async def _dispatch_task(self, dag_ctx, dag_id, run_id, task_id, **kwargs):
        """Emit a scheduler CLIENT span representing task dispatch, then run the task."""
        with self.scheduler_tracer.start_as_current_span(
            "task.dispatch",
            kind=SpanKind.CLIENT,
            attributes={
                "airflow.dag_id": dag_id,
                "airflow.task_id": task_id,
                "peer.service": "airflow-worker",
                "messaging.system": "celery",
                "celery.queue": "default",
            },
        ):
            dispatch_ctx = context.get_current()
        return await self._run_task(dispatch_ctx, dag_id, run_id, task_id, **kwargs)

    async def _run_task(self, parent_ctx, dag_id, run_id, task_id, base_duration,
                        operator, failure_type, is_dbt_trigger=False):
        ctx = context.set_value("parent", parent_ctx)
        token = context.attach(parent_ctx)
        try:
            with self.worker_tracer.start_as_current_span(
                f"task.{task_id}",
                kind=SpanKind.SERVER,
                attributes={
                    "airflow.dag_id": dag_id,
                    "airflow.run_id": run_id,
                    "airflow.task_id": task_id,
                    "airflow.operator": operator,
                    "airflow.pool": "default_pool",
                    "airflow.queue": "default",
                },
            ) as task_span:
                self.log_emitter("airflow-worker", {
                    "dag_id": dag_id, "run_id": run_id, "task_id": task_id,
                    "state": "running", "event": "task_started", "operator": operator,
                    "try_number": 1,
                })

                should_fail = random.random() < AIRFLOW["task_failure_rate"]
                duration = _jitter(base_duration)

                if should_fail and not is_dbt_trigger:
                    duration *= 0.3
                    await asyncio.sleep(min(duration, 5))

                    for retry in range(1, AIRFLOW["max_retries"] + 1):
                        task_span.add_event("retry", attributes={
                            "retry.attempt": retry,
                            "retry.reason": failure_type,
                        })
                        self.log_emitter("airflow-worker", {
                            "dag_id": dag_id, "run_id": run_id, "task_id": task_id,
                            "state": "up_for_retry", "event": "task_retry",
                            "try_number": retry + 1, "error": failure_type,
                        })
                        await asyncio.sleep(random.uniform(0.5, 1.5))

                    if random.random() < 0.5:
                        task_span.set_status(StatusCode.ERROR, failure_type)
                        task_span.set_attribute("error", True)
                        task_span.set_attribute("airflow.exception_type", failure_type.split(":")[0])
                        self.log_emitter("airflow-worker", {
                            "dag_id": dag_id, "run_id": run_id, "task_id": task_id,
                            "state": "failed", "event": "task_failed", "error": failure_type,
                        })
                        self.task_duration_hist.record(
                            duration,
                            {"dag_id": dag_id, "task_id": task_id, "status": "failed"},
                        )
                        return {"success": False, "task_id": task_id, "ctx": context.get_current()}
                elif is_dbt_trigger and hasattr(self, "dbt_sim") and self.dbt_sim:
                    with self.worker_tracer.start_as_current_span(
                        "dbt.trigger_run",
                        kind=SpanKind.CLIENT,
                        attributes={
                            "rpc.system": "subprocess",
                            "peer.service": "dbt-cloud",
                            "dbt.command": "run",
                            "airflow.task_id": task_id,
                        },
                    ):
                        dbt_success = await self.dbt_sim.execute_dbt_run(context.get_current())
                    if not dbt_success:
                        task_span.set_status(StatusCode.ERROR, "dbt run completed with errors")
                        task_span.set_attribute("error", True)
                        self.log_emitter("airflow-worker", {
                            "dag_id": dag_id, "run_id": run_id, "task_id": task_id,
                            "state": "failed", "event": "task_failed",
                            "error": "dbt run completed with errors",
                        })
                        self.task_duration_hist.record(
                            duration,
                            {"dag_id": dag_id, "task_id": task_id, "status": "failed"},
                        )
                        return {"success": False, "task_id": task_id, "ctx": context.get_current()}
                else:
                    await asyncio.sleep(min(duration, 5))

                task_span.set_attribute("airflow.task_duration_seconds", duration)
                task_span.set_status(StatusCode.OK)
                self.log_emitter("airflow-worker", {
                    "dag_id": dag_id, "run_id": run_id, "task_id": task_id,
                    "state": "success", "event": "task_finished",
                    "duration_seconds": round(duration, 2),
                })
                self.task_duration_hist.record(
                    duration,
                    {"dag_id": dag_id, "task_id": task_id, "status": "success"},
                )
                return {"success": True, "task_id": task_id, "ctx": context.get_current()}
        finally:
            context.detach(token)
