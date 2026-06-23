import time
import random
import uuid
import asyncio
import logging

from opentelemetry import trace, context
from opentelemetry.trace import SpanKind, StatusCode

from config import DBT, SNOWFLAKE

log = logging.getLogger("dbt_sim")


def _jitter(base, variance=0.4):
    return base * random.uniform(1 - variance, 1 + variance)


class DbtSimulator:
    def __init__(self, dbt_tp, snowflake_tp, meter_provider, log_emitter):
        self.tracer = dbt_tp.get_tracer("dbt-cloud", "1.7.4")
        self.wh_tracer = snowflake_tp.get_tracer("snowflake", "8.40.0")
        self.meter = meter_provider.get_meter("dbt-cloud")
        self.log_emitter = log_emitter

        self.model_duration_hist = self.meter.create_histogram(
            "dbt.model_execution_seconds",
            description="dbt model execution duration",
            unit="s",
        )
        self.test_results_counter = self.meter.create_counter(
            "dbt.test_results",
            description="dbt test pass/fail counts",
        )

    async def execute_dbt_run(self, parent_context=None):
        invocation_id = str(uuid.uuid4())
        is_full_refresh = random.random() < DBT["full_refresh_rate"]

        ctx_token = None
        if parent_context:
            ctx_token = context.attach(parent_context)

        try:
            with self.tracer.start_as_current_span(
                "dbt_run",
                kind=SpanKind.SERVER,
                attributes={
                    "dbt.invocation_id": invocation_id,
                    "dbt.project": DBT["project"],
                    "dbt.target": DBT["target"],
                    "dbt.command": "run --full-refresh" if is_full_refresh else "run",
                    "dbt.version": "1.7.4",
                    "snowflake.account": SNOWFLAKE["account"],
                    "snowflake.warehouse": SNOWFLAKE["warehouse"],
                    "snowflake.database": SNOWFLAKE["database"],
                    "snowflake.schema": SNOWFLAKE["schema"],
                },
            ) as run_span:
                self.log_emitter("dbt-cloud", {
                    "event": "run_started", "invocation_id": invocation_id,
                    "command": "dbt run", "full_refresh": is_full_refresh,
                })

                compile_duration = random.uniform(2, 5)
                with self.tracer.start_as_current_span(
                    "dbt_compile",
                    kind=SpanKind.INTERNAL,
                    attributes={"dbt.phase": "compile", "dbt.invocation_id": invocation_id},
                ):
                    await asyncio.sleep(min(compile_duration, 1.5))

                self.log_emitter("dbt-cloud", {
                    "event": "compilation_complete", "invocation_id": invocation_id,
                    "models_to_run": len(DBT["models"]),
                })

                model_results = []
                for i, model in enumerate(DBT["models"], 1):
                    result = await self._run_model(invocation_id, model, i, len(DBT["models"]), is_full_refresh)
                    model_results.append(result)

                test_results = []
                for test in DBT["tests"]:
                    result = await self._run_test(invocation_id, test)
                    test_results.append(result)

                any_model_failed = any(not r["success"] for r in model_results)
                any_test_failed = any(not r["success"] for r in test_results)

                if any_model_failed or any_test_failed:
                    run_span.set_status(StatusCode.ERROR, "dbt run completed with errors")
                    run_span.set_attribute("dbt.run_status", "error")
                else:
                    run_span.set_status(StatusCode.OK)
                    run_span.set_attribute("dbt.run_status", "success")

                run_span.set_attribute("dbt.models_run", len(model_results))
                run_span.set_attribute("dbt.tests_run", len(test_results))
                run_span.set_attribute("dbt.tests_passed", sum(1 for r in test_results if r["success"]))

                self.log_emitter("dbt-cloud", {
                    "event": "run_finished", "invocation_id": invocation_id,
                    "status": "error" if (any_model_failed or any_test_failed) else "success",
                    "models_run": len(model_results), "tests_run": len(test_results),
                    "tests_passed": sum(1 for r in test_results if r["success"]),
                })

                return not (any_model_failed or any_test_failed)
        finally:
            if ctx_token:
                context.detach(ctx_token)

    async def _run_model(self, invocation_id, model, index, total, is_full_refresh):
        query_id = str(uuid.uuid4()).replace("-", "")[:16].upper()
        materialization = "table" if is_full_refresh else model["materialization"]
        duration = _jitter(model["avg_seconds"])
        if is_full_refresh:
            duration *= 3

        rows = int(_jitter(model["avg_rows"]))

        self.log_emitter("dbt-cloud", {
            "event": "model_started", "invocation_id": invocation_id,
            "msg": f"{index} of {total} START {materialization} model {SNOWFLAKE['schema']}.{model['name']}",
            "node_id": f"model.{DBT['project']}.{model['name']}",
        })

        with self.tracer.start_as_current_span(
            f"model.{model['name']}",
            kind=SpanKind.INTERNAL,
            attributes={
                "dbt.model_name": model["name"],
                "dbt.materialization": materialization,
                "dbt.invocation_id": invocation_id,
                "dbt.node_id": f"model.{DBT['project']}.{model['name']}",
                "dbt.schema": SNOWFLAKE["schema"],
                "dbt.database": SNOWFLAKE["database"],
            },
        ) as model_span:
            with self.tracer.start_as_current_span(
                "snowflake.query",
                kind=SpanKind.CLIENT,
                attributes={
                    "db.system": "snowflake",
                    "db.operation": materialization.upper(),
                    "peer.service": "snowflake",
                    "snowflake.warehouse": SNOWFLAKE["warehouse"],
                    "snowflake.database": SNOWFLAKE["database"],
                },
            ):
                with self.wh_tracer.start_as_current_span(
                    "query_execution",
                    kind=SpanKind.SERVER,
                    attributes={
                        "snowflake.query_id": query_id,
                        "snowflake.warehouse": SNOWFLAKE["warehouse"],
                        "snowflake.database": SNOWFLAKE["database"],
                        "snowflake.schema": SNOWFLAKE["schema"],
                        "snowflake.statement_type": "CREATE TABLE AS SELECT" if materialization == "table" else "MERGE",
                        "db.system": "snowflake",
                        "db.name": SNOWFLAKE["database"],
                    },
                ) as query_span:
                    await asyncio.sleep(min(duration, 3))
                    query_span.set_attribute("snowflake.rows_produced", rows)
                    query_span.set_attribute("snowflake.bytes_scanned", rows * random.randint(200, 800))

            model_span.set_attribute("dbt.rows_affected", rows)
            model_span.set_attribute("dbt.execution_time_seconds", duration)
            model_span.set_attribute("snowflake.query_id", query_id)
            model_span.set_status(StatusCode.OK)

            self.model_duration_hist.record(
                duration,
                {"model_name": model["name"], "materialization": materialization},
            )

            self.log_emitter("dbt-cloud", {
                "event": "model_finished", "invocation_id": invocation_id,
                "msg": f"{index} of {total} OK {materialization} model {SNOWFLAKE['schema']}.{model['name']} [{rows} rows in {duration:.1f}s]",
                "node_id": f"model.{DBT['project']}.{model['name']}",
                "rows_affected": rows, "execution_time": round(duration, 2),
            })

            return {"success": True, "model": model["name"], "duration": duration}

    async def _run_test(self, invocation_id, test):
        passes = random.random() < test["pass_rate"]
        duration = random.uniform(1, 5)

        with self.tracer.start_as_current_span(
            f"test.{test['name']}",
            kind=SpanKind.INTERNAL,
            attributes={
                "dbt.test_name": test["name"],
                "dbt.model": test["model"],
                "dbt.invocation_id": invocation_id,
                "dbt.node_id": f"test.{DBT['project']}.{test['name']}",
            },
        ) as test_span:
            await asyncio.sleep(min(duration, 1))

            if passes:
                test_span.set_status(StatusCode.OK)
                test_span.set_attribute("dbt.test_status", "pass")
                self.test_results_counter.add(1, {"test_name": test["name"], "status": "pass"})
            else:
                test_span.set_status(StatusCode.ERROR, f"Test {test['name']} failed")
                test_span.set_attribute("dbt.test_status", "fail")
                test_span.set_attribute("dbt.failures", random.randint(1, 15))
                self.test_results_counter.add(1, {"test_name": test["name"], "status": "fail"})
                self.log_emitter("dbt-cloud", {
                    "event": "test_failed", "invocation_id": invocation_id,
                    "test_name": test["name"], "model": test["model"],
                    "failures": random.randint(1, 15),
                    "level": "error",
                })

            return {"success": passes, "test": test["name"]}
