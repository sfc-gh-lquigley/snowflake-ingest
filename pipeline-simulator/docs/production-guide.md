# Instrumenting a Data Pipeline Stack with OpenTelemetry → Observe

This document covers how each component in this pipeline simulator was instrumented,
whether that approach is feasible in a real production environment, and what you would
actually do in production for each source.

---

## How the Simulator Works

The `pipeline-simulator` is a standalone Python process that uses the OpenTelemetry Python
SDK to emit traces, metrics, and logs directly to an OTel collector running on the same host
(localhost:4318, OTLP/HTTP). The collector forwards everything to Observe.

Each pipeline component runs as a separate asyncio coroutine and gets its own
`TracerProvider`, `MeterProvider`, and `LoggerProvider`, each backed by an OTel `Resource`
with a distinct `service.name`. This is what produces the separate service nodes in Observe's
APM service map.

Cross-service trace propagation works via OpenTelemetry's in-process context API
(`context.attach` / `context.get_current`). When Airflow's scheduler span is active and
dispatches a task, the worker's span inherits the same `trace_id` and records the scheduler
span's `span_id` as its `parent_span_id`. This is what draws the edges on the service map.

---

## Service-by-Service Assessment

### Airflow

**What the simulator does:**
Manually creates `dag_run` (scheduler) and `task.*` (worker) spans with appropriate
attributes (`airflow.dag_id`, `airflow.run_id`, `airflow.task_id`, `airflow.operator`).
The scheduler emits a `task.dispatch` CLIENT span before each task, and the worker emits a
SERVER span per task instance.

**Is this production-feasible?**
Yes — and it is largely unnecessary to do manually. Apache Airflow has built-in
OpenTelemetry support:

- **Airflow 2.7+**: Configure the `[metrics]` and `[traces]` sections in `airflow.cfg`:
  ```ini
  [traces]
  otel_on = True
  otel_host = localhost
  otel_port = 4318
  otel_ssl = False
  otel_prefix = airflow
  otel_interval_milliseconds = 30000
  ```
- The `apache-airflow-providers-opentelemetry` package adds richer span instrumentation
  for DAG runs and task instances.
- Each TaskInstance gets its own span with status, duration, retries, and SLA information.

**Production deployment pattern:**
Run an OTel collector as a sidecar container alongside the Airflow scheduler and worker
pods (Kubernetes) or as a background process on the same host. Point `otel_host` at it.
The collector handles batching, retry, and forwarding to Observe.

---

### dbt

**What the simulator does:**
Manually creates `dbt_run`, `dbt_compile`, `model.*`, and `test.*` spans. The `dbt_run`
span receives Airflow's trace context as a parent (passed via `execute_dbt_run(parent_context)`),
which is what links the dbt service into the Airflow trace.

**Is this production-feasible?**
Yes. dbt has OTel support via two paths:

- **dbt Cloud**: Provides a webhook on job completion. A lightweight receiver can translate
  these webhook payloads into OTel spans and push them to a collector. dbt Cloud also
  supports native structured logs that carry run and model timing.
- **dbt Core 1.8+**: Supports `--log-format json`. The structured logs include
  `node_started`, `node_finished`, and `run_result` events per model. A log parser (or the
  `dbt-opentelemetry` community package) can convert these into spans.

**Critical caveat — trace context propagation:**
The simulator passes Airflow's trace context to dbt explicitly in Python. In a real deployment,
Airflow runs dbt as a subprocess via `BashOperator` or `DbtCloudRunJobOperator`. To propagate
the trace context:
- Inject the W3C `traceparent` header as an environment variable before invoking dbt:
  ```python
  os.environ["TRACEPARENT"] = format_traceparent(current_span.get_span_context())
  ```
- The dbt OTel instrumentation reads `TRACEPARENT` and uses it as the parent context.

Without this, dbt traces are disconnected from Airflow traces in production.

---

### Fivetran

**What the simulator does:**
Emits `sync.*` spans (SERVER) with Fivetran connector attributes, plus child `extracting`,
`loading`, and `snowflake.write` (CLIENT → Snowflake) spans. This makes Fivetran appear
as a first-class service in the APM map.

**Is this production-feasible? — No, not directly.**

Fivetran is a closed SaaS product. You cannot deploy custom code inside it to emit OTel
spans. There is no native Fivetran OTel exporter.

**What you would actually do in production:**

1. **Fivetran Webhooks** (recommended): Fivetran fires webhook events on sync start,
   success, and failure. Build a small webhook receiver (AWS Lambda, Cloud Run, etc.) that
   receives these events and emits OTel spans:
   ```
   [Fivetran Webhook] → [Lambda receiver] → [OTel Collector] → [Observe]
   ```
   The Lambda creates a span per sync with start/end timestamps from the webhook payload.
   Failure events set the span status to ERROR.

2. **Polling the Fivetran REST API**: Poll `/v1/connectors/{connector_id}/last_sync` on a
   schedule and emit a span per completed sync. Less real-time than webhooks.

3. **Observe's native Fivetran integration**: Observe has a built-in Fivetran log connector
   that ingests sync logs directly via the Fivetran Log service. This is the lowest-effort
   path and does not require OTel at all — Observe maps the logs to its APM model natively.

**Trade-off**: The webhook/polling approach gives you OTel-native spans that join the same
trace as Airflow (if you propagate the context). The Observe native integration is simpler
but produces logs rather than traces, so you won't get the service map edge.

---

### Snowpipe

**What the simulator does:**
Emits `pipe.*` (SERVER), `file_notification` (CONSUMER), `snowflake.copy_into` (CLIENT →
Snowflake), and `pipe_status_update` spans on every file ingest cycle.

**Is this production-feasible? — Partially.**

Snowpipe is a Snowflake-managed service. You cannot instrument its internals. However,
there are two viable production approaches:

1. **Instrument the notification side**: The S3 event notification (or SQS message) that
   triggers Snowpipe is code you own (or at least can observe). An AWS Lambda that listens
   to the S3 event before forwarding to Snowpipe can emit the `file_notification` span.
   The `COPY_INTO` completion can be detected by polling `SYSTEM$PIPE_STATUS` or
   `COPY_HISTORY`.

2. **Observe's native Snowflake connector**: Observe ingests Snowflake's `COPY_HISTORY`,
   `QUERY_HISTORY`, and `PIPE_USAGE_HISTORY` views via the Snowflake integration. This gives
   you load latency, file counts, error rates, and bytes loaded — the same signals the
   simulator produces as metrics — without any custom instrumentation.

**Honest assessment**: The simulator's Snowpipe traces are a useful demo artifact. In
production you would not typically emit OTel spans for Snowpipe; you would use Observe's
Snowflake integration for the load monitoring metrics and build alerts on `COPY_HISTORY`
anomalies.

---

### Snowflake Warehouse

**What the simulator does:**
Emits `query_execution` (SERVER) and `COPY_INTO` (SERVER) spans representing Snowflake
executing queries dispatched from dbt and Snowpipe. Also emits warehouse utilization
metrics (credits, queue depth, active queries) on a 30-second interval.

**Is this production-feasible?**
Partially. Snowflake added native OpenTelemetry event table support in 2024:

- Snowflake stored procedures can emit spans using the `SYSTEM$SET_SPAN_ATTRIBUTES` and
  related functions.
- The event table can be queried for span data, which Observe can ingest.

However, for ad-hoc `SELECT` / `MERGE` queries issued by dbt or external clients, Snowflake
does not automatically emit OTel spans. For those, the practical production approach is:

1. **Observe's Snowflake integration**: Ingests `QUERY_HISTORY`, `WAREHOUSE_METERING_HISTORY`,
   and `WAREHOUSE_EVENTS_HISTORY` on a polling interval. This covers query duration, credit
   consumption, queuing, and errors without any code changes.
2. **dbt → Snowflake trace linkage**: When dbt emits a CLIENT span for a query, it can set
   `db.snowflake.query_id` on the span. Observe can correlate this with query history data.

---

## Deployment Architecture

### What the simulator uses

```
[pipeline-simulator (EC2)]
  └── Python asyncio process
      ├── TracerProvider per service (7 providers)
      ├── BatchSpanProcessor → OTLP/HTTP → localhost:4318
      └── [OTel Collector on same host]
              └── OTLP/HTTP → Observe OTEL endpoint
```

This is a valid pattern for development and demo environments. The OTel collector handles
batching, retry logic, and authentication so the application code stays simple.

### What production looks like

In production, each service runs independently and instruments itself:

```
[Airflow Scheduler/Worker pods]
  └── airflow.cfg: otel_host = otel-collector.internal
      └── [OTel Collector - DaemonSet or sidecar]

[dbt runs (as Airflow tasks or dbt Cloud jobs)]
  └── TRACEPARENT env var + dbt-opentelemetry
      └── → same OTel Collector

[Fivetran Webhook Lambda]
  └── opentelemetry-sdk → OTLP → OTel Collector

[Snowflake monitoring]
  └── Observe Snowflake native connector (QUERY_HISTORY, COPY_HISTORY, etc.)

[All OTel Collectors] → Observe OTEL ingest endpoint
```

### Sending to Observe

Observe accepts OTLP/HTTP directly. Configure your OTel collector's exporter:

```yaml
# otelcol-config.yaml
exporters:
  otlphttp/observe:
    endpoint: "https://<customer-id>.collect.observeinc.com/v2/otel"
    headers:
      authorization: "Bearer <your-datastream-token>"

service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [otlphttp/observe]
    metrics:
      receivers: [otlp]
      exporters: [otlphttp/observe]
    logs:
      receivers: [otlp]
      exporters: [otlphttp/observe]
```

The simulator sends directly without an intermediate collector config file because it
uses the Python SDK's `OTLPSpanExporter` pointed at `localhost:4318`, and a pre-configured
collector instance handles the Observe forwarding on the EC2 host.

---

## What This Simulator Does and Does Not Prove

| Aspect | Simulated accurately | Caveats |
|---|---|---|
| OTel span structure and attributes | Yes | Follows OTel semantic conventions |
| Cross-service trace propagation | Yes | Uses the standard W3C context API |
| Service map topology in Observe | Yes | CLIENT→SERVER pattern required for edges |
| Airflow instrumentation | Approximately | Native Airflow OTel is simpler in practice |
| dbt instrumentation | Approximately | `traceparent` env var injection needed for real propagation |
| Fivetran instrumentation | No | Fivetran is SaaS; requires webhook bridge or Observe native connector |
| Snowpipe instrumentation | No | Snowpipe is managed; Observe Snowflake integration is the real path |
| Snowflake query spans | No | Requires Snowflake stored proc OTel or `QUERY_HISTORY` polling |
| Error rates and failure modes | Yes | Configurable failure rates per component |
| Production deployment pattern | Partially | OTel collector + OTLP is correct; the single-process simulator is not |

The simulator is most useful for demonstrating the **observability model** (how these
services relate, what signals matter, what failure modes look like in Observe) rather than
as a reference implementation for instrumentation code.
