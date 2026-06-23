# Pipeline Simulator — OpenTelemetry → Observe

A production-style telemetry simulator for a Snowflake data platform. Emits realistic
OpenTelemetry traces, metrics, and logs for Airflow, dbt, Fivetran, Snowpipe, and Snowflake
Warehouse — wired together with proper cross-service trace propagation so the full pipeline
appears as a connected service map in Observe.

## Service Map

```
airflow-scheduler ──► airflow-worker ──► dbt-cloud ──┐
fivetran-connector ───────────────────────────────────►  snowflake
snowpipe-ingest ──────────────────────────────────────┘
       │
       └── alertmanager (on DAG failure)
```

Each node is a separate OTel `service.name`. Edges are drawn by Observe when a CLIENT span
in one service is the parent of a SERVER span in another — following the standard
[OpenTelemetry semantic conventions](https://opentelemetry.io/docs/specs/semconv/).

## What It Emits

| Service | Entry span | Kind | Cadence |
|---|---|---|---|
| `airflow-scheduler` | `dag_run` | SERVER | Every 5 min |
| `airflow-worker` | `task.<id>` | SERVER | 4× per DAG run |
| `dbt-cloud` | `dbt_run` | SERVER | 1× per DAG run |
| `snowflake` | `query_execution`, `COPY_INTO`, `fivetran.ingest` | SERVER | Per query / load |
| `fivetran-connector` | `sync.<type>` | SERVER | Every 5 min per connector |
| `snowpipe-ingest` | `pipe.RAW_EVENTS_PIPE` | SERVER | Every 5–30 s |
| `alertmanager` | `notify.slack`, `notify.pagerduty` | CLIENT | On DAG failure |

---

## Architecture

```
┌─────────────────────────────────────────────┐
│  EC2 host (or any Linux VM)                 │
│                                             │
│  pipeline_simulator.py (systemd service)    │
│    ├── AirflowSimulator                     │
│    ├── DbtSimulator                         │
│    ├── FivetranSimulator                    │
│    ├── SnowpipeSimulator                    │
│    ├── WarehouseMetricsSimulator            │
│    └── AlertManagerSimulator               │
│           │                                 │
│           │ OTLP/HTTP → localhost:4318       │
│           ▼                                 │
│  otelcol-contrib (systemd service)          │
│    config: otelcol-config.yaml              │
│           │                                 │
│           │ OTLP/HTTP + zstd               │
│           ▼                                 │
│  Observe ingest endpoint                    │
└─────────────────────────────────────────────┘
```

The simulator sends to a local OTel Collector (port 4318). The collector handles batching,
retry, compression, and authentication before forwarding to Observe. Application code never
holds Observe credentials.

---

## Prerequisites

- Python 3.9+
- [OpenTelemetry Collector Contrib](https://github.com/open-telemetry/opentelemetry-collector-contrib/releases)
- An Observe account with a datastream token

---

## Quick Start

### 1. Install the OTel Collector

Download `otelcol-contrib` for your platform and install as a service, or run directly:

```bash
# Example: Amazon Linux / RHEL
sudo rpm -ivh otelcol-contrib_*.rpm

# Or run directly
./otelcol-contrib --config=otelcol-config.yaml
```

### 2. Configure the Collector

Set your Observe credentials as environment variables:

```bash
export OBSERVE_CUSTOMER_ID=123456789012   # found in Observe Settings > Customer ID
export OBSERVE_TOKEN=ds1Abc...            # create in Observe: Data > Datastreams > New Token
```

Then start the collector:

```bash
otelcol-contrib --config=pipeline-simulator/otelcol-config.yaml
```

The collector listens on `localhost:4318` (HTTP) and `localhost:4317` (gRPC).

### 3. Run the Simulator

```bash
cd pipeline-simulator
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

python pipeline_simulator.py
```

You should see:

```
2026-01-01T00:00:00 [pipeline_simulator] INFO Starting Snowflake Ingest Pipeline Simulator
2026-01-01T00:00:00 [pipeline_simulator] INFO OTel endpoint: http://localhost:4318
2026-01-01T00:00:00 [pipeline_simulator] INFO All simulators initialized. Starting event loops...
```

Within 30 seconds Snowpipe spans will start appearing in Observe. The first Airflow DAG run
fires after 5 minutes.

---

## EC2 Deployment

The `deploy.sh` script copies all files to an EC2 host, installs the Python venv, and
registers the simulator as a systemd service:

```bash
# Edit deploy.sh to set REMOTE_HOST and KEY path, then:
bash pipeline-simulator/deploy.sh
```

The script assumes `otelcol-contrib` is already installed and running on the host with the
config from `otelcol-config.yaml`. The systemd unit sets:

```ini
Environment="OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318"
```

---

## Configuration

All tunable parameters live in [`config.py`](config.py). No code changes needed for
common adjustments.

### Timing

```python
AIRFLOW = {
    "schedule_interval_seconds": 300,   # DAG run frequency (default: every 5 min)
    "task_failure_rate": 0.08,          # 8% chance any task fails
    "max_retries": 2,
}

FIVETRAN = {
    "connectors": [
        {"type": "salesforce", "schedule_seconds": 300, "failure_rate": 0.03, ...},
        {"type": "stripe",     "schedule_seconds": 300, "failure_rate": 0.05, ...},
        {"type": "hubspot",    "schedule_seconds": 300, "failure_rate": 0.10, ...},
    ]
}

SNOWPIPE = {
    "file_interval_seconds": (5, 30),   # random interval between file arrivals
    "format_error_rate": 0.02,          # 2% of files have format errors
}
```

### Environment

```python
DEPLOYMENT_ENV = "snowflake-ingest-prod"   # sets deployment.environment on all spans
SERVICE_NAMESPACE = "data-platform"        # sets service.namespace on all spans
```

### OTel Endpoint

Override via environment variable (no config.py change needed):

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://my-collector:4318
```

---

## How Trace Propagation Works

Each service gets its own `TracerProvider` backed by an OTel `Resource` with the correct
`service.name`. Cross-service trace context flows via the OpenTelemetry Python context API:

```
airflow-scheduler                 airflow-worker               dbt-cloud
─────────────────                 ──────────────               ─────────
dag_run (SERVER)
  │
  └─ task.dispatch (CLIENT) ────► task.run_dbt (SERVER)
                                    │
                                    └─ dbt.trigger_run (CLIENT) ──► dbt_run (SERVER)
                                                                       │
                                                                       └─ snowflake.query (CLIENT) ──► query_execution (SERVER)
                                                                                                        snowflake
```

All spans in a single DAG run share the same `trace_id`. Observe uses the `parent_span_id`
references to draw edges on the service map.

The critical pattern for Observe to draw a service map edge:
```
SERVICE A: SpanKind.CLIENT  (outbound call)
SERVICE B: SpanKind.SERVER  (receives the call, child of A's CLIENT span)
```

SERVER → SERVER or INTERNAL → SERVER relationships do not produce edges.

---

## Files

```
pipeline-simulator/
├── pipeline_simulator.py      # Entry point; creates per-service OTel providers, wires simulators
├── config.py                  # All tunable parameters (timing, failure rates, Snowflake config)
├── requirements.txt           # Python dependencies (opentelemetry-sdk, exporters)
├── otelcol-config.yaml        # OTel Collector config — receives OTLP, exports to Observe
├── deploy.sh                  # Deploys to EC2 and registers as systemd service
├── simulators/
│   ├── airflow_sim.py         # DAG runs and task execution spans
│   ├── dbt_sim.py             # dbt run, compile, model, and test spans
│   ├── fivetran_sim.py        # Connector sync spans with Snowflake load CLIENT spans
│   ├── snowpipe_sim.py        # File notification, ingest, and COPY_INTO spans
│   ├── warehouse_metrics.py   # Snowflake warehouse credit/queue/load metrics
│   └── alertmanager_sim.py    # Slack and PagerDuty notification spans on failure
└── docs/
    └── production-guide.md    # Per-service assessment: what's production-ready vs simulated
```

---

## Production Fidelity

This simulator is designed to produce the correct observability *model* — the right
service topology, span kinds, attribute names, and failure patterns — rather than to be
a drop-in production instrumentation library.

See [`docs/production-guide.md`](docs/production-guide.md) for a detailed breakdown of
what each service emits vs what you would actually instrument in a production environment,
including notes on Fivetran webhook bridges, Snowflake `QUERY_HISTORY` polling, and
Airflow's native OTel support.
