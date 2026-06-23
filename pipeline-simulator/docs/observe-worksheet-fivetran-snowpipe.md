# Fivetran & Snowpipe Monitoring — Observe Worksheet

Paste each OPAL block into a separate worksheet stage in Observe.
All queries target the shared **Logs** dataset (42759865) filtered by `service.name`.

---

## Fivetran Widgets

### 1. Connector Health Summary (Table)
_Overall sync success rate, row throughput, and average duration per connector._

```
filter string(resource_attributes."service.name") = "fivetran-connector"
| make_col parsed:parse_json(string(body))
| make_col event:string(parsed.event)
| filter event = "sync_end"
| make_col
    connector_id:string(parsed.connector_id),
    status:string(parsed.status),
    rows:int64(parsed.rows_updated),
    duration:float64(parsed.sync_duration_seconds)
| statsby
    total_syncs:count(),
    successful:countif(status = "SUCCESSFUL"),
    failed:countif(status = "FAILURE"),
    total_rows:sum(rows),
    avg_duration_s:avg(duration),
    group_by(connector_id)
| make_col success_rate_pct:round(100.0 * float64(successful) / float64(total_syncs), 1)
| pick_col connector_id, total_syncs, successful, failed, success_rate_pct, total_rows, avg_duration_s
| sort desc(total_rows)
```

### 2. Rows Synced Over Time (Timeseries)
_Row throughput per connector — good for spotting volume anomalies._

```
filter string(resource_attributes."service.name") = "fivetran-connector"
| make_col parsed:parse_json(string(body))
| filter string(parsed.event) = "sync_end" and string(parsed.status) = "SUCCESSFUL"
| make_col
    connector_id:string(parsed.connector_id),
    rows:int64(parsed.rows_updated)
| timechart 10m, sum(rows), connector_id
```

### 3. Sync Duration Trend (Timeseries)
_Duration trend per connector — rising duration = source API slowdown or schema growth._

```
filter string(resource_attributes."service.name") = "fivetran-connector"
| make_col parsed:parse_json(string(body))
| filter string(parsed.event) = "sync_end"
| make_col
    connector_id:string(parsed.connector_id),
    duration:float64(parsed.sync_duration_seconds)
| timechart 10m, avg(duration), connector_id
```

### 4. Recent Sync Failures (Table)
_Last 20 failed syncs with error type for immediate triage._

```
filter string(resource_attributes."service.name") = "fivetran-connector"
| make_col parsed:parse_json(string(body))
| filter string(parsed.event) = "sync_end"
  and (string(parsed.status) = "FAILURE" or string(parsed.status) = "SUCCESS_WITH_WARNINGS")
| make_col
    connector_id:string(parsed.connector_id),
    status:string(parsed.status),
    error:string(parsed.error),
    warning:string(parsed.warning),
    duration_s:float64(parsed.sync_duration_seconds)
| pick_col timestamp, connector_id, status, error, warning, duration_s
| sort desc(timestamp)
| limit 20
```

---

## Snowpipe Widgets

### 5. Snowpipe Load Summary (KPIs)
_Total files, total rows, average latency, and failure count._

```
filter string(resource_attributes."service.name") = "snowpipe-ingest"
| make_col parsed:parse_json(string(body))
| make_col event:string(parsed.event)
| make_col
    rows:int64(parsed.rows_loaded),
    bytes:int64(parsed.bytes_loaded),
    duration:float64(parsed.load_duration_seconds)
| statsby
    files_loaded:countif(event = "file_loaded"),
    load_failures:countif(event = "load_failed"),
    queue_stalls:countif(event = "queue_stall"),
    total_rows:sum(if(event = "file_loaded", rows, 0)),
    total_bytes:sum(if(event = "file_loaded", bytes, 0)),
    avg_load_latency_s:avg(if(event = "file_loaded", duration, null)),
    group_by()
| make_col
    failure_rate_pct:round(100.0 * float64(load_failures) / float64(files_loaded + load_failures), 2),
    total_gb:round(float64(total_bytes) / 1073741824.0, 3)
| pick_col files_loaded, load_failures, failure_rate_pct, queue_stalls, total_rows, total_gb, avg_load_latency_s
```

### 6. Files Loaded Per Interval (Timeseries)
_File ingest rate — use to spot backlog buildup or gaps._

```
filter string(resource_attributes."service.name") = "snowpipe-ingest"
| make_col parsed:parse_json(string(body))
| filter string(parsed.event) = "file_loaded"
| timechart 5m, count()
```

### 7. Load Latency Over Time (Timeseries)
_Average load duration — a sustained rise means Snowflake queue pressure._

```
filter string(resource_attributes."service.name") = "snowpipe-ingest"
| make_col parsed:parse_json(string(body))
| filter string(parsed.event) = "file_loaded"
| make_col duration:float64(parsed.load_duration_seconds)
| timechart 5m, avg(duration)
```

### 8. Rows Loaded Per Interval (Timeseries)
_Data volume flowing through Snowpipe over time._

```
filter string(resource_attributes."service.name") = "snowpipe-ingest"
| make_col parsed:parse_json(string(body))
| filter string(parsed.event) = "file_loaded"
| make_col rows:int64(parsed.rows_loaded)
| timechart 5m, sum(rows)
```

### 9. Recent Load Failures & Queue Stalls (Table)
_Last 20 error/stall events for active investigation._

```
filter string(resource_attributes."service.name") = "snowpipe-ingest"
| make_col parsed:parse_json(string(body))
| make_col event:string(parsed.event)
| filter event = "load_failed" or event = "queue_stall"
| make_col
    pipe_name:string(parsed.pipe_name),
    file:string(parsed.file),
    error:string(parsed.error_message),
    queue_depth:int64(parsed.queue_depth),
    stall_duration_s:float64(parsed.stall_duration_seconds)
| pick_col timestamp, event, pipe_name, file, error, queue_depth, stall_duration_s
| sort desc(timestamp)
| limit 20
```

---

## Usage Notes

- **Time range**: Set the worksheet interval to `Past 1 hour` for live monitoring, `Past 24 hours` for trend analysis.
- **Alerts to create**: 
  - Fivetran: `failure_count > 0` on widget 4 → page on consecutive failures per connector
  - Snowpipe: `failure_rate_pct > 5` on widget 5 → warn; `queue_stalls > 3` in 30 min → warn
- **Production extension**: In a real environment, widgets 1–4 would be fed by the Fivetran Log Connector writing to Snowflake → O4S, and widgets 5–9 by O4S COPY_HISTORY ingestion. The OPAL structure stays the same; only the source dataset changes.
