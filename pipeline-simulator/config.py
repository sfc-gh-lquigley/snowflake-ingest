import os

OTEL_ENDPOINT = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")

DEPLOYMENT_ENV = "snowflake-ingest-prod"
SERVICE_NAMESPACE = "data-platform"

SERVICES = {
    "airflow_scheduler": "airflow-scheduler",
    "airflow_worker": "airflow-worker",
    "dbt_cloud": "dbt-cloud",
    "fivetran": "fivetran-connector",
    "snowpipe": "snowpipe-ingest",
    "snowflake_warehouse": "snowflake-warehouse",
    "alertmanager": "alertmanager",
}

SNOWFLAKE = {
    "account": "SFCOGSOPS-SNOWHOUSE_AWS_US_WEST_2",
    "warehouse": "TRANSFORM_WH_XS",
    "database": "ANALYTICS",
    "schema": "DBT_PROD",
    "role": "TRANSFORMER",
}

AIRFLOW = {
    "dag_id": "snowflake_ingest_pipeline",
    "schedule_interval_seconds": 300,
    "task_failure_rate": 0.08,
    "max_retries": 2,
    "sla_seconds": 300,
}

DBT = {
    "project": "analytics",
    "target": "prod",
    "models": [
        {"name": "stg_orders", "materialization": "incremental", "avg_rows": 50000, "avg_seconds": 5},
        {"name": "stg_payments", "materialization": "incremental", "avg_rows": 30000, "avg_seconds": 4},
        {"name": "int_orders_pivoted", "materialization": "incremental", "avg_rows": 45000, "avg_seconds": 14},
        {"name": "fct_revenue", "materialization": "incremental", "avg_rows": 100000, "avg_seconds": 30},
    ],
    "tests": [
        {"name": "unique_orders_order_id", "model": "stg_orders", "pass_rate": 0.95},
        {"name": "not_null_fct_revenue_amount", "model": "fct_revenue", "pass_rate": 0.99},
        {"name": "accepted_values_stg_orders_status", "model": "stg_orders", "pass_rate": 0.98},
    ],
    "full_refresh_rate": 0.05,
}

FIVETRAN = {
    "connectors": [
        {
            "id": "fivetran_salesforce_crm",
            "type": "salesforce",
            "schema": "salesforce",
            "schedule_seconds": 300,
            "avg_duration_seconds": 35,
            "avg_rows": 2500,
            "failure_rate": 0.03,
            "failure_type": "credential_expired",
        },
        {
            "id": "fivetran_stripe_payments",
            "type": "stripe",
            "schema": "stripe",
            "schedule_seconds": 300,
            "avg_duration_seconds": 15,
            "avg_rows": 500,
            "failure_rate": 0.05,
            "failure_type": "rate_limited",
        },
        {
            "id": "fivetran_hubspot_marketing",
            "type": "hubspot",
            "schema": "hubspot",
            "schedule_seconds": 300,
            "avg_duration_seconds": 75,
            "avg_rows": 5000,
            "failure_rate": 0.10,
            "failure_type": "schema_drift",
        },
    ]
}

SNOWPIPE = {
    "pipe_name": "RAW_EVENTS_PIPE",
    "bucket": "analytics-landing",
    "prefix": "raw_events",
    "file_interval_seconds": (5, 30),
    "avg_file_size_mb": 12,
    "avg_load_seconds": 8,
    "format_error_rate": 0.02,
    "stall_rate": 0.01,
}

WAREHOUSE_METRICS = {
    "poll_interval_seconds": 30,
    "base_credits_per_hour": 1.0,
    "base_queue_depth": 0,
    "base_active_queries": 2,
}
