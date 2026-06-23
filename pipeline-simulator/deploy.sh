#!/bin/bash
set -euo pipefail

REMOTE_HOST="44.251.113.45"
KEY="/Users/lquigley/Downloads/postgres-otel-demo/postgres-otel-demo-key.pem"
REMOTE_DIR="/opt/pipeline-simulator"
LOCAL_DIR="/Users/lquigley/Downloads/postgres-otel-demo/pipeline-simulator"

echo "=== Deploying pipeline simulator to app-host ==="

# Copy files
echo "Copying files..."
ssh -i "$KEY" -o StrictHostKeyChecking=no ec2-user@$REMOTE_HOST "sudo rm -rf $REMOTE_DIR && sudo mkdir -p $REMOTE_DIR/simulators && sudo chown -R ec2-user:ec2-user $REMOTE_DIR"

scp -i "$KEY" -o StrictHostKeyChecking=no \
  "$LOCAL_DIR/pipeline_simulator.py" \
  "$LOCAL_DIR/config.py" \
  "$LOCAL_DIR/requirements.txt" \
  "$LOCAL_DIR/otelcol-config.yaml" \
  ec2-user@$REMOTE_HOST:$REMOTE_DIR/

scp -i "$KEY" -o StrictHostKeyChecking=no \
  "$LOCAL_DIR/simulators/__init__.py" \
  "$LOCAL_DIR/simulators/airflow_sim.py" \
  "$LOCAL_DIR/simulators/dbt_sim.py" \
  "$LOCAL_DIR/simulators/fivetran_sim.py" \
  "$LOCAL_DIR/simulators/snowpipe_sim.py" \
  "$LOCAL_DIR/simulators/warehouse_metrics.py" \
  "$LOCAL_DIR/simulators/alertmanager_sim.py" \
  ec2-user@$REMOTE_HOST:$REMOTE_DIR/simulators/

# Install dependencies and create service
echo "Setting up venv and systemd service..."
ssh -i "$KEY" -o StrictHostKeyChecking=no ec2-user@$REMOTE_HOST << 'EOF'
set -euo pipefail

cd /opt/pipeline-simulator

# Create venv
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Create systemd service
sudo tee /etc/systemd/system/pipeline-simulator.service > /dev/null << 'UNIT'
[Unit]
Description=Snowflake Ingest Pipeline Telemetry Simulator
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ec2-user
WorkingDirectory=/opt/pipeline-simulator
ExecStart=/opt/pipeline-simulator/venv/bin/python /opt/pipeline-simulator/pipeline_simulator.py
Restart=on-failure
RestartSec=10
Environment="OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318"

[Install]
WantedBy=multi-user.target
UNIT

sudo systemctl daemon-reload
sudo systemctl enable pipeline-simulator
sudo systemctl restart pipeline-simulator

echo "=== Service started ==="
sleep 3
sudo systemctl status pipeline-simulator --no-pager || true
EOF

echo "=== Deployment complete ==="
