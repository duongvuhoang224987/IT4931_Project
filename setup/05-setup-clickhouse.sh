#!/bin/bash

set -e

echo "=================================================="
echo "Setting up ClickHouse: Database and Tables"
echo "=================================================="

# ClickHouse credentials
CLICKHOUSE_USER="it4931"
CLICKHOUSE_PASSWORD="it4931"

# SQL file path (mounted from ConfigMap)
SQL_FILE="/etc/clickhouse-server/to_clickhouse.sql"

# Wait for ClickHouse pod to be ready
echo "Waiting for ClickHouse pod to be ready..."
kubectl wait --for=condition=ready pod -l app=clickhouse --timeout=300s
echo "✓ ClickHouse pod is ready"

# Get ClickHouse pod name
CLICKHOUSE_POD=$(kubectl get pods -l app=clickhouse -o jsonpath='{.items[0].metadata.name}')
echo "ClickHouse Pod: $CLICKHOUSE_POD"

echo ""
echo "=================================================="
echo "Executing SQL from mounted ConfigMap"
echo "=================================================="

# Execute SQL file in ClickHouse
kubectl exec "$CLICKHOUSE_POD" -- clickhouse-client \
    --user "$CLICKHOUSE_USER" \
    --password "$CLICKHOUSE_PASSWORD" \
    --multiquery \
    --queries-file "$SQL_FILE"

echo "✓ SQL executed successfully"

echo ""
echo "=================================================="
echo "Verifying Database and Tables"
echo "=================================================="

echo ""
echo "Databases:"
kubectl exec "$CLICKHOUSE_POD" -- clickhouse-client \
    --user "$CLICKHOUSE_USER" \
    --password "$CLICKHOUSE_PASSWORD" \
    --query "SHOW DATABASES"

echo ""
echo "Tables in nyc_taxi_dw:"
kubectl exec "$CLICKHOUSE_POD" -- clickhouse-client \
    --user "$CLICKHOUSE_USER" \
    --password "$CLICKHOUSE_PASSWORD" \
    --query "SHOW TABLES FROM nyc_taxi_dw"

echo ""
echo "=================================================="
echo "ClickHouse Setup Complete!"
echo "=================================================="
