#!/bin/bash

set -e

echo "=================================================="
echo "Setting up Kafka: Topic, Connector, and Schema"
echo "=================================================="

# Get the project root directory
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Get Minikube IP
MINIKUBE_IP=$(minikube ip)

# Service endpoints (using NodePort for external access)
KAFKA_BROKER="k-broker-0.k-broker-svc:9094"
SCHEMA_REGISTRY_URL="http://${MINIKUBE_IP}:30818"
KAFKA_CONNECT_URL="http://${MINIKUBE_IP}:30838"

# File paths
CONNECTOR_JSON="$PROJECT_ROOT/k8s/kafka/k-connectors/hdfs3-taxi-parquet.json"
SCHEMA_FILE="$PROJECT_ROOT/k8s/kafka/yellow_taxi.avsc"

# Topic configuration
TOPIC_NAME="taxi-topic"
PARTITIONS=3
REPLICATION_FACTOR=2

echo ""
echo "Minikube IP: $MINIKUBE_IP"
echo "Schema Registry: $SCHEMA_REGISTRY_URL"
echo "Kafka Connect: $KAFKA_CONNECT_URL"
echo ""

# Function to wait for service to be ready
wait_for_service() {
    local url=$1
    local service_name=$2
    local max_attempts=30
    local attempt=1

    echo "Waiting for $service_name to be ready..."
    while [ $attempt -le $max_attempts ]; do
        if curl -s "$url" > /dev/null 2>&1; then
            echo "✓ $service_name is ready!"
            return 0
        fi
        echo "  Attempt $attempt/$max_attempts - $service_name not ready yet..."
        sleep 5
        attempt=$((attempt + 1))
    done

    echo "✗ $service_name is not available after $max_attempts attempts"
    return 1
}

echo "=================================================="
echo "Step 1: Creating Kafka Topic '$TOPIC_NAME'"
echo "=================================================="

# Wait for Kafka broker to be ready
kubectl wait --for=condition=ready pod -l app=k-broker --timeout=300s

# Create topic using kafka-topics.sh inside the broker pod
kubectl exec k-broker-0 -- kafka-topics \
    --create \
    --topic "$TOPIC_NAME" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION_FACTOR" \
    --bootstrap-server "$KAFKA_BROKER" \
    --if-not-exists

echo "✓ Topic '$TOPIC_NAME' created successfully"

# List topics to verify
echo ""
echo "Listing all topics:"
kubectl exec k-broker-0 -- kafka-topics \
    --list \
    --bootstrap-server "$KAFKA_BROKER"

echo ""
echo "=================================================="
echo "Step 2: Deploying Connector to Kafka Connect"
echo "=================================================="

# Wait for Kafka Connect to be ready
wait_for_service "$KAFKA_CONNECT_URL/connectors" "Kafka Connect"

# Check available connector plugins
echo ""
echo "Available connector plugins:"
curl -s "$KAFKA_CONNECT_URL/connector-plugins" | python3 -m json.tool 2>/dev/null || \
    curl -s "$KAFKA_CONNECT_URL/connector-plugins"

# Check if connector already exists
CONNECTOR_NAME=$(cat "$CONNECTOR_JSON" | python3 -c "import sys, json; print(json.load(sys.stdin)['name'])" 2>/dev/null || echo "hdfs3-taxi-parquet")

echo ""
echo "Deploying connector: $CONNECTOR_NAME"

# Delete existing connector if exists
curl -s -X DELETE "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME" > /dev/null 2>&1 || true

# Deploy connector
RESPONSE=$(curl -s -X POST "$KAFKA_CONNECT_URL/connectors" \
    -H "Content-Type: application/json" \
    -d @"$CONNECTOR_JSON")

echo "Response: $RESPONSE"

# Check connector status
sleep 3
echo ""
echo "Connector status:"
curl -s "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/status" | python3 -m json.tool 2>/dev/null || \
    curl -s "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/status"

echo ""
echo "✓ Connector deployed successfully"

echo ""
echo "=================================================="
echo "Step 3: Registering Schema to Schema Registry"
echo "=================================================="

# Wait for Schema Registry to be ready
wait_for_service "$SCHEMA_REGISTRY_URL/subjects" "Schema Registry"

# Read the schema file and prepare JSON payload
SCHEMA_CONTENT=$(cat "$SCHEMA_FILE" | python3 -c "import sys, json; print(json.dumps(json.dumps(json.load(sys.stdin))))")

# Subject name follows Confluent naming convention: <topic>-value
SUBJECT_NAME="${TOPIC_NAME}-value"

echo ""
echo "Registering schema for subject: $SUBJECT_NAME"

# Register the schema
REGISTER_RESPONSE=$(curl -s -X POST "$SCHEMA_REGISTRY_URL/subjects/$SUBJECT_NAME/versions" \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d "{\"schemaType\": \"AVRO\", \"schema\": $SCHEMA_CONTENT}")

echo "Response: $REGISTER_RESPONSE"

# Verify registration
echo ""
echo "Registered subjects:"
curl -s "$SCHEMA_REGISTRY_URL/subjects"

echo ""
echo ""
echo "Schema for $SUBJECT_NAME:"
curl -s "$SCHEMA_REGISTRY_URL/subjects/$SUBJECT_NAME/versions/latest" | python3 -m json.tool 2>/dev/null || \
    curl -s "$SCHEMA_REGISTRY_URL/subjects/$SUBJECT_NAME/versions/latest"

echo ""
echo "✓ Schema registered successfully"

echo ""
echo "=================================================="
echo "Kafka Setup Complete!"
echo "=================================================="
echo ""
echo "Summary:"
echo "  - Topic: $TOPIC_NAME (partitions: $PARTITIONS, replication: $REPLICATION_FACTOR)"
echo "  - Connector: $CONNECTOR_NAME"
echo "  - Schema Subject: $SUBJECT_NAME"
echo ""
echo "Useful commands:"
echo "  - List topics: kubectl exec k-broker-0 -- /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server $KAFKA_BROKER"
echo "  - Check connectors: curl $KAFKA_CONNECT_URL/connectors"
echo "  - Check schemas: curl $SCHEMA_REGISTRY_URL/subjects"