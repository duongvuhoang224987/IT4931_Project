#!/bin/bash

set -e

echo "=================================================="
echo "Deploying Services to Kubernetes"
echo "=================================================="

# Get the project root directory
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
K8S_DIR="$PROJECT_ROOT/k8s"

# Function to apply manifests with retry
apply_manifest() {
    local file=$1
    local retries=3
    local count=0

    while [ $count -lt $retries ]; do
        if kubectl apply -f "$file"; then
            echo "✓ Applied: $file"
            return 0
        else
            count=$((count + 1))
            if [ $count -lt $retries ]; then
                echo "! Retry $count/$retries for: $file"
                sleep 2
            fi
        fi
    done

    echo "✗ Failed to apply: $file"
    return 1
}

echo ""
echo "=================================================="
echo "1. Deploying HDFS (Storage Layer)"
echo "=================================================="
apply_manifest "$K8S_DIR/hdfs/hdfs-configmap.yaml"
apply_manifest "$K8S_DIR/hdfs/hdfs-pvc.yaml"
apply_manifest "$K8S_DIR/hdfs/hdfs-service.yaml"
apply_manifest "$K8S_DIR/hdfs/namenode-statefulset.yaml"
apply_manifest "$K8S_DIR/hdfs/datanode-statefulset.yaml"

echo ""
echo "=================================================="
echo "2. Deploying Kafka (Message Broker)"
echo "=================================================="
apply_manifest "$K8S_DIR/kafka/kafka-service.yaml"
apply_manifest "$K8S_DIR/kafka/k-broker-statefulset.yaml"

echo "Deploying Schema Registry..."
apply_manifest "$K8S_DIR/kafka/SR-deployment.yaml"

echo "Deploying Kafka UI..."
apply_manifest "$K8S_DIR/kafka/k-ui-deployment.yaml"

echo "Deploying Kafka Connect..."
apply_manifest "$K8S_DIR/kafka/k-connect-deployment.yaml"

echo ""
echo "=================================================="
echo "3. Deploying ClickHouse (Analytical Database)"
echo "=================================================="
apply_manifest "$K8S_DIR/clickhouse/clickhouse-configmap.yaml"
apply_manifest "$K8S_DIR/clickhouse/clickhouse-pvc.yaml"
apply_manifest "$K8S_DIR/clickhouse/clickhouse-deployment.yaml"
apply_manifest "$K8S_DIR/clickhouse/clickhouse-service.yaml"

echo ""
echo "=================================================="
echo "4. Deploying MongoDB (Document Database)"
echo "=================================================="
apply_manifest "$K8S_DIR/mongodb/mongo-configmap.yaml"
apply_manifest "$K8S_DIR/mongodb/mongo-pvc.yaml"
apply_manifest "$K8S_DIR/mongodb/mongo-statefulset.yaml"
apply_manifest "$K8S_DIR/mongodb/mongo-service.yaml"

echo ""
echo "=================================================="
echo "5. Deploying Spark (Processing Engine)"
echo "=================================================="
apply_manifest "$K8S_DIR/spark/spark-configmap.yaml"
apply_manifest "$K8S_DIR/spark/spark-service.yaml"
apply_manifest "$K8S_DIR/spark/spark-master-deployment.yaml"
apply_manifest "$K8S_DIR/spark/spark-worker-deployment.yaml"

echo ""
echo "=================================================="
echo "6. Deploying Trino (Query Engine)"
echo "=================================================="
apply_manifest "$K8S_DIR/trino/trino-configmap.yaml"
apply_manifest "$K8S_DIR/trino/trino-pvc.yaml"
apply_manifest "$K8S_DIR/trino/trino-deployment.yaml"
apply_manifest "$K8S_DIR/trino/trino-service.yaml"

echo ""
echo "=================================================="
echo "7. Deploying Superset (Visualization)"
echo "=================================================="
apply_manifest "$K8S_DIR/superset/superset-configmap.yaml"
apply_manifest "$K8S_DIR/superset/superset-pvc.yaml"
apply_manifest "$K8S_DIR/superset/superset-deployment.yaml"
apply_manifest "$K8S_DIR/superset/superset-service.yaml"

echo ""
echo "=================================================="
echo "8. Deploying Dagster (Orchestration)"
echo "=================================================="
apply_manifest "$K8S_DIR/dagster/dagster-secret.yaml"
apply_manifest "$K8S_DIR/dagster/dagster-rbac.yaml"
apply_manifest "$K8S_DIR/dagster/dagster-main-deployment.yaml"
apply_manifest "$K8S_DIR/dagster/dagster-service.yaml"

echo ""
echo "=================================================="
echo "Deployment Summary"
echo "=================================================="
kubectl get pods -o wide
kubectl get services

echo ""
echo "=================================================="
echo "All services deployed successfully!"
echo "=================================================="
echo ""
echo "To access services, use:"
echo "  minikube service <service-name>"
echo ""
echo "Or get all services:"
echo "  kubectl get svc"
