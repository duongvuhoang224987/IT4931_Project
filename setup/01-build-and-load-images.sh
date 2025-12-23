#!/bin/bash

set -e

echo "=================================================="
echo "Building and Loading Docker Images into Minikube"
echo "=================================================="

# Get the project root directory
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# Check if minikube is running
if ! minikube status > /dev/null 2>&1; then
    echo "Error: Minikube is not running. Please start minikube first."
    exit 1
fi

echo ""
echo "=================================================="
echo "1. Building Spark Image"
echo "=================================================="
docker build -f Dockerfile.spark -t it4931-spark:400 .

echo ""
echo "=================================================="
echo "2. Building Dagster Image"
echo "=================================================="
cd ./k8s/dagster
docker build -f ./Dockerfile.dagster -t dagster:k8s .

echo ""
echo "=================================================="
echo "Loading images into Minikube"
echo "=================================================="
minikube image load it4931-spark:400
minikube image load dagster:k8s

echo ""
echo "=================================================="
echo "Successfully built and loaded all images!"
echo "=================================================="

minikube image ls --format table | grep -E "it4931-spark|dagster"
