#!/bin/bash

set -e

echo "=================================================="
echo "Setting up Superset: Database and Admin User"
echo "=================================================="

# Superset admin credentials
ADMIN_USERNAME="admin"
ADMIN_FIRSTNAME="Admin"
ADMIN_LASTNAME="User"
ADMIN_EMAIL="admin@superset.local"
ADMIN_PASSWORD="admin"

# Wait for Superset pod to be ready
echo "Waiting for Superset pod to be ready..."
kubectl wait --for=condition=ready pod -l app=superset --timeout=300s
echo "✓ Superset pod is ready"

# Get Superset pod name
SUPERSET_POD=$(kubectl get pods -l app=superset -o jsonpath='{.items[0].metadata.name}')
echo "Superset Pod: $SUPERSET_POD"

echo ""
echo "=================================================="
echo "Step 1: Waiting for PostgreSQL to be ready"
echo "=================================================="

# Wait for PostgreSQL inside pod to be ready
kubectl exec "$SUPERSET_POD" -c superset-db -- bash -c '
until pg_isready -U superset -d superset; do
    echo "Waiting for PostgreSQL..."
    sleep 2
done
'
echo "✓ PostgreSQL is ready"

echo ""
echo "=================================================="
echo "Step 2: Upgrading Superset Database"
echo "=================================================="

kubectl exec "$SUPERSET_POD" -c superset -- superset db upgrade
echo "✓ Database upgraded"

echo ""
echo "=================================================="
echo "Step 3: Creating Admin User"
echo "=================================================="

kubectl exec "$SUPERSET_POD" -c superset -- superset fab create-admin \
    --username "$ADMIN_USERNAME" \
    --firstname "$ADMIN_FIRSTNAME" \
    --lastname "$ADMIN_LASTNAME" \
    --email "$ADMIN_EMAIL" \
    --password "$ADMIN_PASSWORD" || echo "Admin user may already exist"

echo "✓ Admin user created"

echo ""
echo "=================================================="
echo "Step 4: Initializing Superset"
echo "=================================================="

kubectl exec "$SUPERSET_POD" -c superset -- superset init
echo "✓ Superset initialized"

echo ""
echo "=================================================="
echo "Superset Setup Complete!"
echo "=================================================="
echo ""
echo "Summary:"
echo "  - Admin Username: $ADMIN_USERNAME"
echo "  - Admin Password: $ADMIN_PASSWORD"
echo ""
echo "Access Superset:"
echo "  minikube service superset-svc"
echo "  or: kubectl port-forward $SUPERSET_POD 8088:8088"
echo ""
echo "Then open: http://localhost:8088"

# Sau đó thao tác connect Trino trên UI Superset: trino://trino:@trino-service:8080