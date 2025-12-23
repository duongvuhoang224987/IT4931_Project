#!/bin/bash

set -e

echo "=================================================="
echo "Setting up MongoDB: Database and Collections"
echo "=================================================="

# MongoDB credentials
MONGO_USER="root"
MONGO_PASSWORD="root"
MONGO_DATABASE="taxi"

# Wait for MongoDB pod to be ready
echo "Waiting for MongoDB pod to be ready..."
kubectl wait --for=condition=ready pod -l app=mongo-db --timeout=300s
echo "✓ MongoDB pod is ready"

# Get MongoDB pod name
MONGO_POD=$(kubectl get pods -l app=mongo-db -o jsonpath='{.items[0].metadata.name}')
echo "MongoDB Pod: $MONGO_POD"

echo ""
echo "=================================================="
echo "Creating Database and Collections"
echo "=================================================="

# Create database and collections with schema validation
kubectl exec "$MONGO_POD" -- mongosh \
    --username "$MONGO_USER" \
    --password "$MONGO_PASSWORD" \
    --authenticationDatabase admin \
    --eval "
// Switch to taxi database
use('$MONGO_DATABASE');

// Create collection: yellow_taxi_raw (raw_des_df after join with dim_location)
db.createCollection('yellow_taxi_raw');
print('✓ Created collection: yellow_taxi_raw');

// Create collection: yellow_window10min (tripCount aggregation)
db.createCollection('yellow_window10min');
print('✓ Created collection: yellow_window10min');

// Create index on window_start for upsert operations
db.yellow_window10min.createIndex({ window_start: 1 }, { unique: true });
print('✓ Created unique index on window_start');

// Show collections
print('');
print('Collections in $MONGO_DATABASE:');
db.getCollectionNames().forEach(c => print('  - ' + c));
"

echo ""
echo "=================================================="
echo "Verifying Collections"
echo "=================================================="

kubectl exec "$MONGO_POD" -- mongosh \
    --username "$MONGO_USER" \
    --password "$MONGO_PASSWORD" \
    --authenticationDatabase admin \
    --quiet \
    --eval "
use('$MONGO_DATABASE');
print('Database: $MONGO_DATABASE');
print('');
print('Collections:');
db.getCollectionNames().forEach(function(c) {
    var stats = db.getCollection(c).stats();
    print('  - ' + c + ' (documents: ' + stats.count + ')');
});
"

echo ""
echo "=================================================="
echo "MongoDB Setup Complete!"
echo "=================================================="
echo ""
echo "Summary:"
echo "  - Database: $MONGO_DATABASE"
echo "  - Collections:"
echo "      - yellow_taxi_raw (24 fields - raw data joined with location)"
echo "      - yellow_window10min (3 fields - 10min window aggregation)"
echo ""
echo "Useful commands:"
echo "  - Connect to MongoDB: kubectl exec -it $MONGO_POD -- mongosh -u $MONGO_USER -p $MONGO_PASSWORD --authenticationDatabase admin"
echo "  - Query raw data: db.yellow_taxi_raw.find().limit(5)"
echo "  - Query aggregated: db.yellow_window10min.find().sort({window_start: -1}).limit(10)"
