#!/bin/bash

# Configuration
NAMESPACE="default"  # Thay đổi nếu HDFS của bạn ở namespace khác
NAMENODE_POD="namenode-0"
echo "Sử dụng namenode pod: $NAMENODE_POD"

# Lấy đường dẫn data directory
DATA_DIR="$(dirname "$PWD")"

# Disable safe mode
echo "Tắt safe mode..."
kubectl exec -n $NAMESPACE $NAMENODE_POD -- hdfs dfsadmin -safemode leave

# Tạo thư mục trong HDFS
echo "Tạo thư mục HDFS..."
kubectl exec -n $NAMESPACE $NAMENODE_POD -- hdfs dfs -mkdir -p /user/it4931

# Tạo thư mục tạm trong pod
echo "Tạo thư mục tạm trong pod..."
kubectl exec -n $NAMESPACE $NAMENODE_POD -- mkdir -p /tmp/data

# Copy file lookup CSV vào pod
echo "Copy taxi_zone_lookup.csv vào pod..."
kubectl cp -n $NAMESPACE $DATA_DIR/data/taxi_zone_lookup.csv $NAMENODE_POD:/tmp/data/taxi_zone_lookup.csv

# Copy thư mục yellow_taxi vào pod
echo "Copy yellow_taxi directory vào pod..."
kubectl cp -n $NAMESPACE $DATA_DIR/data/yellow_taxi/ $NAMENODE_POD:/tmp/data/yellow_taxi/

# Upload vào HDFS
echo "Upload yellow_taxi vào HDFS..."
kubectl exec -n $NAMESPACE $NAMENODE_POD -- hdfs dfs -put /tmp/data/yellow_taxi/ /user/it4931/

echo "Upload taxi_zone_lookup.csv vào HDFS..."
kubectl exec -n $NAMESPACE $NAMENODE_POD -- hdfs dfs -put /tmp/data/taxi_zone_lookup.csv /user/it4931/

# Dọn dẹp file tạm
echo "Dọn dẹp file tạm trong pod..."
kubectl exec -n $NAMESPACE $NAMENODE_POD -- rm -rf /tmp/data

echo "Hoàn tất!"
