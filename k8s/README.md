# Kubernetes Deployment Guide

Các file YAML đã được khôi phục và sẵn sàng để deploy lên Minikube.

## Cấu trúc thư mục

```
k8s/
├── hdfs/               # Hadoop HDFS
│   ├── hdfs-configmap.yaml
│   ├── hdfs-pvc.yaml
│   ├── hdfs-service.yaml
│   ├── namenode-statefulset.yaml
│   └── datanode-statefulset.yaml
├── mongodb/            # MongoDB
│   ├── mongo-configmap.yaml
│   ├── mongo-pvc.yaml
│   ├── mongo-service.yaml
│   └── mongo-statefulset.yaml
├── spark/              # Apache Spark
│   ├── spark-configmap.yaml
│   ├── spark-service.yaml
│   ├── spark-master-deployment.yaml
│   └── spark-worker-deployment.yaml
└── trino/              # Trino Query Engine
    ├── trino-configmap.yaml
    ├── trino-pvc.yaml
    ├── trino-service.yaml
    └── trino-deployment.yaml
```

## Thứ tự deployment (QUAN TRỌNG)

### 1. HDFS (Deploy trước tiên)
```bash
kubectl apply -f k8s/hdfs/hdfs-configmap.yaml
kubectl apply -f k8s/hdfs/hdfs-pvc.yaml
kubectl apply -f k8s/hdfs/hdfs-service.yaml
kubectl apply -f k8s/hdfs/namenode-statefulset.yaml
kubectl apply -f k8s/hdfs/datanode-statefulset.yaml

# Chờ HDFS ready
kubectl wait --for=condition=ready pod -l app=namenode --timeout=300s
kubectl wait --for=condition=ready pod -l app=datanode --timeout=300s
```

### 2. MongoDB
```bash
kubectl apply -f k8s/mongodb/mongo-configmap.yaml
kubectl apply -f k8s/mongodb/mongo-pvc.yaml
kubectl apply -f k8s/mongodb/mongo-service.yaml
kubectl apply -f k8s/mongodb/mongo-statefulset.yaml

# Chờ MongoDB ready
kubectl wait --for=condition=ready pod -l app=mongo-db --timeout=300s
```

### 3. Spark
```bash
kubectl apply -f k8s/spark/spark-configmap.yaml
kubectl apply -f k8s/spark/spark-service.yaml
kubectl apply -f k8s/spark/spark-master-deployment.yaml

# Chờ Spark Master ready trước
kubectl wait --for=condition=ready pod -l app=spark-master --timeout=300s

# Sau đó deploy Worker
kubectl apply -f k8s/spark/spark-worker-deployment.yaml
```

### 4. Trino (Deploy cuối cùng)
```bash
kubectl apply -f k8s/trino/trino-configmap.yaml
kubectl apply -f k8s/trino/trino-pvc.yaml
kubectl apply -f k8s/trino/trino-service.yaml
kubectl apply -f k8s/trino/trino-deployment.yaml

# Chờ Trino ready
kubectl wait --for=condition=ready pod -l app=trino --timeout=600s
```

## Kiểm tra deployment

```bash
# Xem tất cả pods
kubectl get pods

# Xem tất cả services
kubectl get svc

# Xem PVCs
kubectl get pvc

# Xem logs của một pod
kubectl logs <pod-name>

# Xem chi tiết pod
kubectl describe pod <pod-name>
```

## Truy cập Web UI

```bash
# Lấy Minikube IP
minikube ip

# Hoặc dùng service URL
minikube service <service-name> --url
```

**Web UIs:**
- HDFS NameNode: http://<minikube-ip>:30870
- Spark Master: http://<minikube-ip>:30808
- MongoDB: http://<minikube-ip>:30027
- Trino: http://<minikube-ip>:30080

## Xóa toàn bộ deployment

```bash
# Xóa theo thứ tự ngược lại
kubectl delete -f k8s/trino/
kubectl delete -f k8s/spark/
kubectl delete -f k8s/mongodb/
kubectl delete -f k8s/hdfs/
```

## Lưu ý quan trọng

1. **Spark Image**: Cần build image `it4931-spark:400` trước khi deploy
2. **Resource**: Đảm bảo Minikube có đủ resources (đã kiểm tra: 23GB RAM, 8 CPU cores)
3. **HDFS Initialization**: Có thể cần format NameNode lần đầu
4. **MongoDB Password**: Hiện tại dùng plaintext trong ConfigMap (cần cải thiện bảo mật)
5. **Trino Dependencies**: Trino phụ thuộc vào HDFS và MongoDB, deploy sau cùng

## Troubleshooting

### Pod pending
```bash
kubectl describe pod <pod-name>
# Kiểm tra events để xem lỗi
```

### PVC pending
```bash
kubectl get pvc
kubectl describe pvc <pvc-name>
```

### Container crash
```bash
kubectl logs <pod-name>
kubectl logs <pod-name> --previous  # Log của container trước khi crash
```
