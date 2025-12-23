# Taxi Data Processing Platform

Hệ thống xử lý và phân tích dữ liệu taxi sử dụng kiến trúc Lambda Architecture, kết hợp batch processing và stream processing.

## 👥 Thành viên nhóm

| STT | Họ và tên | MSSV |
|-----|-----------|------|
| 1   | Dương Vũ Hoàng | 20224987 |
| 2   | Lê Nhật Hoàng | 20224989 |
| 3   | Nguyễn Thành Trung | 20225105 |
| 4   | Chu Tuấn Nghĩa | 20225056 |

## 🏗️ Kiến trúc hệ thống

Hệ thống được xây dựng dựa trên **Lambda Architecture** với 3 tầng chính:

- **Batch Layer**: Xử lý dữ liệu lớn định kỳ với Apache Spark và Dagster
- **Speed Layer**: Xử lý dữ liệu streaming real-time với Kafka và Spark Streaming
- **Serving Layer**: Truy vấn và visualize dữ liệu với Trino, ClickHouse và Superset

## 🛠️ Công nghệ sử dụng

### Data Storage
- **HDFS**: Distributed file system cho batch data
- **ClickHouse**: OLAP database cho analytical queries
- **MongoDB**: Document store cho metadata

### Data Processing
- **Apache Spark**: Batch và stream processing engine
- **Dagster**: Orchestration cho batch jobs
- **Kafka**: Message broker cho data streaming
- **Kafka Connect**: Data integration framework

### Query & Visualization
- **Trino**: Distributed SQL query engine
- **Apache Superset**: Data visualization và dashboarding

### Infrastructure
- **Kubernetes**: Container orchestration
- **Minikube**: Local Kubernetes cluster
- **Docker**: Containerization

## 📁 Cấu trúc thư mục

```
it4931_project/
│
│
├── 🐳 docker_infra/                  # Docker infrastructure
├── ☸️  k8s/                           # Kubernetes manifests
│   ├── clickhouse/                   # ClickHouse deployment
│   ├── dagster/                      # Dagster deployment
│   ├── hdfs/                         # HDFS deployment
│   ├── kafka/                        # Kafka cluster deployment
│   ├── mongodb/                      # MongoDB deployment
│   ├── spark/                        # Spark cluster deployment
│   ├── superset/                     # Superset deployment
│   └── trino/                        # Trino deployment
│
├── 🔧 setup/                         # Setup scripts
│   ├── 01-build-and-load-images.sh   # Build và load Docker images
│   ├── 02-deploy-services.sh         # Deploy K8s services
│   ├── 03-setup-hdfs.sh              # Khởi tạo HDFS
│   ├── 04-setup-kafka.sh             # Khởi tạo Kafka topics
│   ├── 05-setup-clickhouse.sh        # Khởi tạo ClickHouse
│   ├── 06-setup-mongo.sh             # Khởi tạo MongoDB
│   └── 07-setup-superset.sh          # Khởi tạo Superset
│
├── 💻 src/                           # Source code
│   ├── ingestion/                    # Data ingestion
│   │   └── nghiact_producer.py       # Kafka producer
│   ├── spark_jobs/                   # Batch processing jobs
│   │   ├── batch_job.py              # Main batch job
│   │   └── star_schema_transform.py  # Transform to star schema
│   └── stream_job/                   # Stream processing jobs
│       └── streaming_job.py          # Spark streaming job
├── 📝 requirements.txt               # Python dependencies
├── 🐳 Dockerfile.spark               # Spark Docker image
└── 📖 README.md                      # This file
```

## 🚀 Hướng dẫn cài đặt và triển khai

### Yêu cầu hệ thống

- **Minikube** >= 1.30
- **kubectl** >= 1.27
- **Docker** >= 20.10
- **Python** >= 3.9
- RAM: Tối thiểu 16GB (khuyến nghị 32GB)
- CPU: Tối thiểu 4 cores (khuyến nghị 8 cores)

### 📦 Bước 1: Khởi động Minikube

Khởi động Minikube với tài nguyên tối đa:

```bash
minikube start --cpus=max --memory=max
```

> **Lưu ý**: Bạn có thể điều chỉnh số lượng CPU và memory theo nhu cầu:
> ```bash
> minikube start --cpus=8 --memory=16384
> ```

### 🔧 Bước 2: Cài đặt các thành phần

Chạy các script setup theo thứ tự:

```bash
# 1. Build và load Docker images vào Minikube
bash ./setup/01-build-and-load-images.sh

# 2. Deploy tất cả services lên Kubernetes
bash ./setup/02-deploy-services.sh

# 3. Khởi tạo HDFS và upload dữ liệu
bash ./setup/03-setup-hdfs.sh

# 4. Tạo Kafka topics và deploy connectors
bash ./setup/04-setup-kafka.sh

# 5. Khởi tạo ClickHouse database và tables
bash ./setup/05-setup-clickhouse.sh

# 6. Khởi tạo MongoDB collections
bash ./setup/06-setup-mongo.sh

# 7. Khởi tạo Superset admin user và dashboards
bash ./setup/07-setup-superset.sh
```

### 🌐 Bước 3: Port Forwarding

Để truy cập các services từ máy local, mở terminal mới và chạy:

```bash
# Forward Kafka brokers
kubectl port-forward k-broker-0 9094:9095 &
kubectl port-forward k-broker-1 9095:9095 &
```

### 🔍 Lấy địa chỉ IP của Minikube

```bash
minikube ip
```

Ghi lại địa chỉ IP này để sử dụng khi kết nối đến các services. Thường sẽ là 192.168.49.2

## 🎯 Hướng dẫn sử dụng

### 1️⃣ Chạy Data Producer (Speed Layer)

Giả lập luồng dữ liệu taxi real-time vào Kafka:

```bash
python ./src/ingestion/nghiact_producer.py
```

Producer sẽ đọc dữ liệu từ Parquet files và gửi vào Kafka topic với tốc độ điều chỉnh được.
Quan sát Kafka UI tại http://192.168.49.2:30808

### 2️⃣ Submit Spark Streaming Job

Xử lý dữ liệu streaming từ Kafka:

```bash
kubectl exec $(kubectl get pods -l app=spark-master -o jsonpath='{.items[0].metadata.name}') -it -- \
    spark-submit ./src/stream_job/streaming_job.py
```

Streaming job sẽ:
- Đọc dữ liệu từ Kafka
- Transform và enrich dữ liệu
- Ghi kết quả vào ClickHouse

### 3️⃣ Chạy Batch Job (Batch Layer)

Truy cập Dagster UI để trigger batch jobs:

1. Mở trình duyệt: http://192.168.49.2:30300

2. Chọn job và click **"Launchpad"** để chạy

### 4️⃣ Visualize dữ liệu với Superset

#### Kết nối Superset với Trino:

1. Truy cập http://192.168.49.2:30088
   - Username: `admin`
   - Password: `admin`

2. Thêm Trino connection:
   - **Database**: Apache Superset
   - **SQLAlchemy URI**: `trino://trino@trino-service:8080/`

3. Tạo datasets và charts từ Trino queries

