## Cài đặt
Khởi động các Container: 

`docker compose up -d`

Cài đặt container Kafka:

    docker exec -it broker01 bash

    kafka-topics --create --topic taxi-topic --replication-factor 2 --partitions 3 --bootstrap-server broker01:9092`

Deploy Connector lên Kafka Connect:

    curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @kafka/kafka-connect/configs/hdfs3-taxi-parquet.json

Cài đặt container HDFS:
    
    cd hdfs
    bash copy_data_batchjob.sh 

Cài đặt container ClickHouse:
    
    docker exec -it clickhouse clickhouse-client

Thực hiện code tại `/src/spark_jobs/to_clickhouse.sql` để tạo Database và các Table

Có thể dùng Trino hoặc Clickhouse-client để truy vấn dữ liệu trong Clickhouse.

Cài đặt container Superset:

    docker exec -it superset superset-init

Truy cập localhost:8088
Connect Trino: trino://trino:@trino-qe:8080/mongodb
Tạo Dashboard với các Chart bằng cách thực hiện các câu SQL trong `/superset/build_chart.sql`

Tải sẵn các dependencies cần thiết cho Spark thực hiện các Job:

    docker exec -it sparkMaster spark-sql 

## Triển khai các job:

Giả lập luồng dữ liệu liên tục:
    
    python ./src/ingestion/nghiact_producer.py

Submit streaming job vào Spark Cluster:

    docker exec -it sparkMaster spark-submit ./src/streaming_submit.py

Chạy Batch Job:

Truy cập `localhost:3000` (Dagster UI) để  trigger batch job ("Launchpad")

