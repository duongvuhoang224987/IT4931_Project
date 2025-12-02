- Lưu ý: Khi muốn chạy lại sạch sẽ, xóa hết các container thì `docker compose down -v` là chưa đủ, còn cần thêm xóa các thư mục `/current` và file `in_use.lock` bên trong `/hdfs/hdfs_namenode/` và `/hdfs/hdfs_datanode/` (Do mount volumes. Chỉ xóa file va thư mục con, không xóa `/hdfs_namenode` và `/hdfs_datanode`)
  - Có thể thực hiện bằng cách chạy `delete_data.sh` trong thư mục `/hdfs` (Chạy với PWD là /hdfs)

### Batch Layer

- Chuẩn bị data cho batch job: `copy_data_batchjob.sh` trong `/hdfs` (Chạy với PWD là /hdfs)

### Speed Layer
- Start các container:
`docker compose up -d`
- Tắt safemode của NameNode: 

  - `docker exec -it nameNode bash`
  - `hdfs dfsadmin -safemode leave`

- Trên local, chạy `python ./src/yellow_taxi_producer.py` để có schema trên schema-registry (fix sau)
- Submit Streaming job:

    - `docker exec -it sparkMaster bash`
    - `spark-submit ./src/streaming_submit.py`

- Theo dõi table ghi thành công vào `mongo-db` qua `mongo-express`: truy cập `localhost:18081` có username: admin và password: pass

- Khởi tạo user của superset:

    - `docker exec -it superset superset-init`
    - Nhập tài khoản mật khẩu mong muốn
    - Đợi 1 khoảng thời gian cho container thiết lập, chạy ổn định
    - Truy cập superset UI tại `localhost:8088`, tạo các chart trên dashboard dựa theo hình ảnh



## Rerun
cd hdfs

bash delete_data.sh

docker compose up -d

bash copy_data_batchjob.sh 

docker exec -it clickhouse clickhouse-client

Copy paste SQL code (`/src/spark_jobs/to_clickhouse.sql`)
Truy cập `localhost:3000` (Dagster UI): Trigger batch job ("Launchpad")
Có thể dùng Trino hoặc Clickhouse-client để truy vấn dữ liệu trong Clickhouse.

docker exec -it superset superset-init

Truy cập localhost:8088
Connect Trino: trino://trino:@trino-qe:8080/mongodb
Tạo Dashboard với các Chart thực hiện các câu SQL:
  - Big Number Chart: `SELECT count(*) as num_trip from yellow_taxi_raw ;`
  - Line Chart: `Select window_start, trip_count from yellow_window10min`
  - Pie Chart: `SELECT vendorid, COUNT(*) AS trip_count FROM yellow_taxi_raw group by vendorid;`
  - Bar Chart: `select cast(dolocationid as varchar) as doid, sum(total_amount) as total_revenue from mongodb.taxi.yellow_taxi_raw group by cast(dolocationid as varchar);`
  - Scatter Plot: ``

python ./src/yellow_taxi_producer.py

docker exec -it sparkMaster bash

spark-submit ./src/streaming_submit.py