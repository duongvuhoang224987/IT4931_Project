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
