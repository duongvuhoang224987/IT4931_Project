# pyspark_write_mongo_test.py
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

def main():
    # Thay URI nếu Mongo của bạn khác (host:port, user:pass, authSource...).
    # Ví dụ dùng Mongo container local của bạn:
    mongo_uri = "mongodb://root:root@127.0.0.1:27017/"

    spark = SparkSession.builder \
        .appName("pyspark-mongo-write-test") \
        .config("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.12:10.5.0") \
        .config("spark.mongodb.write.connection.uri", mongo_uri) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Dữ liệu mẫu
    data = [
        ("device1", "2025-10-22T12:00:00Z", 25),
        ("device2", "2025-10-22T12:01:00Z", 26),
    ]
    schema = StructType() \
        .add("device_id", StringType()) \
        .add("ts", StringType()) \
        .add("temperature", IntegerType())

    df = spark.createDataFrame(data, schema)
    print("=== DataFrame to write ===")
    df.show(truncate=False)

    # Ghi vào MongoDB: db = iot_db, collection = device_events
    df.write \
      .format("mongodb") \
      .option("database", "iot_db") \
      .option("collection", "device_events") \
      .mode("append") \
      .save()

    print("Write finished. Check MongoDB for inserted documents.")
    spark.stop()

if __name__ == "__main__":
    main()
