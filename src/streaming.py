from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr, when
import requests


BOOTSTRAP_SERVERS = "localhost:9092,localhost:9094"
SCHEMA_REGISTRY_URL = "http://localhost:9091"
TOPIC = "test"

avro_options = {
    "schema.registry.url": SCHEMA_REGISTRY_URL,
    "mode": "PERMISSIVE"
}

def get_schema_from_SR(subject):
    response = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest/schema")
    response.raise_for_status()
    return response.text

spark = SparkSession.builder \
    .appName("test streaming job") \
    .config("spark.jars.repositories", "https://packages.confluent.io/maven/") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,"
            "org.apache.spark:spark-avro_2.12:3.5.6,"
            "io.confluent:kafka-avro-serializer:8.0.0,org.mongodb.spark:mongo-spark-connector_2.12:10.5.0") \
    .config("spark.mongodb.write.connection.uri", "mongodb://root:root@127.0.0.1:27017/") \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", TOPIC) \
    .load()

# Nếu byte 0 == 0 (Confluent wire-format), ta lấy substring bắt đầu từ byte thứ 6 (1-indexed)

deserialized_df = df \
    .selectExpr("substring(value, 6) as avro_value") \
    .select(
        from_avro( 
            col("avro_value"), 
            get_schema_from_SR("test-value")
        ).alias("data")) \
    .select(col("data.*"))

query = deserialized_df \
    .writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "/tmp/spark-mongo-checkpoint") \
    .option("spark.mongodb.write.connection.uri", "mongodb://root:root@127.0.0.1:27017/") \
    .option("spark.mongodb.write.database", "iot_db") \
    .option("spark.mongodb.write.collection", "device_events") \
    .outputMode("append") \
    .start()

query.awaitTermination()
