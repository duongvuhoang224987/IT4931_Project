from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr, when, to_timestamp, window, count, date_format
import requests

# BOOTSTRAP_SERVERS = "broker01:9094,broker02:9094"
BOOTSTRAP_SERVERS = "broker01:9092,broker02:9092"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
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
    .config("spark.mongodb.write.connection.uri", "mongodb://root:root@mongo-db:27017/") \
    .config("spark.sql.session.timeZone", "America/New_York") \
    .getOrCreate()

print(">>>>>>>>>>>>>>>>>>>> START READ STREAM")
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", TOPIC) \
    .load()

# Nếu byte 0 == 0 (Confluent wire-format), ta lấy substring bắt đầu từ byte thứ 6 (1-indexed)

raw_des_df = df \
    .selectExpr("substring(value, 6) as avro_value") \
    .select(
    from_avro(
        col("avro_value"),
        get_schema_from_SR("test-value")
    ).alias("data")) \
    .select(col("data.*"))

w_df = raw_des_df.withColumn("pickup_ts", to_timestamp(col("tpep_pickup_datetime")))

tripCount = w_df.withWatermark("pickup_ts", "30 minutes") \
    .groupBy(
    window(col("pickup_ts"), "10 minutes").alias("w")
).agg(
    count("*").alias("trip_count")
).select(
    date_format(col("w.start"), "yyyy-MM-dd HH:mm:ss").alias("window_start"),
    date_format(col("w.end"), "yyyy-MM-dd HH:mm:ss").alias("window_end"),
    col("trip_count")
)


print(">>>>>>>>>>>>>>>>>>>> WRITE RAW ")
raw_query = raw_des_df \
    .writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "/tmp/spark-mongo-checkpoint/raw") \
    .option("spark.mongodb.write.connection.uri", "mongodb://root:root@mongo-db:27017/") \
    .option("spark.mongodb.write.database", "taxi") \
    .option("spark.mongodb.write.collection", "yellow_taxi_raw") \
    .outputMode("append") \
    .start()


def upsert_to_mongo(batch_df, batch_id):
    batch_df.write \
        .format("mongodb") \
        .option("spark.mongodb.write.connection.uri", "mongodb://root:root@mongo-db:27017/") \
        .option("spark.mongodb.write.database", "taxi") \
        .option("spark.mongodb.write.collection", "yellow_window10min") \
        .option("spark.mongodb.write.operationType", "update") \
        .option("spark.mongodb.write.upsert", "true") \
        .option("spark.mongodb.write.idFieldList", "window_start") \
        .mode("append") \
        .save()

print(">>>>>>>>>>>>>>>>>>>> WRITE AGGREGATED")
tripcount_query = tripCount \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(upsert_to_mongo) \
    .option("checkpointLocation", "/tmp/spark-mongo-checkpoint/agg/") \
    .start()

spark.streams.awaitAnyTermination()
