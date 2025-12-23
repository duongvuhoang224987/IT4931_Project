from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
        .appName("NYC-Taxi-StarSchema") \
        .master("local[*]") \
        .getOrCreate()

clickhouse_url = "jdbc:clickhouse://clickhouse:8123/nyc_taxi_dw"
clickhouse_properties = {
    "user": "it4931",
    "password": "it4931",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}

path = "hdfs:///user/it4931/taxi_zone_lookup.csv"
print("🔵 Building DimLocation ...")
# Ensure this CSV path exists on your HDFS
zone = spark.read.csv(path, header=True)
dim_location = zone.select(
    col("LocationID").cast("int").alias("location_id"),
    col("Borough").alias("borough"),
    col("Zone").alias("zone"),
    col("service_zone")
)
print("🔵 Writing DimLocation to ClickHouse ...")
dim_location.write \
    .mode("overwrite") \
    .format("jdbc") \
    .option("url", clickhouse_url) \
    .option("dbtable", "nyc_taxi_dw.dim_location") \
    .option("user", clickhouse_properties["user"]) \
    .option("password", clickhouse_properties["password"]) \
    .option("driver", clickhouse_properties["driver"]) \
    .option("createTableOptions", "ENGINE = MergeTree ORDER BY location_id") \
    .save()
print("✔ DimLocation saved to ClickHouse.")