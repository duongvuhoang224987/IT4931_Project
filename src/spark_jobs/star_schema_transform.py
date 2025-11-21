from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofmonth, hour, minute,
    date_format, when, monotonically_increasing_id,
    unix_timestamp, lit, coalesce
)

def build_dim_time(df, colname):
    """Create DimTime table from a datetime column."""
    return df.select(col(colname).alias("timestamp_id")) \
        .where(col(colname).isNotNull()) \
        .distinct() \
        .withColumn("date", col("timestamp_id").cast("date")) \
        .withColumn("year", year("timestamp_id")) \
        .withColumn("month", month("timestamp_id")) \
        .withColumn("day", dayofmonth("timestamp_id")) \
        .withColumn("hour", hour("timestamp_id")) \
        .withColumn("minute", minute("timestamp_id")) \
        .withColumn("day_of_week", date_format(col("timestamp_id"), "E")) \
        .withColumn("is_weekend", col("day_of_week").isin("Sat", "Sun"))


def main():

    spark = SparkSession.builder \
        .appName("NYC-Taxi-StarSchema") \
        .getOrCreate()

    # Connection string includes the database 'nyc_taxi_dw'
    clickhouse_url = "jdbc:clickhouse://clickhouse:8123/nyc_taxi_dw"
    clickhouse_properties = {
        "user": "it4931",
        "password": "it4931",
        "driver": "com.clickhouse.jdbc.ClickHouseDriver"
    }

    print("🔵 Loading RAW data from HDFS ...")
    df = spark.read.parquet("hdfs://nameNode:9000/user/it4931/yellow_taxi/2025")
    
    # Limit for testing; remove this line in production
    df = df.limit(1_000_000) 

    # =====================================================================
    # 🧹 1. CLEANSING RAW DATA
    # =====================================================================
    print("🔵 Applying cleansing rules ...")

    # 1. Drop rows with null essential fields
    df = df.dropna(subset=["VendorID", "tpep_pickup_datetime",
                           "tpep_dropoff_datetime", "PULocationID",
                           "DOLocationID", "payment_type"])

    # 2. Valid timestamps
    df = df.filter(col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))

    # 3. Passenger + distance
    df = df.filter(col("passenger_count") > 0)
    df = df.filter(col("trip_distance") > 0)

    # 4. Fare rules
    df = df.filter(col("fare_amount") > 0)
    df = df.filter(col("total_amount") > 0)

    # 5. VendorID valid
    df = df.filter(col("VendorID").isin(1, 2))

    # 6. RatecodeID valid
    # FIX: Map invalid RateCodeIDs to 99 (Unknown) instead of NULL
    valid_rates = [1, 2, 3, 4, 5, 6]
    df = df.withColumn(
        "RatecodeID",
        when(col("RatecodeID").isin(valid_rates), col("RatecodeID")).otherwise(lit(99))
    )

    # 7. Payment type valid
    valid_payment = [1, 2, 3, 4, 5, 6]
    df = df.withColumn(
        "payment_type",
        when(col("payment_type").isin(valid_payment), col("payment_type")).otherwise(5)
    )

    # 8. Location validity
    df = df.filter((col("PULocationID") > 0) & (col("PULocationID") <= 263))
    df = df.filter((col("DOLocationID") > 0) & (col("DOLocationID") <= 263))

    # 9. Replace NULL fee columns
    fee_columns = [
        "Airport_fee", "cbd_congestion_fee", "congestion_surcharge",
        "extra", "mta_tax", "tolls_amount", "tip_amount"
    ]
    for c in fee_columns:
        df = df.withColumn(c, when(col(c).isNull(), lit(0)).otherwise(col(c)))

    print("✔ Cleansing finished.")

    # =====================================================================
    # 📘 2. BUILD DIMENSION TABLES
    # =====================================================================

    # --- DimTime ---
    print("🔵 Building DimTime ...")
    dim_pickup = build_dim_time(df, "tpep_pickup_datetime")
    dim_dropoff = build_dim_time(df, "tpep_dropoff_datetime")
    
    dim_time = dim_pickup.unionByName(dim_dropoff).dropDuplicates()
    dim_time = dim_time.withColumn(
        "is_weekend",
        when(col("is_weekend") == True, lit(1)).otherwise(lit(0)).cast("tinyint")
    )
    
    dim_time.write.mode("overwrite").parquet("hdfs:///dw/dim_time/")
    print("✔ DimTime saved to HDFS.")

    print("🔵 Writing DimTime to ClickHouse ...")
    dim_time.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", clickhouse_url) \
        .option("dbtable", "nyc_taxi_dw.dim_time") \
        .option("user", clickhouse_properties["user"]) \
        .option("password", clickhouse_properties["password"]) \
        .option("driver", clickhouse_properties["driver"]) \
        .option("createTableOptions", "ENGINE = MergeTree ORDER BY timestamp_id") \
        .save()
    print("✔ DimTime saved to ClickHouse.")

    # --- DimVendor ---
    print("🔵 Building DimVendor ...")
    dim_vendor = spark.createDataFrame(
        [(1, "Creative Mobile Technologies"),
         (2, "VeriFone Inc")],
        ["vendor_id", "provider_name"]
    )
    dim_vendor.write.mode("overwrite").parquet("hdfs:///dw/dim_vendor/")
    print("✔ DimVendor saved to HDFS.")

    print("🔵 Writing DimVendor to ClickHouse ...")
    dim_vendor.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", clickhouse_url) \
        .option("dbtable", "nyc_taxi_dw.dim_vendor") \
        .option("user", clickhouse_properties["user"]) \
        .option("password", clickhouse_properties["password"]) \
        .option("driver", clickhouse_properties["driver"]) \
        .option("createTableOptions", "ENGINE = MergeTree ORDER BY vendor_id") \
        .save()
    print("✔ DimVendor saved to ClickHouse.")

    # --- DimPayment ---
    print("🔵 Building DimPayment ...")
    dim_payment = spark.createDataFrame(
        [
            (1, "Credit card", "Paid by credit card"),
            (2, "Cash", "Cash payment"),
            (3, "No charge", "No charge"),
            (4, "Dispute", "Disputed fare"),
            (5, "Unknown", "Unknown"),
            (6, "Voided trip", "Voided trip")
        ],
        ["payment_id", "payment_type_name", "description"]
    )
    dim_payment.write.mode("overwrite").parquet("hdfs:///dw/dim_payment/")
    print("✔ DimPayment saved to HDFS.")

    print("🔵 Writing DimPayment to ClickHouse ...")
    dim_payment.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", clickhouse_url) \
        .option("dbtable", "nyc_taxi_dw.dim_payment") \
        .option("user", clickhouse_properties["user"]) \
        .option("password", clickhouse_properties["password"]) \
        .option("driver", clickhouse_properties["driver"]) \
        .option("createTableOptions", "ENGINE = MergeTree ORDER BY payment_id") \
        .save()
    print("✔ DimPayment saved to ClickHouse.")

    # --- DimRateCode ---
    print("🔵 Building DimRateCode ...")
    # FIX: Added entry for 99 (Unknown)
    dim_ratecode = spark.createDataFrame(
        [
            (1, "Standard rate", "Standard metered fare"),
            (2, "JFK", "Flat fare to/from JFK Airport"),
            (3, "Newark", "Fare to/from Newark Airport"),
            (4, "Nassau/Westchester", "Outside NYC"),
            (5, "Negotiated", "Negotiated fare"),
            (6, "Group ride", "Group/shared ride"),
            (99, "Unknown", "Unknown rate")
        ],
        ["rate_code_id", "rate_code_name", "description"]
    )
    dim_ratecode.write.mode("overwrite").parquet("hdfs:///dw/dim_ratecode/")
    print("✔ DimRateCode saved to HDFS.")

    print("🔵 Writing DimRateCode to ClickHouse ...")
    dim_ratecode.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", clickhouse_url) \
        .option("dbtable", "nyc_taxi_dw.dim_ratecode") \
        .option("user", clickhouse_properties["user"]) \
        .option("password", clickhouse_properties["password"]) \
        .option("driver", clickhouse_properties["driver"]) \
        .option("createTableOptions", "ENGINE = MergeTree ORDER BY rate_code_id") \
        .save()
    print("✔ DimRateCode saved to ClickHouse.")

    # --- DimLocation ---
    print("🔵 Building DimLocation ...")
    # Ensure this CSV path exists on your HDFS
    zone = spark.read.csv("hdfs:///user/it4931/taxi_zone_lookup.csv", header=True)
    dim_location = zone.select(
        col("LocationID").cast("int").alias("location_id"),
        col("Borough").alias("borough"),
        col("Zone").alias("zone"),
        col("service_zone")
    )
    dim_location.write.mode("overwrite").parquet("hdfs:///dw/dim_location/")
    print("✔ DimLocation saved to HDFS.")

    print("🔵 Writing DimLocation to ClickHouse ...")
    dim_location.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", clickhouse_url) \
        .option("dbtable", "nyc_taxi_dw.dim_location") \
        .option("user", clickhouse_properties["user"]) \
        .option("password", clickhouse_properties["password"]) \
        .option("driver", clickhouse_properties["driver"]) \
        .option("createTableOptions", "ENGINE = MergeTree ORDER BY location_id") \
        .save()
    print("✔ DimLocation saved to ClickHouse.")

    # =====================================================================
    # 📘 3. BUILD FACT TABLE
    # =====================================================================

    print("🔵 Building Fact Table ...")

    dim_time_df = spark.read.parquet("hdfs:///dw/dim_time/")
    dim_vendor_df = spark.read.parquet("hdfs:///dw/dim_vendor/")
    dim_payment_df = spark.read.parquet("hdfs:///dw/dim_payment/")
    dim_ratecode_df = spark.read.parquet("hdfs:///dw/dim_ratecode/")
    dim_location_df = spark.read.parquet("hdfs:///dw/dim_location/")

    # IMPORTANT: The alias names here must match the columns in your SQL table exactly.
    # Specifically, 'airport_fee' was 'Airport_fee' in raw data, but SQL uses 'airport_fee' (lowercase).
    fact = df \
        .join(dim_vendor_df, df.VendorID == dim_vendor_df.vendor_id, "left") \
        .join(dim_ratecode_df, df.RatecodeID == dim_ratecode_df.rate_code_id, "left") \
        .join(dim_payment_df, df.payment_type == dim_payment_df.payment_id, "left") \
        .join(dim_location_df.alias("pu"), df.PULocationID == col("pu.location_id"), "left") \
        .join(dim_location_df.alias("do"), df.DOLocationID == col("do.location_id"), "left") \
        .join(dim_time_df.alias("pt"), df.tpep_pickup_datetime == col("pt.timestamp_id"), "left") \
        .join(dim_time_df.alias("dt"), df.tpep_dropoff_datetime == col("dt.timestamp_id"), "left") \
        .select(
            monotonically_increasing_id().alias("fact_trip_sk"),
            col("pt.timestamp_id").alias("pickup_time_sk"),
            col("dt.timestamp_id").alias("dropoff_time_sk"),
            col("vendor_id").alias("vendor_sk"),
            
            # Coalesce ensures that even if the join fails somehow, we default to 99 (Unknown)
            coalesce(col("rate_code_id"), lit(99)).alias("ratecode_sk"),
            
            col("payment_id").alias("payment_type_sk"),
            col("pu.location_id").alias("pickup_location_sk"),
            col("do.location_id").alias("dropoff_location_sk"),

            "passenger_count",
            col("trip_distance").alias("trip_distance_miles"),
            "store_and_fwd_flag",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "improvement_surcharge",
            "total_amount",
            "congestion_surcharge",
            col("Airport_fee").alias("airport_fee"), # Renaming to lowercase to match ClickHouse DDL
            "cbd_congestion_fee",

            (unix_timestamp("tpep_dropoff_datetime") -
             unix_timestamp("tpep_pickup_datetime")).alias("trip_time_seconds"),

            df["tpep_pickup_datetime"].alias("created_at_sk")
        )

    fact.write.mode("overwrite").parquet("hdfs:///dw/fact_table/")
    print("✔ Fact Table saved to HDFS!")

    print("🔵 Writing Fact Table to ClickHouse ...")
    fact.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", clickhouse_url) \
        .option("dbtable", "nyc_taxi_dw.fact_trip") \
        .option("user", clickhouse_properties["user"]) \
        .option("password", clickhouse_properties["password"]) \
        .option("driver", clickhouse_properties["driver"]) \
        .option("createTableOptions", "ENGINE = MergeTree ORDER BY (pickup_time_sk, dropoff_time_sk, vendor_sk)") \
        .save()
    print("✔ Fact Table saved to ClickHouse!")

    print("🎉 ETL Star Schema completed!")
    spark.stop()


if __name__ == "__main__":
    main()