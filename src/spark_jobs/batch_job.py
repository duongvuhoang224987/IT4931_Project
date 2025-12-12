from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, avg, count, max, min, round,
    row_number, rank, dense_rank, lag, lead,
    year, month, dayofmonth, hour,
    when, lit, coalesce
)
from pyspark.sql.window import Window


def read_from_clickhouse(spark, table_name, clickhouse_url, properties):
    """Helper function to read a table from ClickHouse."""
    print(f"🔵 Reading {table_name} from ClickHouse...")
    return spark.read \
        .format("jdbc") \
        .option("url", clickhouse_url) \
        .option("dbtable", table_name) \
        .option("user", properties["user"]) \
        .option("password", properties["password"]) \
        .option("driver", properties["driver"]) \
        .load()


def write_to_clickhouse(df, table_name, clickhouse_url, properties, order_by):
    """Helper function to write a DataFrame to ClickHouse."""
    print(f"🔵 Writing {table_name} to ClickHouse...")
    df.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", clickhouse_url) \
        .option("dbtable", table_name) \
        .option("user", properties["user"]) \
        .option("password", properties["password"]) \
        .option("driver", properties["driver"]) \
        .option("createTableOptions", f"ENGINE = MergeTree ORDER BY {order_by}") \
        .save()
    print(f"✔ {table_name} saved successfully!")


def compute_daily_aggregations(fact_df, dim_time_df):
    """Compute daily trip statistics."""
    print("🔵 Computing daily aggregations...")
    
    daily_stats = fact_df \
        .join(dim_time_df, fact_df.pickup_time_sk == dim_time_df.timestamp_id, "left") \
        .groupBy("date", "year", "month", "day", "is_weekend") \
        .agg(
            count("*").alias("total_trips"),
            round(sum("total_amount"), 2).alias("total_revenue"),
            round(avg("trip_distance_miles"), 2).alias("avg_distance"),
            round(avg("trip_time_seconds"), 2).alias("avg_duration_seconds"),
            round(avg("total_amount"), 2).alias("avg_fare"),
            round(sum("tip_amount"), 2).alias("total_tips"),
            round(avg("passenger_count"), 2).alias("avg_passengers"),
            max("total_amount").alias("max_fare"),
            min("total_amount").alias("min_fare")
        ) \
        .orderBy("date")
    
    # Add 7-day moving averages
    window_7day = Window.orderBy("date").rowsBetween(-6, 0)
    
    daily_stats = daily_stats \
        .withColumn("moving_avg_trips_7d", round(avg("total_trips").over(window_7day), 2)) \
        .withColumn("moving_avg_revenue_7d", round(avg("total_revenue").over(window_7day), 2))
    
    print("✔ Daily aggregations completed!")
    return daily_stats


def compute_hourly_patterns(fact_df, dim_time_df):
    """Compute hourly patterns for demand analysis."""
    print("🔵 Computing hourly patterns...")
    
    hourly_stats = fact_df \
        .join(dim_time_df, fact_df.pickup_time_sk == dim_time_df.timestamp_id, "left") \
        .groupBy("hour", "day_of_week", "is_weekend") \
        .agg(
            count("*").alias("total_trips"),
            round(avg("total_amount"), 2).alias("avg_fare"),
            round(avg("trip_distance_miles"), 2).alias("avg_distance"),
            round(avg("trip_time_seconds"), 2).alias("avg_duration_seconds")
        ) \
        .orderBy("is_weekend", "hour")
    
    print("✔ Hourly patterns completed!")
    return hourly_stats


def compute_location_analytics(fact_df, dim_location_df):
    """Compute location-based metrics for pickup zones."""
    print("🔵 Computing location analytics...")
    
    # Pickup location stats
    pickup_stats = fact_df \
        .join(dim_location_df, fact_df.pickup_location_sk == dim_location_df.location_id, "left") \
        .groupBy("location_id", "borough", "zone", "service_zone") \
        .agg(
            count("*").alias("pickup_count"),
            round(avg("total_amount"), 2).alias("avg_fare"),
            round(avg("trip_distance_miles"), 2).alias("avg_distance"),
            round(sum("total_amount"), 2).alias("total_revenue")
        )
    
    # Add ranking
    window_rank = Window.orderBy(col("pickup_count").desc())
    pickup_stats = pickup_stats \
        .withColumn("popularity_rank", rank().over(window_rank)) \
        .orderBy("pickup_count", ascending=False)
    
    print("✔ Location analytics completed!")
    return pickup_stats


def compute_dropoff_analytics(fact_df, dim_location_df):
    """Compute location-based metrics for dropoff zones."""
    print("🔵 Computing dropoff analytics...")
    
    dropoff_stats = fact_df \
        .join(dim_location_df, fact_df.dropoff_location_sk == dim_location_df.location_id, "left") \
        .groupBy("location_id", "borough", "zone", "service_zone") \
        .agg(
            count("*").alias("dropoff_count"),
            round(avg("total_amount"), 2).alias("avg_fare"),
            round(avg("trip_distance_miles"), 2).alias("avg_distance"),
            round(sum("total_amount"), 2).alias("total_revenue")
        )
    
    # Add ranking
    window_rank = Window.orderBy(col("dropoff_count").desc())
    dropoff_stats = dropoff_stats \
        .withColumn("popularity_rank", rank().over(window_rank)) \
        .orderBy("dropoff_count", ascending=False)
    
    print("✔ Dropoff analytics completed!")
    return dropoff_stats


def compute_vendor_performance(fact_df, dim_vendor_df, dim_time_df):
    """Compute vendor performance metrics over time."""
    print("🔵 Computing vendor performance...")
    
    vendor_daily = fact_df \
        .join(dim_vendor_df, fact_df.vendor_sk == dim_vendor_df.vendor_id, "left") \
        .join(dim_time_df, fact_df.pickup_time_sk == dim_time_df.timestamp_id, "left") \
        .groupBy("date", "vendor_id", "provider_name") \
        .agg(
            count("*").alias("daily_trips"),
            round(sum("total_amount"), 2).alias("daily_revenue"),
            round(avg("trip_distance_miles"), 2).alias("avg_distance"),
            round(avg("total_amount"), 2).alias("avg_fare")
        )
    
    # Add daily ranking
    window_rank = Window.partitionBy("date").orderBy(col("daily_trips").desc())
    vendor_daily = vendor_daily \
        .withColumn("daily_rank", rank().over(window_rank)) \
        .orderBy("date", "daily_rank")
    
    print("✔ Vendor performance completed!")
    return vendor_daily


def compute_payment_analysis(fact_df, dim_payment_df, dim_time_df):
    """Analyze payment methods and trends."""
    print("🔵 Computing payment analysis...")
    
    payment_stats = fact_df \
        .join(dim_payment_df, fact_df.payment_type_sk == dim_payment_df.payment_id, "left") \
        .join(dim_time_df, fact_df.pickup_time_sk == dim_time_df.timestamp_id, "left") \
        .groupBy("year", "month", "payment_id", "payment_type_name") \
        .agg(
            count("*").alias("transaction_count"),
            round(sum("total_amount"), 2).alias("total_revenue"),
            round(sum("tip_amount"), 2).alias("total_tips"),
            round(avg("total_amount"), 2).alias("avg_transaction"),
            round(avg("tip_amount"), 2).alias("avg_tip")
        ) \
        .orderBy("year", "month", col("transaction_count").desc())
    
    print("✔ Payment analysis completed!")
    return payment_stats


def compute_route_analysis(fact_df, pickup_loc_df, dropoff_loc_df):
    """Analyze popular routes between pickup and dropoff locations."""
    print("🔵 Computing route analysis...")
    
    route_stats = fact_df \
        .join(pickup_loc_df.alias("pu"), 
              fact_df.pickup_location_sk == col("pu.location_id"), "left") \
        .join(dropoff_loc_df.alias("do"), 
              fact_df.dropoff_location_sk == col("do.location_id"), "left") \
        .groupBy(
            col("pu.location_id").alias("pickup_location_id"),
            col("pu.borough").alias("pickup_borough"),
            col("pu.zone").alias("pickup_zone"),
            col("do.location_id").alias("dropoff_location_id"),
            col("do.borough").alias("dropoff_borough"),
            col("do.zone").alias("dropoff_zone")
        ) \
        .agg(
            count("*").alias("trip_count"),
            round(avg("trip_distance_miles"), 2).alias("avg_distance"),
            round(avg("trip_time_seconds"), 2).alias("avg_duration_seconds"),
            round(avg("total_amount"), 2).alias("avg_fare")
        )
    
    # Add ranking
    window_rank = Window.orderBy(col("trip_count").desc())
    route_stats = route_stats \
        .withColumn("route_rank", rank().over(window_rank)) \
        .filter(col("route_rank") <= 1000)  # Keep top 1000 routes
    
    print("✔ Route analysis completed!")
    return route_stats


def compute_revenue_breakdown(fact_df, dim_time_df):
    """Break down revenue components by time period."""
    print("🔵 Computing revenue breakdown...")
    
    revenue_breakdown = fact_df \
        .join(dim_time_df, fact_df.pickup_time_sk == dim_time_df.timestamp_id, "left") \
        .groupBy("year", "month") \
        .agg(
            count("*").alias("total_trips"),
            round(sum("fare_amount"), 2).alias("total_base_fare"),
            round(sum("extra"), 2).alias("total_extra"),
            round(sum("mta_tax"), 2).alias("total_mta_tax"),
            round(sum("tip_amount"), 2).alias("total_tips"),
            round(sum("tolls_amount"), 2).alias("total_tolls"),
            round(sum("improvement_surcharge"), 2).alias("total_improvement_surcharge"),
            round(sum("congestion_surcharge"), 2).alias("total_congestion_surcharge"),
            round(sum("airport_fee"), 2).alias("total_airport_fee"),
            round(sum("cbd_congestion_fee"), 2).alias("total_cbd_fee"),
            round(sum("total_amount"), 2).alias("total_revenue")
        ) \
        .orderBy("year", "month")
    
    print("✔ Revenue breakdown completed!")
    return revenue_breakdown


def main():
    """Main ETL pipeline for batch analytics."""
    
    print("=" * 70)
    print("🚕 NYC Taxi Batch Analytics Job")
    print("=" * 70)
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("NYC-Taxi-Batch-Analytics") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    # ClickHouse connection settings
    clickhouse_url = "jdbc:clickhouse://clickhouse:8123/nyc_taxi_dw"
    clickhouse_properties = {
        "user": "it4931",
        "password": "it4931",
        "driver": "com.clickhouse.jdbc.ClickHouseDriver"
    }
    
    # =====================================================================
    # 1. READ DATA FROM CLICKHOUSE
    # =====================================================================
    print("\n📥 STEP 1: Loading data from ClickHouse...")
    
    fact_df = read_from_clickhouse(spark, "nyc_taxi_dw.fact_trip", 
                                   clickhouse_url, clickhouse_properties)
    dim_time_df = read_from_clickhouse(spark, "nyc_taxi_dw.dim_time", 
                                       clickhouse_url, clickhouse_properties)
    dim_vendor_df = read_from_clickhouse(spark, "nyc_taxi_dw.dim_vendor", 
                                         clickhouse_url, clickhouse_properties)
    dim_payment_df = read_from_clickhouse(spark, "nyc_taxi_dw.dim_payment", 
                                          clickhouse_url, clickhouse_properties)
    dim_location_df = read_from_clickhouse(spark, "nyc_taxi_dw.dim_location", 
                                           clickhouse_url, clickhouse_properties)
    
    print(f"✔ Loaded {fact_df.count()} trip records")
    
    # =====================================================================
    # 2. COMPUTE ANALYTICS
    # =====================================================================
    print("\n🔄 STEP 2: Computing analytics...")
    
    # Daily aggregations with moving averages
    daily_stats = compute_daily_aggregations(fact_df, dim_time_df)
    
    # Hourly patterns
    hourly_patterns = compute_hourly_patterns(fact_df, dim_time_df)
    
    # Location analytics
    pickup_analytics = compute_location_analytics(fact_df, dim_location_df)
    dropoff_analytics = compute_dropoff_analytics(fact_df, dim_location_df)
    
    # Vendor performance
    vendor_performance = compute_vendor_performance(fact_df, dim_vendor_df, dim_time_df)
    
    # Payment analysis
    payment_analysis = compute_payment_analysis(fact_df, dim_payment_df, dim_time_df)
    
    # Route analysis
    route_analysis = compute_route_analysis(fact_df, dim_location_df, dim_location_df)
    
    # Revenue breakdown
    revenue_breakdown = compute_revenue_breakdown(fact_df, dim_time_df)
    
    # =====================================================================
    # 3. WRITE RESULTS TO CLICKHOUSE
    # =====================================================================
    print("\n📤 STEP 3: Writing analytics tables to ClickHouse...")
    
    write_to_clickhouse(daily_stats, "nyc_taxi_dw.agg_daily_trips", 
                       clickhouse_url, clickhouse_properties, 
                       "(year, month, day)")
    
    write_to_clickhouse(hourly_patterns, "nyc_taxi_dw.agg_hourly_patterns", 
                       clickhouse_url, clickhouse_properties, 
                       "(is_weekend, hour)")
    
    write_to_clickhouse(pickup_analytics, "nyc_taxi_dw.agg_pickup_locations", 
                       clickhouse_url, clickhouse_properties, 
                       "popularity_rank")
    
    write_to_clickhouse(dropoff_analytics, "nyc_taxi_dw.agg_dropoff_locations", 
                       clickhouse_url, clickhouse_properties, 
                       "popularity_rank")
    
    write_to_clickhouse(vendor_performance, "nyc_taxi_dw.agg_vendor_performance", 
                       clickhouse_url, clickhouse_properties, 
                       "(date, vendor_id)")
    
    write_to_clickhouse(payment_analysis, "nyc_taxi_dw.agg_payment_analysis", 
                       clickhouse_url, clickhouse_properties, 
                       "(year, month, payment_id)")
    
    write_to_clickhouse(route_analysis, "nyc_taxi_dw.agg_popular_routes", 
                       clickhouse_url, clickhouse_properties, 
                       "route_rank")
    
    write_to_clickhouse(revenue_breakdown, "nyc_taxi_dw.agg_revenue_breakdown", 
                       clickhouse_url, clickhouse_properties, 
                       "(year, month)")
    
    # =====================================================================
    # 4. SUMMARY
    # =====================================================================
    print("\n" + "=" * 70)
    print("📊 ANALYTICS SUMMARY")
    print("=" * 70)
    print(f"✔ Daily Stats: {daily_stats.count()} days")
    print(f"✔ Hourly Patterns: {hourly_patterns.count()} hour combinations")
    print(f"✔ Pickup Locations: {pickup_analytics.count()} zones")
    print(f"✔ Dropoff Locations: {dropoff_analytics.count()} zones")
    print(f"✔ Vendor Performance: {vendor_performance.count()} daily records")
    print(f"✔ Payment Analysis: {payment_analysis.count()} monthly records")
    print(f"✔ Popular Routes: {route_analysis.count()} routes")
    print(f"✔ Revenue Breakdown: {revenue_breakdown.count()} months")
    print("=" * 70)
    print("🎉 Batch Analytics Job Completed Successfully!")
    print("=" * 70)
    
    spark.stop()


if __name__ == "__main__":
    main()