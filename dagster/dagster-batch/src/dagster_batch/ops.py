from dagster import op, Out
from pyspark.sql import SparkSession
from datetime import datetime

@op(out=Out())
def load_parquet(context):
    spark = SparkSession.builder.master("local[*]").appName("Dagster Batch").getOrCreate()

    df = spark.read.parquet("data/raw/yellow_tripdata_2025-08.parquet")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"data/transformed/nyc_taxi_loaded_{timestamp}.parquet"
    df.write.mode("overwrite").parquet(output_path)

    context.log.info(f"Data saved to {output_path}")
    return output_path


@op(out=Out())
def transform_data(context, parquet_path: str):
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.parquet(parquet_path)

    result = df.groupBy("passenger_count").avg("fare_amount")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    result_path = f"data/transformed/nyc_taxi_transformed_{timestamp}.parquet"

    result.write.mode("overwrite").parquet(result_path)
    context.log.info(f"Transformed data saved to {result_path}")
    return result_path
