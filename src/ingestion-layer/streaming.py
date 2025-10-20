import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct, col, lit, concat_ws, date_format

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True, help="path to bootstrap kafka")
    ap.add_argument("--topic", required=True, help="topic name")
    ap.add_argument("--input", required=True, help="path to input folder")
    ap.add_argument("--checkpoint", required=True, help="path to checkpoint")
    ap.add_argument("--max-files-per-trigger", type=int, default=1, help="Files per micro-batch")
    ap.add_argument("--latest-first", action="store_true", help="Process newest files first", default=True)
    return ap.parse_args()

def main():
    args = parse_args()
    spark = (SparkSession.builder
             .appName("Streaming")
             .config("spark.streaming.stopGracefullyOnShutdown", True)
             .getOrCreate())

    schema = spark.read.parquet(args.input).schema
    src = spark \
        .readStream \
        .schema(schema) \
        .format("parquet") \
        .option("maxFilesPerTrigger", str(args.max_files_per_trigger)) \
        .option("latestFirst", str(bool(args.latest_first))) .load(args.input)

    out = (src
           .withColumn(
        "key",
                concat_ws(":", col("VendorID"),
                date_format(col("tpep_pickup_datetime"), "yyyy-MM-dd"))
            )
           .withColumn("value", to_json(struct([col(c) for c in src.columns]))))

    out.selectExpr("CAST (key AS BINARY) AS key", "CAST (value AS BINARY) AS value")\
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", args.bootstrap) \
        .option("topic", args.topic) \
        .option("checkpointLocation", args.checkpoint) \
        .trigger(processingTime="3 seconds") \
        .start() \
        .awaitTermination()

if __name__ == '__main__':
    main()