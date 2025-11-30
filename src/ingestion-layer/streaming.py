import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, date_format

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from datetime import datetime


VALUE_SCHEMA_STR = """
{
  "type": "record",
  "name": "YellowTripdata2025_01",
  "namespace": "com.example.taxi",
  "fields": [
    { "name": "VendorID",              "type": ["null", "int"],   "default": null },

    {
      "name": "tpep_pickup_datetime",
      "type": ["null", { "type": "long", "logicalType": "timestamp-micros" }],
      "default": null
    },
    {
      "name": "tpep_dropoff_datetime",
      "type": ["null", { "type": "long", "logicalType": "timestamp-micros" }],
      "default": null
    },

    { "name": "passenger_count",       "type": ["null", "long"],  "default": null },
    { "name": "trip_distance",         "type": ["null", "double"],"default": null },
    { "name": "RatecodeID",            "type": ["null", "long"],  "default": null },
    { "name": "store_and_fwd_flag",    "type": ["null", "string"],"default": null },
    { "name": "PULocationID",          "type": ["null", "int"],   "default": null },
    { "name": "DOLocationID",          "type": ["null", "int"],   "default": null },
    { "name": "payment_type",          "type": ["null", "long"],  "default": null },
    { "name": "fare_amount",           "type": ["null", "double"],"default": null },
    { "name": "extra",                 "type": ["null", "double"],"default": null },
    { "name": "mta_tax",               "type": ["null", "double"],"default": null },
    { "name": "tip_amount",            "type": ["null", "double"],"default": null },
    { "name": "tolls_amount",          "type": ["null", "double"],"default": null },
    { "name": "improvement_surcharge", "type": ["null", "double"],"default": null },
    { "name": "total_amount",          "type": ["null", "double"],"default": null },
    { "name": "congestion_surcharge",  "type": ["null", "double"],"default": null },
    { "name": "Airport_fee",           "type": ["null", "double"],"default": null },
    { "name": "cbd_congestion_fee",    "type": ["null", "double"],"default": null }
  ]
}
"""

KEY_SCHEMA_STR = """
"string"
"""



def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True, help="Kafka bootstrap servers, ví dụ: broker01:9092,broker02:9092")
    ap.add_argument("--topic", required=True, help="Kafka topic name, ví dụ: taxi-avro-topic")
    ap.add_argument("--input", required=True, help="Path tới thư mục Parquet input")
    ap.add_argument("--checkpoint", required=True, help="Path tới thư mục checkpoint cho streaming")
    ap.add_argument("--max-files-per-trigger", type=int, default=1, help="Files per micro-batch")
    ap.add_argument("--latest-first", action="store_true", help="Process newest files first", default=True)
    ap.add_argument("--schema-registry-url", default="http://schema-registry:8081",
                    help="URL Schema Registry, mặc định: http://schema-registry:8081")
    return ap.parse_args()


def make_send_batch_to_kafka_avro(bootstrap_servers: str,
                                  topic: str,
                                  schema_registry_url: str):
    value_schema = avro.loads(VALUE_SCHEMA_STR)
    key_schema = avro.loads(KEY_SCHEMA_STR)
    producer_conf = {
        "bootstrap.servers": bootstrap_servers,
        "schema.registry.url": schema_registry_url,
        "queue.buffering.max.messages": 500000,
        "queue.buffering.max.kbytes": 1048576,
        "batch.num.messages": 5000,
        "linger.ms": 50,
    }

    def send_batch_to_kafka_avro(df, batch_id):
        if df.rdd.isEmpty():
            return

        producer = AvroProducer(
            producer_conf,
            default_key_schema=key_schema,
            default_value_schema=value_schema,
        )

        df = df.repartition(8)

        from datetime import datetime

        count = 0
        for row in df.toLocalIterator():
            record = row.asDict(recursive=True)

            key_str = record.pop("key", None)
            if key_str is None:
                # fallback: tự build key nếu thiếu
                key_str = f"{record.get('VendorID')}"

            # convert datetime -> long microseconds
            for ts_col in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
                ts = record.get(ts_col)
                if isinstance(ts, datetime):
                    record[ts_col] = int(ts.timestamp() * 1_000_000)
                elif ts is None:
                    record[ts_col] = None
                else:
                    record[ts_col] = int(ts)

            # produce với retry khi queue full
            while True:
                try:
                    producer.produce(
                        topic=topic,
                        key=key_str,
                        value=record,
                    )
                    break
                except BufferError:
                    producer.poll(0.1)

            count += 1
            if count % 1000 == 0:
                producer.poll(0)

        producer.flush()

    return send_batch_to_kafka_avro

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
        .option("maxBytesPerTrigger", "64m") \
        .option("latestFirst", str(bool(args.latest_first))) .load(args.input)

    stream_with_key = (
        src.withColumn(
            "key",
            concat_ws(
                ":",
                col("VendorID"),
                date_format(col("tpep_pickup_datetime"), "yyyy-MM-dd")
            )
        )
    )

    send_batch_fn = make_send_batch_to_kafka_avro(
        bootstrap_servers=args.bootstrap,
        topic=args.topic,
        schema_registry_url=args.schema_registry_url
    )

    query = (
        stream_with_key.writeStream
        .foreachBatch(send_batch_fn)
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime="3 seconds")
        .start()
    )

    query.awaitTermination()

if __name__ == '__main__':
    main()