from confluent_kafka import Producer, Consumer, KafkaException
from pyspark.sql import SparkSession
import socket

conf = {
    "bootstrap.servers": 'localhost:19092,localhost:29092',
    "client.id": socket.gethostname(),
}

producer = Producer(conf)

def check_kafka_connection():
    try:
        producer.produce('test-topic', key='key', value='value')
        producer.flush()
        print("Connection to Kafka broker is established")
    except KafkaException as e:
        print(f"Failed to connect to Kafka broker: {e}")

def check_spark_connection():
    try:
        spark = SparkSession.builder.appName("Streaming").getOrCreate()
        print("Connection to Spark broker is established")
        return spark
    except Exception as e:
        print(f"Failed to connect to Spark broker: {e}")

check_spark_connection()