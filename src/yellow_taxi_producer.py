from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import os
import time
import pyarrow.parquet as pq

# -------------------------
BOOTSTRAP_SERVERS = "localhost:9094,localhost:9095"
SCHEMA_REGISTRY_URL = "http://localhost:9091"

data_file_names = os.listdir("../data/yellow_taxi/2025")
data_file_paths = [os.path.join("../data/yellow_taxi/2025", f) for f in data_file_names]
schema_file_path = "../kafka/schema-registry/yellow_taxi.avsc"
# -------------------------

def get_SR_client(schema_registry_conf):
    schema_registry_conf = {"url": schema_registry_conf}
    return SchemaRegistryClient(schema_registry_conf)

def value_to_dict(obj, ctx):
    return dict(obj)

def get_avro_producer(schema_registry_client, avro_schema_str, bootstrap_servers):
    avro_serializer = AvroSerializer(
        schema_registry_client,
        avro_schema_str,
        value_to_dict
    )

    key_serializer = StringSerializer('utf_8')

    producer_conf = {
        "bootstrap.servers": bootstrap_servers,
        "key.serializer": key_serializer,
        "value.serializer": avro_serializer
    }

    return SerializingProducer(producer_conf)

def convert(record):
    record1 = record.copy()
    record1["tpep_pickup_datetime"] = str(record1["tpep_pickup_datetime"])
    record1["tpep_dropoff_datetime"] = str(record1["tpep_dropoff_datetime"])  
    return record1

if __name__ == "__main__":
    
    with open(schema_file_path, "r") as f:
        avro_schema_str = f.read()

    sr_client = get_SR_client(SCHEMA_REGISTRY_URL)
    producer = get_avro_producer(
        schema_registry_client = sr_client,
        avro_schema_str = avro_schema_str,
        bootstrap_servers = BOOTSTRAP_SERVERS 
    )

    try:
        for path in data_file_paths:
            pf = pq.ParquetFile(path)
            for group_id in range(pf.num_row_groups):
                table = pf.read_row_group(group_id)
                for batch in table.to_batches(max_chunksize=5000):
                    records = batch.to_pylist()
                    for record in records:
                        producer.produce(
                            topic = "test",
                            key = "yellow",
                            value = convert(record)
                        )
                        producer.flush()
                        print("Successfull !")
                        time.sleep(1)
                        
    except KeyboardInterrupt:
        print("Exiting...")
        producer.flush()
        print("Exited successfully.")
        exit(0) 