from dagster import job
from src.ops import load_parquet, transform_data

@job
def dagster_batch_job():
    parquet_path = load_parquet()
    transformed = transform_data(parquet_path)
