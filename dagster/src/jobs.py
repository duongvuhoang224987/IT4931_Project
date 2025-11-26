from dagster import job
from src.ops import run_star_schema_transform

# @job
# def dagster_batch_job():
#     parquet_path = load_parquet()
#     transformed = transform_data(parquet_path)

@job
def star_schema_etl_job():
    """
    Job to run the star schema transformation on Spark cluster.
    """
    run_star_schema_transform()
