from dagster import define_asset_job, AssetSelection

from src.assets import clickhouse_schema, star_schema_data, run_batch_job

# Define a job that materializes the star schema ETL assets
create_star_schema_job = define_asset_job(
    name="create_star_schema_job",
    selection=AssetSelection.assets(clickhouse_schema),
    description="Job to create the star schema in ClickHouse"
)

batch_job = define_asset_job(
    name="batch_job",
    selection=AssetSelection.assets(star_schema_data, run_batch_job),
    description="Job to run the batch job on Spark cluster"
)
