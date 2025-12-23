from dagster import define_asset_job, AssetSelection

from app.assets_k8s import star_schema_data, run_batch_job

# Define a job that materializes the star schema ETL assets
batch_job = define_asset_job(
    name="batch_job",
    selection=AssetSelection.assets(star_schema_data, run_batch_job),
    description="Job to run the batch job on Spark cluster"
)
