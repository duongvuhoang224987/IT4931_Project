from dagster import Definitions
from app.assets_k8s import star_schema_data, run_batch_job
from app.jobs import batch_job
from app.schedules import batch_job_schedule

defs = Definitions(
    assets=[star_schema_data, run_batch_job],
    jobs=[batch_job],
    schedules=[batch_job_schedule]
)