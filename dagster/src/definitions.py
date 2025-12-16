from dagster import Definitions
from src.assets import clickhouse_schema, star_schema_data, run_batch_job
from src.jobs import create_star_schema_job, batch_job
from src.schedules import batch_job_schedule

defs = Definitions(
    assets=[clickhouse_schema, star_schema_data, run_batch_job],
    jobs=[create_star_schema_job, batch_job],
    schedules=[batch_job_schedule]
)