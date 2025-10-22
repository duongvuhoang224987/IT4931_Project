from dagster import Definitions
from dagster_batch.jobs import dagster_batch_job
from dagster_batch.schedules import dagster_daily_schedule

defs = Definitions(
    jobs=[dagster_batch_job],
    schedules=[dagster_daily_schedule],
    resources={},
)