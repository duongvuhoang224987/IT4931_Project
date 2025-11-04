from dagster import Definitions
from src.jobs import dagster_batch_job
from src.schedules import dagster_daily_schedule

defs = Definitions(
    jobs=[dagster_batch_job],
    schedules=[dagster_daily_schedule],
    resources={},
)