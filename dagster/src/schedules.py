from dagster import ScheduleDefinition
from src.jobs import star_schema_etl_job

# dagster_daily_schedule = ScheduleDefinition(
#     job=dagster_batch_job,
#     cron_schedule="*/1 * * * *",  # Run every 1 minutes
# )

star_schema_schedule = ScheduleDefinition(
    job=star_schema_etl_job,
    cron_schedule="0 2 * * *",  # Run daily at 2 AM
)