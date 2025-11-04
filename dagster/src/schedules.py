from dagster import ScheduleDefinition
from src.jobs import dagster_batch_job

dagster_daily_schedule = ScheduleDefinition(
    job=dagster_batch_job,
    cron_schedule="*/1 * * * *",  # Run every 1 minutes
)