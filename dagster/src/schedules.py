from dagster import ScheduleDefinition
from src.jobs import batch_job

batch_job_schedule = ScheduleDefinition(
    job=batch_job,
    cron_schedule="0 3 * * *",  # Run daily at 3 AM
)