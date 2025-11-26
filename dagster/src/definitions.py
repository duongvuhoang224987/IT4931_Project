from dagster import Definitions
from src.jobs import star_schema_etl_job
from src.schedules import star_schema_schedule

defs = Definitions(
    jobs=[star_schema_etl_job],
    schedules=[star_schema_schedule],
    resources={},
)