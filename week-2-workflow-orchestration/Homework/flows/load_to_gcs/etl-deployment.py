from etl_web_to_gcs import etl
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

from datetime import datetime

schedule = CronSchedule(
    cron="0 5 1 * *",
    day_or=True,
    timezone="UTC"
)

deployment = Deployment.build_from_flow(
    flow=etl,
    name="ETL GCS Deployment - Homework",
    schedule=schedule
)

if __name__ == "__main__":
    deployment.apply()