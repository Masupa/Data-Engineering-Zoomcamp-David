from etl_gcs_to_bq import etl
from prefect.deployments import Deployment

deployment = Deployment.build_from_flow(
    flow=etl,
    name="Ingest data from GCS to BigQuery"
)

if __name__ == "__main__":
    deployment.apply()
    