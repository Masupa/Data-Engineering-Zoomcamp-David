# Import libraries
import os
import ssl
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

from dotenv import load_dotenv

# load env var
load_dotenv()

from datetime import timedelta

# Disabling SSL Certificate verification on my machine - not related to code
ssl._create_default_https_context = ssl._create_unverified_context


@task(log_prints=True, retries=3)
def extract_from_gcs(color: str, dataset_file: str) -> Path:
    """Download trip data from GCS"""
    # load GCS Bucket
    gcp_cloud_storage_bucket_block = GcsBucket.load("zoom-gcs")

    # Dataset file-path in GCS
    gcs_path = f"Data/{color}/{dataset_file}.parquet"

    # Get dataset from GCS to local directory
    gcp_cloud_storage_bucket_block.get_directory(
        from_path=gcs_path,
        local_path=f"../"
    )

    # Return
    return Path(f"../{gcs_path}")


@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    # load `parquet` file
    df = pd.read_parquet(path)

    # Print number of missing passenger count
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")

    # Fill NaN values with `0`
    df["passenger_count"].fillna(0, inplace=True)

    # Print number of missing passender count
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task(log_prints=True)
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""

    # load credentials
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table=os.getenv("destination_table"),
        project_id=os.getenv("project_id"),
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=100_000,
        if_exists="append"
    )


@flow(name="Ingest to Big Query")
def etl_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""

    color = "yellow"
    year = 2021
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"

    # Extract
    path = extract_from_gcs(color, dataset_file)

    # Transform data
    df = transform(path)

    # load
    write_bq(df)

if __name__ == "__main__":
    etl_gcs_to_bq()
