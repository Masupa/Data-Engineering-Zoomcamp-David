# Import libraries
import pandas_gbq
import pandas as pd
from pathlib import Path

from prefect import flow, task
from prefect_gcp import GcsBucket
from prefect.tasks import task_input_hash
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import BigQueryWarehouse

from datetime import timedelta

@task(log_prints=True, retries=2, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=2))
def extract_from_gcs(color: str, file_name: str) -> None:
    """Extract data from GCS and dumps it on local machine"""

    gcp_block = GcsBucket.load("cs-dtc-practice-block")

    gcp_block.get_directory(
        from_path=f"Data/{color}/{file_name}",
        local_path=f"."
    )

@task(log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def extract_from_local_to_dataframe() -> pd.DataFrame:
    """Load `parquet` file into DataFrame"""
    # Import file
    df = pd.read_parquet("Data/green/green_tripdata_2020-01.gz.parquet")

    return df

@task(log_prints=True)
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Transform and clean the data"""

    # Change date-column dtypes to datetime
    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])

    return df

@task(log_prints=True, retries=2, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def load_to_big_query(df: pd.DataFrame) -> None:
    """load Pandas DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("cs-dtc-practice-credentials")

    pandas_gbq.to_gbq(
        dataframe=df,
        project_id="root-setting-384013",
        destination_table="foo.green_rides",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        if_exists="append"
    )

@flow(name="Ingest data from GCS to BigQuery", log_prints=True)
def etl(color: str, file_name: str, data_url: str) -> None:
    """Extract data from GCS and loads it into BigQuery"""
    # Extract
    extract_from_gcs(color, file_name)
    # Extract local
    df = extract_from_local_to_dataframe()
    # Transform
    df = transform(df)
    # Load to BigQuery
    load_to_big_query(df)


    print(df.shape)



if __name__ == "__main__":
    # Data attributes
    color = "green"
    month = 1
    year = 2020

    file_name = f"{color}_tripdata_{year}-{month:02}"

    data_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{file_name}.csv.gz"

    etl(color, file_name, data_url)
