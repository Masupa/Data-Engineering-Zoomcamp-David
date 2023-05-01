# Import libraries
import ssl
import pandas as pd

from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket

from datetime import timedelta

# Disabling SSL Certificate verification on my machine - not related to code
ssl._create_default_https_context = ssl._create_unverified_context

@task(log_prints=True, retries=2, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=2))
def extract_green_taxi_data(data_url: str) -> pd.DataFrame:
    """load green taxi data from url(csv) to pandas DataFrame"""

    # Import CSV
    df_iter = pd.read_csv(data_url, chunksize=100_000, iterator=True)

    # df = None
    # for df_ in df_iter:
    #     if df is None:
    #         df = df_
    #     else:
    #         df = pd.concat([df, df_])

    return next(df_iter)

@task(log_prints=True)
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Perform transformations on the data"""

    # Convert `tpep_pickup_datetime` and `tpep_dropoff_datetime` types to datetime
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    assert pd.api.types.is_datetime64_any_dtype(df['lpep_pickup_datetime'])

    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    assert pd.api.types.is_datetime64_any_dtype(df['lpep_dropoff_datetime'])

    return df


@task(log_prints=True, retries=2, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def load_to_gcs(df: pd.DataFrame, color: str, file_name: str) -> None:
    """load Pandas DataFrame into CSV"""

    # GCS Block
    gcp_block = GcsBucket.load("cs-dtc-practice-block")

    # Upload DataFrame to GCS
    gcp_block.upload_from_dataframe(
        df,
        to_path=f"Data/{color}/{file_name}.parquet",
        serialization_format="parquet_gzip"
    )


@flow(name="Ingest Green Taxi into GCS")
def etl(data_url: str, color: str, file_name: str) -> None:
    """load data into GCS"""
    # Extract
    df = extract_green_taxi_data(data_url)
    # Transform
    df = transform_data(df)
    # Load
    load_to_gcs(df, color=color, file_name=file_name)


if __name__ == "__main__":
    # Data attributes
    color = "green"
    month = 1
    year = 2020

    # Data-file
    file_name = f"{color}_tripdata_{year}-{month:02}"

    # Green taxi URL
    data_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{file_name}.csv.gz"
    
    etl(data_url=data_url, color=color, file_name=file_name)
