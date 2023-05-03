# Import libraries
import ssl
import pandas as pd
from prefect import task, flow
from prefect_gcp import GcsBucket
from prefect.tasks import task_input_hash

from datetime import timedelta

# Disabling SSL Certificate verification on my machine - not related to code
ssl._create_default_https_context = ssl._create_unverified_context


@task(log_prints=True, retries=2, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=2))
def extract_data_from_web(taxi_data_url: str) -> pd.DataFrame:
    """Extract data from URL and store into Pandas DataFrame"""

    # load into Pandas iterator
    df_iter = pd.read_csv(taxi_data_url, iterator=True, chunksize=100_000)

    df = None
    # loop through each DataFrame in iterator
    for df_ in df_iter:
        if df is None:
            df = df_
        else:
            df = pd.concat([df, df_])

    return df

@task(log_prints=True)
def clean_data(df: str) -> pd.DataFrame:
    """Convert date column data-types to datetime format"""

    # Convert `tpep_pickup_datetime` and `tpep_dropoff_datetime` types to datetime
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    assert pd.api.types.is_datetime64_any_dtype(df['tpep_pickup_datetime'])

    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    assert pd.api.types.is_datetime64_any_dtype(df['tpep_dropoff_datetime'])

    return df

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=2))
def load_to_gcs(df: pd.DataFrame, file_name: str, color: str) -> None:
    """load DataFrame to GCS"""

    gcp_block = GcsBucket.load("cs-dtc-practice-block")
    gcp_block.upload_from_dataframe(
        df,
        to_path=f"Data/{color}/{file_name}.csv",
        serialization_format="csv_gzip"
    )



@flow(log_prints=True)
def etl(taxi_data_url: str, file_name: str, color: str) -> None:
    """Extract taxi data, transform, and load into GCS"""

    # Extract
    df = extract_data_from_web(taxi_data_url)
    # Transform
    df = clean_data(df)
    # Load
    load_to_gcs(df, file_name, color)


if __name__ == "__main__":
    # data attributes
    year = 2020
    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    colors = ["yellow", "green"]

    for color in colors:
        for month in months:
            file_name = f"{color}_tripdata_{year}-{month:02}"
            taxi_data_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{file_name}.csv.gz"
        
            etl(taxi_data_url, file_name, color)
