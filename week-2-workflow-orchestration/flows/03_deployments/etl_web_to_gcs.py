# Import libraries
import os
import ssl
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket

from datetime import timedelta

# Disabling SSL Certificate verification on my machine - not related to code
ssl._create_default_https_context = ssl._create_unverified_context


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read data from web into pandas DataFrame"""

    df_iter = pd.read_csv(dataset_url, iterator=True, chunksize=100_000)

    df = None
    for df_ in df_iter:
        if df is None:
            df = df_
        else:
            df = pd.concat([df, df_])

    return df

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")

    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> os.path:
    """Write DataFrame out as a parquet file"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(current_dir, '..', '..', 'Data', f'{color}')
    file_path = os.path.join(data_dir, f"{dataset_file}.parquet")
    df.to_parquet(file_path, compression="gzip")
    return file_path

@task()
def write_gcs(path: Path, color: str, dataset_file: str) -> None:
    """Upload local parquet to GCS"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("zoom-gcs")
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path=f"{path}",
        to_path=Path(f"Data/{color}/{dataset_file}.parquet")
    )
    return


@flow(name="Ingest Flow")
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    # Extract
    df = fetch(dataset_url)
    # Clean
    df_clean = clean(df=df)
    # Extract to local
    path = write_local(df_clean, color, dataset_file)
    # Write to GCS
    write_gcs(path, color, dataset_file)


@flow(name="Parent Flow")
def etl_parent_flow(year: int = 1, months: list[int] = [1, 2], color: str = "yellow") -> None:
    """Run `sub-flow` for multiple months"""
    # 
    for month in months:
        etl_web_to_gcs(year, month, color)


if __name__ == "__main__":
    color = "yellow"
    months = [1, 2, 3]
    year = 2021

    etl_parent_flow(year, months, color)
