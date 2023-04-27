# Import libraries
import os
import pandas as pd
import sqlalchemy as db

from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector

from dotenv import load_dotenv

from datetime import timedelta

# Load env var in OS namespace
load_dotenv()


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data():
    """
        Doc String...
    """
    # Import CSV file
    df_iter = pd.read_csv("Data/yellow_tripdata_2021-07.csv.gz", compression="gzip", iterator=True, chunksize=100_000)

    df = None

    # loop through chunks in `df_iter`
    for df_ in df_iter:
        if df is None:
            df = df_
        else:
            df = pd.concat([df, df_])

    return df


@task(log_prints=True)
def transform_data(df):
    """
        Doc String
    """

    # Remove rows with `Passenger count` at zero
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df["passenger_count"] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")

    # Convert `date` variables dtype to datetime
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    return df


@task(log_prints=True, retries=3)
def ingest_data(table_name, df):
    """
        Doc String...
    """

    # Connect to Postgres DB
    with SqlAlchemyConnector.load("postgres-connector") as database_block:
        engine = database_block.get_connection(begin=False)
        # Ingest DB to Postgres DB
        df.to_sql(name=table_name, con=engine, index=False, if_exists="append")

    print(f"Finished ingesting data into the postgres database: Size={df.shape[0]}")


@flow(name="Ingest Flow")
def main_flow(table_name: str):
    table_name = os.getenv("table_name")

    # Extract
    raw_data = extract_data()
    print(type(raw_data), raw_data.shape)
    # Transform
    data = transform_data(raw_data)
    # Load
    ingest_data(table_name, data)


if __name__ == "__main__":
    main_flow(table_name="yellow_taxi_trips")
