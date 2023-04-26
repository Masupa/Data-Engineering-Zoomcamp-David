# Import libraries
import os
import pandas as pd
import sqlalchemy as db
from prefect import flow, task
from dotenv import load_dotenv

# Load env var in OS namespace
load_dotenv()


@task(log_prints=True, retries=3)
def ingest_data(host, port, user, password, db_name, table_name):
    """
        Doc String...
    """

    try:
        # Connect to DB
        engine = db.create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db_name}")
    except Exception as err:
        print(err)
        return err
    
    # Import CSV file
    df_iter = pd.read_csv("Data/yellow_tripdata_2021-07.csv.gz", compression="gzip", iterator=True, chunksize=100_000)

    # Iterate through `DataFrames`
    for df in df_iter:
        # Convert `date` dtypes to `datetime`
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

        # Ingest DB to Postgres DB
        df.to_sql(name=table_name, con=engine, index=False, if_exists="append")

        # log results
        print(f"Ingesting a chunk of data ------- Size: {df.shape[0]}")

    print("Finished ingesting data into the postgres database")


@flow(name="Ingest Flow")
def main_flow():
    host = os.getenv("host")
    port = os.getenv("port")
    user = os.getenv("user")
    password = os.getenv("password")
    db_name = os.getenv("db_name")
    table_name = os.getenv("table_name")

    ingest_data(host, port, user, password, db_name, table_name)


if __name__ == "__main__":
    main_flow()
