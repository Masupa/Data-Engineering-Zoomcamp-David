import os
import ssl

import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta

# Disabling SSL Certificate verification on my machine - not related to code
ssl._create_default_https_context = ssl._create_unverified_context


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_tripdata_from_web(data_url, color, year, data_file):
    """Given a url with data, function extracts
    the data and stores in locally

    Parameters:
    -----------
    data_url : str
        A url with taxi trips data
    color : str
        Taxi color
    year : int
        The year for the trips data
    data_file : str
        Name of the file

    Returns:
    --------
        None
    """

    # load `parquet` files into pandas
    df = pd.read_parquet(data_url)

    # Create the directory path
    directory = f"data/raw/{color}/{year}"
    
    # Create the directory if it does not exist
    os.makedirs(directory, exist_ok=True)

    # Export `DataFrame` to `parquet`
    local_path = os.path.join(directory, data_file)
    df.to_parquet(path=local_path)


@flow(name="ETL", description="Extract trip data from website and store locally")
def etl(taxi_color: list, years: list, months: list):
    """loop through each year, month and taxi_colors
    and extract the data to the local file.

    Parameters:
    -----------
    taxi_color : list
        A list of taxi color
    years : list
        A list of years; 2021 and 2020
    months : months
        A list of numbers ranging from 1 to 12, representative of months

    Returns:
    --------
        # TODO: Insert return type
    """

    for color in taxi_color:
        for year in years:
            for month in months:
                data_file = f'{color}_tripdata_{year}-{month:02d}.parquet'
                data_url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{data_file}'

                extract_tripdata_from_web(data_url, color, year, data_file)


if __name__ == '__main__':
    years = [2020, 2021]
    months = list(range(1, 13))

    taxi_color = ['yellow', 'green']

    etl(taxi_color=taxi_color, years=years, months=months)
