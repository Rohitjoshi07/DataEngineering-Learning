from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

''' write data from bucket to bigquery'''

@task(log_prints=True, retries=3)
def extract_from_gcs(color:str, year: int, month: int)-> Path:
    "Download data from GCS"

    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("gcs-bucket")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"./data/")
    return Path(f"./{gcs_path}")


@task(log_prints=True)
def transform(path: Path)-> pd.DataFrame:
    "data cleaning"
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task(log_prints=True, retries=3)
def write_bq(df:pd.DataFrame)-> None:
    "write dataframe to bigQuery"
    gcp_credentials_block =  GcpCredentials.load("gcp-creds")


    df.to_gbq(destination_table="zoomcamp.trips",
    project_id="data-engineering-rj",
    credentials=gcp_credentials_block.get_credentials_from_service_account(),
    chunksize=500_000,
    if_exists='append')

@flow(name="gcs_to_bq")
def etl_gcs_to_bq():
    "Main ETL flow"
    color ="yellow"
    year=2021
    month=1

    path= extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)

if __name__=="__main__":
    etl_gcs_to_bq()
