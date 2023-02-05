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



@task(log_prints=True, retries=3)
def write_bq(df:pd.DataFrame)-> None:
    "write dataframe to bigQuery"
    gcp_credentials_block =  GcpCredentials.load("gcp-creds")

    df.to_gbq(destination_table="zoomcamp.rides",
    project_id="data-engineering-rj",
    credentials=gcp_credentials_block.get_credentials_from_service_account(),
    chunksize=500_000,
    if_exists='append')

@flow(name="gcs_to_bq_hw")
def etl_gcs_to_bq(month, year, color):
    "ETL  sub flow"
    path= extract_from_gcs(color, year, month)
    df = pd.read_parquet(path)
    write_bq(df)

@flow(name="parent_flow_gcs_2_bq")
def parent_flow(months: list, year:int, color: str):
    for month in months:
        etl_gcs_to_bq(month, year, color)

if __name__=="__main__":
    color ="yellow"
    year=2019
    months=[2,3]
    parent_flow(months,year,color)

'''
ans1: 447770
ans2: 14851920
'''
