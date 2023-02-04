#fetch data from web csv and store in data lake(GCS Bucket) after some transformation or cleanup as a parquet file

from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str)-> pd.DataFrame:
    "Read taxi data from web and put into a pandas dataframe"
    
    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df= pd.DataFrame) -> pd.DataFrame:
    "fix some issues in df and do some cleaning"

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2))
    print(f"Columns: {df.dtypes}")
    print(f"Rows: {len(df)}")
    return df


@task(log_prints=True)
def write_local(df: pd.DataFrame, color:str, dataset_file:str)-> Path:
    "write dataframe out locally as parquet file"
    path =Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")    
    return path

@task(log_prints=True)
def write_gcs(path: Path)-> None:
    "write data into google storage from local file"
    
    gcs_block = GcsBucket.load("gcs-bucket")
    gcs_block.upload_from_path(from_path= path,
    to_path=path)

    return 

@flow()
def etl_web_to_gcs(color:str, month:int, year:int) -> None:
    "subflow ETL function"

    dataset_file=f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean= clean(df)
    path = write_local(df_clean, color, dataset_file)
    # write_gcs(path)
    

@flow()
def etl_parent_flow(months: list[int]= [1,2], year:int= 2021, color: str="yellow") -> None:
    for month in months:
        etl_web_to_gcs(color, month, year)



if __name__=="__main__":
    color='yellow'
    year=2021
    months=[1,2,3]
    etl_parent_flow(months, year, color)
