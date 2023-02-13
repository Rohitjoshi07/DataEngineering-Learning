from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(log_prints=True)
def fetch_data(url):
    df = pd.read_csv(url)
    
    return df

@task(log_prints=True)
def write_local( df ,filename):
    print(f"filename={filename} Data size: {df.size}")
    path= Path(f"./data/{filename}")
    df.to_csv(path)
    return path


@task(log_prints=True)
def write_gcs(path):
    gcs = GcsBucket.load("gcs-bucket")
    gcs.upload_from_path(from_path=path,
    to_path=path)

    return

@flow(name= "week_4_hw")
def main():
  
    url= "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/"
    for i in range(1,13):
        if i<10:
            filename = f"fhv_tripdata_2019-0{i}.csv.gz"
        else:
            filename = f"fhv_tripdata_2019-{i}.csv.gz"
        df = fetch_data(url+filename)
        path = write_local(df, filename)
        write_gcs(path)

if __name__=="__main__":
    main()
