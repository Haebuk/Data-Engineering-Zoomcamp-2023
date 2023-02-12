import requests
from pathlib import Path
from datetime import timedelta

import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch_write_local(dataset_url: str, dataset_file: str) -> Path:

    # make path if it doesn't exist
    Path(f"data/").mkdir(parents=True, exist_ok=True)
    
    res = requests.get(dataset_url)
    with open(f"data/{dataset_file}.csv.gz", "wb") as f:
        f.write(res.content)
    

    path = Path(f"data/{dataset_file}.csv.gz")

    return path

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    df['dropOff_datetime'] = pd.to_datetime(df['dropOff_datetime'])
    
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    
    return df


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(
        from_path=f"{path}",
        to_path=path
    )
    return

@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    
    year = 2019
    for month in range(1, 13):
        dataset_file = f"fhv_tripdata_{year}-{month:02}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"
        print(dataset_url)
    
        path = fetch_write_local(dataset_url, dataset_file)
        write_gcs(path)
    
if __name__ == "__main__":
    etl_web_to_gcs()