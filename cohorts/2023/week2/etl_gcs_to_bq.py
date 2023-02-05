###
# Copyright 2013-2023 AFI, Inc. All Rights Reserved.
###
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"./")
    return Path(gcs_path)
    
@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"Dataframe from {path} has {len(df)} rows")
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write data to BigQuery"""
    
    gcp_credentials_block = GcpCredentials.load("zoom-credentials")
    
    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="silken-quasar-350808",
        if_exists="append",
        chunksize=500_000,
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
    )
    return
    

@flow(log_prints=True)
def etl_gcs_to_bq(color: str, year: int, month: int):
    """Main ETL flow to load data to bigquery"""
    
    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)
    return len(df)
    
@flow()
def parent_etl_gcs_to_bq(
    months: list = [2, 3],
    color: str = "yellow",
    year: int = 2019
    ):
    """Parent flow to load data to bigquery"""
    total_len_df = 0
    for month in months:
        len_df = etl_gcs_to_bq(month=month, color=color, year=year)
        
        total_len_df += len_df
        
    print(f"Total rows loaded: {total_len_df}")
    
    
if __name__ == "__main__":
    parent_etl_gcs_to_bq()
    