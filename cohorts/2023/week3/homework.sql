--- create external TABLE

CREATE
OR
REPLACE EXTERNAL
TABLE
    `dezoomcamp.fhv_tripdata` OPTIONS (
        format = 'CSV',
        uris = ['gs://prefect-de-zoomcamp-kade/data/fhv_tripdata_2019-*.csv.gz']
    );

--- Q1: 43244696
SELECT COUNT(1) FROM `dezoomcamp.fhv_tripdata`;

--- Q2 ex: 0B, bq: 317.94MB
SELECT COUNT(DISTINCT affiliated_base_number) FROM `dezoomcamp.fhv_tripdata`;
SELECT COUNT(DISTINCT affiliated_base_number) FROM `dezoomcamp.fhv_tripdata_in`;

--- Q3 717,748
SELECT COUNT(1) FROM `dezoomcamp.fhv_tripdata_in`
WHERE PUlocationID IS NULL AND DOlocationID IS NULL;

--- Q4 partition by pickup_datetime and cluster by affiliated_base_number
CREATE TABLE IF NOT EXISTS `dezoomcamp.fhv_tripdata_opt`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number
AS
SELECT * FROM `dezoomcamp.fhv_tripdata_in`;

--- Q5 partition: 23.05MB, no partition: 647.87MB
SELECT COUNT(DISTINCT affiliated_base_number) 
FROM `dezoomcamp.fhv_tripdata_opt`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';
SELECT COUNT(DISTINCT affiliated_base_number) 
FROM `dezoomcamp.fhv_tripdata_in`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

--- Q6 GCP Bucket

--- Q7 False

--- Q8