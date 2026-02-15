## Project Summary
In this project, I replicated the BigQuery + GCS homework from the Data Engineering Zoomcamp using AWS.

Instead of GCP services, I used:

Amazon S3 – data storage

AWS Glue – metadata catalog

Amazon Athena – serverless SQL engine

Terraform – infrastructure provisioning

The dataset used was the NYC Yellow Taxi Trip Records (January–June 2024).

Total records: 20,332,093

## Architecture

Local Machine (WSL2)
        ↓
Terraform → S3 bucket
        ↓
Upload Parquet files
        ↓
Athena External Table
        ↓
Materialized Table (CTAS)
        ↓
Partitioned + Bucketed Table

## Step 1 - Infrastructure (Terraform)
Created S3 bucket using Terraform
```bash
resource "aws_s3_bucket" "taxi_data" {
  bucket = "zoomcamp-yellow-taxi-<random-suffix>"
    }
```
- Applied with 
```bash
terraform init
terraform plan
terraform apply
```

## Step 2 — Upload Data to S3

Downloaded Yellow Taxi Parquet files (Jan - June 2024) and uploaded to 

```bash
s3://zoomcamp-yellow-taxi-<bucket>/yellow/
```

and verified using
```bash
aws s3 ls s3://zoomcamp-yellow-taxi-<bucket>/yellow/
```

## Step 3 - Create Athena Database

```bash
CREATE DATABASE zoomcamp_dwh;
```

Configured Athena query result location

```bash
s3://zoomcamp-yellow-taxi-<bucket>/athena-results/
```

## Step 4 - External Table

```bash
CREATE EXTERNAL TABLE yellow_taxi_external (
    VendorID BIGINT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count BIGINT,
    trip_distance DOUBLE,
    RatecodeID BIGINT,
    store_and_fwd_flag STRING,
    PULocationID BIGINT,
    DOLocationID BIGINT,
    payment_type BIGINT,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE,
    airport_fee DOUBLE
)
STORED AS PARQUET
LOCATION 's3://zoomcamp-yellow-taxi-<bucket>/yellow/';
```

## Answers to questions:

- Question 1 - Count records

```bash
SELECT COUNT(*) FROM yellow_taxi_external;
```
Ans: 20,332,093

## Step 5 - Materialized Table (CTAS)
```bash
CREATE TABLE yellow_taxi_materialized
WITH (
    format = 'PARQUET',
    external_location = 's3://zoomcamp-yellow-taxi-<bucket>/materialized/',
    write_compression = 'SNAPPY'
) AS
SELECT *
FROM yellow_taxi_external;
```

- Question 2 - Data Read Estimation

```bash
SELECT COUNT(DISTINCT PULocationID)
FROM yellow_taxi_external;
```
Data Scanned: 13.91MB (External Table)

```bash
SELECT COUNT(DISTINCT PULocationID)
FROM yellow_taxi_materialized;
```
Data Scanned: 20.79MB

- Observation:
Athena scans only the required column because it is columnar.


- Question 3 - Columnar Storage
- Single Column
```bash
SELECT PULocationID
FROM yellow_taxi_materialized;
```
Data Scanned: 20.79  

- Two Columns 
```bash
SELECT PULocationID, DOLocationID
FROM yellow_taxi_materialized;
```
Data Scanned: 42.52MB

- Question 4 - Zero Fare Trips
```bash
SELECT COUNT(*)
FROM yellow_taxi_materialized
WHERE fare_amount = 0;
```
Answer: 8,333
Data Scanned: 30.72

## Step 6 - Partitioned and Bucketed Tables
```bash
CREATE TABLE yellow_taxi_partitioned
WITH (
    format = 'PARQUET',
    external_location = 's3://zoomcamp-yellow-taxi-<bucket>/partitioned/',
    partitioned_by = ARRAY['dropoff_date'],
    bucketed_by = ARRAY['VendorID'],
    bucket_count = 4
) AS
SELECT *,
       DATE(tpep_dropoff_datetime) AS dropoff_date
FROM yellow_taxi_external;
```

- Question 6
Non-Partitioned
```bash
SELECT DISTINCT VendorID
FROM yellow_taxi_materialized
WHERE tpep_dropoff_datetime BETWEEN TIMESTAMP '2024-03-01 00:00:00'
AND TIMESTAMP '2024-03-15 23:59:59';
```
Data Scanned: 121.09MB

Partitioned
```bash
SELECT DISTINCT VendorID
FROM yellow_taxi_partitioned
WHERE dropoff_date BETWEEN DATE '2024-03-01'
AND DATE '2024-03-15';
```
- Observation:
  Partition pruning dramatically reduces scanned data.

- Question 7:
External Table Data is stored in:
Amazon S3 or BigQuery for GCP

- Question 8:
Is it best practice to always cluster?

Answer: False

Clustering should only be used when frequently filtering or grouping by that column.


## Clean Up

### Step 1 - Drop Athena Tables

- In Athena
```bash        
USE zoomcamp_dwh;
```

- Drop Tables
```bash 
DROP TABLE yellow_taxi_partitioned;
DROP TABLE yellow_taxi_materialized;
DROP TABLE yellow_taxi_external;
```

- Drop Database
```bash
DROP DATABASE zoomcamp_dwh;
```

### Step 2 - Empty the S3 Bucket

```bash
aws s3 rm s3://zoomcamp-yellow-taxi-dfe9a979 --recursive
```

- Verify its empty
```bash
aws s3 ls s3://zoomcamp-yellow-taxi-dfe9a979
```

### Run Terraform Destroy
- Navigate to the appropriate folder 
```bash
terraform destroy
```

- Answer "yes" to prompt after reviewing changes to be made

- Verify on AWS Console that bucket has been removed 






