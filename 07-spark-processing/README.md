# Spark Batch Processing – Data Engineering Zoomcamp (Module 6)

This repository contains my solution for **Module 6: Batch Processing with Apache Spark** from the **Data Engineering Zoomcamp 2026** by DataTalksClub.

The goal of this homework was to practice using **PySpark for distributed batch processing**, working with a large NYC taxi dataset and performing transformations, aggregations, and joins.

---

# Technologies Used

* Python 3.12
* Apache Spark 4.1.1
* PySpark
* WSL2 (Ubuntu)
* Parquet
* Git / GitHub

---

# Environment Setup

The environment was configured inside **WSL2 (Ubuntu)**.

### Install Java (required by Spark)

```bash
sudo apt update
sudo apt install openjdk-17-jdk -y
```

Verify installation:

```bash
java -version
```

---

### Create Python Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate
```

---

### Install PySpark

```bash
pip install pyspark
```

Verify installation:

```python
import pyspark
pyspark.__version__
```

Start Spark:

```bash
pyspark
```

Spark UI runs on:

```
http://localhost:4040
```

---

# Dataset

The dataset used is the **NYC Yellow Taxi Trip Data (November 2025)**.

Download the dataset:

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet
```

Taxi zone lookup table:

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

---

# Question 1 – Spark Installation

Spark was started using PySpark and the version was verified:

```python
spark.version
```

Result:

```
4.1.1
```

---

# Question 2 – Repartition Dataset

The parquet file was loaded into a Spark DataFrame.

```python
df = spark.read.parquet("yellow_tripdata_2025-11.parquet")
```

The dataset was repartitioned into **4 partitions**:

```python
df = df.repartition(4)
```

The DataFrame was written back to parquet:

```python
df.write.parquet("yellow_tripdata_2025-11_repartitioned")
```

Checking the output files:

```bash
ls -lh yellow_tripdata_2025-11_repartitioned
```

Result:

```
4 parquet files (~25MB each)
```

Average file size:

```
25MB
```

---

# Question 3 – Trips on November 15

Filter trips that started on **2025-11-15**.

```python
from pyspark.sql import functions as F

df.filter(
    F.to_date("tpep_pickup_datetime") == "2025-11-15"
).count()
```

Result:

```
162,604 trips
```

---

# Question 4 – Longest Trip

Trip duration was calculated using pickup and dropoff timestamps.

```python
df_with_duration = df.withColumn(
    "trip_hours",
    (F.unix_timestamp("tpep_dropoff_datetime") -
     F.unix_timestamp("tpep_pickup_datetime")) / 3600
)

df_with_duration.orderBy(F.desc("trip_hours")).show(1)
```

Result:

```
90.6 hours
```

---

# Question 5 – Spark UI

Spark provides a web interface for monitoring jobs.

Default port:

```
http://localhost:4040
```

---

# Question 6 – Least Frequent Pickup Zone

The taxi zone lookup dataset was loaded:

```python
zones = spark.read.csv(
    "taxi_zone_lookup.csv",
    header=True,
    inferSchema=True
)
```

Taxi data was joined with the zone lookup table:

```python
df_join = df.join(
    zones,
    df.PULocationID == zones.LocationID
)
```

Trips were grouped by pickup zone:

```python
zone_counts = df_join.groupBy("Zone").count()

zone_counts.orderBy("count").show(1)
```

Example result:

```
Eltingville/Annadale/Prince's Bay | 1
```

Multiple zones share the same minimum pickup count. One valid answer from the homework options is:

```
Governor's Island/Ellis Island/Liberty Island
```

---

# Final Answers

| Question | Answer                                        |
| -------- | --------------------------------------------- |
| Q1       | Spark version 4.1.1                           |
| Q2       | 25MB                                          |
| Q3       | 162,604                                       |
| Q4       | 90.6                                          |
| Q5       | 4040                                          |
| Q6       | Governor's Island/Ellis Island/Liberty Island |

---

# Key Concepts Learned

* Setting up **Apache Spark with PySpark**
* Reading and writing **Parquet files**
* **Repartitioning** datasets for distributed processing
* Filtering and aggregating large datasets
* Calculating derived metrics
* Performing **joins in Spark**
* Using the **Spark Web UI for monitoring**

---

# Learning in Public

This project is part of the **Data Engineering Zoomcamp** by DataTalksClub.

Course repository:

https://github.com/DataTalksClub/data-engineering-zoomcamp

---

# Author

Joe
Data Engineering Zoomcamp 2026
