## Module 1 - Docker and Terraform

This module covers containerization with Docker, running PostgreSQL using Docker Compose, basic SQL analytics on NYC Taxi data, and an introduction to Terraform workflows.

### 🛠️ Tech Stack

Docker & Docker Compose

PostgreSQL

Python

SQL

PgAdmin

Terraform

### Docker command for Q1 Module 1
- docker run --rm python:3.13 pip --version

### Data Ingestion

Data ingestion is performed using a Python container that:
- Downloads NYC Taxi data and zones lookup
- Loads it into PostgreSQL using SQLAlchemy

Run ingestion:
```bash
docker compose up --build - build containers 

docker compose up - start containers/services

docker compose down - stop and remove containers
```

### Answers to SQL questions

-- Question 3. For the trips in November 2025, how many trips had a trip_distance of less than or equal to 1 mile?


    SELECT COUNT(*) AS short_trips_count
    FROM green_trips
    WHERE trip_distance <= 1
    AND DATE_TRUNC('month', lpep_pickup_datetime) = '2025-11-01';  

-- Question 4: Which was the pick up day with the longest trip distance? Only consider trips with trip_distance less than 100 miles.


    SELECT DATE_TRUNC('day', lpep_pickup_datetime) AS pickup_day,
        MAX(trip_distance) AS max_trip_distance
    FROM green_trips
    WHERE trip_distance < 100
    GROUP BY pickup_day
    ORDER BY max_trip_distance DESC
    LIMIT 1;

-- Question 5:  Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025? 


    SELECT
        z."Zone" AS pickup_zone,
        SUM(t.total_amount) AS total_revenue
    FROM green_trips t
    JOIN taxi_zones z
        ON t."PULocationID" = z."LocationID"
    WHERE DATE(t.lpep_pickup_datetime) = '2025-11-18'
    GROUP BY z."Zone"
    ORDER BY total_revenue DESC
    LIMIT 1;

--Question 6:  For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?


    SELECT
        dz."Zone" AS dropoff_zone,
        SUM(t.tip_amount) AS total_tip
    FROM green_trips t
    JOIN taxi_zones pu
        ON t."PULocationID" = pu."LocationID"
    JOIN taxi_zones dz
        ON t."DOLocationID" = dz."LocationID"
    WHERE pu."Zone" = 'East Harlem North'
    AND t.lpep_pickup_datetime >= '2025-11-01'
    AND t.lpep_pickup_datetime <  '2025-12-01'
    AND dz."Zone" IN (
        'JFK Airport',
        'Yorkville West',
        'East Harlem North',
        'LaGuardia Airport'
    )
    GROUP BY dz."Zone"
    ORDER BY total_tip DESC;

