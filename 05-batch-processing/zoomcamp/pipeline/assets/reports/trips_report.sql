/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

# TODO: Set the asset name (recommended: reports.trips_report).
name: reports.trips_report

# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: duckdb.sql

# TODO: Declare dependency on the staging asset(s) this report reads from.
depends:
  - staging.trips

# TODO: Choose materialization strategy.
# For reports, `time_interval` is a good choice to rebuild only the relevant time window.
# Important: Use the same `incremental_key` as staging (e.g., pickup_datetime) for consistency.
materialization:
  type: table

columns:
  - name: pickup_date
    type: DATE
    description: Trip pickup date
    primary_key: true
  - name: taxi_type
    type: VARCHAR
    description: Taxi service type (e.g., yellow, green)
    primary_key: true
  - name: payment_type_name
    type: VARCHAR
    description: Human-readable payment type
    primary_key: true
  - name: trip_count
    type: BIGINT
    description: Number of trips in the aggregation grain
    checks:
      - name: non_negative
  - name: total_fare_amount
    type: DOUBLE
    description: Sum of fare amount in the aggregation grain
    checks:
      - name: non_negative
  - name: total_amount
    type: DOUBLE
    description: Sum of total trip amount in the aggregation grain
    checks:
      - name: non_negative
  - name: total_trip_distance
    type: DOUBLE
    description: Sum of trip distance in the aggregation grain
    checks:
      - name: non_negative

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

WITH filtered_trips AS (
    SELECT
        CAST(pickup_datetime AS DATE) AS pickup_date,
        taxi_type,
        COALESCE(payment_type_name, 'unknown') AS payment_type_name,
        fare_amount,
        total_amount,
        trip_distance
    FROM staging.trips
    WHERE pickup_datetime >= CAST('{{ start_datetime }}' AS TIMESTAMP)
      AND pickup_datetime < CAST('{{ end_datetime }}' AS TIMESTAMP)
)
SELECT
    pickup_date,
    taxi_type,
    payment_type_name,
    COUNT(*) AS trip_count,
    COALESCE(SUM(fare_amount), 0) AS total_fare_amount,
    COALESCE(SUM(total_amount), 0) AS total_amount,
    COALESCE(SUM(trip_distance), 0) AS total_trip_distance
FROM filtered_trips
GROUP BY 1, 2, 3
