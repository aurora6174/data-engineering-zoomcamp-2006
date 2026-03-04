/* @bruin

# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

# TODO: Set the asset name (recommended: staging.trips).
name: staging.trips
# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: duckdb.sql

# TODO: Declare dependencies so `bruin run ... --downstream` and lineage work.
# Examples:
# depends:
#   - ingestion.trips
#   - ingestion.payment_lookup
depends:
  - ingestion.trips
  - ingestion.payment_lookup

# TODO: Choose time-based incremental processing if the dataset is naturally time-windowed.
# - This module expects you to use `time_interval` to reprocess only the requested window.
materialization:
  # What is materialization?
  # Materialization tells Bruin how to turn your SELECT query into a persisted dataset.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  #
  # Materialization "type":
  # - table: persisted table
  # - view: persisted view (if the platform supports it)
  type: table

# TODO: Add one custom check that validates a staging invariant (uniqueness, ranges, etc.)
# Docs: https://getbruin.com/docs/bruin/quality/custom
custom_checks:
  - name: duplicate_keys
    description: Ensure there are no duplicate trips by staging composite key.
    query: |
      SELECT COUNT(*)
      FROM (
          SELECT
              taxi_type,
              pickup_datetime,
              dropoff_datetime,
              pickup_location_id,
              dropoff_location_id,
              fare_amount
          FROM staging.trips
          GROUP BY 1, 2, 3, 4, 5, 6
          HAVING COUNT(*) > 1
      ) d
    value: 0

@bruin */

WITH filtered_trips AS (
    SELECT
        CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
        CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
        CAST(pu_location_id AS INTEGER) AS pickup_location_id,
        CAST(do_location_id AS INTEGER) AS dropoff_location_id,
        CAST(payment_type AS INTEGER) AS payment_type_id,
        CAST(fare_amount AS DOUBLE) AS fare_amount,
        CAST(total_amount AS DOUBLE) AS total_amount,
        CAST(trip_distance AS DOUBLE) AS trip_distance,
        CAST(passenger_count AS INTEGER) AS passenger_count,
        taxi_type,
        extracted_at
    FROM ingestion.trips
    WHERE CAST(tpep_pickup_datetime AS TIMESTAMP) >= CAST('{{ start_datetime }}' AS TIMESTAMP)
      AND CAST(tpep_pickup_datetime AS TIMESTAMP) < CAST('{{ end_datetime }}' AS TIMESTAMP)
),
deduplicated_trips AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                taxi_type,
                pickup_datetime,
                dropoff_datetime,
                pickup_location_id,
                dropoff_location_id,
                fare_amount
            ORDER BY extracted_at DESC NULLS LAST
        ) AS rn
    FROM filtered_trips
    WHERE pickup_datetime IS NOT NULL
      AND dropoff_datetime IS NOT NULL
      AND pickup_location_id IS NOT NULL
      AND dropoff_location_id IS NOT NULL
      AND fare_amount >= 0
)
SELECT
    d.pickup_datetime,
    d.dropoff_datetime,
    d.pickup_location_id,
    d.dropoff_location_id,
    d.taxi_type,
    d.payment_type_id,
    COALESCE(p.payment_type_name, 'unknown') AS payment_type_name,
    d.passenger_count,
    d.trip_distance,
    d.fare_amount,
    d.total_amount,
    d.extracted_at
FROM deduplicated_trips d
LEFT JOIN ingestion.payment_lookup p
    ON d.payment_type_id = p.payment_type_id
WHERE d.rn = 1
