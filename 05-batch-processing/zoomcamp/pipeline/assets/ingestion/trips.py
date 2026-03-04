"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# TODO: Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# TODO: Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.12

# TODO: Set the connection.
connection: duckdb-default

# TODO: Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  # TODO: pick a strategy.
  # suggested strategy: append
  strategy: append

# TODO: Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
columns:
  - name: TODO_col1
    type: TODO_type
    description: TODO

@bruin"""

import json
import os
from datetime import datetime, timezone
from io import BytesIO

import pandas as pd
import requests


def materialize():
    start_date = pd.to_datetime(os.environ["BRUIN_START_DATE"])
    end_date = pd.to_datetime(os.environ["BRUIN_END_DATE"])
    if end_date <= start_date:
        return pd.DataFrame()

    bruin_vars = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    taxi_types = bruin_vars.get("taxi_types", ["yellow"])
    if isinstance(taxi_types, str):
        taxi_types = [taxi_types]
    taxi_types = [str(t).strip().lower() for t in taxi_types if str(t).strip()]
    if not taxi_types:
        taxi_types = ["yellow"]

    start_month = start_date.to_period("M")
    end_month = (end_date - pd.Timedelta(days=1)).to_period("M")
    months = pd.period_range(start_month, end_month, freq="M")

    extracted_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    dataframes = []

    with requests.Session() as session:
        for month in months:
            month_str = f"{month.year}-{month.month:02d}"
            for taxi_type in taxi_types:
                url = f"{base_url}/{taxi_type}_tripdata_{month_str}.parquet"
                response = session.get(url, timeout=60)
                if response.status_code == 404:
                    continue
                response.raise_for_status()

                df = pd.read_parquet(BytesIO(response.content))
                df["taxi_type"] = taxi_type
                df["extracted_at"] = extracted_at
                dataframes.append(df)

    if not dataframes:
        return pd.DataFrame()

    return pd.concat(dataframes, ignore_index=True, copy=False)

