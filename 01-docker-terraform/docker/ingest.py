import os
import pandas as pd
from sqlalchemy import create_engine


def get_env(var_name: str) -> str:
    """Fail fast if an environment variable is missing"""
    value = os.getenv(var_name)
    if value is None:
        raise ValueError(f"Missing required environment variable: {var_name}")
    return value


def create_pg_engine():
    user = get_env("POSTGRES_USER")
    password = get_env("POSTGRES_PASSWORD")
    host = get_env("POSTGRES_HOST")
    port = get_env("POSTGRES_PORT")
    db = get_env("POSTGRES_DB")

    return create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/{db}"
    )


def read_data(url: str) -> pd.DataFrame:
    if url.endswith(".parquet"):
        return pd.read_parquet(url)
    elif url.endswith(".csv"):
        return pd.read_csv(url)
    else:
        raise ValueError(f"Unsupported file format: {url}")


def ingest_dataset(engine, url: str, table_name: str):
    print(f"Starting ingestion for table '{table_name}'")
    print(f"Source: {url}")

    df = read_data(url)

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False
    )

    print(f"Finished ingestion for '{table_name}' ({len(df)} rows)")


def main():
    print("Starting ingestion job")

    engine = create_pg_engine()

    # Green taxi trips (Nov 2025)
    ingest_dataset(
        engine=engine,
        url=get_env("GREEN_DATA_URL"),
        table_name=get_env("GREEN_TABLE_NAME"),
    )

    # Taxi zones lookup
    ingest_dataset(
        engine=engine,
        url=get_env("ZONES_DATA_URL"),
        table_name=get_env("ZONES_TABLE_NAME"),
    )

    print("All ingestions completed successfully")


if __name__ == "__main__":
    main()
