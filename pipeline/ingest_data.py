#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import click
from sqlalchemy import create_engine
from tqdm.auto import tqdm

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='yellow_taxi_data_2', help='Target table name')
@click.option('--year', default=2025, type=int, help='Year of data to ingest')
@click.option('--month', default=11, type=int, help='Month of data to ingest')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, year, month):

    prefix = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    url = f"{prefix}/yellow_tripdata_{year:04d}-{month:02d}.parquet"

    engine = create_engine(f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}")

    print(url)

    # Read parquet (no chunks)
    df = pd.read_parquet(url)

    # Optional: ensure datetime types (usually already correct in parquet)
    for col in ["tpep_pickup_datetime", "tpep_dropoff_datetime"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Write in batches
    batch_size = 100_000
    df.head(0).to_sql(name=target_table, con=engine, if_exists="replace", index=False)

    for start in tqdm(range(0, len(df), batch_size)):
        chunk = df.iloc[start:start + batch_size]
        chunk.to_sql(name=target_table, con=engine, if_exists="append", index=False, method="multi")

if __name__ == "__main__":
    run()
