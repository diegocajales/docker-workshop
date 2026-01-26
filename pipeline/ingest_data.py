#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

import pyarrow.dataset as ds

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
}

parse_dates = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]


@click.command()
@click.option("--pg-user", default="root", help="PostgreSQL user")
@click.option("--pg-pass", default="root", help="PostgreSQL password")
@click.option("--pg-host", default="localhost", help="PostgreSQL host")
@click.option("--pg-port", default=5432, type=int, help="PostgreSQL port")
@click.option("--pg-db", default="ny_taxi", help="PostgreSQL database name")
@click.option("--target-table", default="yellow_taxi_data", help="Target table name")
@click.option("--year", default=2025, type=int, help="Year of data to ingest")
@click.option("--month", default=11, type=int, help="Month of data to ingest")

def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, year, month):
    batch_rows = 100_000

    prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
    url = f"{prefix}yellow_tripdata_{year}-{month:02d}.parquet"

    engine = create_engine(f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}")

    # Stream parquet in record batches
    dataset = ds.dataset(url, format="parquet")
    scanner = dataset.scan(batch_size=batch_rows)

    first = True
    for record_batch in tqdm(scanner.to_batches(), desc="Loading batches"):
        df_chunk = record_batch.to_pandas()

        # enforce dtypes + datetime
        for col in parse_dates:
            if col in df_chunk.columns:
                df_chunk[col] = pd.to_datetime(df_chunk[col], errors="coerce")

        df_chunk = df_chunk.astype({k: v for k, v in dtype.items() if k in df_chunk.columns})

        if first:
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists="replace",
                index=False,
            )
            first = False

        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append",
            index=False,
            method="multi",      
            chunksize=10_000,   
        )

if __name__ == "__main__":
    run()
