import os
import requests
import pandas as pd
import click
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
@click.option("--pg-user", default="root")
@click.option("--pg-pass", default="root")
@click.option("--pg-host", default="localhost")
@click.option("--pg-port", default=5432, type=int)
@click.option("--pg-db", default="ny_taxi")
@click.option("--target-table", default="yellow_taxi_data")
@click.option("--year", default=2025, type=int)
@click.option("--month", default=11, type=int)
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, year, month):
    batch_rows = 100_000
    prefix = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    url = f"{prefix}yellow_tripdata_{year}-{month:02d}.parquet"
    local_filename = "temp_data.parquet"

    # 1. Download the file once
    print(f"Downloading {url}...")
    try:
        response = requests.get(url)
        response.raise_for_status()
        with open(local_filename, "wb") as f:
            f.write(response.content)
        
        # 2. Setup Database Connection
        engine = create_engine(f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}")

        # 3. Initialize PyArrow Dataset from local file
        dataset = ds.dataset(local_filename, format="parquet")
        scanner = dataset.scan(batch_size=batch_rows)

        first = True
        
        # 4. Process and Ingest
        for record_batch in tqdm(scanner.to_batches(), desc="Ingesting Data"):
            df_chunk = record_batch.to_pandas()

            for col in parse_dates:
                if col in df_chunk.columns:
                    df_chunk[col] = pd.to_datetime(df_chunk[col], errors="coerce")

            df_chunk = df_chunk.astype({k: v for k, v in dtype.items() if k in df_chunk.columns})

            if first:
                df_chunk.head(0).to_sql(name=target_table, con=engine, if_exists="replace", index=False)
                print(f"Table '{target_table}' created.")
                first = False

            df_chunk.to_sql(
                name=target_table, 
                con=engine, 
                if_exists="append", 
                index=False, 
                method="multi", 
                chunksize=10_000
            )

    finally:
        # 5. Clean up the file even if the script crashes
        if os.path.exists(local_filename):
            os.remove(local_filename)
            print("Cleaned up temporary file.")

if __name__ == "__main__":
    run()