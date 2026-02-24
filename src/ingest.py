import pandas as pd
import psycopg


def main():
    parquet_path = "data/raw/yellow_tripdata_2024-01.parquet"
    conn_str = "postgresql://taxi:taxi@localhost:5433/taxi_dw"

    # Read parquet
    df = pd.read_parquet(parquet_path)

    # Rename columns to match your table (NYC files use VendorID, etc.)
    rename_map = {
        "VendorID": "vendor_id",
        "tpep_pickup_datetime": "tpep_pickup_datetime",
        "tpep_dropoff_datetime": "tpep_dropoff_datetime",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance",
        "RatecodeID": "ratecode_id",
        "store_and_fwd_flag": "store_and_fwd_flag",
        "PULocationID": "pu_location_id",
        "DOLocationID": "do_location_id",
        "payment_type": "payment_type",
        "fare_amount": "fare_amount",
        "extra": "extra",
        "mta_tax": "mta_tax",
        "tip_amount": "tip_amount",
        "tolls_amount": "tolls_amount",
        "improvement_surcharge": "improvement_surcharge",
        "total_amount": "total_amount",
        "congestion_surcharge": "congestion_surcharge",
        "Airport_fee": "airport_fee",
        "airport_fee": "airport_fee",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Keep only columns that exist in your bronze table (and in the right order)
    cols = [
        "vendor_id",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "ratecode_id",
        "store_and_fwd_flag",
        "pu_location_id",
        "do_location_id",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee",
    ]
    df = df[[c for c in cols if c in df.columns]].copy()

    # Convert NaNs to None so psycopg can insert NULLs
    df = df.where(pd.notnull(df), None)

    insert_sql = """
        INSERT INTO bronze.yellow_tripdata_2024_01 (
            vendor_id,
            tpep_pickup_datetime,
            tpep_dropoff_datetime,
            passenger_count,
            trip_distance,
            ratecode_id,
            store_and_fwd_flag,
            pu_location_id,
            do_location_id,
            payment_type,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            total_amount,
            congestion_surcharge,
            airport_fee
        ) VALUES (
            %(vendor_id)s,
            %(tpep_pickup_datetime)s,
            %(tpep_dropoff_datetime)s,
            %(passenger_count)s,
            %(trip_distance)s,
            %(ratecode_id)s,
            %(store_and_fwd_flag)s,
            %(pu_location_id)s,
            %(do_location_id)s,
            %(payment_type)s,
            %(fare_amount)s,
            %(extra)s,
            %(mta_tax)s,
            %(tip_amount)s,
            %(tolls_amount)s,
            %(improvement_surcharge)s,
            %(total_amount)s,
            %(congestion_surcharge)s,
            %(airport_fee)s
        );
    """

    rows = df.to_dict(orient="records")

    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            # Insert in chunks to avoid huge single transactions
            chunk_size = 5000
            for i in range(0, len(rows), chunk_size):
                chunk = rows[i : i + chunk_size]
                cur.executemany(insert_sql, chunk)
                conn.commit()
                print(f"Inserted {min(i + chunk_size, len(rows)):,} / {len(rows):,}")

    print("Done.")


if __name__ == "__main__":
    main()
