DROP TABLE IF EXISTS raw.yellow_tripdata;

CREATE TABLE raw.yellow_tripdata (
    vendorid                INTEGER,
    tpep_pickup_datetime    TIMESTAMP,
    tpep_dropoff_datetime   TIMESTAMP,
    passenger_count         DOUBLE PRECISION,
    trip_distance           DOUBLE PRECISION,
    ratecodeid               DOUBLE PRECISION,
    store_and_fwd_flag      TEXT,
    pulocationid             INTEGER,
    dolocationid             INTEGER,
    payment_type             INTEGER,
    fare_amount              DOUBLE PRECISION,
    extra                    DOUBLE PRECISION,
    mta_tax                  DOUBLE PRECISION,
    tip_amount               DOUBLE PRECISION,
    tolls_amount              DOUBLE PRECISION,
    improvement_surcharge     DOUBLE PRECISION,
    total_amount              DOUBLE PRECISION,
    congestion_surcharge       DOUBLE PRECISION,
    airport_fee                DOUBLE PRECISION,
    _ingest_time                TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
