-- Camada REFINED: tabela final, pronta para consumo/análise.
-- Adiciona apenas colunas de negócio úteis para as perguntas do desafio.

DROP TABLE IF EXISTS refined.trips;

CREATE TABLE refined.trips (
    trip_id                   BIGSERIAL PRIMARY KEY,
    vendorid                  INTEGER,
    pickup_datetime            TIMESTAMP,
    dropoff_datetime           TIMESTAMP,
    pickup_date                 DATE,
    dropoff_date                 DATE,
    passenger_count               INTEGER,
    trip_distance                  DOUBLE PRECISION,
    trip_duration_minutes           DOUBLE PRECISION,
    avg_speed_mph                    DOUBLE PRECISION,
    pulocationid                      INTEGER,
    dolocationid                      INTEGER,
    payment_type                      INTEGER,
    fare_amount                       DOUBLE PRECISION,
    tip_amount                        DOUBLE PRECISION,
    total_amount                      DOUBLE PRECISION
);

INSERT INTO refined.trips (
    vendorid, pickup_datetime, dropoff_datetime, pickup_date, dropoff_date,
    passenger_count, trip_distance, trip_duration_minutes, avg_speed_mph,
    pulocationid, dolocationid, payment_type, fare_amount, tip_amount, total_amount
)
SELECT
    vendorid,
    pickup_datetime,
    dropoff_datetime,
    pickup_date,
    dropoff_date,
    passenger_count,
    trip_distance,
    trip_duration_minutes,
    CASE WHEN trip_duration_minutes > 0
         THEN trip_distance / (trip_duration_minutes / 60.0)
         ELSE NULL END AS avg_speed_mph,
    pulocationid,
    dolocationid,
    payment_type,
    fare_amount,
    tip_amount,
    total_amount
FROM trusted.yellow_tripdata;
