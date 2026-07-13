-- Camada TRUSTED: dados limpos, tipados e sem inconsistências óbvias.
-- Preserva todas as colunas de negócio da raw (maior fidelidade ao dado original).
-- Regras de limpeza aplicadas:
--   * remove linhas com datetime de pickup/dropoff nulos
--   * remove viagens com dropoff antes do pickup (inconsistência de horário)
--   * remove viagens com distância <= 0 ou passenger_count <= 0
--   * remove duplicatas exatas
--   * aplica TRIM/normalização de caixa no único campo texto (store_and_fwd_flag)
--   * calcula colunas derivadas: duração da viagem em minutos e data (sem hora) de pickup/dropoff
-- As colunas de tarifa/pedágio/surcharge (ratecodeid, mta_tax, tolls_amount,
-- improvement_surcharge, congestion_surcharge, airport_fee, extra) não fazem parte do
-- critério de qualidade e permanecem NULL quando a origem é NULL.

DROP TABLE IF EXISTS trusted.yellow_tripdata;

CREATE TABLE trusted.yellow_tripdata (
    vendorid                INTEGER,
    pickup_datetime          TIMESTAMP,
    dropoff_datetime         TIMESTAMP,
    pickup_date               DATE,
    dropoff_date               DATE,
    passenger_count           INTEGER,
    trip_distance              DOUBLE PRECISION,
    trip_duration_minutes       DOUBLE PRECISION,
    ratecodeid                   INTEGER,
    store_and_fwd_flag           TEXT,
    pulocationid                  INTEGER,
    dolocationid                  INTEGER,
    payment_type                   INTEGER,
    fare_amount                    DOUBLE PRECISION,
    extra                          DOUBLE PRECISION,
    mta_tax                        DOUBLE PRECISION,
    tip_amount                     DOUBLE PRECISION,
    tolls_amount                    DOUBLE PRECISION,
    improvement_surcharge            DOUBLE PRECISION,
    total_amount                      DOUBLE PRECISION,
    congestion_surcharge               DOUBLE PRECISION,
    airport_fee                         DOUBLE PRECISION
);

INSERT INTO trusted.yellow_tripdata
SELECT DISTINCT
    vendorid,
    tpep_pickup_datetime                                          AS pickup_datetime,
    tpep_dropoff_datetime                                         AS dropoff_datetime,
    tpep_pickup_datetime::date                                    AS pickup_date,
    tpep_dropoff_datetime::date                                   AS dropoff_date,
    passenger_count::INTEGER                                      AS passenger_count,
    trip_distance,
    EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60.0 AS trip_duration_minutes,
    ratecodeid::INTEGER                                           AS ratecodeid,
    UPPER(TRIM(store_and_fwd_flag))                               AS store_and_fwd_flag,
    pulocationid,
    dolocationid,
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
FROM raw.yellow_tripdata
WHERE tpep_pickup_datetime IS NOT NULL
  AND tpep_dropoff_datetime IS NOT NULL
  AND tpep_dropoff_datetime > tpep_pickup_datetime
  AND trip_distance > 0
  AND passenger_count > 0;
