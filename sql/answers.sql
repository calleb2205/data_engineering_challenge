-- 1) Total de registros na tabela final
SELECT COUNT(*) AS total_registros
FROM refined.trips;

-- 2) Total de viagens iniciadas e finalizadas no dia 17 de junho
SELECT
    (SELECT COUNT(*) FROM refined.trips WHERE pickup_date  = '2022-06-17') AS viagens_iniciadas_17_06,
    (SELECT COUNT(*) FROM refined.trips WHERE dropoff_date = '2022-06-17') AS viagens_finalizadas_17_06;

-- 3) Dia da viagem mais longa percorrida (maior trip_distance)
SELECT
    pickup_date,
    trip_distance
FROM refined.trips
ORDER BY trip_distance DESC
LIMIT 1;

-- 4) Média, desvio padrão, mínimo, máximo e quartis da distância percorrida
SELECT
    AVG(trip_distance)                                             AS media,
    STDDEV(trip_distance)                                          AS desvio_padrao,
    MIN(trip_distance)                                             AS minimo,
    MAX(trip_distance)                                             AS maximo,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY trip_distance)    AS q1,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY trip_distance)    AS mediana_q2,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY trip_distance)    AS q3
FROM refined.trips;
