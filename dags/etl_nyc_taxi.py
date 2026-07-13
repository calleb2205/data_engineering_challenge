"""
DAG: etl_nyc_taxi
------------------
Pipeline de ETL para dados de corridas de táxi de Nova York (Yellow Taxi).

Processa os 12 arquivos parquet mensais de um ano (ex: yellow_tripdata_2022-01.parquet.gz
até yellow_tripdata_2022-12.parquet.gz), lidos localmente do diretório nyc-tlc-data/
(montado no container em /opt/airflow/nyc-tlc-data).

Fluxo:
    prepare_raw_schema
    -> load_raw (1 task instance por mês, via dynamic task mapping)
    -> transform_trusted (roda uma vez, sobre o ano completo já carregado na raw)
    -> transform_refined (roda uma vez)

Camadas:
    raw      -> dados brutos, praticamente 1:1 com os arquivos parquet originais
    trusted  -> dados limpos e tipados
    refined  -> tabela final de consumo, com colunas derivadas para análise

Fonte dos dados:
    (arquivos parquet mensais de Yellow Taxi, já disponibilizados localmente em nyc-tlc-data/)
"""
from __future__ import annotations

import io
import os
from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "postgres_dw"
YEAR = os.environ.get("NYC_TAXI_YEAR", "2022")
MONTHS = [f"{m:02d}" for m in range(1, 13)]  # 01 a 12 -> os 12 meses do ano
LOCAL_DATA_DIR = "/opt/airflow/nyc-tlc-data"
SQL_DIR = "/opt/airflow/sql"
LOAD_RAW_MAX_CONCURRENCY = 4


def _read_sql_file(filename: str) -> str:
    with open(os.path.join(SQL_DIR, filename), "r") as f:
        return f.read()


@dag(
    dag_id="etl_nyc_taxi",
    description="ETL de dados de corridas de táxi de NY - 12 meses (raw -> trusted -> refined)",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # execução manual / sob demanda
    catchup=False,
    tags=["desafio", "nyc-taxi", "etl"],
)
def etl_nyc_taxi():

    @task
    def prepare_raw_schema():
        """Cria os schemas e (re)cria a tabela raw do zero (DDL faz DROP + CREATE)."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        hook.run(_read_sql_file("create_schemas.sql"))
        hook.run(_read_sql_file("create_raw_table.sql"))

    @task(max_active_tis_per_dag=LOAD_RAW_MAX_CONCURRENCY)
    def load_raw(month: str):
        """Extract + Load: lê o parquet local de um mês e grava na camada raw via COPY.

        Usa COPY FROM STDIN (em vez de df.to_sql/INSERT) porque é a forma nativa e
        eficiente do Postgres para carga em massa -- monta um único INSERT gigante por
        chunk (to_sql com method="multi") é lento e consome memória demais para arquivos
        de alguns milhões de linhas.
        """
        file_name = f"yellow_tripdata_{YEAR}-{month}.parquet.gz"
        local_path = os.path.join(LOCAL_DATA_DIR, file_name)

        df = pd.read_parquet(local_path)
        # normaliza nomes de colunas para snake_case / minúsculas
        df.columns = [c.lower() for c in df.columns]

        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        columns = ", ".join(df.columns)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.copy_expert(
                f"COPY raw.yellow_tripdata ({columns}) FROM STDIN WITH (FORMAT csv)",
                buffer,
            )

    @task
    def transform_trusted():
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        hook.run(_read_sql_file("create_trusted_table.sql"))

    @task
    def transform_refined():
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        hook.run(_read_sql_file("create_refined_table.sql"))

    schema_ready = prepare_raw_schema()

    # Dynamic task mapping: 1 task instance de load por mês (12 no total)
    loaded = load_raw.expand(month=MONTHS)

    trusted = transform_trusted()
    refined = transform_refined()

    schema_ready >> loaded >> trusted >> refined


etl_nyc_taxi()
