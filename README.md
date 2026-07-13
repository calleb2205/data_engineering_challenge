# Datarisk Data Engineering Challenge

## Apresentação

Olá candidato(a), esse é o desafio prático para o time de **Engenharia de dados** da Datarisk!

O objetivo desse desafio é entendermos melhor o seu nível técnico, então sugerimos um projeto que utiliza ferramentas bastante comuns na vida de um engenheiro de dados.

O desafio segue uma ideia simples à princípio, mas você pode encontrar algumas dificuldades no meio do caminho. Por isso, **não hesite** em propor qualquer solução diferente para chegar em seu objetivo final.

O importante é **entregar o que você conseguir fazer**, com a devida documentação.

## Desafio

Segue abaixo uma lista de desafios abrangendo vários pontos presentes no dia a dia de um engenheiro de dados. **Sinta-se livre para ir além** das ferramentas pré-determinadas e construir a solução que melhor se aplica ao seu ver.

- Fazer a configuração do **Apache Airflow** e **PostgreSQL** em seu computador com Docker.
    - Sugestão: [Docker Compose](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).
- Criar um processo de **ETL** (Extract, Transform, Load) no **Apache Airflow**, utilizando dados de **corridas de táxi** de Nova York.
    - Disponível em: [nyc-tlc-data](https://github.com/datarisk-io/data-engineering-challenge/tree/master/nyc-tlc-data).
- Armazenar os dados no banco de dados **PostgreSQL**.
    - Sugestão: organizar os dados em camadas de processamento (raw, trusted, refined).
- Responder às seguintes questões:
    1. Qual o total de registros na tabela final?
    2. Qual o total de viagens iniciadas e finalizadas no dia 17 de junho?
    3. Qual foi o dia da viagem mais longa percorrida?
    4. Qual a média, o desvio padrão, o mínimo, o máximo e os quartis da distribuição de distância percorrida nas viagens totais?
- Criar um **README** contendo os passos do processo, decisões tomadas e as respostas das questões passadas junto com as queries SQL utilizadas.
    - Sugestão: Markdown.

## Dúvidas

Caso tenha alguma dúvida ou sugestão, sinta-se a vontade para abrir uma `issue` nesse repositório.

---

# Solução

## Arquitetura

O pipeline roda inteiramente em **Docker Compose**, com 4 serviços:

- `postgres-airflow`: banco de metadados interno do Airflow (DAG runs, tasks, usuários).
- `postgres-dw`: o data warehouse do desafio, com 3 schemas (`raw`, `trusted`, `refined`).
- `airflow-webserver` / `airflow-scheduler`: a UI e o orquestrador do Airflow (`LocalExecutor`).

A DAG `etl_nyc_taxi` (`dags/etl_nyc_taxi.py`), escrita com a **TaskFlow API** (`@dag`/`@task`), processa os 12 arquivos parquet mensais de 2022 já disponíveis em `nyc-tlc-data/` (montados no container via bind mount), seguindo o fluxo:

```
prepare_raw_schema
-> load_raw (1 task instance por mês, via dynamic task mapping .expand())
-> transform_trusted (roda uma vez, sobre o ano completo)
-> transform_refined (roda uma vez)
```

### Camadas (medallion architecture)


`raw.yellow_tripdata` (bronze) Cópia fiel dos parquets originais + `_ingest_time` (auditoria de quando cada carga aconteceu). Nenhuma transformação.
`trusted.yellow_tripdata` (silver) **Qualidade do dado** remove nulos em pickup/dropoff, remove dropoff antes do pickup, remove `trip_distance <= 0` e `passenger_count <= 0`, remove duplicatas exatas, tipa corretamente as colunas, aplica `TRIM`/`UPPER` no único campo texto (`store_and_fwd_flag`). Preserva **todas** as colunas do raw (maior fidelidade ao dado original) e adiciona `pickup_date`/`dropoff_date`/`trip_duration_minutes` como colunas derivadas. |
`refined.trips` (gold) **Curadoria para o negócio**: recorte das colunas relevantes para as perguntas do desafio, mais a métrica derivada `avg_speed_mph` (calculada a partir de `trip_distance`/`trip_duration_minutes`, que já vêm tratados da trusted). É a tabela final de consumo. |

A regra de responsabilidade usada: a **trusted responde "esse dado está certo?"** (qualidade, tipagem, deduplicação — de propósito geral, reutilizável por qualquer consumidor futuro), e a **refined responde "o que o negócio precisa para consumir isso direto?"** (recorte e métricas específicas para este caso de uso).

Todas as tabelas usam `DROP TABLE IF EXISTS` + `CREATE TABLE` (em vez de `CREATE TABLE IF NOT EXISTS` + `TRUNCATE`), o que torna o pipeline idempotente de fato — inclusive quando o schema de uma tabela muda entre execuções.

## Como rodar

Pré-requisito: Docker Desktop instalado e aberto.

```powershell
cd data_engineering_challenge
Copy-Item .env.example .env
docker compose up -d --build
```

Aguarde os containers ficarem saudáveis (`docker compose ps`), acesse `http://localhost:8080` (usuário/senha: `admin`/`admin`), ative e dispare a DAG `etl_nyc_taxi`. Ao final, os dados estarão em `postgres-dw` (porta `5434`, banco/usuário/senha `dw`).

## Decisões técnicas

- **Leitura local em vez de HTTP**: o dataset já vem commitado no repositório em `nyc-tlc-data/` (12 arquivos parquet de 2022). A versão inicial da DAG baixava os mesmos arquivos via HTTP do CloudFront da NYC TLC, o que era redundante, mais lento e dependente de rede. A task `load_raw` agora lê diretamente do diretório montado (`/opt/airflow/nyc-tlc-data`, via bind mount no `docker-compose.yml`).
- **`_ingest_time`**: coluna adicionada na tabela raw (`DEFAULT CURRENT_TIMESTAMP`), preenchida automaticamente em cada carga — permite auditar quando cada linha entrou no pipeline.
- **`COPY` em vez de `df.to_sql(method="multi")`**: a implementação original gravava a raw usando `pandas.DataFrame.to_sql` com `method="multi"` e `chunksize=50_000`. Para arquivos de alguns milhões de linhas, isso monta uma única instrução `INSERT` gigantesca por lote — lento e consumindo memória demais. Rodando os 12 meses em paralelo (comportamento padrão do dynamic task mapping), isso derrubava o container por falta de memória (algumas tasks morriam com `SIGKILL`, outras ficavam travadas consumindo 2-3 GB cada, sem nunca terminar). A solução foi trocar para `COPY FROM STDIN` (via `cursor.copy_expert`), o mecanismo nativo do Postgres para carga em massa — streama os dados em vez de montar uma query enorme. Resultado medido: ~3,5 milhões de linhas em 8 segundos.
- **`max_active_tis_per_dag=4` em `load_raw`**: mesmo com `COPY` sendo bem mais leve, cada task ainda carrega um DataFrame inteiro em memória. Esse limite garante que no máximo 4 meses carregam em paralelo, evitando picos de memória mesmo em máquinas com menos RAM disponível para o Docker.
- **`pandas==2.1.4`** (em vez de `2.2.2`): o Airflow 2.9.3 exige `sqlalchemy<2.0`, mas o `pandas>=2.2` só reconhece corretamente um `Engine` do SQLAlchemy se a versão instalada for `>=2.0.0` — um conflito real entre as duas dependências. Como a task `load_raw` passou a usar `COPY` (conexão psycopg2 crua, sem depender do SQLAlchemy), esse conflito deixou de importar para ela, mas a versão ficou fixada em `2.1.4` por ser a correta para o ambiente do Airflow de qualquer forma.
- **Trusted preserva todas as colunas do raw**: decisão consciente de manter maior fidelidade ao dado original na camada de conformidade, mesmo que nem todas as colunas sejam usadas nas 4 perguntas do desafio.
- **Refined como tabela fato única**: as 4 perguntas são respondidas via `sql/answers.sql` contra `refined.trips`, sem materializar tabelas de resposta separadas — mantém a camada gold simples e a lógica de negócio explícita/auditável no SQL versionado.

## Respostas às perguntas

Execução de referência: `NYC_TAXI_YEAR=2022`, dataset completo (12 meses), `sql/answers.sql`.

### 1. Qual o total de registros na tabela final?

```sql
SELECT COUNT(*) AS total_registros
FROM refined.trips;
```

**Resultado: 37.036.643 registros.**

### 2. Qual o total de viagens iniciadas e finalizadas no dia 17 de junho?

```sql
SELECT
    (SELECT COUNT(*) FROM refined.trips WHERE pickup_date  = '2022-06-17') AS viagens_iniciadas_17_06,
    (SELECT COUNT(*) FROM refined.trips WHERE dropoff_date = '2022-06-17') AS viagens_finalizadas_17_06;
```

**Resultado: 115.937 viagens iniciadas e 115.710 viagens finalizadas em 17/06/2022.**

### 3. Qual foi o dia da viagem mais longa percorrida?

```sql
SELECT
    pickup_date,
    trip_distance
FROM refined.trips
ORDER BY trip_distance DESC
LIMIT 1;
```

**Resultado: 24/06/2022, com 184.340,8 milhas registradas.**

### 4. Qual a média, o desvio padrão, o mínimo, o máximo e os quartis da distribuição de distância percorrida nas viagens totais?

```sql
SELECT
    AVG(trip_distance)                                             AS media,
    STDDEV(trip_distance)                                          AS desvio_padrao,
    MIN(trip_distance)                                             AS minimo,
    MAX(trip_distance)                                             AS maximo,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY trip_distance)    AS q1,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY trip_distance)    AS mediana_q2,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY trip_distance)    AS q3
FROM refined.trips;
```

| média | desvio padrão | mínimo | máximo | Q1 | mediana (Q2) | Q3 |
|---|---|---|---|---|---|---|
| 3,57 | 57,37 | 0,01 | 184.340,8 | 1,12 | 1,90 | 3,52 |

## Qualidade de dados / limitações conhecidas

- O dataset original da NYC TLC contém um pequeno número de registros com `trip_distance` implausivelmente alto (ver pergunta 3). Não foi aplicado filtro sobre isso na camada trusted; fica documentado aqui como limitação conhecida.
- A camada trusted não valida o intervalo de datas dos timestamps (ex: existem poucas viagens com `tpep_pickup_datetime` fora do ano/mês esperado do arquivo de origem) — outra inconsistência conhecida e comum nesse dataset, não tratada nesta versão.
