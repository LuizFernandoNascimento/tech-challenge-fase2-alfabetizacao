"""Schema e coerção de tipos compartilhados entre o consumidor manual
(streaming/consumer.py) e a Cloud Function acionada por Pub/Sub
(main.py) - ambos escrevem na mesma tabela Bronze de streaming.
"""
import datetime as dt

from google.cloud import bigquery

from bronze.sources import BASEDOSDADOS_DATASET, BASEDOSDADOS_PROJECT, STREAMING_SOURCE

_SOURCE_TABLE = f"{BASEDOSDADOS_PROJECT}.{BASEDOSDADOS_DATASET}.{STREAMING_SOURCE.bd_table}"

SCHEMA = [
    bigquery.SchemaField("ano", "INTEGER"),
    bigquery.SchemaField("id_municipio", "STRING"),
    bigquery.SchemaField("id_escola", "STRING"),
    bigquery.SchemaField("id_aluno", "STRING"),
    bigquery.SchemaField("caderno", "INTEGER"),
    bigquery.SchemaField("serie", "INTEGER"),
    bigquery.SchemaField("rede", "INTEGER"),
    bigquery.SchemaField("presenca", "INTEGER"),
    bigquery.SchemaField("preenchimento_caderno", "INTEGER"),
    bigquery.SchemaField("alfabetizado", "INTEGER"),
    bigquery.SchemaField("proficiencia", "FLOAT"),
    bigquery.SchemaField("peso_aluno", "FLOAT"),
    bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
    bigquery.SchemaField("_source_file", "STRING"),
]

INT_FIELDS = {"ano", "caderno", "serie", "rede", "presenca", "preenchimento_caderno", "alfabetizado"}
FLOAT_FIELDS = {"proficiencia", "peso_aluno"}


def ensure_streaming_table(bq_client: bigquery.Client, dataset_id: str) -> str:
    table_id = f"{bq_client.project}.{dataset_id}.{STREAMING_SOURCE.table_name}"
    table = bigquery.Table(table_id, schema=SCHEMA)
    bq_client.create_table(table, exists_ok=True)
    return table_id


def coerce_row(row: dict) -> dict:
    # Bronze normalmente não transforma, mas tipagem mínima é necessária
    # aqui porque a inserção streaming (ao contrário do LoadJob) não
    # faz autodetecção de schema a partir de texto.
    coerced = dict(row)
    for field in INT_FIELDS:
        value = coerced.get(field)
        coerced[field] = int(value) if value not in (None, "") else None
    for field in FLOAT_FIELDS:
        value = coerced.get(field)
        coerced[field] = float(value) if value not in (None, "") else None
    coerced["_ingested_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    coerced["_source_file"] = _SOURCE_TABLE
    return coerced
