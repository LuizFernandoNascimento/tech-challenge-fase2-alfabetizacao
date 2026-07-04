"""Consome os eventos simulados de dados de alunos do Pub/Sub em
micro-lotes e grava na tabela Bronze de streaming no BigQuery.
"""
import argparse
import datetime as dt
import json
import logging
import time

from google.cloud import bigquery, pubsub_v1

from bronze.config import load_settings
from bronze.sources import STREAMING_SOURCE

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

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


def _coerce(row: dict) -> dict:
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
    coerced["_source_file"] = STREAMING_SOURCE.file_name
    return coerced


def run(max_messages: int, batch_window_seconds: float) -> None:
    settings = load_settings()
    bq_client = bigquery.Client(project=settings.project_id)
    table_id = ensure_streaming_table(bq_client, settings.dataset_bronze)

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        settings.project_id, settings.pubsub_subscription_alunos
    )

    buffer: list[dict] = []
    total_inserted = 0

    def flush():
        nonlocal buffer, total_inserted
        if not buffer:
            return
        errors = bq_client.insert_rows_json(table_id, buffer)
        if errors:
            logger.error("Erros ao inserir no BigQuery: %s", errors)
        else:
            total_inserted += len(buffer)
            logger.info("Micro-lote gravado: %d linhas (total %d)", len(buffer), total_inserted)
        buffer = []

    def callback(message):
        row = json.loads(message.data.decode("utf-8"))
        buffer.append(_coerce(row))
        message.ack()
        if len(buffer) >= 100:
            flush()

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    logger.info(
        "Escutando %s por até %.0fs ou %d mensagens...",
        subscription_path,
        batch_window_seconds,
        max_messages,
    )

    start = time.time()
    try:
        while time.time() - start < batch_window_seconds and total_inserted < max_messages:
            time.sleep(1)
    finally:
        streaming_pull_future.cancel()
        flush()

    logger.info("Consumo encerrado. Total de linhas gravadas: %d", total_inserted)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Consumidor de eventos de dados de alunos (simulação de streaming)"
    )
    parser.add_argument("--max-messages", type=int, default=500)
    parser.add_argument("--window-seconds", type=float, default=120.0)
    args = parser.parse_args()
    run(max_messages=args.max_messages, batch_window_seconds=args.window_seconds)
