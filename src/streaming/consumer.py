"""Consome os eventos simulados de dados de alunos do Pub/Sub em
micro-lotes e grava na tabela Bronze de streaming no BigQuery.
"""
import argparse
import json
import logging
import time

from google.cloud import bigquery, pubsub_v1

from bronze.config import load_settings
from bronze.streaming_schema import coerce_row, ensure_streaming_table

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


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
        buffer.append(coerce_row(row))
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
