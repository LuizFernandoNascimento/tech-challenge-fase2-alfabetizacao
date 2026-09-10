"""Consome os eventos simulados de dados de alunos do Pub/Sub em
micro-lotes e grava na tabela Bronze de streaming no BigQuery.

Semântica de entrega
--------------------
O ACK só acontece DEPOIS da gravação no BigQuery confirmar. A versão
anterior dava `message.ack()` assim que a mensagem entrava no buffer,
antes do flush - se o processo caísse (ou o insert falhasse) entre o
ack e a gravação, aquelas mensagens estavam perdidas para sempre, já
que o Pub/Sub não as reentregaria.

Com o ack depois da gravação, trocamos perda por duplicidade: numa
falha, as mensagens não confirmadas voltam a ser entregues e podem ser
gravadas duas vezes. Isso é *at-least-once*, e é a escolha certa aqui -
a Bronze é append-only e a deduplicação por (id_aluno, ano) na Silver
já trata o excedente. Perder microdado de aluno seria irreversível;
duplicá-lo, não.

Em falha de gravação as mensagens recebem `nack()` explícito, para
serem reentregues imediatamente em vez de esperar o ack deadline.
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

    # Cada item do buffer guarda a linha E a mensagem que a originou,
    # para que o ack/nack possa ser decidido depois da gravação.
    buffer: list[tuple[dict, pubsub_v1.subscriber.message.Message]] = []
    total_inserted = 0

    def flush():
        nonlocal buffer, total_inserted
        if not buffer:
            return

        pending, buffer = buffer, []
        rows = [row for row, _ in pending]
        messages = [message for _, message in pending]

        try:
            errors = bq_client.insert_rows_json(table_id, rows)
        except Exception:
            # Falha de rede/API: nada foi gravado com certeza, então
            # devolvemos tudo para o Pub/Sub reentregar.
            logger.exception("Erro ao chamar o BigQuery; devolvendo %d mensagens", len(messages))
            for message in messages:
                message.nack()
            return

        if errors:
            logger.error("Erros ao inserir no BigQuery: %s", errors)
            for message in messages:
                message.nack()
            return

        # Gravação confirmada - só agora é seguro dar ack.
        for message in messages:
            message.ack()
        total_inserted += len(rows)
        logger.info("Micro-lote gravado: %d linhas (total %d)", len(rows), total_inserted)

    def callback(message):
        row = json.loads(message.data.decode("utf-8"))
        buffer.append((coerce_row(row), message))
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
