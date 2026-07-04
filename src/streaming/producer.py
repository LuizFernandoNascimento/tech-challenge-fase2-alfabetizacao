"""Simula a chegada quase em tempo real de resultados de avaliação de
alunos: replaya linhas do CSV de microdados como eventos publicados
no Pub/Sub, em vez de fazer uma carga batch única.
"""
import argparse
import csv
import json
import logging
import time

from google.cloud import pubsub_v1

from bronze.config import load_settings
from bronze.sources import RAW_DATA_DIR, STREAMING_SOURCE

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def stream_rows(csv_path, limit):
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if limit is not None and i >= limit:
                break
            yield row


def run(limit: int, messages_per_second: float) -> None:
    settings = load_settings()
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(settings.project_id, settings.pubsub_topic_alunos)
    csv_path = RAW_DATA_DIR / STREAMING_SOURCE.file_name

    delay = 1.0 / messages_per_second if messages_per_second > 0 else 0
    published = 0
    for row in stream_rows(csv_path, limit):
        data = json.dumps(row, ensure_ascii=False).encode("utf-8")
        publisher.publish(topic_path, data)
        published += 1
        if delay:
            time.sleep(delay)

    logger.info("Publicadas %d mensagens em %s", published, topic_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Produtor de eventos de dados de alunos (simulação de streaming)"
    )
    parser.add_argument("--limit", type=int, default=500, help="Número máximo de eventos a publicar")
    parser.add_argument("--rate", type=float, default=10.0, help="Mensagens por segundo")
    args = parser.parse_args()
    run(limit=args.limit, messages_per_second=args.rate)
