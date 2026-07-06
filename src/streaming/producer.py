"""Simula a chegada quase em tempo real de resultados de avaliação de
alunos: em vez de ler um CSV local, consulta direto a tabela pública
de microdados da Base dos Dados no BigQuery (basedosdados.
br_inep_avaliacao_alfabetizacao.alunos) e replaya as linhas como
eventos publicados no Pub/Sub, em vez de fazer uma carga batch única.
"""
import argparse
import json
import logging
import time

from google.cloud import bigquery, pubsub_v1

from bronze.config import load_settings
from bronze.sources import BASEDOSDADOS_DATASET, BASEDOSDADOS_PROJECT, STREAMING_SOURCE

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def fetch_rows(bq_client: bigquery.Client, limit: int):
    # A tabela pública vive na multi-region "US" - precisa ser essa a
    # location do job de consulta (ver o mesmo ajuste em batch_ingest.py).
    # O LIMIT aqui controla quantos eventos simulamos, não o custo da
    # consulta: a tabela não é particionada, então o BigQuery ainda
    # escaneia as colunas selecionadas para todas as linhas antes de
    # aplicar o limite. Para essa tabela (poucas colunas numéricas,
    # ~3,9M linhas) o volume de bytes escaneados é pequeno de qualquer
    # forma - mas vale ter isso em mente em tabelas maiores.
    query = f"""
        SELECT *
        FROM `{BASEDOSDADOS_PROJECT}.{BASEDOSDADOS_DATASET}.{STREAMING_SOURCE.bd_table}`
        LIMIT @limit
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("limit", "INT64", limit)]
    )
    return bq_client.query(query, job_config=job_config, location="US").result()


def run(limit: int, messages_per_second: float) -> None:
    settings = load_settings()
    bq_client = bigquery.Client(project=settings.project_id)
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(settings.project_id, settings.pubsub_topic_alunos)

    delay = 1.0 / messages_per_second if messages_per_second > 0 else 0
    published = 0
    for row in fetch_rows(bq_client, limit):
        data = json.dumps(dict(row.items()), ensure_ascii=False, default=str).encode("utf-8")
        publisher.publish(topic_path, data)
        published += 1
        if delay:
            time.sleep(delay)

    logger.info("Publicadas %d mensagens em %s (fonte: BigQuery basedosdados)", published, topic_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Produtor de eventos de dados de alunos (simulação de streaming, fonte: BigQuery público)"
    )
    parser.add_argument("--limit", type=int, default=500, help="Número máximo de eventos a publicar")
    parser.add_argument("--rate", type=float, default=10.0, help="Mensagens por segundo")
    args = parser.parse_args()
    run(limit=args.limit, messages_per_second=args.rate)
