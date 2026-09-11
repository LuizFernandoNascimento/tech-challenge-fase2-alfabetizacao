"""Provisiona a infraestrutura mínima da camada Bronze.

Cria (de forma idempotente - pode rodar múltiplas vezes sem erro):
- bucket GCS para a zona raw;
- datasets BigQuery bronze/silver/gold;
- tópico e assinatura Pub/Sub usados pela ingestão streaming.
"""
import logging

from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery, pubsub_v1, storage

from bronze.config import load_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def ensure_bucket(storage_client: storage.Client, bucket_name: str, location: str) -> None:
    bucket = storage_client.bucket(bucket_name)
    if bucket.exists():
        logger.info("Bucket já existe: %s", bucket_name)
        return
    bucket.storage_class = "STANDARD"
    storage_client.create_bucket(bucket, location=location)
    logger.info("Bucket criado: %s (%s)", bucket_name, location)


def ensure_dataset(bq_client: bigquery.Client, dataset_id: str, location: str) -> None:
    dataset_ref = bigquery.DatasetReference(bq_client.project, dataset_id)
    try:
        bq_client.get_dataset(dataset_ref)
        logger.info("Dataset já existe: %s", dataset_id)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        bq_client.create_dataset(dataset)
        logger.info("Dataset criado: %s (%s)", dataset_id, location)


def ensure_topic_and_subscription(project_id: str, topic_id: str, subscription_id: str) -> None:
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    try:
        publisher.create_topic(request={"name": topic_path})
        logger.info("Tópico criado: %s", topic_path)
    except Conflict:
        logger.info("Tópico já existe: %s", topic_path)

    try:
        subscriber.create_subscription(request={"name": subscription_path, "topic": topic_path})
        logger.info("Assinatura criada: %s", subscription_path)
    except Conflict:
        logger.info("Assinatura já existe: %s", subscription_path)


def run() -> None:
    settings = load_settings()
    storage_client = storage.Client(project=settings.project_id)
    bq_client = bigquery.Client(project=settings.project_id)

    ensure_bucket(storage_client, settings.bucket_raw, settings.region)
    for dataset_id in (
        settings.dataset_bronze,
        settings.dataset_silver,
        settings.dataset_gold,
        settings.dataset_quality,
    ):
        ensure_dataset(bq_client, dataset_id, settings.region)

    # Staging na multi-região US: precisa ficar em US porque é lá que
    # vive a fonte pública da Base dos Dados, e o BigQuery só aceita
    # jobs cujas tabelas estejam todas na mesma location.
    ensure_dataset(bq_client, settings.dataset_bronze_us, "US")
    ensure_topic_and_subscription(
        settings.project_id, settings.pubsub_topic_alunos, settings.pubsub_subscription_alunos
    )


if __name__ == "__main__":
    run()
