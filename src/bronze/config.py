"""Configurações compartilhadas da pipeline, carregadas do .env."""
import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    project_id: str
    region: str
    bucket_raw: str
    dataset_bronze: str
    dataset_silver: str
    dataset_gold: str
    pubsub_topic_alunos: str
    pubsub_subscription_alunos: str


def load_settings() -> Settings:
    return Settings(
        project_id=os.environ["GCP_PROJECT_ID"],
        region=os.environ.get("GCP_REGION", "southamerica-east1"),
        bucket_raw=os.environ["GCS_BUCKET_RAW"],
        dataset_bronze=os.environ.get("BQ_DATASET_BRONZE", "bronze"),
        dataset_silver=os.environ.get("BQ_DATASET_SILVER", "silver"),
        dataset_gold=os.environ.get("BQ_DATASET_GOLD", "gold"),
        pubsub_topic_alunos=os.environ.get("PUBSUB_TOPIC_ALUNOS", "dados-alunos-stream"),
        pubsub_subscription_alunos=os.environ.get(
            "PUBSUB_SUBSCRIPTION_ALUNOS", "dados-alunos-stream-sub"
        ),
    )
