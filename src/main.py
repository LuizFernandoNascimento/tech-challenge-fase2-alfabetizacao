"""Pontos de entrada das Cloud Functions (Gen2) da camada Bronze.

Duas funções deployadas a partir deste mesmo arquivo (uma por
--entry-point na hora do `gcloud functions deploy`):

- batch_ingest_http: acionada via HTTP pelo Cloud Scheduler, roda a
  ingestão batch completa (mesma lógica de bronze/batch_ingest.py).
- streaming_consumer_pubsub: acionada automaticamente pelo Pub/Sub a
  cada mensagem publicada em dados-alunos-stream - substitui o
  consumidor manual (streaming/consumer.py) como caminho "real" de
  produção. Cada invocação processa uma única mensagem (sem buffer
  local: a função é stateless por natureza), diferente do
  micro-lote usado no consumidor manual.
"""
import base64
import json

import functions_framework
from cloudevents.http import CloudEvent
from google.cloud import bigquery

from bronze.config import load_settings
from bronze.batch_ingest import reload_bronze_from_gcs
from bronze.streaming_schema import coerce_row, ensure_streaming_table


@functions_framework.http
def batch_ingest_http(request):
    # Não há disco local na nuvem: a função relê os arquivos que o
    # bootstrap local (bronze/batch_ingest.py::run) já deixou no GCS.
    reload_bronze_from_gcs()
    return ("Ingestão batch concluída", 200)


@functions_framework.cloud_event
def streaming_consumer_pubsub(event: CloudEvent):
    settings = load_settings()
    bq_client = bigquery.Client(project=settings.project_id)
    table_id = ensure_streaming_table(bq_client, settings.dataset_bronze)

    payload = base64.b64decode(event.data["message"]["data"])
    row = json.loads(payload.decode("utf-8"))
    errors = bq_client.insert_rows_json(table_id, [coerce_row(row)])
    if errors:
        raise RuntimeError(f"Erro ao inserir no BigQuery: {errors}")
