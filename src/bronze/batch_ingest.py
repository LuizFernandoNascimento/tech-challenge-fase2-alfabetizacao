"""Ingestão batch da camada Bronze.

Duas origens diferentes:
- As 5 fontes do INEP (metas + resultados) e os microdados de aluno já
  existem como tabelas BigQuery públicas da Base dos Dados (projeto
  `basedosdados`) - carregadas via CTAS cross-project, sem baixar CSV
  nem passar pelo GCS.
- A dimensão IBGE (município/UF) não tem tabela pública equivalente,
  então continua vindo de um CSV (bronze/ibge_reference.py) subido ao
  nosso próprio bucket.

Em ambos os casos, a tabela final só ganha metadados de ingestão
(_ingested_at, _source_file) - sem transformação de conteúdo, como
esperado da camada Bronze.
"""
import datetime as dt
import logging

from google.cloud import bigquery, storage

from bronze.config import load_settings
from bronze.sources import (
    BASEDOSDADOS_DATASET,
    BASEDOSDADOS_PROJECT,
    BD_BATCH_SOURCES,
    IBGE_REFERENCE_SOURCE,
    BasedosdadosSource,
    BronzeSource,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def load_from_basedosdados(bq_client: bigquery.Client, dataset_id: str, source: BasedosdadosSource) -> None:
    # A tabela pública da Base dos Dados vive na multi-region "US";
    # nosso dataset Bronze vive em southamerica-east1. O BigQuery exige
    # que todas as tabelas referenciadas por um job estejam na mesma
    # location, então um CTAS cross-region direto não é possível - a
    # consulta roda em "US" e o resultado (poucas linhas, nada disso
    # é a tabela de alunos) é trazido para o processo e gravado na
    # nossa própria location via load_table_from_json.
    source_table = f"{BASEDOSDADOS_PROJECT}.{BASEDOSDADOS_DATASET}.{source.bd_table}"
    rows = [dict(row.items()) for row in bq_client.query(f"SELECT * FROM `{source_table}`", location="US").result()]

    ingested_at = dt.datetime.now(dt.timezone.utc).isoformat()
    for row in rows:
        row["_ingested_at"] = ingested_at
        row["_source_file"] = source_table

    final_table_id = f"{bq_client.project}.{dataset_id}.{source.table_name}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    bq_client.load_table_from_json(rows, final_table_id, job_config=job_config).result()

    table = bq_client.get_table(final_table_id)
    logger.info("Bronze carregada de %s: %s (%d linhas)", source_table, final_table_id, table.num_rows)


# --- fluxo CSV -> GCS -> BigQuery, mantido só para a dimensão IBGE,
# que não existe como tabela pública da Base dos Dados.


def upload_to_gcs(storage_client: storage.Client, bucket_name: str, source: BronzeSource) -> str:
    from bronze.sources import RAW_DATA_DIR

    local_path = RAW_DATA_DIR / source.file_name
    blob_path = f"bronze/{source.table_name}/{source.file_name}"
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(str(local_path))
    gcs_uri = f"gs://{bucket_name}/{blob_path}"
    logger.info("Upload concluído: %s -> %s", local_path, gcs_uri)
    return gcs_uri


def _gcs_uri(bucket_name: str, source: BronzeSource) -> str:
    return f"gs://{bucket_name}/bronze/{source.table_name}/{source.file_name}"


def load_csv_to_bigquery(bq_client: bigquery.Client, dataset_id: str, gcs_uri: str, source: BronzeSource) -> None:
    # Carrega primeiro numa tabela de staging (schema autodetectado),
    # depois materializa a tabela Bronze final já com os metadados de
    # ingestão via CTAS - evita um ALTER + UPDATE de duas passadas.
    stg_table_id = f"{bq_client.project}.{dataset_id}.{source.table_name}_stg"
    final_table_id = f"{bq_client.project}.{dataset_id}.{source.table_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    bq_client.load_table_from_uri(gcs_uri, stg_table_id, job_config=job_config).result()

    ctas_query = f"""
        CREATE OR REPLACE TABLE `{final_table_id}` AS
        SELECT *, CURRENT_TIMESTAMP() AS _ingested_at, @source_file AS _source_file
        FROM `{stg_table_id}`
    """
    query_job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("source_file", "STRING", source.file_name)]
    )
    bq_client.query(ctas_query, job_config=query_job_config).result()
    bq_client.delete_table(stg_table_id, not_found_ok=True)

    table = bq_client.get_table(final_table_id)
    logger.info("Bronze carregada: %s (%d linhas)", final_table_id, table.num_rows)


def run() -> None:
    """Bootstrap local: tabelas do INEP direto do BigQuery público da
    Base dos Dados + dimensão IBGE via CSV (upload + load).
    """
    settings = load_settings()
    bq_client = bigquery.Client(project=settings.project_id)

    for source in BD_BATCH_SOURCES:
        load_from_basedosdados(bq_client, settings.dataset_bronze, source)

    storage_client = storage.Client(project=settings.project_id)
    gcs_uri = upload_to_gcs(storage_client, settings.bucket_raw, IBGE_REFERENCE_SOURCE)
    load_csv_to_bigquery(bq_client, settings.dataset_bronze, gcs_uri, IBGE_REFERENCE_SOURCE)


def reload_bronze_from_gcs() -> None:
    """Usado pela Cloud Function batch (acionada pelo Cloud Scheduler).

    As tabelas do INEP são sempre relidas direto da Base dos Dados -
    não dependem de disco local nem de arquivo prévio no GCS. Só a
    dimensão IBGE depende de um arquivo já existente no bucket (subido
    pelo bootstrap local), já que não há disco na Cloud Function para
    fazer o fetch da API de novo aqui.
    """
    settings = load_settings()
    bq_client = bigquery.Client(project=settings.project_id)

    for source in BD_BATCH_SOURCES:
        load_from_basedosdados(bq_client, settings.dataset_bronze, source)

    gcs_uri = _gcs_uri(settings.bucket_raw, IBGE_REFERENCE_SOURCE)
    load_csv_to_bigquery(bq_client, settings.dataset_bronze, gcs_uri, IBGE_REFERENCE_SOURCE)


if __name__ == "__main__":
    run()
