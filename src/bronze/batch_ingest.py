"""Ingestão batch da camada Bronze.

Sobe cada CSV de origem para o GCS (zona raw) e carrega os dados como
estão no BigQuery (schema autodetectado, sem transformação de
conteúdo) - apenas acrescentando metadados de ingestão
(_ingested_at, _source_file), conforme esperado da camada Bronze.
"""
import logging

from google.cloud import bigquery, storage

from bronze.config import load_settings
from bronze.sources import BATCH_SOURCES, IBGE_REFERENCE_SOURCE, RAW_DATA_DIR, BronzeSource

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def upload_to_gcs(storage_client: storage.Client, bucket_name: str, source: BronzeSource) -> str:
    local_path = RAW_DATA_DIR / source.file_name
    blob_path = f"bronze/{source.table_name}/{source.file_name}"
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(str(local_path))
    gcs_uri = f"gs://{bucket_name}/{blob_path}"
    logger.info("Upload concluído: %s -> %s", local_path, gcs_uri)
    return gcs_uri


def load_to_bigquery(bq_client: bigquery.Client, dataset_id: str, gcs_uri: str, source: BronzeSource) -> None:
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


def _gcs_uri(bucket_name: str, source: BronzeSource) -> str:
    return f"gs://{bucket_name}/bronze/{source.table_name}/{source.file_name}"


def run() -> None:
    """Bootstrap local: sobe os CSVs para o GCS e carrega no BigQuery.

    Usado apenas em desenvolvimento local, onde os arquivos brutos
    existem em disco. Não é o que a Cloud Function deployada roda -
    ver reload_bronze_from_gcs().
    """
    settings = load_settings()
    storage_client = storage.Client(project=settings.project_id)
    bq_client = bigquery.Client(project=settings.project_id)

    sources = [*BATCH_SOURCES, IBGE_REFERENCE_SOURCE]
    for source in sources:
        gcs_uri = upload_to_gcs(storage_client, settings.bucket_raw, source)
        load_to_bigquery(bq_client, settings.dataset_bronze, gcs_uri, source)


def reload_bronze_from_gcs() -> None:
    """Recarrega a Bronze a partir dos arquivos já existentes no GCS.

    É isto que a Cloud Function batch (acionada pelo Cloud Scheduler)
    executa: não há disco local na nuvem, então ela não faz upload -
    apenas relê os arquivos que o processo de origem já deixou no
    bucket raw (aqui, os mesmos publicados pelo bootstrap local).
    """
    settings = load_settings()
    bq_client = bigquery.Client(project=settings.project_id)

    sources = [*BATCH_SOURCES, IBGE_REFERENCE_SOURCE]
    for source in sources:
        gcs_uri = _gcs_uri(settings.bucket_raw, source)
        load_to_bigquery(bq_client, settings.dataset_bronze, gcs_uri, source)


if __name__ == "__main__":
    run()
