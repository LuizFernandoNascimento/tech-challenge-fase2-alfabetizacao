"""Ingestão batch da camada Bronze.

Três origens diferentes:
- As 5 fontes agregadas do INEP (metas + resultados) existem como
  tabelas BigQuery públicas da Base dos Dados (projeto `basedosdados`)
  e são pequenas (até ~24k linhas) - carregadas via consulta
  cross-project trazendo as linhas para o processo, sem baixar CSV.
- Os **microdados de aluno** (~3,87 milhões de linhas) vêm da mesma
  fonte pública, mas são grandes demais para passar pelo processo
  Python. Usam um caminho próprio, todo server-side: CTAS na
  multi-região US + cópia cross-region para a Bronze. Ver
  load_large_table_from_basedosdados().
- A dimensão IBGE (município/UF) não tem tabela pública equivalente,
  então continua vindo de um CSV (bronze/ibge_reference.py) subido ao
  nosso próprio bucket.

Em ambos os casos, a tabela final só ganha metadados de ingestão
(_ingested_at, _source_file) - sem transformação de conteúdo, como
esperado da camada Bronze.

Importante: a Bronze é **append-only**. Cada execução acrescenta um
novo snapshot com seu próprio `_ingested_at`, em vez de sobrescrever o
anterior - é assim que preservamos "histórico completo", como pede o
enunciado. Quem lê a Bronze (a Silver, por exemplo) é responsável por
filtrar o snapshot mais recente quando quiser o estado atual; a Bronze
em si nunca decide isso por conta própria.
"""
import datetime as dt
import logging

from google.cloud import bigquery, storage

from bronze.config import load_settings
from bronze.sources import (
    BASEDOSDADOS_DATASET,
    BASEDOSDADOS_PROJECT,
    BD_BATCH_SOURCES,
    BD_LARGE_BATCH_SOURCES,
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
        # WRITE_APPEND (não WRITE_TRUNCATE): cada execução vira um novo
        # snapshot na Bronze, preservando o histórico completo.
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    bq_client.load_table_from_json(rows, final_table_id, job_config=job_config).result()

    table = bq_client.get_table(final_table_id)
    logger.info(
        "Bronze carregada de %s: %s (snapshot com %d linhas, %d linhas no total acumulado)",
        source_table,
        final_table_id,
        len(rows),
        table.num_rows,
    )


def _table_exists(bq_client: bigquery.Client, table_id: str) -> bool:
    from google.api_core.exceptions import NotFound

    try:
        bq_client.get_table(table_id)
        return True
    except NotFound:
        return False


def load_large_table_from_basedosdados(
    bq_client: bigquery.Client, dataset_id: str, staging_dataset_us: str, source: BasedosdadosSource
) -> None:
    """Carrega uma tabela grande da Base dos Dados sem passar pelo processo.

    Os microdados de aluno têm ~3,87 milhões de linhas: trazer isso para
    a memória do Python (como faz load_from_basedosdados) seria lento e
    caro. Aqui tudo acontece server-side, em três etapas:

    1. CTAS na multi-região US, materializando a fonte pública numa
       tabela de staging no NOSSO projeto (mesma location, então o
       BigQuery aceita o job) - leva poucos segundos.
    2. Cópia cross-region da staging (US) para uma tabela de landing na
       nossa região. A cópia precisa ser WRITE_TRUNCATE: o BigQuery
       **não permite append em cópia cross-region** ("Cross-region table
       copy with append mode is unsupported"), por isso a landing existe.
    3. INSERT ... SELECT da landing para a Bronze final. Esse job é
       same-region, então aceita append - e é assim que preservamos o
       histórico de snapshots exigido da Bronze.

    As duas tabelas intermediárias são descartadas no fim: são só os
    degraus que resolvem a diferença de location entre a fonte pública e
    o nosso data warehouse.
    """
    source_table = f"{BASEDOSDADOS_PROJECT}.{BASEDOSDADOS_DATASET}.{source.bd_table}"
    staging_table_id = f"{bq_client.project}.{staging_dataset_us}.{source.table_name}_stg"
    landing_table_id = f"{bq_client.project}.{dataset_id}.{source.table_name}_landing"
    final_table_id = f"{bq_client.project}.{dataset_id}.{source.table_name}"

    # Etapa 1 - CTAS server-side em US (nada trafega pelo processo).
    ctas = f"""
        CREATE OR REPLACE TABLE `{staging_table_id}` AS
        SELECT *, CURRENT_TIMESTAMP() AS _ingested_at, @source_table AS _source_file
        FROM `{source_table}`
    """
    ctas_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("source_table", "STRING", source_table)]
    )
    bq_client.query(ctas, job_config=ctas_config, location="US").result()
    staging = bq_client.get_table(staging_table_id)
    logger.info("Staging US materializada: %s (%d linhas)", staging_table_id, staging.num_rows)

    # Etapa 2 - cópia cross-region para a landing (truncate obrigatório).
    copy_config = bigquery.CopyJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )
    bq_client.copy_table(staging_table_id, landing_table_id, job_config=copy_config).result()

    # Etapa 3 - append same-region da landing para a Bronze definitiva.
    # Na primeira execução a tabela final ainda não existe, então o
    # INSERT falharia - nesse caso criamos a partir da landing.
    if _table_exists(bq_client, final_table_id):
        sql = f"INSERT INTO `{final_table_id}` SELECT * FROM `{landing_table_id}`"
    else:
        sql = f"CREATE TABLE `{final_table_id}` AS SELECT * FROM `{landing_table_id}`"
    bq_client.query(sql).result()

    # Etapa 4 - as intermediárias já cumpriram o papel; não vale pagar storage.
    bq_client.delete_table(staging_table_id, not_found_ok=True)
    bq_client.delete_table(landing_table_id, not_found_ok=True)

    table = bq_client.get_table(final_table_id)
    logger.info(
        "Bronze carregada de %s: %s (snapshot com %d linhas, %d linhas no total acumulado)",
        source_table,
        final_table_id,
        staging.num_rows,
        table.num_rows,
    )


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
    # A staging table é sempre truncada (é só um degrau intermediário
    # e descartável); a tabela final é que precisa ser append-only.
    stg_table_id = f"{bq_client.project}.{dataset_id}.{source.table_name}_stg"
    final_table_id = f"{bq_client.project}.{dataset_id}.{source.table_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    bq_client.load_table_from_uri(gcs_uri, stg_table_id, job_config=job_config).result()

    # Acrescenta o snapshot na tabela final via query com destino
    # (WRITE_APPEND), em vez de CREATE OR REPLACE - preserva as
    # cargas anteriores.
    append_query = f"""
        SELECT *, CURRENT_TIMESTAMP() AS _ingested_at, @source_file AS _source_file
        FROM `{stg_table_id}`
    """
    query_job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("source_file", "STRING", source.file_name)],
        destination=final_table_id,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )
    bq_client.query(append_query, job_config=query_job_config).result()
    bq_client.delete_table(stg_table_id, not_found_ok=True)

    table = bq_client.get_table(final_table_id)
    logger.info("Bronze carregada: %s (%d linhas no total acumulado)", final_table_id, table.num_rows)


def run() -> None:
    """Bootstrap local: tabelas do INEP direto do BigQuery público da
    Base dos Dados (agregadas + microdados de aluno) + dimensão IBGE
    via CSV (upload + load).
    """
    settings = load_settings()
    bq_client = bigquery.Client(project=settings.project_id)

    for source in BD_BATCH_SOURCES:
        load_from_basedosdados(bq_client, settings.dataset_bronze, source)

    for source in BD_LARGE_BATCH_SOURCES:
        load_large_table_from_basedosdados(
            bq_client, settings.dataset_bronze, settings.dataset_bronze_us, source
        )

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

    for source in BD_LARGE_BATCH_SOURCES:
        load_large_table_from_basedosdados(
            bq_client, settings.dataset_bronze, settings.dataset_bronze_us, source
        )

    gcs_uri = _gcs_uri(settings.bucket_raw, IBGE_REFERENCE_SOURCE)
    load_csv_to_bigquery(bq_client, settings.dataset_bronze, gcs_uri, IBGE_REFERENCE_SOURCE)


if __name__ == "__main__":
    run()
