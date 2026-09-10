"""Executa as transformações SQL para construir a Camada Silver a partir da Bronze.

Todas as transformações rodam diretamente no BigQuery para máxima eficiência de custo/tempo (FinOps).

Duas coisas importantes que mudaram depois da primeira versão:

1. A Bronze agora é append-only (cada execução da ingestão batch
   guarda um novo snapshot, para preservar histórico completo - ver
   bronze/batch_ingest.py). Isso significa que a Silver não pode mais
   fazer um `SELECT * FROM bronze.X` direto: precisa filtrar
   explicitamente o snapshot mais recente (`_ingested_at` máximo).
   A tabela de streaming (`dados_alunos_streaming`) é exceção: lá,
   cada linha já é um evento individual, não um snapshot completo, e
   a deduplicação por `id_aluno`/`ano` continua cuidando disso.
2. Registros sem correspondência em `dim_localidades` não são mais
   mascarados com `sigla_uf = 'ND'`. Eles são roteados para tabelas de
   quarentena (`silver.quarentena_*`), com o motivo da rejeição - a
   tabela Silver "de verdade" só contém registros com integridade
   referencial garantida por construção (INNER JOIN).
"""
import logging
from google.cloud import bigquery
from bronze.config import load_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# Mapeamento oficial de código -> rótulo de rede, conforme a tabela
# `dicionario` do próprio dataset `basedosdados.br_inep_avaliacao_alfabetizacao`.
# Os rótulos de "Pública" (códigos 5 e 6) foram encurtados para bater
# com o valor literal que já vem em meta_alfabetizacao_uf/brasil
# ("Pública", sem o detalhamento entre parênteses) - sem isso, os
# joins da camada Gold entre avaliação e meta nunca casam (ver PR #8).
def _rede_label_case(column: str) -> str:
    return f"""
    CASE {column}
      WHEN 1 THEN 'Federal'
      WHEN 2 THEN 'Estadual'
      WHEN 3 THEN 'Municipal'
      WHEN 4 THEN 'Privada'
      WHEN 5 THEN 'Pública'
      WHEN 6 THEN 'Pública'
      WHEN 0 THEN 'Total'
      ELSE 'Desconhecida'
    END
    """


def _latest_snapshot(project: str, bronze: str, table: str) -> str:
    """Fragmento SQL que filtra apenas o snapshot mais recente de uma
    tabela Bronze append-only (a Bronze pode ter vários `_ingested_at`
    acumulados; a Silver só quer o estado mais atual)."""
    fq_table = f"`{project}.{bronze}.{table}`"
    return f"""(
        SELECT * FROM {fq_table}
        WHERE _ingested_at = (SELECT MAX(_ingested_at) FROM {fq_table})
    )"""


def get_queries(project: str, bronze: str, silver: str) -> dict[str, str]:
    """Retorna o dicionário de queries SQL de criação das tabelas da camada Silver."""
    ibge_municipios = _latest_snapshot(project, bronze, "ibge_municipios")
    meta_brasil = _latest_snapshot(project, bronze, "meta_alfabetizacao_brasil")
    meta_uf = _latest_snapshot(project, bronze, "meta_alfabetizacao_uf")
    meta_municipio = _latest_snapshot(project, bronze, "meta_alfabetizacao_municipio")
    avaliacao_uf = _latest_snapshot(project, bronze, "avaliacao_alfabetizacao_uf")
    avaliacao_municipio = _latest_snapshot(project, bronze, "avaliacao_alfabetizacao_municipio")
    alunos_batch = _latest_snapshot(project, bronze, "dados_alunos")

    return {
        "dim_localidades": f"""
            CREATE OR REPLACE TABLE `{project}.{silver}.dim_localidades`
            CLUSTER BY sigla_uf AS
            SELECT
              LPAD(CAST(id_municipio AS STRING), 7, '0') AS id_municipio,
              TRIM(nome_municipio) AS nome_municipio,
              UPPER(TRIM(sigla_uf)) AS sigla_uf,
              TRIM(nome_uf) AS nome_uf,
              CAST(id_uf AS INT64) AS id_uf,
              UPPER(TRIM(sigla_regiao)) AS sigla_regiao,
              TRIM(nome_regiao) AS nome_regiao,
              CURRENT_TIMESTAMP() AS _processed_at
            FROM {ibge_municipios}
        """,
        "meta_alfabetizacao_brasil": f"""
            CREATE OR REPLACE TABLE `{project}.{silver}.meta_alfabetizacao_brasil` AS
            SELECT
              CAST(ano AS INT64) AS ano,
              TRIM(rede) AS rede,
              CAST(taxa_alfabetizacao AS FLOAT64) AS taxa_alfabetizacao,
              CAST(meta_alfabetizacao_2024 AS FLOAT64) AS meta_2024,
              CAST(meta_alfabetizacao_2025 AS FLOAT64) AS meta_2025,
              CAST(meta_alfabetizacao_2026 AS FLOAT64) AS meta_2026,
              CAST(meta_alfabetizacao_2027 AS FLOAT64) AS meta_2027,
              CAST(meta_alfabetizacao_2028 AS FLOAT64) AS meta_2028,
              CAST(meta_alfabetizacao_2029 AS FLOAT64) AS meta_2029,
              CAST(meta_alfabetizacao_2030 AS FLOAT64) AS meta_2030,
              CAST(percentual_participacao AS FLOAT64) AS percentual_participacao,
              CURRENT_TIMESTAMP() AS _processed_at
            FROM {meta_brasil}
        """,
        "meta_alfabetizacao_uf": f"""
            CREATE OR REPLACE TABLE `{project}.{silver}.meta_alfabetizacao_uf`
            CLUSTER BY sigla_uf AS
            SELECT
              CAST(ano AS INT64) AS ano,
              UPPER(TRIM(sigla_uf)) AS sigla_uf,
              TRIM(rede) AS rede,
              CAST(taxa_alfabetizacao AS FLOAT64) AS taxa_alfabetizacao,
              CAST(meta_alfabetizacao_2024 AS FLOAT64) AS meta_2024,
              CAST(meta_alfabetizacao_2025 AS FLOAT64) AS meta_2025,
              CAST(meta_alfabetizacao_2026 AS FLOAT64) AS meta_2026,
              CAST(meta_alfabetizacao_2027 AS FLOAT64) AS meta_2027,
              CAST(meta_alfabetizacao_2028 AS FLOAT64) AS meta_2028,
              CAST(meta_alfabetizacao_2029 AS FLOAT64) AS meta_2029,
              CAST(meta_alfabetizacao_2030 AS FLOAT64) AS meta_2030,
              CAST(percentual_participacao AS FLOAT64) AS percentual_participacao,
              CURRENT_TIMESTAMP() AS _processed_at
            FROM {meta_uf}
        """,
        # meta_alfabetizacao_municipio: só entra na Silver "de verdade"
        # quem casa com dim_localidades (INNER JOIN). Quem não casa vai
        # para a quarentena, com o motivo - em vez de virar 'ND'.
        "meta_alfabetizacao_municipio": f"""
            CREATE OR REPLACE TABLE `{project}.{silver}.meta_alfabetizacao_municipio`
            CLUSTER BY sigla_uf, id_municipio AS
            SELECT
              CAST(m.ano AS INT64) AS ano,
              LPAD(CAST(m.id_municipio AS STRING), 7, '0') AS id_municipio,
              loc.sigla_uf AS sigla_uf,
              TRIM(m.rede) AS rede,
              CAST(m.taxa_alfabetizacao AS FLOAT64) AS taxa_alfabetizacao,
              CAST(m.meta_alfabetizacao_2024 AS FLOAT64) AS meta_2024,
              CAST(m.meta_alfabetizacao_2025 AS FLOAT64) AS meta_2025,
              CAST(m.meta_alfabetizacao_2026 AS FLOAT64) AS meta_2026,
              CAST(m.meta_alfabetizacao_2027 AS FLOAT64) AS meta_2027,
              CAST(m.meta_alfabetizacao_2028 AS FLOAT64) AS meta_2028,
              CAST(m.meta_alfabetizacao_2029 AS FLOAT64) AS meta_2029,
              CAST(m.meta_alfabetizacao_2030 AS FLOAT64) AS meta_2030,
              CAST(m.nivel_alfabetizacao AS INT64) AS nivel_alfabetizacao,
              CAST(m.percentual_participacao AS FLOAT64) AS percentual_participacao,
              CURRENT_TIMESTAMP() AS _processed_at
            FROM {meta_municipio} m
            INNER JOIN `{project}.{silver}.dim_localidades` loc
              ON LPAD(CAST(m.id_municipio AS STRING), 7, '0') = loc.id_municipio
        """,
        "quarentena_meta_alfabetizacao_municipio": f"""
            CREATE OR REPLACE TABLE `{project}.{silver}.quarentena_meta_alfabetizacao_municipio` AS
            SELECT
              m.*,
              'id_municipio não encontrado em dim_localidades' AS _motivo_quarentena,
              CURRENT_TIMESTAMP() AS _quarantined_at
            FROM {meta_municipio} m
            LEFT JOIN `{project}.{silver}.dim_localidades` loc
              ON LPAD(CAST(m.id_municipio AS STRING), 7, '0') = loc.id_municipio
            WHERE loc.id_municipio IS NULL
        """,
        "avaliacao_alfabetizacao_uf": f"""
            CREATE OR REPLACE TABLE `{project}.{silver}.avaliacao_alfabetizacao_uf`
            PARTITION BY RANGE_BUCKET(ano, GENERATE_ARRAY(2020, 2040, 1))
            CLUSTER BY sigla_uf AS
            SELECT
              CAST(ano AS INT64) AS ano,
              UPPER(TRIM(sigla_uf)) AS sigla_uf,
              CAST(serie AS INT64) AS serie,
              {_rede_label_case("rede")} AS rede,
              CAST(taxa_alfabetizacao AS FLOAT64) AS taxa_alfabetizacao,
              CAST(media_portugues AS FLOAT64) AS media_portugues,
              CAST(proporcao_aluno_nivel_0 AS FLOAT64) AS proporcao_aluno_nivel_0,
              CAST(proporcao_aluno_nivel_1 AS FLOAT64) AS proporcao_aluno_nivel_1,
              CAST(proporcao_aluno_nivel_2 AS FLOAT64) AS proporcao_aluno_nivel_2,
              CAST(proporcao_aluno_nivel_3 AS FLOAT64) AS proporcao_aluno_nivel_3,
              CAST(proporcao_aluno_nivel_4 AS FLOAT64) AS proporcao_aluno_nivel_4,
              CAST(proporcao_aluno_nivel_5 AS FLOAT64) AS proporcao_aluno_nivel_5,
              CAST(proporcao_aluno_nivel_6 AS FLOAT64) AS proporcao_aluno_nivel_6,
              CAST(proporcao_aluno_nivel_7 AS FLOAT64) AS proporcao_aluno_nivel_7,
              CAST(proporcao_aluno_nivel_8 AS FLOAT64) AS proporcao_aluno_nivel_8,
              CURRENT_TIMESTAMP() AS _processed_at
            FROM {avaliacao_uf}
        """,
        "avaliacao_alfabetizacao_municipio": f"""
            CREATE OR REPLACE TABLE `{project}.{silver}.avaliacao_alfabetizacao_municipio`
            PARTITION BY RANGE_BUCKET(ano, GENERATE_ARRAY(2020, 2040, 1))
            CLUSTER BY sigla_uf, id_municipio AS
            SELECT
              CAST(m.ano AS INT64) AS ano,
              LPAD(CAST(m.id_municipio AS STRING), 7, '0') AS id_municipio,
              loc.sigla_uf AS sigla_uf,
              CAST(m.serie AS INT64) AS serie,
              {_rede_label_case("m.rede")} AS rede,
              CAST(m.taxa_alfabetizacao AS FLOAT64) AS taxa_alfabetizacao,
              CAST(m.media_portugues AS FLOAT64) AS media_portugues,
              CAST(m.proporcao_aluno_nivel_0 AS FLOAT64) AS proporcao_aluno_nivel_0,
              CAST(m.proporcao_aluno_nivel_1 AS FLOAT64) AS proporcao_aluno_nivel_1,
              CAST(m.proporcao_aluno_nivel_2 AS FLOAT64) AS proporcao_aluno_nivel_2,
              CAST(m.proporcao_aluno_nivel_3 AS FLOAT64) AS proporcao_aluno_nivel_3,
              CAST(m.proporcao_aluno_nivel_4 AS FLOAT64) AS proporcao_aluno_nivel_4,
              CAST(m.proporcao_aluno_nivel_5 AS FLOAT64) AS proporcao_aluno_nivel_5,
              CAST(m.proporcao_aluno_nivel_6 AS FLOAT64) AS proporcao_aluno_nivel_6,
              CAST(m.proporcao_aluno_nivel_7 AS FLOAT64) AS proporcao_aluno_nivel_7,
              CAST(m.proporcao_aluno_nivel_8 AS FLOAT64) AS proporcao_aluno_nivel_8,
              CURRENT_TIMESTAMP() AS _processed_at
            FROM {avaliacao_municipio} m
            INNER JOIN `{project}.{silver}.dim_localidades` loc
              ON LPAD(CAST(m.id_municipio AS STRING), 7, '0') = loc.id_municipio
        """,
        "quarentena_avaliacao_alfabetizacao_municipio": f"""
            CREATE OR REPLACE TABLE `{project}.{silver}.quarentena_avaliacao_alfabetizacao_municipio` AS
            SELECT
              m.*,
              'id_municipio não encontrado em dim_localidades' AS _motivo_quarentena,
              CURRENT_TIMESTAMP() AS _quarantined_at
            FROM {avaliacao_municipio} m
            LEFT JOIN `{project}.{silver}.dim_localidades` loc
              ON LPAD(CAST(m.id_municipio AS STRING), 7, '0') = loc.id_municipio
            WHERE loc.id_municipio IS NULL
        """,
        # dados_alunos: os microdados COMPLETOS (~3,87M linhas), vindos
        # da carga batch. Esta é a tabela autoritativa de aluno - é dela
        # que a Gold calcula o indicador de alfabetização. Como a Bronze
        # é append-only e cada execução grava um snapshot inteiro, aqui
        # vale o _latest_snapshot (diferente da tabela de streaming, em
        # que cada linha é um evento independente).
        "dados_alunos": f"""
            CREATE OR REPLACE TABLE `{project}.{silver}.dados_alunos`
            PARTITION BY RANGE_BUCKET(ano, GENERATE_ARRAY(2020, 2040, 1))
            CLUSTER BY sigla_uf, id_municipio AS
            WITH RankedAlunos AS (
              SELECT
                CAST(ano AS INT64) AS ano,
                LPAD(CAST(id_municipio AS STRING), 7, '0') AS id_municipio,
                TRIM(id_escola) AS id_escola,
                TRIM(id_aluno) AS id_aluno,
                CAST(caderno AS INT64) AS caderno,
                CAST(serie AS INT64) AS serie,
                -- Na carga batch, `rede` vem como STRING direto da fonte
                -- (no caminho streaming o coerce_row já converte para
                -- INT64), por isso o CAST explícito antes do mapeamento.
                {_rede_label_case("CAST(rede AS INT64)")} AS rede,
                CAST(presenca AS INT64) AS presenca,
                CAST(preenchimento_caderno AS INT64) AS preenchimento_caderno,
                CAST(alfabetizado AS INT64) AS alfabetizado,
                CAST(proficiencia AS FLOAT64) AS proficiencia,
                CAST(peso_aluno AS FLOAT64) AS peso_aluno,
                ROW_NUMBER() OVER (
                  PARTITION BY id_aluno, ano
                  ORDER BY _ingested_at DESC
                ) as rn
              FROM {alunos_batch}
            )
            SELECT
              a.ano,
              a.id_municipio,
              loc.sigla_uf AS sigla_uf,
              a.id_escola,
              a.id_aluno,
              a.caderno,
              a.serie,
              a.rede,
              a.presenca,
              a.preenchimento_caderno,
              a.alfabetizado,
              a.proficiencia,
              a.peso_aluno,
              CURRENT_TIMESTAMP() AS _processed_at
            FROM RankedAlunos a
            INNER JOIN `{project}.{silver}.dim_localidades` loc
              ON a.id_municipio = loc.id_municipio
            WHERE a.rn = 1
        """,
        "quarentena_dados_alunos": f"""
            CREATE OR REPLACE TABLE `{project}.{silver}.quarentena_dados_alunos` AS
            WITH RankedAlunos AS (
              SELECT
                *,
                LPAD(CAST(id_municipio AS STRING), 7, '0') AS id_municipio_padded,
                ROW_NUMBER() OVER (
                  PARTITION BY id_aluno, ano
                  ORDER BY _ingested_at DESC
                ) as rn
              FROM {alunos_batch}
            )
            SELECT
              a.* EXCEPT(id_municipio_padded, rn),
              'id_municipio não encontrado em dim_localidades' AS _motivo_quarentena,
              CURRENT_TIMESTAMP() AS _quarantined_at
            FROM RankedAlunos a
            LEFT JOIN `{project}.{silver}.dim_localidades` loc
              ON a.id_municipio_padded = loc.id_municipio
            WHERE a.rn = 1 AND loc.id_municipio IS NULL
        """,
        # dados_alunos_streaming: cada linha na Bronze já é um evento
        # individual (não um snapshot completo), então aqui não se
        # aplica _latest_snapshot - a deduplicação por id_aluno/ano
        # continua sendo o que decide qual versão do evento vale.
        # Esta tabela mede FRESCOR do caminho streaming; o indicador
        # de alfabetização sai de `dados_alunos` (carga completa).
        "dados_alunos_streaming": f"""
            CREATE OR REPLACE TABLE `{project}.{silver}.dados_alunos_streaming`
            PARTITION BY RANGE_BUCKET(ano, GENERATE_ARRAY(2020, 2040, 1))
            CLUSTER BY sigla_uf, id_municipio AS
            WITH RankedAlunos AS (
              SELECT
                CAST(ano AS INT64) AS ano,
                LPAD(CAST(id_municipio AS STRING), 7, '0') AS id_municipio,
                TRIM(id_escola) AS id_escola,
                TRIM(id_aluno) AS id_aluno,
                CAST(caderno AS INT64) AS caderno,
                CAST(serie AS INT64) AS serie,
                {_rede_label_case("rede")} AS rede,
                CAST(presenca AS INT64) AS presenca,
                CAST(preenchimento_caderno AS INT64) AS preenchimento_caderno,
                CAST(alfabetizado AS INT64) AS alfabetizado,
                CAST(proficiencia AS FLOAT64) AS proficiencia,
                CAST(peso_aluno AS FLOAT64) AS peso_aluno,
                ROW_NUMBER() OVER (
                  PARTITION BY id_aluno, ano
                  ORDER BY _ingested_at DESC
                ) as rn
              FROM `{project}.{bronze}.dados_alunos_streaming`
            )
            SELECT
              a.ano,
              a.id_municipio,
              loc.sigla_uf AS sigla_uf,
              a.id_escola,
              a.id_aluno,
              a.caderno,
              a.serie,
              a.rede,
              a.presenca,
              a.preenchimento_caderno,
              a.alfabetizado,
              a.proficiencia,
              a.peso_aluno,
              CURRENT_TIMESTAMP() AS _processed_at
            FROM RankedAlunos a
            INNER JOIN `{project}.{silver}.dim_localidades` loc
              ON a.id_municipio = loc.id_municipio
            WHERE a.rn = 1
        """,
        "quarentena_dados_alunos_streaming": f"""
            CREATE OR REPLACE TABLE `{project}.{silver}.quarentena_dados_alunos_streaming` AS
            WITH RankedAlunos AS (
              SELECT
                *,
                LPAD(CAST(id_municipio AS STRING), 7, '0') AS id_municipio_padded,
                ROW_NUMBER() OVER (
                  PARTITION BY id_aluno, ano
                  ORDER BY _ingested_at DESC
                ) as rn
              FROM `{project}.{bronze}.dados_alunos_streaming`
            )
            SELECT
              a.* EXCEPT(id_municipio_padded, rn),
              'id_municipio não encontrado em dim_localidades' AS _motivo_quarentena,
              CURRENT_TIMESTAMP() AS _quarantined_at
            FROM RankedAlunos a
            LEFT JOIN `{project}.{silver}.dim_localidades` loc
              ON a.id_municipio_padded = loc.id_municipio
            WHERE a.rn = 1 AND loc.id_municipio IS NULL
        """,
    }


def execute_transformations() -> None:
    settings = load_settings()
    client = bigquery.Client(project=settings.project_id)
    queries = get_queries(settings.project_id, settings.dataset_bronze, settings.dataset_silver)

    # dim_localidades DEVE rodar primeiro pois outras tabelas dependem dela para JOIN.
    # As tabelas de quarentena rodam logo depois da tabela Silver correspondente,
    # reaproveitando o mesmo filtro de snapshot mais recente.
    order = [
        "dim_localidades",
        "meta_alfabetizacao_brasil",
        "meta_alfabetizacao_uf",
        "meta_alfabetizacao_municipio",
        "quarentena_meta_alfabetizacao_municipio",
        "avaliacao_alfabetizacao_uf",
        "avaliacao_alfabetizacao_municipio",
        "quarentena_avaliacao_alfabetizacao_municipio",
        "dados_alunos",
        "quarentena_dados_alunos",
        "dados_alunos_streaming",
        "quarentena_dados_alunos_streaming",
    ]

    for table_name in order:
        logger.info("Iniciando processamento da tabela Silver: %s", table_name)
        query = queries[table_name]
        query_job = client.query(query)
        query_job.result()  # Aguarda a conclusão da query
        logger.info("Tabela Silver criada com sucesso: %s", table_name)


if __name__ == "__main__":
    execute_transformations()
