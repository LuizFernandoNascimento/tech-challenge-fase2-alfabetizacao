"""Executa as transformações SQL para construir a Camada Gold a partir da Silver.
Todas as transformações rodam diretamente no BigQuery para máxima eficiência de custo/tempo (FinOps).
A camada Gold consolidará três visões analíticas principais (marts):
1. mart_comparativo_municipio: visão comparativa no nível de município/rede/ano (oficial vs. streaming vs. metas).
2. mart_comparativo_uf: visão comparativa no nível de UF/rede/ano.
3. mart_comparativo_brasil: visão comparativa no nível nacional de rede/ano.
"""
import logging
from google.cloud import bigquery
from bronze.config import load_settings
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
def get_queries(project: str, silver: str, gold: str) -> dict[str, str]:
    """Retorna o dicionário de queries SQL de criação das tabelas da camada Gold."""
    return {
        "mart_comparativo_municipio": f"""
            CREATE OR REPLACE TABLE `{project}.{gold}.mart_comparativo_municipio`
            PARTITION BY RANGE_BUCKET(ano, GENERATE_ARRAY(2020, 2040, 1))
            CLUSTER BY sigla_uf, id_municipio AS
            WITH streaming_agg AS (
              SELECT
                ano,
                id_municipio,
                rede,
                COUNT(id_aluno) AS total_alunos_streaming,
                SUM(presenca) AS total_presentes_streaming,
                SUM(preenchimento_caderno) AS total_validos_streaming,
                SUM(alfabetizado) AS total_alfabetizados_streaming,
                SAFE_DIVIDE(SUM(alfabetizado), SUM(preenchimento_caderno)) * 100 AS taxa_alfabetizacao_streaming,
                AVG(proficiencia) AS media_proficiencia_streaming
              FROM `{project}.{silver}.dados_alunos_streaming`
              GROUP BY 1, 2, 3
            ),
            combined_keys AS (
              SELECT DISTINCT ano, id_municipio, rede FROM `{project}.{silver}.avaliacao_alfabetizacao_municipio`
              UNION DISTINCT
              SELECT DISTINCT ano, id_municipio, rede FROM `{project}.{silver}.meta_alfabetizacao_municipio`
              UNION DISTINCT
              SELECT DISTINCT ano, id_municipio, rede FROM streaming_agg
            ),
            flat_metas AS (
              SELECT
                ano,
                id_municipio,
                rede,
                nivel_alfabetizacao,
                percentual_participacao AS percentual_participacao_meta,
                taxa_alfabetizacao AS taxa_alfabetizacao_base_meta,
                CASE
                  WHEN ano = 2024 THEN meta_2024
                  WHEN ano = 2025 THEN meta_2025
                  WHEN ano = 2026 THEN meta_2026
                  WHEN ano = 2027 THEN meta_2027
                  WHEN ano = 2028 THEN meta_2028
                  WHEN ano = 2029 THEN meta_2029
                  WHEN ano = 2030 THEN meta_2030
                  ELSE NULL
                END AS meta_taxa_alfabetizacao
              FROM `{project}.{silver}.meta_alfabetizacao_municipio`
            )
            SELECT
              k.ano,
              k.id_municipio,
              loc.nome_municipio,
              loc.sigla_uf,
              loc.nome_uf,
              loc.nome_regiao,
              k.rede,
              
              -- Avaliação Oficial
              a.taxa_alfabetizacao AS taxa_alfabetizacao_real,
              a.media_portugues AS media_portugues_real,
              
              -- Meta
              m.meta_taxa_alfabetizacao,
              m.taxa_alfabetizacao_base_meta,
              m.nivel_alfabetizacao,
              
              -- Comparativo Oficial vs Meta
              a.taxa_alfabetizacao - m.meta_taxa_alfabetizacao AS desvio_meta_real,
              CASE
                WHEN a.taxa_alfabetizacao IS NOT NULL AND m.meta_taxa_alfabetizacao IS NOT NULL THEN
                  a.taxa_alfabetizacao >= m.meta_taxa_alfabetizacao
                ELSE NULL
              END AS atingiu_meta_real,
              
              -- Dados Streaming agregados
              s.total_alunos_streaming,
              s.total_presentes_streaming,
              s.total_validos_streaming,
              s.total_alfabetizados_streaming,
              s.taxa_alfabetizacao_streaming,
              s.media_proficiencia_streaming,
              
              -- Comparativo Streaming vs Meta
              s.taxa_alfabetizacao_streaming - m.meta_taxa_alfabetizacao AS desvio_meta_streaming,
              CASE
                WHEN s.taxa_alfabetizacao_streaming IS NOT NULL AND m.meta_taxa_alfabetizacao IS NOT NULL THEN
                  s.taxa_alfabetizacao_streaming >= m.meta_taxa_alfabetizacao
                ELSE NULL
              END AS atingiu_meta_streaming,
              
              CURRENT_TIMESTAMP() AS _processed_at
            FROM combined_keys k
            INNER JOIN `{project}.{silver}.dim_localidades` loc
              ON k.id_municipio = loc.id_municipio
            LEFT JOIN `{project}.{silver}.avaliacao_alfabetizacao_municipio` a
              ON k.ano = a.ano AND k.id_municipio = a.id_municipio AND k.rede = a.rede
            LEFT JOIN flat_metas m
              ON k.ano = m.ano AND k.id_municipio = m.id_municipio AND k.rede = m.rede
            LEFT JOIN streaming_agg s
              ON k.ano = s.ano AND k.id_municipio = s.id_municipio AND k.rede = s.rede
        """,
        "mart_comparativo_uf": f"""
            CREATE OR REPLACE TABLE `{project}.{gold}.mart_comparativo_uf`
            PARTITION BY RANGE_BUCKET(ano, GENERATE_ARRAY(2020, 2040, 1))
            CLUSTER BY sigla_uf AS
            WITH local_uf AS (
              SELECT DISTINCT sigla_uf, nome_uf, nome_regiao FROM `{project}.{silver}.dim_localidades`
            ),
            streaming_agg AS (
              SELECT
                ano,
                sigla_uf,
                rede,
                COUNT(id_aluno) AS total_alunos_streaming,
                SUM(presenca) AS total_presentes_streaming,
                SUM(preenchimento_caderno) AS total_validos_streaming,
                SUM(alfabetizado) AS total_alfabetizados_streaming,
                SAFE_DIVIDE(SUM(alfabetizado), SUM(preenchimento_caderno)) * 100 AS taxa_alfabetizacao_streaming,
                AVG(proficiencia) AS media_proficiencia_streaming
              FROM `{project}.{silver}.dados_alunos_streaming`
              GROUP BY 1, 2, 3
            ),
            combined_keys AS (
              SELECT DISTINCT ano, sigla_uf, rede FROM `{project}.{silver}.avaliacao_alfabetizacao_uf`
              UNION DISTINCT
              SELECT DISTINCT ano, sigla_uf, rede FROM `{project}.{silver}.meta_alfabetizacao_uf`
              UNION DISTINCT
              SELECT DISTINCT ano, sigla_uf, rede FROM streaming_agg
            ),
            flat_metas AS (
              SELECT
                ano,
                sigla_uf,
                rede,
                percentual_participacao AS percentual_participacao_meta,
                taxa_alfabetizacao AS taxa_alfabetizacao_base_meta,
                CASE
                  WHEN ano = 2024 THEN meta_2024
                  WHEN ano = 2025 THEN meta_2025
                  WHEN ano = 2026 THEN meta_2026
                  WHEN ano = 2027 THEN meta_2027
                  WHEN ano = 2028 THEN meta_2028
                  WHEN ano = 2029 THEN meta_2029
                  WHEN ano = 2030 THEN meta_2030
                  ELSE NULL
                END AS meta_taxa_alfabetizacao
              FROM `{project}.{silver}.meta_alfabetizacao_uf`
            )
            SELECT
              k.ano,
              k.sigla_uf,
              loc.nome_uf,
              loc.nome_regiao,
              k.rede,
              
              -- Avaliação Oficial
              a.taxa_alfabetizacao AS taxa_alfabetizacao_real,
              a.media_portugues AS media_portugues_real,
              
              -- Meta
              m.meta_taxa_alfabetizacao,
              m.taxa_alfabetizacao_base_meta,
              
              -- Comparativo Oficial vs Meta
              a.taxa_alfabetizacao - m.meta_taxa_alfabetizacao AS desvio_meta_real,
              CASE
                WHEN a.taxa_alfabetizacao IS NOT NULL AND m.meta_taxa_alfabetizacao IS NOT NULL THEN
                  a.taxa_alfabetizacao >= m.meta_taxa_alfabetizacao
                ELSE NULL
              END AS atingiu_meta_real,
              
              -- Dados Streaming agregados
              s.total_alunos_streaming,
              s.total_presentes_streaming,
              s.total_validos_streaming,
              s.total_alfabetizados_streaming,
              s.taxa_alfabetizacao_streaming,
              s.media_proficiencia_streaming,
              
              -- Comparativo Streaming vs Meta
              s.taxa_alfabetizacao_streaming - m.meta_taxa_alfabetizacao AS desvio_meta_streaming,
              CASE
                WHEN s.taxa_alfabetizacao_streaming IS NOT NULL AND m.meta_taxa_alfabetizacao IS NOT NULL THEN
                  s.taxa_alfabetizacao_streaming >= m.meta_taxa_alfabetizacao
                ELSE NULL
              END AS atingiu_meta_streaming,
              
              CURRENT_TIMESTAMP() AS _processed_at
            FROM combined_keys k
            LEFT JOIN local_uf loc
              ON k.sigla_uf = loc.sigla_uf
            LEFT JOIN `{project}.{silver}.avaliacao_alfabetizacao_uf` a
              ON k.ano = a.ano AND k.sigla_uf = a.sigla_uf AND k.rede = a.rede
            LEFT JOIN flat_metas m
              ON k.ano = m.ano AND k.sigla_uf = m.sigla_uf AND k.rede = m.rede
            LEFT JOIN streaming_agg s
              ON k.ano = s.ano AND k.sigla_uf = s.sigla_uf AND k.rede = s.rede
        """,
        "mart_comparativo_brasil": f"""
            CREATE OR REPLACE TABLE `{project}.{gold}.mart_comparativo_brasil` AS
            WITH streaming_agg AS (
              SELECT
                ano,
                rede,
                COUNT(id_aluno) AS total_alunos_streaming,
                SUM(presenca) AS total_presentes_streaming,
                SUM(preenchimento_caderno) AS total_validos_streaming,
                SUM(alfabetizado) AS total_alfabetizados_streaming,
                SAFE_DIVIDE(SUM(alfabetizado), SUM(preenchimento_caderno)) * 100 AS taxa_alfabetizacao_streaming,
                AVG(proficiencia) AS media_proficiencia_streaming
              FROM `{project}.{silver}.dados_alunos_streaming`
              GROUP BY 1, 2
            ),
            combined_keys AS (
              SELECT DISTINCT ano, rede FROM `{project}.{silver}.meta_alfabetizacao_brasil`
              UNION DISTINCT
              SELECT DISTINCT ano, rede FROM streaming_agg
            ),
            flat_metas AS (
              SELECT
                ano,
                rede,
                percentual_participacao AS percentual_participacao_meta,
                taxa_alfabetizacao AS taxa_alfabetizacao_real,
                CASE
                  WHEN ano = 2024 THEN meta_2024
                  WHEN ano = 2025 THEN meta_2025
                  WHEN ano = 2026 THEN meta_2026
                  WHEN ano = 2027 THEN meta_2027
                  WHEN ano = 2028 THEN meta_2028
                  WHEN ano = 2029 THEN meta_2029
                  WHEN ano = 2030 THEN meta_2030
                  ELSE NULL
                END AS meta_taxa_alfabetizacao
              FROM `{project}.{silver}.meta_alfabetizacao_brasil`
            )
            SELECT
              k.ano,
              k.rede,
              
              -- Avaliação Oficial
              m.taxa_alfabetizacao_real,
              
              -- Meta
              m.meta_taxa_alfabetizacao,
              
              -- Comparativo Oficial vs Meta
              m.taxa_alfabetizacao_real - m.meta_taxa_alfabetizacao AS desvio_meta_real,
              CASE
                WHEN m.taxa_alfabetizacao_real IS NOT NULL AND m.meta_taxa_alfabetizacao IS NOT NULL THEN
                  m.taxa_alfabetizacao_real >= m.meta_taxa_alfabetizacao
                ELSE NULL
              END AS atingiu_meta_real,
              
              -- Dados Streaming agregados
              s.total_alunos_streaming,
              s.total_presentes_streaming,
              s.total_validos_streaming,
              s.total_alfabetizados_streaming,
              s.taxa_alfabetizacao_streaming,
              s.media_proficiencia_streaming,
              
              -- Comparativo Streaming vs Meta
              s.taxa_alfabetizacao_streaming - m.meta_taxa_alfabetizacao AS desvio_meta_streaming,
              CASE
                WHEN s.taxa_alfabetizacao_streaming IS NOT NULL AND m.meta_taxa_alfabetizacao IS NOT NULL THEN
                  s.taxa_alfabetizacao_streaming >= m.meta_taxa_alfabetizacao
                ELSE NULL
              END AS atingiu_meta_streaming,
              
              CURRENT_TIMESTAMP() AS _processed_at
            FROM combined_keys k
            LEFT JOIN flat_metas m
              ON k.ano = m.ano AND k.rede = m.rede
            LEFT JOIN streaming_agg s
              ON k.ano = s.ano AND k.rede = s.rede
        """
    }
def execute_gold_transformations() -> None:
    settings = load_settings()
    client = bigquery.Client(project=settings.project_id)
    queries = get_queries(settings.project_id, settings.dataset_silver, settings.dataset_gold)
    order = [
        "mart_comparativo_municipio",
        "mart_comparativo_uf",
        "mart_comparativo_brasil",
    ]
    for table_name in order:
        logger.info("Iniciando processamento da tabela Gold: %s", table_name)
        query = queries[table_name]
        query_job = client.query(query)
        query_job.result()  # Aguarda a conclusão da query
        logger.info("Tabela Gold criada com sucesso: %s", table_name)
if __name__ == "__main__":
    execute_gold_transformations()
