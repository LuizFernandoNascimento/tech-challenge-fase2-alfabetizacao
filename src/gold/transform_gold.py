"""Executa as transformações SQL para construir a Camada Gold a partir da Silver.

Todas as transformações rodam diretamente no BigQuery para máxima eficiência de custo/tempo (FinOps).

A camada Gold consolida quatro visões analíticas (marts):
1. mart_comparativo_municipio: comparativo no nível de município/rede/ano (oficial vs. microdados vs. metas).
2. mart_comparativo_uf: comparativo no nível de UF/rede/ano.
3. mart_comparativo_brasil: comparativo no nível nacional de rede/ano.
4. mart_frescor_streaming: saúde e latência do caminho de ingestão streaming.

Distinção importante entre as duas fontes de aluno
--------------------------------------------------
Os microdados de aluno chegam por dois caminhos, e eles NÃO servem
para a mesma coisa:

- `silver.dados_alunos` (carga BATCH completa, ~3,87M linhas) é a
  fonte autoritativa. Todo agregado de aluno nos marts comparativos
  (colunas com sufixo `_microdados`) sai daqui.
- `silver.dados_alunos_streaming` é uma AMOSTRA por natureza - o
  produtor decide quantos eventos replaya. Usar essa amostra para
  calcular taxa de alfabetização por município produziria um
  indicador não representativo. Por isso ela alimenta apenas o
  `mart_frescor_streaming`, que mede o que a amostra realmente
  consegue medir: volume ingerido e latência do caminho streaming.
"""
import logging
from google.cloud import bigquery
from bronze.config import load_settings
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
# Volumetria em contagem simples; indicadores PONDERADOS por `peso_aluno`.
#
# O Indicador Criança Alfabetizada do INEP é calculado com o peso amostral
# de cada aluno. Reproduzir isso não é preciosismo: medimos as duas formas
# contra a taxa oficial publicada por UF e o erro médio absoluto caiu de
# 0,54 p.p. (sem peso) para 0,02 p.p. (com peso) - ou seja, ponderando a
# pipeline reconstrói o indicador oficial. O mesmo vale para a proficiência
# média (0,616 -> 0,030).
#
# Alunos sem `peso_aluno` (ausentes, ~508k linhas) saem naturalmente do
# cálculo, porque NULL propaga no produto e o SUM ignora - é o
# comportamento correto, já que eles não foram avaliados.
_MICRODADOS_METRICS = """                COUNT(id_aluno) AS total_alunos_microdados,
                SUM(presenca) AS total_presentes_microdados,
                SUM(preenchimento_caderno) AS total_validos_microdados,
                SUM(alfabetizado) AS total_alfabetizados_microdados,
                SAFE_DIVIDE(
                  SUM(alfabetizado * peso_aluno),
                  SUM(preenchimento_caderno * peso_aluno)
                ) * 100 AS taxa_alfabetizacao_microdados,
                SAFE_DIVIDE(
                  SUM(proficiencia * peso_aluno),
                  SUM(IF(proficiencia IS NULL, NULL, peso_aluno))
                ) AS media_proficiencia_microdados"""


def _microdados_agg(project: str, silver: str, dims: tuple[str, ...]) -> str:
    """Corpo do CTE que agrega os microdados de aluno por rede.

    Além da quebra por rede específica (Municipal, Estadual, Privada),
    produz um rollup "Pública" = Estadual + Municipal, conforme a tabela
    `dicionario` da Base dos Dados (código 5 = "Pública (Estadual e
    Municipal)").

    Esse rollup não é cosmético: as metas de UF e Brasil só existem para
    a rede "Pública" agregada. Sem ele, os agregados de microdados nesses
    dois níveis nunca casariam com nenhuma meta - as colunas existiriam
    mas seriam impossíveis de comparar.
    """
    dim_select = "".join(f"{d},\n                " for d in dims)
    n_dims = len(dims)
    group_by = ", ".join(str(i) for i in range(1, n_dims + 3))  # ano + dims + rede
    group_by_rollup = ", ".join(str(i) for i in range(1, n_dims + 2))  # ano + dims
    return f"""
              SELECT
                ano,
                {dim_select}rede,
{_MICRODADOS_METRICS}
              FROM `{project}.{silver}.dados_alunos`
              GROUP BY {group_by}
              UNION ALL
              SELECT
                ano,
                {dim_select}'Pública' AS rede,
{_MICRODADOS_METRICS}
              FROM `{project}.{silver}.dados_alunos`
              WHERE rede IN ('Estadual', 'Municipal')
              GROUP BY {group_by_rollup}
    """


def get_queries(project: str, silver: str, gold: str) -> dict[str, str]:
    """Retorna o dicionário de queries SQL de criação das tabelas da camada Gold."""
    return {
        "mart_comparativo_municipio": f"""
            CREATE OR REPLACE TABLE `{project}.{gold}.mart_comparativo_municipio`
            PARTITION BY RANGE_BUCKET(ano, GENERATE_ARRAY(2020, 2040, 1))
            CLUSTER BY sigla_uf, id_municipio AS
            WITH microdados_agg AS (
{_microdados_agg(project, silver, ("id_municipio",))}
            ),
            combined_keys AS (
              SELECT DISTINCT ano, id_municipio, rede FROM `{project}.{silver}.avaliacao_alfabetizacao_municipio`
              UNION DISTINCT
              SELECT DISTINCT ano, id_municipio, rede FROM `{project}.{silver}.meta_alfabetizacao_municipio`
              UNION DISTINCT
              SELECT DISTINCT ano, id_municipio, rede FROM microdados_agg
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
              
              -- Agregados dos microdados de aluno (carga batch completa)
              s.total_alunos_microdados,
              s.total_presentes_microdados,
              s.total_validos_microdados,
              s.total_alfabetizados_microdados,
              s.taxa_alfabetizacao_microdados,
              s.media_proficiencia_microdados,
              
              -- Comparativo Microdados vs Meta
              s.taxa_alfabetizacao_microdados - m.meta_taxa_alfabetizacao AS desvio_meta_microdados,
              CASE
                WHEN s.taxa_alfabetizacao_microdados IS NOT NULL AND m.meta_taxa_alfabetizacao IS NOT NULL THEN
                  s.taxa_alfabetizacao_microdados >= m.meta_taxa_alfabetizacao
                ELSE NULL
              END AS atingiu_meta_microdados,
              
              CURRENT_TIMESTAMP() AS _processed_at
            FROM combined_keys k
            INNER JOIN `{project}.{silver}.dim_localidades` loc
              ON k.id_municipio = loc.id_municipio
            LEFT JOIN `{project}.{silver}.avaliacao_alfabetizacao_municipio` a
              ON k.ano = a.ano AND k.id_municipio = a.id_municipio AND k.rede = a.rede
            LEFT JOIN flat_metas m
              ON k.ano = m.ano AND k.id_municipio = m.id_municipio AND k.rede = m.rede
            LEFT JOIN microdados_agg s
              ON k.ano = s.ano AND k.id_municipio = s.id_municipio AND k.rede = s.rede
        """,
        "mart_comparativo_uf": f"""
            CREATE OR REPLACE TABLE `{project}.{gold}.mart_comparativo_uf`
            PARTITION BY RANGE_BUCKET(ano, GENERATE_ARRAY(2020, 2040, 1))
            CLUSTER BY sigla_uf AS
            WITH local_uf AS (
              SELECT DISTINCT sigla_uf, nome_uf, nome_regiao FROM `{project}.{silver}.dim_localidades`
            ),
            microdados_agg AS (
{_microdados_agg(project, silver, ("sigla_uf",))}
            ),
            combined_keys AS (
              SELECT DISTINCT ano, sigla_uf, rede FROM `{project}.{silver}.avaliacao_alfabetizacao_uf`
              UNION DISTINCT
              SELECT DISTINCT ano, sigla_uf, rede FROM `{project}.{silver}.meta_alfabetizacao_uf`
              UNION DISTINCT
              SELECT DISTINCT ano, sigla_uf, rede FROM microdados_agg
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
              
              -- Agregados dos microdados de aluno (carga batch completa)
              s.total_alunos_microdados,
              s.total_presentes_microdados,
              s.total_validos_microdados,
              s.total_alfabetizados_microdados,
              s.taxa_alfabetizacao_microdados,
              s.media_proficiencia_microdados,
              
              -- Comparativo Microdados vs Meta
              s.taxa_alfabetizacao_microdados - m.meta_taxa_alfabetizacao AS desvio_meta_microdados,
              CASE
                WHEN s.taxa_alfabetizacao_microdados IS NOT NULL AND m.meta_taxa_alfabetizacao IS NOT NULL THEN
                  s.taxa_alfabetizacao_microdados >= m.meta_taxa_alfabetizacao
                ELSE NULL
              END AS atingiu_meta_microdados,
              
              CURRENT_TIMESTAMP() AS _processed_at
            FROM combined_keys k
            LEFT JOIN local_uf loc
              ON k.sigla_uf = loc.sigla_uf
            LEFT JOIN `{project}.{silver}.avaliacao_alfabetizacao_uf` a
              ON k.ano = a.ano AND k.sigla_uf = a.sigla_uf AND k.rede = a.rede
            LEFT JOIN flat_metas m
              ON k.ano = m.ano AND k.sigla_uf = m.sigla_uf AND k.rede = m.rede
            LEFT JOIN microdados_agg s
              ON k.ano = s.ano AND k.sigla_uf = s.sigla_uf AND k.rede = s.rede
        """,
        "mart_comparativo_brasil": f"""
            CREATE OR REPLACE TABLE `{project}.{gold}.mart_comparativo_brasil` AS
            WITH microdados_agg AS (
{_microdados_agg(project, silver, ())}
            ),
            combined_keys AS (
              SELECT DISTINCT ano, rede FROM `{project}.{silver}.meta_alfabetizacao_brasil`
              UNION DISTINCT
              SELECT DISTINCT ano, rede FROM microdados_agg
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
              
              -- Agregados dos microdados de aluno (carga batch completa)
              s.total_alunos_microdados,
              s.total_presentes_microdados,
              s.total_validos_microdados,
              s.total_alfabetizados_microdados,
              s.taxa_alfabetizacao_microdados,
              s.media_proficiencia_microdados,
              
              -- Comparativo Microdados vs Meta
              s.taxa_alfabetizacao_microdados - m.meta_taxa_alfabetizacao AS desvio_meta_microdados,
              CASE
                WHEN s.taxa_alfabetizacao_microdados IS NOT NULL AND m.meta_taxa_alfabetizacao IS NOT NULL THEN
                  s.taxa_alfabetizacao_microdados >= m.meta_taxa_alfabetizacao
                ELSE NULL
              END AS atingiu_meta_microdados,
              
              CURRENT_TIMESTAMP() AS _processed_at
            FROM combined_keys k
            LEFT JOIN flat_metas m
              ON k.ano = m.ano AND k.rede = m.rede
            LEFT JOIN microdados_agg s
              ON k.ano = s.ano AND k.rede = s.rede
        """,
        # Mart de observabilidade do caminho streaming. Não calcula
        # indicador educacional (a amostra não é representativa) -
        # mede a saúde da ingestão: volume, cobertura e latência entre
        # o evento ser gravado na Bronze e chegar à Silver.
        "mart_frescor_streaming": f"""
            CREATE OR REPLACE TABLE `{project}.{gold}.mart_frescor_streaming` AS
            SELECT
              ano,
              COUNT(*) AS eventos_ingeridos,
              COUNT(DISTINCT id_aluno) AS alunos_distintos,
              COUNT(DISTINCT id_municipio) AS municipios_cobertos,
              MIN(_processed_at) AS primeiro_processamento,
              MAX(_processed_at) AS ultimo_processamento,
              -- Cobertura da amostra streaming sobre a base completa:
              -- deixa explícito que o caminho streaming é uma amostra.
              SAFE_DIVIDE(
                COUNT(DISTINCT id_aluno),
                (SELECT COUNT(DISTINCT id_aluno) FROM `{project}.{silver}.dados_alunos`)
              ) * 100 AS pct_cobertura_sobre_base_completa,
              CURRENT_TIMESTAMP() AS _processed_at
            FROM `{project}.{silver}.dados_alunos_streaming`
            GROUP BY ano
        """,
    }
def execute_gold_transformations() -> None:
    settings = load_settings()
    client = bigquery.Client(project=settings.project_id)
    queries = get_queries(settings.project_id, settings.dataset_silver, settings.dataset_gold)
    order = [
        "mart_comparativo_municipio",
        "mart_comparativo_uf",
        "mart_comparativo_brasil",
        "mart_frescor_streaming",
    ]
    for table_name in order:
        logger.info("Iniciando processamento da tabela Gold: %s", table_name)
        query = queries[table_name]
        query_job = client.query(query)
        query_job.result()  # Aguarda a conclusão da query
        logger.info("Tabela Gold criada com sucesso: %s", table_name)
if __name__ == "__main__":
    execute_gold_transformations()
