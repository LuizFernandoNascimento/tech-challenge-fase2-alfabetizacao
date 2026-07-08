"""Executa validações de qualidade e consistência (QA) na Camada Silver.

Verifica duplicidades, valores nulos em chaves primárias, integridade referencial
e sanidade de limites de valores.
"""
import logging
from google.cloud import bigquery
from bronze.config import load_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def run_check(client: bigquery.Client, query: str, check_name: str, critical: bool = True) -> int:
    """Executa uma query de validação e retorna a contagem de falhas.

    Se critical for True e houver falhas, levanta um erro.
    """
    logger.info("Executando QA: %s", check_name)
    query_job = client.query(query)
    results = query_job.result()

    # As queries de QA devem retornar uma linha com a coluna 'failed_count'
    row = next(iter(results))
    failed_count = row["failed_count"]

    if failed_count > 0:
        msg = f"Falha no teste [{check_name}]: {failed_count} registros violaram a regra."
        if critical:
            logger.error(msg)
            raise ValueError(msg)
        else:
            logger.warning(msg)
    else:
        logger.info("Sucesso no teste [%s]", check_name)

    return failed_count


def validate_silver_layer() -> None:
    settings = load_settings()
    client = bigquery.Client(project=settings.project_id)
    silver = settings.dataset_silver

    # 1. Testes de Unicidade (Chaves Primárias)
    run_check(
        client,
        f"SELECT COUNT(*) - COUNT(DISTINCT id_municipio) AS failed_count FROM `{client.project}.{silver}.dim_localidades`",
        "dim_localidades - unicidade de id_municipio"
    )

    run_check(
        client,
        f"""
        SELECT COUNT(*) - COUNT(DISTINCT CONCAT(COALESCE(id_aluno, ''), '-', CAST(ano AS STRING))) AS failed_count
        FROM `{client.project}.{silver}.dados_alunos_streaming`
        """,
        "dados_alunos_streaming - unicidade de id_aluno por ano"
    )

    run_check(
        client,
        f"""
        SELECT COUNT(*) - COUNT(DISTINCT CONCAT(CAST(ano AS STRING), '-', COALESCE(sigla_uf, ''), '-', COALESCE(rede, ''))) AS failed_count
        FROM `{client.project}.{silver}.meta_alfabetizacao_uf`
        """,
        "meta_alfabetizacao_uf - unicidade de ano + sigla_uf + rede"
    )

    # 2. Testes de Valores Nulos em Colunas Críticas
    run_check(
        client,
        f"""
        SELECT COUNT(*) AS failed_count
        FROM `{client.project}.{silver}.dim_localidades`
        WHERE id_municipio IS NULL OR sigla_uf IS NULL OR nome_municipio IS NULL
        """,
        "dim_localidades - nulos em chaves e colunas obrigatórias"
    )

    run_check(
        client,
        f"""
        SELECT COUNT(*) AS failed_count
        FROM `{client.project}.{silver}.dados_alunos_streaming`
        WHERE id_aluno IS NULL OR id_municipio IS NULL OR ano IS NULL OR alfabetizado IS NULL
        """,
        "dados_alunos_streaming - nulos em colunas obrigatórias"
    )

    # 3. Testes de Integridade Referencial (Chaves Estrangeiras)
    #
    # As tabelas Silver principais agora usam INNER JOIN com
    # dim_localidades (ver transform_silver.py), então essas contagens
    # devem sempre dar zero por construção - mantidas aqui como um
    # teste de regressão barato, caso alguém volte a usar LEFT JOIN.
    run_check(
        client,
        f"""
        SELECT COUNT(*) AS failed_count
        FROM `{client.project}.{silver}.dados_alunos_streaming` a
        LEFT JOIN `{client.project}.{silver}.dim_localidades` l ON a.id_municipio = l.id_municipio
        WHERE l.id_municipio IS NULL
        """,
        "dados_alunos_streaming - id_municipio inexistente em dim_localidades"
    )

    run_check(
        client,
        f"""
        SELECT COUNT(*) AS failed_count
        FROM `{client.project}.{silver}.avaliacao_alfabetizacao_municipio` m
        LEFT JOIN `{client.project}.{silver}.dim_localidades` l ON m.id_municipio = l.id_municipio
        WHERE l.id_municipio IS NULL
        """,
        "avaliacao_alfabetizacao_municipio - id_municipio inexistente em dim_localidades"
    )

    run_check(
        client,
        f"""
        SELECT COUNT(*) AS failed_count
        FROM `{client.project}.{silver}.meta_alfabetizacao_municipio` m
        LEFT JOIN `{client.project}.{silver}.dim_localidades` l ON m.id_municipio = l.id_municipio
        WHERE l.id_municipio IS NULL
        """,
        "meta_alfabetizacao_municipio - id_municipio inexistente em dim_localidades"
    )

    # 3b. Observabilidade das tabelas de quarentena: registros que não
    # casaram com dim_localidades não travam mais o pipeline (ver
    # transform_silver.py), mas o volume de quarentena é reportado
    # como aviso - se crescer muito, é sinal de que a dimensão IBGE
    # precisa de atenção.
    run_check(
        client,
        f"SELECT COUNT(*) AS failed_count FROM `{client.project}.{silver}.quarentena_meta_alfabetizacao_municipio`",
        "meta_alfabetizacao_municipio - volume em quarentena",
        critical=False,
    )

    run_check(
        client,
        f"SELECT COUNT(*) AS failed_count FROM `{client.project}.{silver}.quarentena_avaliacao_alfabetizacao_municipio`",
        "avaliacao_alfabetizacao_municipio - volume em quarentena",
        critical=False,
    )

    run_check(
        client,
        f"SELECT COUNT(*) AS failed_count FROM `{client.project}.{silver}.quarentena_dados_alunos_streaming`",
        "dados_alunos_streaming - volume em quarentena",
        critical=False,
    )

    # 4. Testes de Ranges e Sanidade (Valores Válidos)
    run_check(
        client,
        f"""
        SELECT COUNT(*) AS failed_count
        FROM `{client.project}.{silver}.dados_alunos_streaming`
        WHERE presenca NOT IN (0, 1) OR alfabetizado NOT IN (0, 1)
        """,
        "dados_alunos_streaming - presenca e alfabetizado devem ser 0 ou 1"
    )

    run_check(
        client,
        f"""
        SELECT COUNT(*) AS failed_count
        FROM `{client.project}.{silver}.avaliacao_alfabetizacao_uf`
        WHERE taxa_alfabetizacao < 0.0 OR taxa_alfabetizacao > 100.0
        """,
        "avaliacao_alfabetizacao_uf - taxa_alfabetizacao deve estar entre 0.0 e 100.0",
        critical=False # Aviso, não trava pipeline caso ocorram taxas fora de limites (para tratar erros de fonte de dados)
    )

    logger.info("Todas as validações de qualidade da camada Silver passaram com sucesso!")


if __name__ == "__main__":
    validate_silver_layer()
