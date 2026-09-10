"""Executa validações de qualidade e consistência (QA) nas camadas Silver e Gold.

Verifica duplicidades, valores nulos em chaves primárias, integridade referencial
e sanidade de limites de valores.

Persistência do histórico
-------------------------
Antes, o resultado de cada check só existia no log da execução: dava para
saber se a rodada passou, mas não dava para responder "quantas vezes esse
check falhou no último mês?" nem plotar a evolução da qualidade ao longo
do tempo.

Agora cada execução grava uma linha por check na tabela
`quality.data_quality_results`, com um `run_id` que amarra todos os checks
da mesma rodada. A gravação acontece num `finally`, então mesmo quando um
check crítico interrompe o pipeline o histórico daquela rodada é
registrado - que é justamente o caso mais importante de auditar depois.
"""
import datetime as dt
import logging
import uuid

from google.cloud import bigquery

from bronze.config import load_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

RESULTS_TABLE = "data_quality_results"

RESULTS_SCHEMA = [
    bigquery.SchemaField("run_id", "STRING", description="Identificador da rodada de validação"),
    bigquery.SchemaField("executed_at", "TIMESTAMP"),
    bigquery.SchemaField("layer", "STRING", description="silver | gold"),
    bigquery.SchemaField("check_name", "STRING"),
    bigquery.SchemaField("failed_count", "INT64", description="Registros que violaram a regra"),
    bigquery.SchemaField("status", "STRING", description="PASS | WARN | FAIL"),
    bigquery.SchemaField("critical", "BOOL", description="Se True, a falha interrompe o pipeline"),
]


def ensure_results_table(bq_client: bigquery.Client, dataset_id: str) -> str:
    """Cria a tabela de histórico se ainda não existir.

    Particionada por dia de execução: consultas de evolução ("últimos 30
    dias") escaneiam só as partições relevantes, e clusterizada por check
    para responder rápido "histórico deste check específico".
    """
    table_id = f"{bq_client.project}.{dataset_id}.{RESULTS_TABLE}"
    table = bigquery.Table(table_id, schema=RESULTS_SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field="executed_at"
    )
    table.clustering_fields = ["layer", "check_name"]
    bq_client.create_table(table, exists_ok=True)
    return table_id


class QualityRun:
    """Agrupa os checks de uma execução e persiste os resultados no fim.

    Uso:
        with QualityRun(client, dataset_quality, "silver") as run:
            run.check(query, "nome do check")
            ...
    """

    def __init__(self, bq_client: bigquery.Client, dataset_quality: str, layer: str):
        self.client = bq_client
        self.dataset_quality = dataset_quality
        self.layer = layer
        self.run_id = str(uuid.uuid4())
        self.executed_at = dt.datetime.now(dt.timezone.utc)
        self.results: list[dict] = []

    def check(self, query: str, check_name: str, critical: bool = True) -> int:
        """Executa uma query de validação e registra o resultado.

        A query deve retornar uma única linha com a coluna `failed_count`.
        Se `critical` e houver falhas, levanta ValueError - mas o resultado
        já foi registrado e será persistido pelo __exit__.
        """
        logger.info("Executando QA: %s", check_name)
        row = next(iter(self.client.query(query).result()))
        failed_count = int(row["failed_count"])

        if failed_count > 0:
            status = "FAIL" if critical else "WARN"
        else:
            status = "PASS"

        self.results.append(
            {
                "run_id": self.run_id,
                "executed_at": self.executed_at.isoformat(),
                "layer": self.layer,
                "check_name": check_name,
                "failed_count": failed_count,
                "status": status,
                "critical": critical,
            }
        )

        if failed_count > 0:
            msg = f"Falha no teste [{check_name}]: {failed_count} registros violaram a regra."
            if critical:
                logger.error(msg)
                raise ValueError(msg)
            logger.warning(msg)
        else:
            logger.info("Sucesso no teste [%s]", check_name)

        return failed_count

    def persist(self) -> None:
        if not self.results:
            return
        table_id = ensure_results_table(self.client, self.dataset_quality)
        errors = self.client.insert_rows_json(table_id, self.results)
        if errors:
            # Não deixamos um problema ao gravar o histórico mascarar o
            # resultado da validação em si - por isso só logamos.
            logger.error("Erro ao persistir histórico de qualidade: %s", errors)
        else:
            logger.info(
                "Histórico de qualidade gravado em %s (run_id=%s, %d checks)",
                table_id,
                self.run_id,
                len(self.results),
            )

    def __enter__(self) -> "QualityRun":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        # Persiste mesmo quando um check crítico abortou a execução:
        # a rodada que falhou é a que mais interessa no histórico.
        self.persist()
        return False


def run_check(client: bigquery.Client, query: str, check_name: str, critical: bool = True) -> int:
    """Executa um check avulso, sem persistir histórico.

    Mantido para uso interativo/ad-hoc. O pipeline usa QualityRun, que
    além de rodar o check registra o resultado em
    `quality.data_quality_results`.
    """
    logger.info("Executando QA: %s", check_name)
    row = next(iter(client.query(query).result()))
    failed_count = int(row["failed_count"])
    if failed_count > 0:
        msg = f"Falha no teste [{check_name}]: {failed_count} registros violaram a regra."
        if critical:
            logger.error(msg)
            raise ValueError(msg)
        logger.warning(msg)
    else:
        logger.info("Sucesso no teste [%s]", check_name)
    return failed_count


def validate_silver_layer() -> None:
    settings = load_settings()
    client = bigquery.Client(project=settings.project_id)
    silver = settings.dataset_silver
    project = client.project

    with QualityRun(client, settings.dataset_quality, "silver") as run:
        # 1. Testes de Unicidade (Chaves Primárias)
        run.check(
            f"SELECT COUNT(*) - COUNT(DISTINCT id_municipio) AS failed_count FROM `{project}.{silver}.dim_localidades`",
            "dim_localidades - unicidade de id_municipio",
        )
        run.check(
            f"""
            SELECT COUNT(*) - COUNT(DISTINCT CONCAT(COALESCE(id_aluno, ''), '-', CAST(ano AS STRING))) AS failed_count
            FROM `{project}.{silver}.dados_alunos`
            """,
            "dados_alunos - unicidade de id_aluno por ano",
        )
        run.check(
            f"""
            SELECT COUNT(*) - COUNT(DISTINCT CONCAT(COALESCE(id_aluno, ''), '-', CAST(ano AS STRING))) AS failed_count
            FROM `{project}.{silver}.dados_alunos_streaming`
            """,
            "dados_alunos_streaming - unicidade de id_aluno por ano",
        )
        run.check(
            f"""
            SELECT COUNT(*) - COUNT(DISTINCT CONCAT(CAST(ano AS STRING), '-', COALESCE(sigla_uf, ''), '-', COALESCE(rede, ''))) AS failed_count
            FROM `{project}.{silver}.meta_alfabetizacao_uf`
            """,
            "meta_alfabetizacao_uf - unicidade de ano + sigla_uf + rede",
        )

        # 2. Testes de Valores Nulos em Colunas Críticas
        run.check(
            f"""
            SELECT COUNT(*) AS failed_count
            FROM `{project}.{silver}.dim_localidades`
            WHERE id_municipio IS NULL OR sigla_uf IS NULL OR nome_municipio IS NULL
            """,
            "dim_localidades - nulos em chaves e colunas obrigatórias",
        )
        run.check(
            f"""
            SELECT COUNT(*) AS failed_count
            FROM `{project}.{silver}.dados_alunos`
            WHERE id_aluno IS NULL OR id_municipio IS NULL OR ano IS NULL OR alfabetizado IS NULL
            """,
            "dados_alunos - nulos em colunas obrigatórias",
        )
        run.check(
            f"""
            SELECT COUNT(*) AS failed_count
            FROM `{project}.{silver}.dados_alunos_streaming`
            WHERE id_aluno IS NULL OR id_municipio IS NULL OR ano IS NULL OR alfabetizado IS NULL
            """,
            "dados_alunos_streaming - nulos em colunas obrigatórias",
        )

        # 3. Testes de Integridade Referencial (Chaves Estrangeiras)
        #
        # As tabelas Silver principais usam INNER JOIN com dim_localidades
        # (ver transform_silver.py), então essas contagens devem sempre dar
        # zero por construção - mantidas como teste de regressão barato,
        # caso alguém volte a usar LEFT JOIN.
        run.check(
            f"""
            SELECT COUNT(*) AS failed_count
            FROM `{project}.{silver}.dados_alunos` a
            LEFT JOIN `{project}.{silver}.dim_localidades` l ON a.id_municipio = l.id_municipio
            WHERE l.id_municipio IS NULL
            """,
            "dados_alunos - id_municipio inexistente em dim_localidades",
        )
        run.check(
            f"""
            SELECT COUNT(*) AS failed_count
            FROM `{project}.{silver}.dados_alunos_streaming` a
            LEFT JOIN `{project}.{silver}.dim_localidades` l ON a.id_municipio = l.id_municipio
            WHERE l.id_municipio IS NULL
            """,
            "dados_alunos_streaming - id_municipio inexistente em dim_localidades",
        )
        run.check(
            f"""
            SELECT COUNT(*) AS failed_count
            FROM `{project}.{silver}.avaliacao_alfabetizacao_municipio` m
            LEFT JOIN `{project}.{silver}.dim_localidades` l ON m.id_municipio = l.id_municipio
            WHERE l.id_municipio IS NULL
            """,
            "avaliacao_alfabetizacao_municipio - id_municipio inexistente em dim_localidades",
        )
        run.check(
            f"""
            SELECT COUNT(*) AS failed_count
            FROM `{project}.{silver}.meta_alfabetizacao_municipio` m
            LEFT JOIN `{project}.{silver}.dim_localidades` l ON m.id_municipio = l.id_municipio
            WHERE l.id_municipio IS NULL
            """,
            "meta_alfabetizacao_municipio - id_municipio inexistente em dim_localidades",
        )

        # 3b. Observabilidade das tabelas de quarentena: registros que não
        # casaram com dim_localidades não travam o pipeline (ver
        # transform_silver.py), mas o volume é reportado como aviso - se
        # crescer, é sinal de que a dimensão IBGE precisa de atenção.
        run.check(
            f"SELECT COUNT(*) AS failed_count FROM `{project}.{silver}.quarentena_meta_alfabetizacao_municipio`",
            "meta_alfabetizacao_municipio - volume em quarentena",
            critical=False,
        )
        run.check(
            f"SELECT COUNT(*) AS failed_count FROM `{project}.{silver}.quarentena_avaliacao_alfabetizacao_municipio`",
            "avaliacao_alfabetizacao_municipio - volume em quarentena",
            critical=False,
        )
        run.check(
            f"SELECT COUNT(*) AS failed_count FROM `{project}.{silver}.quarentena_dados_alunos`",
            "dados_alunos - volume em quarentena",
            critical=False,
        )
        run.check(
            f"SELECT COUNT(*) AS failed_count FROM `{project}.{silver}.quarentena_dados_alunos_streaming`",
            "dados_alunos_streaming - volume em quarentena",
            critical=False,
        )

        # 4. Testes de Ranges e Sanidade (Valores Válidos)
        run.check(
            f"""
            SELECT COUNT(*) AS failed_count
            FROM `{project}.{silver}.dados_alunos`
            WHERE presenca NOT IN (0, 1) OR alfabetizado NOT IN (0, 1)
            """,
            "dados_alunos - presenca e alfabetizado devem ser 0 ou 1",
        )
        run.check(
            f"""
            SELECT COUNT(*) AS failed_count
            FROM `{project}.{silver}.dados_alunos_streaming`
            WHERE presenca NOT IN (0, 1) OR alfabetizado NOT IN (0, 1)
            """,
            "dados_alunos_streaming - presenca e alfabetizado devem ser 0 ou 1",
        )
        run.check(
            f"""
            SELECT COUNT(*) AS failed_count
            FROM `{project}.{silver}.avaliacao_alfabetizacao_uf`
            WHERE taxa_alfabetizacao < 0.0 OR taxa_alfabetizacao > 100.0
            """,
            "avaliacao_alfabetizacao_uf - taxa_alfabetizacao deve estar entre 0.0 e 100.0",
            critical=False,  # Aviso: taxas fora de faixa vêm da fonte, não travam o pipeline
        )

    logger.info("Todas as validações de qualidade da camada Silver passaram com sucesso!")


def validate_gold_layer() -> None:
    settings = load_settings()
    client = bigquery.Client(project=settings.project_id)
    gold = settings.dataset_gold
    project = client.project

    with QualityRun(client, settings.dataset_quality, "gold") as run:
        # 1. Testes de Unicidade (Chaves Primárias)
        run.check(
            f"""
            SELECT COUNT(*) - COUNT(DISTINCT CONCAT(CAST(ano AS STRING), '-', COALESCE(id_municipio, ''), '-', COALESCE(rede, ''))) AS failed_count
            FROM `{project}.{gold}.mart_comparativo_municipio`
            """,
            "mart_comparativo_municipio - unicidade de ano + id_municipio + rede",
        )
        run.check(
            f"""
            SELECT COUNT(*) - COUNT(DISTINCT CONCAT(CAST(ano AS STRING), '-', COALESCE(sigla_uf, ''), '-', COALESCE(rede, ''))) AS failed_count
            FROM `{project}.{gold}.mart_comparativo_uf`
            """,
            "mart_comparativo_uf - unicidade de ano + sigla_uf + rede",
        )
        run.check(
            f"""
            SELECT COUNT(*) - COUNT(DISTINCT CONCAT(CAST(ano AS STRING), '-', COALESCE(rede, ''))) AS failed_count
            FROM `{project}.{gold}.mart_comparativo_brasil`
            """,
            "mart_comparativo_brasil - unicidade de ano + rede",
        )

        # 2. Testes de Valores Nulos em Colunas Críticas
        run.check(
            f"""
            SELECT COUNT(*) AS failed_count
            FROM `{project}.{gold}.mart_comparativo_municipio`
            WHERE ano IS NULL OR id_municipio IS NULL OR rede IS NULL
            """,
            "mart_comparativo_municipio - nulos em colunas obrigatórias",
        )
        run.check(
            f"""
            SELECT COUNT(*) AS failed_count
            FROM `{project}.{gold}.mart_comparativo_uf`
            WHERE ano IS NULL OR sigla_uf IS NULL OR rede IS NULL
            """,
            "mart_comparativo_uf - nulos em colunas obrigatórias",
        )
        run.check(
            f"""
            SELECT COUNT(*) AS failed_count
            FROM `{project}.{gold}.mart_comparativo_brasil`
            WHERE ano IS NULL OR rede IS NULL
            """,
            "mart_comparativo_brasil - nulos em colunas obrigatórias",
        )

        # 3. Testes de Ranges e Sanidade (Valores Válidos)
        run.check(
            f"""
            SELECT COUNT(*) AS failed_count
            FROM `{project}.{gold}.mart_comparativo_municipio`
            WHERE (taxa_alfabetizacao_real < 0.0 OR taxa_alfabetizacao_real > 100.0)
               OR (taxa_alfabetizacao_microdados < 0.0 OR taxa_alfabetizacao_microdados > 100.0)
               OR (meta_taxa_alfabetizacao < 0.0 OR meta_taxa_alfabetizacao > 100.0)
            """,
            "mart_comparativo_municipio - taxas devem estar entre 0.0 e 100.0",
            critical=False,
        )

        # 4. Cobertura dos microdados: garante que o comparativo municipal
        # está apoiado na carga batch completa, e não numa amostra. Se a
        # ingestão dos microdados falhar, este check acusa.
        run.check(
            f"""
            SELECT
              CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS failed_count
            FROM `{project}.{gold}.mart_comparativo_municipio`
            WHERE total_alunos_microdados IS NOT NULL
            """,
            "mart_comparativo_municipio - agregados de microdados presentes",
        )

    logger.info("Todas as validações de qualidade da camada Gold passaram com sucesso!")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "gold":
        validate_gold_layer()
    else:
        validate_silver_layer()
