"""Catálogo das fontes brutas e para onde cada uma vai na camada Bronze.

Há dois tipos de fonte aqui, e vale a pena entender a diferença:

- BasedosdadosSource: tabelas do INEP que a própria Base dos Dados já
  publica como tabelas BigQuery públicas (projeto `basedosdados`).
  Carregamos direto de lá via consulta cross-project - sem baixar CSV,
  sem passar por GCS. Confirmado manualmente que o dataset
  `basedosdados.br_inep_avaliacao_alfabetizacao` tem exatamente as
  tabelas que a gente precisa (`meta_alfabetizacao_brasil/uf/municipio`,
  `uf`, `municipio`, `alunos`).
- BronzeSource: a dimensão IBGE, que não existe como tabela pública
  equivalente - por isso continua vindo de um CSV buscado da API do
  IBGE (ibge_reference.py) e subido ao nosso próprio bucket GCS.
"""
import os
from dataclasses import dataclass
from pathlib import Path

# Pasta com os CSVs originais (fora do repositório, ver docs/data_dictionary.md).
# Só é usada no bootstrap local (bronze/batch_ingest.py::run); nas
# Cloud Functions deployadas não existe disco local nem essa
# profundidade de diretórios, então o cálculo é best-effort.
# Pode ser sobrescrita por RAW_DATA_DIR para rodar contra data/sample/ em dev.
def _default_raw_data_dir() -> Path:
    try:
        return Path(__file__).resolve().parents[3] / "tech challenge" / "base de dados"
    except IndexError:
        return Path("data/sample")


RAW_DATA_DIR = Path(os.environ.get("RAW_DATA_DIR", _default_raw_data_dir()))


@dataclass(frozen=True)
class BronzeSource:
    """Fonte baseada em arquivo local -> GCS -> BigQuery (bootstrap)."""

    file_name: str
    table_name: str


@dataclass(frozen=True)
class BasedosdadosSource:
    """Fonte já publicada como tabela BigQuery pública pela Base dos
    Dados - carregada via CTAS cross-project, sem passar por GCS.
    """

    bd_table: str
    table_name: str


BASEDOSDADOS_PROJECT = "basedosdados"
BASEDOSDADOS_DATASET = "br_inep_avaliacao_alfabetizacao"

# Fontes de ingestão batch: metas e resultados agregados, direto do
# BigQuery público da Base dos Dados (baixo volume, atualização
# periódica na origem) -> Bronze, sem transformação.
BD_BATCH_SOURCES = [
    BasedosdadosSource("meta_alfabetizacao_brasil", "meta_alfabetizacao_brasil"),
    BasedosdadosSource("meta_alfabetizacao_uf", "meta_alfabetizacao_uf"),
    BasedosdadosSource("meta_alfabetizacao_municipio", "meta_alfabetizacao_municipio"),
    BasedosdadosSource("uf", "avaliacao_alfabetizacao_uf"),
    BasedosdadosSource("municipio", "avaliacao_alfabetizacao_municipio"),
]

# Microdados de aluno via BATCH - a carga completa e autoritativa.
#
# Esta é a entidade "Dados de alunos" exigida pelo enunciado, ingerida
# de verdade: ~3,87 milhões de linhas. Ela NÃO cabe no mesmo caminho
# das fontes acima (que trazem as linhas para o processo Python), então
# tem um fluxo próprio - CTAS server-side em US + cópia cross-region -
# implementado em batch_ingest.py::load_large_table_from_basedosdados.
BD_LARGE_BATCH_SOURCES = [
    BasedosdadosSource("alunos", "dados_alunos"),
]

# Microdados de aluno via STREAMING: a MESMA fonte, replayada evento a
# evento no Pub/Sub para exercitar o caminho quase em tempo real da
# arquitetura híbrida. É uma amostra por natureza (o produtor controla
# quantos eventos publica) e serve para medir frescor/latência de
# ingestão - NÃO para calcular o indicador de alfabetização, que sai da
# carga batch completa acima. Ver a distinção explicitada na Gold.
STREAMING_SOURCE = BasedosdadosSource("alunos", "dados_alunos_streaming")

# Dimensão de referência (UF/Município), buscada da API do IBGE - ver
# ibge_reference.py. Continua sendo CSV -> GCS -> BigQuery porque a
# Base dos Dados não publica uma tabela equivalente com nomes/região.
IBGE_REFERENCE_SOURCE = BronzeSource("ibge_municipios.csv", "ibge_municipios")
