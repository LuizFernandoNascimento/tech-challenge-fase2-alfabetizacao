"""Catálogo das fontes brutas e para onde cada uma vai na camada Bronze."""
import os
from dataclasses import dataclass
from pathlib import Path

# Pasta com os CSVs originais (fora do repositório, ver docs/data_dictionary.md).
# Pode ser sobrescrita por RAW_DATA_DIR para rodar contra data/sample/ em dev.
_REPO_ROOT = Path(__file__).resolve().parents[3]
RAW_DATA_DIR = Path(
    os.environ.get("RAW_DATA_DIR", _REPO_ROOT / "tech challenge" / "base de dados")
)


@dataclass(frozen=True)
class BronzeSource:
    file_name: str
    table_name: str


# Fontes de ingestão batch: metas e resultados agregados (baixo volume,
# atualização periódica) -> GCS + BigQuery bronze, sem transformação.
BATCH_SOURCES = [
    BronzeSource(
        "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_brasil.csv",
        "meta_alfabetizacao_brasil",
    ),
    BronzeSource(
        "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf.csv",
        "meta_alfabetizacao_uf",
    ),
    BronzeSource(
        "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio.csv",
        "meta_alfabetizacao_municipio",
    ),
    BronzeSource(
        "br_inep_avaliacao_alfabetizacao_uf.csv",
        "avaliacao_alfabetizacao_uf",
    ),
    BronzeSource(
        "br_inep_avaliacao_alfabetizacao_municipio.csv",
        "avaliacao_alfabetizacao_municipio",
    ),
]

# Microdados de aluno: granularidade de evento, usados na simulação de streaming.
STREAMING_SOURCE = BronzeSource("Dados de alunos.csv", "dados_alunos_streaming")

# Dimensão de referência (UF/Município), buscada da API do IBGE - ver ibge_reference.py.
IBGE_REFERENCE_SOURCE = BronzeSource("ibge_municipios.csv", "ibge_municipios")
