"""Catálogo das fontes brutas e para onde cada uma vai na camada Bronze."""
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
