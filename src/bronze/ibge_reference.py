"""Busca a tabela de referência de municípios/UF na API do IBGE.

Necessária porque nenhuma fonte do INEP traz nome de UF, nome de
município ou região - apenas os códigos (sigla_uf, id_municipio).
Ver "Gap identificado" em docs/data_dictionary.md.
"""
import csv
import logging

import requests

from bronze.sources import RAW_DATA_DIR, IBGE_REFERENCE_SOURCE

IBGE_MUNICIPIOS_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

FIELDNAMES = [
    "id_municipio",
    "nome_municipio",
    "sigla_uf",
    "nome_uf",
    "id_uf",
    "sigla_regiao",
    "nome_regiao",
]


def fetch_municipios() -> list[dict]:
    response = requests.get(IBGE_MUNICIPIOS_URL, timeout=30)
    response.raise_for_status()
    municipios = response.json()

    rows = []
    for m in municipios:
        # "microrregiao" vem nulo para alguns municípios (ex.: Fernando de
        # Noronha); "regiao-imediata" é o caminho estável para chegar na UF.
        uf = m["regiao-imediata"]["regiao-intermediaria"]["UF"]
        regiao = uf["regiao"]
        rows.append(
            {
                "id_municipio": m["id"],
                "nome_municipio": m["nome"],
                "sigla_uf": uf["sigla"],
                "nome_uf": uf["nome"],
                "id_uf": uf["id"],
                "sigla_regiao": regiao["sigla"],
                "nome_regiao": regiao["nome"],
            }
        )
    return rows


def save_csv(rows: list[dict]) -> None:
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RAW_DATA_DIR / IBGE_REFERENCE_SOURCE.file_name
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    logger.info("Referência IBGE salva: %s (%d municípios)", out_path, len(rows))


def run():
    rows = fetch_municipios()
    save_csv(rows)


if __name__ == "__main__":
    run()
