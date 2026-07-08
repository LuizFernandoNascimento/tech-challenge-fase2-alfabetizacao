"""Orquestrador da camada Silver.

Executa as transformações SQL no BigQuery para povoar as tabelas Silver
e em seguida roda as validações de qualidade de dados.
"""
import logging
import sys

from silver.transform_silver import execute_transformations
from quality.data_quality import validate_silver_layer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    try:
        logger.info("======= Iniciando Orquestração da Camada Silver =======")
        
        # 1. Executa as transformações
        execute_transformations()
        
        # 2. Executa as validações de qualidade
        validate_silver_layer()
        
        logger.info("======= Camada Silver concluída com SUCESSO! =======")
    except Exception as e:
        logger.error("Erro durante o processamento da Camada Silver: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
