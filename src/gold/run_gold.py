"""Orquestrador da camada Gold.
Executa as transformações SQL no BigQuery para povoar as tabelas Gold
e em seguida roda as validações de qualidade de dados na camada Gold.
"""
import logging
import sys
from gold.transform_gold import execute_gold_transformations
from quality.data_quality import validate_gold_layer
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
def main() -> None:
    try:
        logger.info("======= Iniciando Orquestração da Camada Gold =======")
        
        # 1. Executa as transformações
        execute_gold_transformations()
        
        # 2. Executa as validações de qualidade
        validate_gold_layer()
        
        logger.info("======= Camada Gold concluída com SUCESSO! =======")
    except Exception as e:
        logger.error("Erro durante o processamento da Camada Gold: %s", e)
        sys.exit(1)
if __name__ == "__main__":
    main()
