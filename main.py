import logging
import time

from src.extract_files import run_extraction
from src.consolidate_csv import run_consolidation
from src.database_loader import run_loader
from src.config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def main_pipeline():
    """
    Executa o pipeline de ETL completo, passo a passo.
    """
    logging.info("==================================================")
    logging.info("🚀 INICIANDO PIPELINE DE DADOS COMPLETO 🚀")
    logging.info("==================================================")

    start_time = time.time()

    try:
        # --- ETAPA 1: EXTRAÇÃO ---
        logging.info("--- [ETAPA 1 de 3] Iniciando extração dos arquivos .zip ---")
        run_extraction()
        logging.info("--- [ETAPA 1 de 3] Extração concluída com sucesso! ---")

        # --- ETAPA 2: CONSOLIDAÇÃO ---
        logging.info("--- [ETAPA 2 de 3] Iniciando consolidação dos arquivos .csv ---")
        run_consolidation()
        logging.info("--- [ETAPA 2 de 3] Consolidação concluída com sucesso! ---")

        # --- ETAPA 3: CARGA NO BANCO DE DADOS ---
        logging.info(
            "--- [ETAPA 3 de 3] Iniciando carga de dados para o banco de dados ---"
        )
        run_loader()
        logging.info("--- [ETAPA 3 de 3] Carga de dados concluída com sucesso! ---")

    except Exception as e:
        logging.error(f"❌ O PIPELINE FALHOU. Erro: {e}", exc_info=True)
        return

    end_time = time.time()
    total_time = end_time - start_time
    logging.info("==================================================")
    logging.info(f"✅ PIPELINE DE DADOS FINALIZADO COM SUCESSO! ✅")
    logging.info(f"⏱️ Tempo total de execução: {total_time:.2f} segundos.")
    logging.info("==================================================")


if __name__ == "__main__":
    main_pipeline()
