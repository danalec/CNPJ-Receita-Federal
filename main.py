import logging
import time

from src.extract_files import run_extraction
from src.downloader import run_download
from src.consolidate_csv import run_consolidation
from src.database_loader import run_loader
from src.settings import setup_logging
from src.check_update import check_updates, update_local_version

setup_logging()
logger = logging.getLogger(__name__)


def main_pipeline():
    logging.info("==================================================")
    logging.info("🚀 INICIANDO PIPELINE DE DADOS COMPLETO 🚀")
    logging.info("==================================================")

    start_time = time.time()

    # --- VERIFICAÇÃO DA VERSÃO ---
    logging.info("Verificando versão mais recente disponível...")

    # Retorna a data se tiver atualização, ou None se não tiver
    new_version_date = check_updates()

    if new_version_date is None:
        logger.info("Dados já estão atualizados. Nada a fazer. Encerrando.")
        return

    try:
        # --- ETAPA 1: DOWNLOAD ---
        logging.info(f"[ETAPA 1 de 4] Iniciando download da versão {new_version_date}")
        run_download()
        logging.info("[ETAPA 1 de 4] Download concluído!")

        # --- ETAPA 2: EXTRAÇÃO ---
        logging.info("[ETAPA 2 de 4] Iniciando extração dos arquivos .zip")
        run_extraction()
        logging.info("[ETAPA 2 de 4] Extração concluída com sucesso!")

        # --- ETAPA 3: CONSOLIDAÇÃO ---
        logging.info("[ETAPA 3 de 4] Iniciando consolidação dos arquivos .csv")
        run_consolidation()
        logging.info("[ETAPA 3 de 4] Consolidação concluída com sucesso!")

        # --- ETAPA 4: CARGA NO BANCO ---
        logging.info("[ETAPA 4 de 4] Iniciando carga de dados para o PostgreSQL")
        run_loader()
        logging.info("[ETAPA 4 de 4] Carga de dados concluída com sucesso!")

        # --- SUCESSO TOTAL: ATUALIZA O ARQUIVO DE ESTADO ---
        logger.info(f"Atualizando arquivo de versão local para: {new_version_date}")
        update_local_version(new_version_date)

    except Exception as e:
        logging.error(f"❌ O PIPELINE FALHOU. Erro: {e}", exc_info=True)
        # Não atualizamos a versão aqui, para que ele tente novamente na próxima execução
        return

    end_time = time.time()
    total_time = end_time - start_time
    logging.info("==================================================")
    logging.info("✅ PIPELINE DE DADOS FINALIZADO COM SUCESSO! ✅")
    logging.info(f"⏱️ Tempo total de execução: {total_time:.2f} segundos.")
    logging.info("==================================================")


if __name__ == "__main__":
    main_pipeline()
