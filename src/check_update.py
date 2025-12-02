import requests
import logging
import shutil
import sys
from typing import Optional
from bs4 import BeautifulSoup
import re

from .settings import settings, state, StepStatus, PipelineStep

# Configura logger local para este script
logger = logging.getLogger("updater")


def get_latest_remote_date() -> str:
    """
    Acessa o site da Receita e descobre qual é a pasta de data mais recente.
    Retorna uma string ex: '2025-11'
    """
    try:
        response = requests.get(settings.rfb_base_url, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        links = soup.find_all("a")

        dates = []
        # Padrão regex para pegar pastas no formato YYYY-MM
        date_pattern = re.compile(r"^(\d{4}-\d{2})/$")

        for link in links:
            href = link.get("href")
            match = date_pattern.match(href)
            if match:
                dates.append(match.group(1))

        if not dates:
            logger.warning("Nenhuma data encontrada na página da Receita.")
            return None

        # Ordena decrescente para pegar a mais recente (2025-11 > 2025-10)
        dates.sort(reverse=True)
        return dates[0]

    except Exception as e:
        logger.error(f"Erro ao checar atualizações: {e}")
        return None


def clean_data_dirs() -> None:
    """
    Limpa as pastas de dados antigos antes de baixar os novos.
    Isso é crucial para não misturar dados de meses diferentes e estourar o disco.
    """
    logger.info("Limpando diretórios de dados antigos...")

    compressed_dir = settings.compressed_dir
    extracted_dir = settings.extracted_dir

    # Limpa compressed_files
    for item in compressed_dir.glob("*"):
        if item.is_file():
            item.unlink()

    # Limpa extracted_files (remove pastas inteiras)
    for item in extracted_dir.glob("*"):
        if item.is_dir():
            shutil.rmtree(item)
        elif item.is_file():
            item.unlink()

    logger.info("Diretórios limpos.")


def check_updates() -> Optional[str]:
    logger.info("Verificando atualizações na Receita Federal...")

    latest_remote = get_latest_remote_date()
    # Leitura correta do estado persistido
    last_processed = state.target_date

    if not latest_remote:
        logger.error("Não foi possível determinar a versão remota. Abortando.")
        raise RuntimeError(
            "Falha crítica: Não foi possível obter a última "
            "versão no site da Receita Federal."
        )

    logger.info(f"Última versão disponível: {latest_remote}")
    logger.info(f"Última versão processada: {last_processed}")

    if latest_remote == last_processed:
        return None

    logger.info(f"Nova versão encontrada: {latest_remote}. Iniciando atualização.")

    # Ao definir isso, o setter do state já reseta os passos anteriores automaticamente.
    state.target_date = latest_remote

    logger.info("Removendo arquivos antigos! Caso existam")
    clean_data_dirs()

    return latest_remote


def run_check_step() -> None:
    """
    Executa a verificação. Se achar nova versão, reseta o estado.
    Se não achar e não tiver histórico, encerra o script.
    """

    new_date = check_updates()
    # Runtime: Atualiza a configuração em memória para gerar a URL correta
    settings.target_date = new_date

    # Se tiver uma nova versão, começa um novo fluxo da primeira etapa
    if new_date:
        logger.info(f"📅 Nova versão detectada: {new_date}")

        # Persistência: Salva no JSON (Isso reseta os steps também)
        state.target_date = new_date

    # Checa se o pipeline já não foi concluido por completo
    elif (
        state.current_state == PipelineStep.CONSTRAINTS
        and state.current_status == StepStatus.COMPLETED.value
    ):
        logger.info(
            f"✅ Versão {state.target_date} já processada por completo. Não há nada a fazer"
        )
        sys.exit(0)

    # Continua de onde parou
    else:
        logger.info(f"🔄 Nenhuma novidade. Retomando versão: {state.target_date}")


if __name__ == "__main__":
    check_updates()
