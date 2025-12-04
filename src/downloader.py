import requests
import random
from typing import Optional
import logging
from pathlib import Path
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm
import zipfile
import time

# Importa as configurações
from .settings import settings

logger = logging.getLogger(__name__)


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edg/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; SM-S921B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (iPad; CPU OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/604.1",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

LANGS = [
    "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "en-US,en;q=0.9,pt-BR;q=0.8,pt;q=0.7",
    "pt-PT,pt;q=0.9,en-US;q=0.8,en;q=0.7",
]

_UA_INDEX = 0

def get_user_agents() -> list[str]:
    return settings.user_agents if settings.user_agents else USER_AGENTS

def choose_user_agent() -> str:
    uas = get_user_agents()
    if settings.user_agent_rotation == "sequential":
        global _UA_INDEX
        ua = uas[_UA_INDEX % len(uas)]
        _UA_INDEX += 1
        return ua
    return random.choice(uas)

def choose_accept_language() -> str:
    return random.choice(LANGS)

def build_headers(referrer: Optional[str] = None) -> dict:
    headers = {
        "User-Agent": choose_user_agent(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": choose_accept_language(),
    }
    if referrer:
        headers["Referer"] = referrer
    return headers


def get_session(referrer: Optional[str] = None):
    """
    Cria uma sessão requests com estratégia de retentativa (Retry).
    Isso torna o download resiliente a falhas de rede momentâneas.
    """
    session = requests.Session()
    session.headers.update(build_headers(referrer))
    retry = Retry(
        total=3,
        backoff_factor=3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def get_zip_links(base_url: str) -> list[str]:
    """
    Acessa a URL da data (ex: .../2025-11/) e raspa todos os links .zip.
    """
    logger.info(f"Buscando lista de arquivos em: {base_url}")
    session = get_session(referrer=base_url)

    try:
        response = session.get(base_url, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        zip_urls = [
            href if href.startswith("http") else f"{base_url.rstrip('/')}/{href}"
            for a in soup.find_all("a")
            if (href := a.get("href")) and href.lower().endswith(".zip")
        ]

        logger.info(f"Encontrados {len(zip_urls)} arquivos para baixar.")
        return zip_urls

    except Exception as e:
        logger.error(f"Erro ao obter links de download: {e}")
        raise


def download_file(url: str, dest_dir: Path):
    """
    Baixa um único arquivo com barra de progresso e stream.
    """
    filename = url.split("/")[-1]
    dest_path = dest_dir / filename
    download_chunk_size = settings.download_chunk_size

    try:
        session = get_session(referrer=url)
    except TypeError:
        session = get_session()

    try:
        existing_size = dest_path.stat().st_size if dest_path.exists() else 0
        try:
            h = session.head(url, timeout=30, allow_redirects=True)
            h.raise_for_status()
            total_size = int(h.headers.get("content-length", 0))
        except Exception:
            total_size = 0

        if total_size and existing_size >= total_size:
            logger.info(f"Download já completo: {filename}")
            return True

        headers = {}
        initial = 0
        mode = "wb"
        if existing_size and total_size and existing_size < total_size:
            headers["Range"] = f"bytes={existing_size}-"
            initial = existing_size
            mode = "ab"

        with session.get(url, stream=True, timeout=60, headers=headers) as r:
            r.raise_for_status()
            content_range = r.headers.get("content-range")
            if headers and not content_range:
                mode = "wb"
                initial = 0

            total = total_size if total_size > 0 else None

            with (
                open(dest_path, mode) as f,
                tqdm(
                    desc=filename,
                    total=total,
                    unit="iB",
                    unit_scale=True,
                    unit_divisor=1024,
                    leave=False,
                    initial=initial,
                ) as bar,
            ):
                for chunk in r.iter_content(chunk_size=download_chunk_size):
                    if chunk:
                        size = f.write(chunk)
                        bar.update(size)
                        if settings.rate_limit_per_sec and settings.rate_limit_per_sec > 0:
                            time.sleep(size / settings.rate_limit_per_sec)

        if settings.verify_zip_integrity and filename.lower().endswith(".zip"):
            try:
                with zipfile.ZipFile(dest_path) as z:
                    bad = z.testzip()
                    if bad is not None:
                        dest_path.unlink(missing_ok=True)
                        logger.error(f"Arquivo corrompido: {filename}")
                        return False
            except Exception:
                dest_path.unlink(missing_ok=True)
                logger.error(f"Falha ao validar zip: {filename}")
                return False

        logger.info(f"Download concluído: {filename}")
        return True

    except Exception as e:
        logger.error(f"Falha ao baixar {filename}: {e}")
        # Remove arquivo parcial se houver erro
        if dest_path.exists():
            dest_path.unlink()
        return False


def run_download():
    """
    Orquestrador do download multithread.
    Recebe a data (ex: '2025-11') para montar a URL correta.
    """

    logger.info("Iniciando pipeline de download MULTITHREAD...")

    max_workers = settings.max_workers
    target_url = settings.download_url
    dest_dir = settings.compressed_dir

    # 3. Pega a lista de arquivos
    try:
        zip_links = get_zip_links(target_url)
    except Exception:
        logger.error("Abortando download devido a erro na listagem de arquivos.")
        raise

    if not zip_links:
        logger.error("Nenhum arquivo encontrado para baixar.")
        raise

    # 4. Inicia o ThreadPool para baixar em paralelo
    logger.info(
        f"Iniciando download de {len(zip_links)} arquivos com {max_workers} threads."
    )

    files_downloaded = 0
    errors = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Mapeia cada URL para a função de download
        # future_to_url é um dicionário {future: url}
        future_to_url = {
            executor.submit(download_file, url, dest_dir): url for url in zip_links
        }

        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                success = future.result()
                if success:
                    files_downloaded += 1
                else:
                    errors += 1
            except Exception as exc:
                logger.error(f"Exceção não tratada ao baixar {url}: {exc}")
                errors += 1

    logger.info("=" * 40)
    logger.info("Resumo do Download:")
    logger.info(f"Sucessos: {files_downloaded}")
    logger.info(f"Erros:    {errors}")
    logger.info("=" * 40)

    if errors > 0:
        raise Exception(
            f"Ocorreram {errors} erros durante o download. Verifique os logs."
        )
