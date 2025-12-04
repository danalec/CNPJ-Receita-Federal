import requests
import logging
from pathlib import Path
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm  # Barra de progresso

# Importa as configurações
from .settings import settings

logger = logging.getLogger(__name__)


def get_session():
    """
    Cria uma sessão requests com estratégia de retentativa (Retry).
    Isso torna o download resiliente a falhas de rede momentâneas.
    """
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "cnpj-etl/0.1 (+https://github.com/folclore/cnpj-receita-federal)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )
    retry = Retry(
        total=3,  # Tenta 3 vezes
        backoff_factor=3,  # Espera 1s, 2s, 4s... entre tentativas
        status_forcelist=[500, 502, 503, 504],  # Retenta se o servidor der erro
        allowed_methods=["HEAD", "GET", "OPTIONS"],
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
    session = get_session()

    try:
        response = session.get(base_url, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        links = soup.find_all("a")

        zip_urls = []
        for link in links:
            href = link.get("href")
            # Filtra apenas arquivos .zip e ignora links de navegação
            if href and href.lower().endswith(".zip"):
                # Garante a URL completa
                if not href.startswith("http"):
                    full_url = f"{base_url.rstrip('/')}/{href}"
                else:
                    full_url = href
                zip_urls.append(full_url)

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
    download_chunck_size = settings.download_chunk_size

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
                for chunk in r.iter_content(chunk_size=download_chunck_size):
                    if chunk:
                        size = f.write(chunk)
                        bar.update(size)

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
