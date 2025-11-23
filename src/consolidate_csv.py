import logging
import shutil
from pathlib import Path
from typing import List
from .settings import settings


logger = logging.getLogger(__name__)


def get_subdirectories(base_path: Path):
    """Retorna um iterador de subdiretórios."""
    if not base_path.is_dir():
        logger.error(f"O diretório base '{base_path}' não existe.")
        return iter([])
    return (item for item in base_path.iterdir() if item.is_dir())


def get_source_csv_files(directory: Path, output_filename: str) -> List[Path]:
    """
    Encontra todos os arquivos .csv (ou .CSV), ignorando o arquivo de saída.
    """
    all_files = directory.glob("*")
    return [
        f
        for f in all_files
        if f.name != output_filename and f.suffix.lower() == ".csv"
    ]


def concatenate_files_in_directory(dir_path: Path):
    """
    Concatena TODOS os bytes de todos os arquivos de origem,
    pois eles não têm cabeçalho. Evitando também erros de charset
    """
    logger.info(f"Iniciando processamento do diretório: '{dir_path.name}'")
    output_filename = f"{dir_path.name}.csv"
    output_filepath = dir_path / output_filename

    source_files = get_source_csv_files(dir_path, output_filename)

    if not source_files:
        logger.warning(
            f"Nenhum arquivo .csv de origem encontrado em '{dir_path.name}'. Pulando."
        )
        return

    logger.info(
        f"Encontrados {len(source_files)} arquivos para concatenar em '{dir_path.name}'."
    )

    try:
        with open(output_filepath, "wb") as f_out:
            for filepath in source_files:
                logger.info(f"   + Adicionando conteúdo completo de: {filepath.name}")
                with open(filepath, "rb") as f_in:
                    first = True
                    while True:
                        chunk = f_in.read(1024 * 64)
                        if not chunk:
                            break
                        if first and settings.strip_bom:
                            # Remove BOM UTF-8 se presente
                            if chunk.startswith(b"\xEF\xBB\xBF"):
                                chunk = chunk[3:]
                            first = False
                        if settings.normalize_line_endings:
                            # Normaliza CRLF para LF
                            chunk = chunk.replace(b"\r\n", b"\n")
                        f_out.write(chunk)

        logger.info(f"✅ SUCESSO: Arquivo consolidado '{output_filepath}' criado.")

    except Exception as e:
        logger.error(
            f"❌ ERRO inesperado no diretório '{dir_path.name}': {e}", exc_info=True
        )
        raise


def run_consolidation():
    """
    Função principal que orquestra todo o processo de concatenação.
    """
    logger.info("Iniciando processo de consolidação de dados...")

    extracted_dir = settings.extracted_dir

    if not extracted_dir.exists():
        msg = f"Diretório de extração não encontrado: {extracted_dir}"
        logger.error(msg)
        raise FileNotFoundError(msg)

    subdirectories = get_subdirectories(extracted_dir)

    for subdir in subdirectories:
        concatenate_files_in_directory(subdir)

    logger.info("Processo de consolidação finalizado.")
