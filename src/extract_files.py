import re
import zipfile
from pathlib import Path
from itertools import groupby
from typing import List, Iterator, Tuple
import logging
from .settings import settings

logger = logging.getLogger(__name__)


def get_file_base_name(path: Path) -> str:
    """
    Extrai o nome base de um arquivo, removendo números e a extensão.
    Exemplo: "Empresas4.zip" -> "Empresas"
    """
    # path.stem retorna o nome do arquivo sem a extensão final (ex: "Empresas4")
    match = re.match(r"([a-zA-Z]+)", path.stem)
    return match.group(1) if match else "desconhecido"


def group_files(paths: List[Path]) -> Iterator[Tuple[str, Iterator[Path]]]:
    """
    Agrupa uma lista de caminhos de arquivo pelo seu nome base.
    A função requer que a lista de entrada esteja pré-ordenada
    pela chave de agrupamento.
    """
    # Pré-ordena a lista para que o groupby funcione corretamente
    sorted_paths = sorted(paths, key=get_file_base_name)
    return groupby(sorted_paths, key=get_file_base_name)


def create_directory_if_not_exists(directory: Path):
    """
    Cria um diretório de forma segura, sem falhar se ele já existir.
    Esta função é projetada para executar um efeito colateral:
    modificar o sistema de arquivos.
    """
    try:
        directory.mkdir(parents=True, exist_ok=True)
        logging.info(f"✔️ Diretório '{directory}' garantido.")
    except OSError as e:
        logging.error(f"❌ Erro ao criar o diretório '{directory}': {e}")
        raise


def extract_single_zip(zip_path: Path, destination_dir: Path):
    """
    Extrai um único arquivo .zip para um diretório de destino.
    Isola a operação de extração e lida com erros específicos.
    """
    logging.info(f"   -> Extraindo '{zip_path.name}'...")

    try:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(destination_dir)

    except zipfile.BadZipFile:
        logging.error(
            f"⚠️ AVISO: O arquivo '{zip_path.name}' está corrompido "
            "ou não é um ZIP válido. Pulando."
        )

    except Exception as e:
        logging.error(f"   ❌ Erro inesperado ao extrair '{zip_path.name}': {e}")


# --- Função Principal (Orquestrador) ---


def run_extraction():
    """
    Orquestra todo o processo de descompactação.
    """

    compressed_dir = settings.compressed_dir
    extracted_dir = settings.extracted_dir

    if not compressed_dir.is_dir():
        logging.error(
            f"❌ Erro: O diretório de origem '{compressed_dir}' não foi encontrado."
        )
        return

    logging.info(
        f"Iniciando processo de descompactação...\n"
        f"Origem: '{compressed_dir}'\n"
        f"Destino: '{extracted_dir}'\n"
    )

    # 2. Leitura do sistema de arquivos para obter a lista de arquivos.
    zip_files = list(compressed_dir.glob("*.zip"))

    if not zip_files:
        logging.error("🟡 Nenhum arquivo .zip encontrado no diretório de origem.")
        return

    # 3. Lógica pura para agrupar os arquivos.
    file_groups = group_files(zip_files)

    # 4. Itera sobre os grupos e aplica os efeitos colaterais (criar dir e extrair).
    for base_name, files_iterator in file_groups:
        target_subdir_name = base_name.lower()
        target_path = extracted_dir / target_subdir_name

        logging.info(f"\n📂 Processando grupo: '{base_name}'")

        create_directory_if_not_exists(target_path)

        # O files_iterator precisa ser convertido para uma lista para ser reutilizado
        # ou iterado múltiplas vezes, se necessário. Aqui, iteramos uma vez.
        for file_path in files_iterator:
            extract_single_zip(file_path, target_path)

    logging.info("\n✅ Processo de descompactação concluído com sucesso!")


if __name__ == "__main__":
    run_extraction(settings.compressed_dir, settings.extracted_dir)
