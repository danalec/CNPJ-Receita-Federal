from pathlib import Path

import logging

# --- Base Paths ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"


def setup_logging():
    """
    Configura um logger raiz único para todo o projeto.
    - Escreve para o console e para um único arquivo.
    - Formato: LEVEL - TIMESTAMP - MÓDULO - MENSAGEM

    Esta função deve ser chamada APENAS UMA VEZ em main.py.
    """

    log_dir = PROJECT_ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / "cnpj.log"

    log_format = "%(levelname)s - %(asctime)s - [%(name)s] - %(message)s"

    # 2. Configura o logger raiz usando basicConfig. É a forma mais direta.
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt="%Y-%m-%d %H:%M:%S",  # Formato de data opcional, mas limpo
        handlers=[
            logging.FileHandler(log_file, mode="w"),  # Handler para o arquivo
            logging.StreamHandler(),  # Handler para o console
        ],
    )


# --- Caminhos Base ---

# Define a raiz do projeto de forma robusta
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Define os diretórios de dados a partir da raiz
DATA_DIR = PROJECT_ROOT / "data"
COMPRESSED_DIR = DATA_DIR / "compressed_files"
EXTRACTED_DIR = DATA_DIR / "extracted_files"

# --- Configurações do Banco de Dados ---
DATABASE_FILE = DATA_DIR / "cnpj_dados.sqlite"
DATABASE_URI = f"sqlite:///{DATABASE_FILE}"

# --- Configurações de ETL ---
FILE_ENCODING = "latin1"
CHUNKSIZE = 100_000
