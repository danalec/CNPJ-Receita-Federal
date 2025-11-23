import logging
from pathlib import Path
from typing import Literal
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import computed_field


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # Sobe 2 níveis para chegar na raiz do projeto (source/config.py)
    project_root: Path = Path(__file__).resolve().parents[1]

    # URL Base da Receita (sem a data)
    rfb_base_url: str = (
        "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"
    )

    # Esta variável será preenchida dinamicamente pelo script de update
    # Default vazio ou uma data específica se for rodar manual
    target_date: str = ""

    postgres_user: str
    postgres_password: str
    postgres_host: str
    postgres_port: int
    postgres_database: str

    """
    Configurações de Download
    """

    # Número de downloads simultâneos (não exagere para não tomar block)
    max_workers: int = 4
    extract_workers: int = 2
    # Tamanho do pedaço lido na memória durante download (8KB)
    download_chunk_size: int = 8192

    """
    O script por padrão desativa o log de transação (WAL) 
    Otização que torna as tabelas unlogged.
    A escrita fica muito mais rápida.
    
    Se quiser segurança após a carga, volte para LOGGED. 
    Mas demora um pouco pois ele vai escrever o log agora.
    Para dados analíticos, pode deixar UNLOGGED se tiver backup do CSV.
    """

    set_logged_after_copy: bool = False
    use_unlogged: bool = True
    cluster_after_copy: bool = False
    partition_estabelecimentos_by: Literal["none", "uf"] = "none"

    """
    Configurações da migração de dados, caso tenha mais memória
    você pode aumentar, o padrão é 200_000 o que da um consumo de
    200-500 megas de memória. Essa variação ocorre por conta do tamanho
    das tabelas
    """

    file_encoding: str = "latin1"
    chunk_size: int = 200_000

    # Set log level
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"

    @computed_field
    def download_url(self) -> str:
        """Monta a URL completa baseada na data alvo."""
        if not self.target_date:
            return ""
        return f"{self.rfb_base_url}{self.target_date}/"

    @computed_field
    def state_file(self) -> Path:
        return self.data_dir / "last_version_processed.txt"

    @computed_field
    def data_dir(self) -> Path:
        return self.project_root / "data"

    @computed_field
    def log_dir(self) -> Path:
        return self.project_root / "logs"

    @computed_field
    def compressed_dir(self) -> Path:
        return self.data_dir / "compressed_files"

    @computed_field
    def extracted_dir(self) -> Path:
        return self.data_dir / "extracted_files"

    @computed_field
    def database_uri(self) -> str:
        return (
            f"postgresql://{self.postgres_user}"
            f":{self.postgres_password}"
            f"@{self.postgres_host}"
            f":{self.postgres_port}"
            f"/{self.postgres_database}"
        )

    def create_dirs(self):
        """Garante que a estrutura de pastas exista."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.compressed_dir.mkdir(parents=True, exist_ok=True)
        self.extracted_dir.mkdir(parents=True, exist_ok=True)


# Instancia e cria diretórios
settings = Settings()
settings.create_dirs()


def setup_logging():
    """Configura o logger raiz."""
    log_file = settings.log_dir / "cnpj.log"
    level = getattr(logging, settings.log_level)

    logging.basicConfig(
        level=level,
        format="%(levelname)s - %(asctime)s - [%(name)s] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_file, mode="w"),
            logging.StreamHandler(),
        ],
    )
