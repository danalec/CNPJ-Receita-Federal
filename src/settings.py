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

    postgres_user: str
    postgres_password: str
    postgres_host: str
    postgres_port: int
    postgres_database: str

    # --- ETL ---
    file_encoding: str = "latin1"
    chunck_size: int = 200_000
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"

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
