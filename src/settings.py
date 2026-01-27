import logging
import sys
import io
from pathlib import Path
from typing import Literal, Optional, cast
from pydantic_settings import BaseSettings, SettingsConfigDict
from enum import Enum
from pydantic import computed_field


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
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

    postgres_user: Optional[str] = None
    postgres_password: Optional[str] = None
    postgres_host: Optional[str] = None
    postgres_port: Optional[int] = None
    postgres_database: Optional[str] = None

    """
    Configurações de Download
    """

    # Número de downloads simultâneos (não exagere para não tomar block)
    max_workers: int = 4
    extract_workers: int = 2
    # Tamanho do pedaço lido na memória durante download (8KB)
    download_chunk_size: int = 8192
    rate_limit_per_sec: int = 0
    verify_zip_integrity: bool = True

    # Configurações Stealth/Async
    enable_http2: bool = True
    stealth_mode: bool = True
    max_concurrent_requests: Optional[int] = None  # Se None, usa max_workers * 2
    retry_max_attempts: int = 10
    retry_backoff_factor: float = 0.5
    retry_jitter: bool = True
    
    # Proxy Configuration
    proxies: Optional[list[str]] = None
    proxy_rotation_strategy: Literal["round_robin", "random"] = "round_robin"
    
    # Browser Impersonation
    impersonate: Literal["chrome", "chrome110", "edge99", "safari15_3"] = "chrome110"


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
    normalize_line_endings: bool = True
    strip_bom: bool = True

    # Comportamento de schema e constraints
    allow_drop: bool = False
    skip_constraints: bool = False

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
    strict_fk_validation: bool = True
    auto_repair_level: Literal["none", "basic", "aggressive"] = "basic"
    telemetry_max_bytes: int = 10_000_000
    quarantine_max_bytes: int = 10_000_000
    telemetry_rotate_daily: bool = True
    quarantine_rotate_daily: bool = True
    enable_constraints_backfill: bool = True
    enable_cep_enrichment: bool = False
    cep_map_path: Optional[Path] = None
    enable_quality_gates: bool = True
    gate_max_changed_ratio: float = 0.3
    gate_max_null_delta_ratio: float = 0.3
    gate_log_level: Literal["INFO", "WARNING"] = "WARNING"
    gate_min_rows: int = 20
    enable_metrics_prometheus: bool = False
    prometheus_metrics_path: Optional[Path] = None
    enable_prometheus_push: bool = False
    prometheus_push_url: Optional[str] = None
    prometheus_job: str = "cnpj_auto_repair"
    prometheus_instance: str = "local"
    enable_otlp_push: bool = False
    otlp_endpoint: Optional[str] = None
    skip_invalid_estabelecimentos_cnpj: bool = False
    cep_correct_uf_only_if_null: bool = True
    municipio_name_map_path: Optional[Path] = None

    user_agent_rotation: Literal["random", "sequential"] = "random"
    user_agents: list[str] = []

    
    @computed_field
    def download_url(self) -> str:
        """Monta a URL completa baseada na data alvo."""
        if not self.target_date:
            return ""
        return f"{self.rfb_base_url}{self.target_date}/"

    @computed_field
    def state_file(self) -> Path:
        base = cast(Path, self.data_dir)
        return base / "last_version_processed.txt"

    @computed_field
    def data_dir(self) -> Path:
        return self.project_root / "data"

    @computed_field
    def log_dir(self) -> Path:
        return self.project_root / "logs"

    @computed_field
    def compressed_dir(self) -> Path:
        base = cast(Path, self.data_dir)
        return base / "compressed_files"

    @computed_field
    def extracted_dir(self) -> Path:
        base = cast(Path, self.data_dir)
        return base / "extracted_files"

    @computed_field
    def queries_dir(self) -> Path:
        return self.project_root / "queries"

    @computed_field
    def telemetry_dir(self) -> Path:
        base = cast(Path, self.log_dir)
        return base / "telemetry"

    @computed_field
    def auto_repair_dir(self) -> Path:
        base = cast(Path, self.log_dir)
        return base / "auto_repair"

    @computed_field
    def quarantine_dir(self) -> Path:
        base = cast(Path, self.log_dir)
        return base / "quarantine"

    @computed_field
    def database_uri(self) -> str:
        if not all(
            [
                self.postgres_user,
                self.postgres_password,
                self.postgres_host,
                self.postgres_port,
                self.postgres_database,
            ]
        ):
            return ""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_database}"
        )

    def create_dirs(self):
        """Garante que a estrutura de pastas exista."""
        cast(Path, self.data_dir).mkdir(parents=True, exist_ok=True)
        cast(Path, self.log_dir).mkdir(parents=True, exist_ok=True)
        cast(Path, self.compressed_dir).mkdir(parents=True, exist_ok=True)
        cast(Path, self.extracted_dir).mkdir(parents=True, exist_ok=True)
        cast(Path, self.queries_dir).mkdir(parents=True, exist_ok=True)
        cast(Path, self.telemetry_dir).mkdir(parents=True, exist_ok=True)
        cast(Path, self.quarantine_dir).mkdir(parents=True, exist_ok=True)
        cast(Path, self.auto_repair_dir).mkdir(parents=True, exist_ok=True)


# Conjunto de UF único para todo o projeto
UF_SET: set[str] = {
    'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN','RO','RS','RR','SC','SE','SP','TO'
}


# Instancia e cria diretórios
settings = Settings()
settings.create_dirs()


def setup_logging():
    """Configura o logger raiz."""
    log_file = cast(Path, settings.log_dir) / "cnpj.log"
    level = getattr(logging, settings.log_level)
    # Garante UTF-8 no console mesmo em ambientes Windows com CP1252
    console_stream = sys.stdout
    try:
        if hasattr(console_stream, "reconfigure"):
            console_stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        try:
            console_stream = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        except Exception:
            pass

    logging.basicConfig(
        level=level,
        format="%(levelname)s - %(asctime)s - [%(name)s] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_file, mode="w", encoding="utf-8"),
            logging.StreamHandler(console_stream),
        ],
    )


class PipelineStep(Enum):
    CHECK = "check"
    DOWNLOAD = "download"
    EXTRACT = "extract"
    CONSOLIDATE = "consolidate"
    LOAD = "load"


__all__ = [
    "Settings",
    "settings",
    "setup_logging",
    "PipelineStep",
]
