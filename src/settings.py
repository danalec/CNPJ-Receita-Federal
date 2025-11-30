import json
import logging
from pathlib import Path

from typing import Literal, Dict, Optional
from enum import Enum
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import computed_field


def setup_logging():
    log_file = settings.log_dir / "cnpj.log"
    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format="%(levelname)s - %(asctime)s - [%(name)s] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_file, mode="w"),
            logging.StreamHandler(),
        ],
    )


class PipelineStep(str, Enum):
    CHECK = "check"
    DOWNLOAD = "download"
    EXTRACT = "extract"
    CONSOLIDATE = "consolidate"
    LOAD = "load"
    CONSTRAINTS = "constraints"


class StepStatus(str, Enum):
    COMPLETED = "completed"
    FAILED = "failed"
    RUNNING = "running"


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

    # Configurações de Download e Extração

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
    skip_constraints: bool = False

    # Lógicas de tratamento de texto
    normalize_line_endings: bool = True
    strip_bom: bool = True

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
    def queries_dir(self) -> Path:
        return self.project_root / "queries"

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
        dirs = [
            self.data_dir,
            self.compressed_dir,
            self.extracted_dir,
            self.queries_dir,
            self.log_dir,
        ]
        for p in dirs:
            p.mkdir(parents=True, exist_ok=True)


class ProcessState:
    def __init__(self, data_dir: Path):
        self.file_path = data_dir / "state.json"
        self.data = self._load()

    def _load(self) -> Dict:
        """Carrega o JSON. Retorna vazio se não existir."""
        if not self.file_path.exists():
            return {}
        try:
            return json.loads(self.file_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {}

    def _save(self):
        """Persiste o estado atual no disco."""
        self.file_path.write_text(json.dumps(self.data, indent=2), encoding="utf-8")

    @property
    def target_date(self) -> Optional[str]:
        return self.data.get("target_date")

    @property
    def current_status(self) -> Optional[str]:
        return self.data.get("status")

    @property
    def current_state(self) -> Optional[str]:
        return self.data.get("stage")

    @target_date.setter
    def target_date(self, value: str):
        """Define a data alvo. Reseta o estágio para None se a data mudar."""
        current = self.data.get("target_date")
        if value != current:
            self.data = {"target_date": value, "stage": None, "status": None}
            self._save()

    def update(self, step: PipelineStep, status: StepStatus):
        """
        Atualiza o estágio atual e seu status.
        Ex: stage='download', status='completed'
        """
        self.data["stage"] = step.value
        self.data["status"] = status.value
        self._save()

    def should_skip(self, current_step: PipelineStep) -> bool:
        """
        Verifica se deve pular a etapa atual baseada no histórico salvo.
        Retorna True se a etapa já foi concluída ou superada.
        """

        if not self.target_date:
            return False

        saved_stage = self.data.get("stage")
        saved_status = self.data.get("status")

        if not saved_stage:
            return False

        # Define a ordem exata de execução
        # (Isso precisa bater com a ordem do PipelineStep Enum na main)
        order = [
            PipelineStep.CHECK.value,
            PipelineStep.DOWNLOAD.value,
            PipelineStep.EXTRACT.value,
            PipelineStep.CONSOLIDATE.value,
            PipelineStep.LOAD.value,
            PipelineStep.CONSTRAINTS.value,
        ]

        try:
            current_index = order.index(current_step.value)
            saved_index = order.index(saved_stage)
        except ValueError:
            # Se tiver algum valor estranho no JSON, não pula por segurança
            return False

        # Se a etapa salva é SUPERIOR a atual (ex: Salvo=Load, Atual=Download) -> PULA
        if saved_index > current_index:
            return True

        # Se é a MESMA etapa e está COMPLETED -> PULA
        if saved_index == current_index and saved_status == StepStatus.COMPLETED.value:
            return True

        return False


# --- Instanciação Global ---
settings = Settings()
settings.create_dirs()

# Instância única do Estado
state = ProcessState(settings.data_dir)
