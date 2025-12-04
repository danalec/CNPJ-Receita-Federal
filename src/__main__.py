import argparse
import logging
import sys
import time

from enum import Enum
from . import extract_files, downloader, consolidate_csv, database_loader, check_update
from .settings import setup_logging
from .state import state


class PipelineStep(Enum):
    CHECK = "check"
    DOWNLOAD = "download"
    EXTRACT = "extract"
    CONSOLIDATE = "consolidate"
    LOAD = "load"
    CONSTRAINTS = "constraints"


def main():
    setup_logging()
    logger = logging.getLogger("orchestrator")

    PIPELINE_MAP = {
        PipelineStep.DOWNLOAD: downloader.run_download,
        PipelineStep.EXTRACT: extract_files.run_extraction,
        PipelineStep.CONSOLIDATE: consolidate_csv.run_consolidation,
        PipelineStep.LOAD: database_loader.run_loader,
        PipelineStep.CONSTRAINTS: database_loader.run_constraints,
    }

    def run_pipeline(force: bool = False):
        start_time = time.time()
        logger.info("🚀 Iniciando Pipeline CNPJ...")

        check_update.check_updates()

        for step_name, step_func in PIPELINE_MAP.items():
            if not force and state.should_skip(step_name.value):
                logger.info(f"⏭️  [SKIP] {step_name.value.upper()}")
                continue
            logger.info(f"▶️  [RUN] {step_name.value.upper()}...")
            state.update(step_name.value, "running")
            try:
                step_func()
                state.update(step_name.value, "completed")
                logger.info(f"✅ [OK] {step_name.value.upper()}")
            except KeyboardInterrupt:
                logger.warning("\n⚠️ Interrompido pelo usuário.")
                state.update(step_name.value, "failed")
                sys.exit(130)
            except SystemExit:
                raise
            except Exception as e:
                logger.error(f"❌ [ERRO] {step_name.value.upper()}: {e}", exc_info=True)
                state.update(step_name.value, "failed")
                sys.exit(1)

        elapsed = time.time() - start_time
        logger.info(f"🏁 Pipeline Finalizado com Sucesso em {elapsed:.2f}s")

    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="Ignora o histórico e roda tudo.")
    parser.add_argument(
        "--step",
        choices=[s.value for s in PipelineStep],
        help="Executa apenas a etapa especificada do pipeline",
    )
    parser.add_argument(
        "--no-csv-filter",
        action="store_true",
        help="Desabilita filtros de CSV (linhas malformadas e vazias)",
    )
    args = parser.parse_args()

    if args.no_csv_filter:
        pass

    if args.step:
        step_value = args.step
        if step_value == PipelineStep.CHECK.value:
            check_update.check_updates()
        else:
            func = PIPELINE_MAP.get(PipelineStep(step_value))
            if func:
                state.update(step_value, "running")
                func()
                state.update(step_value, "completed")
            else:
                logger.error(f"Etapa desconhecida: {step_value}")
                sys.exit(2)
    else:
        run_pipeline(force=args.force)


if __name__ == "__main__":
    main()

