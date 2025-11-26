import logging
import time
import argparse

from src.extract_files import run_extraction
from src.downloader import run_download
from src.consolidate_csv import run_consolidation
from src.database_loader import run_loader, run_queries_in_dir
from src.settings import setup_logging, settings
from src.check_update import check_updates, update_local_version
from src.state import start_run, mark_stage, get_run_for_date, print_status

setup_logging()
logger = logging.getLogger(__name__)


def cmd_check(args):
    date = check_updates()
    if date:
        start_run(date)
        mark_stage(date, "check", "completed")
        logger.info(f"Nova versão detectada: {date}")
    else:
        logger.info("Dados já estão atualizados.")


def cmd_download(args):
    run_download()
    if settings.target_date:
        mark_stage(settings.target_date, "download", "completed")


def cmd_extract(args):
    run_extraction()
    if settings.target_date:
        mark_stage(settings.target_date, "extract", "completed")


def cmd_consolidate(args):
    run_consolidation(delete_sources=getattr(args, "delete_sources", False))
    if settings.target_date:
        mark_stage(settings.target_date, "consolidate", "completed")


def cmd_load(args):
    only = args.only or None
    exclude = args.exclude or None
    run_loader(only=only, exclude=exclude)
    if settings.target_date:
        mark_stage(settings.target_date, "load", "completed")
    if getattr(args, "run_queries", False):
        run_queries_in_dir(settings.queries_dir)


def cmd_full(args):
    start = time.time()
    date = check_updates(skip_clean=args.resume)
    if date is None and not args.resume:
        logger.info("Dados já estão atualizados.")
        return
    if date:
        start_run(date)
        mark_stage(date, "check", "completed")
    current = date or settings.target_date
    if args.dry_run:
        logger.info("Dry-run ativo. Etapas: check, download, extract, consolidate, load.")
        return
    if not current:
        current = date
    statuses = print_status(current, return_map=True) if current else {}
    if not args.resume or statuses.get("download") != "completed":
        run_download()
        mark_stage(current, "download", "completed")
    if not args.resume or statuses.get("extract") != "completed":
        run_extraction()
        mark_stage(current, "extract", "completed")
    if not args.resume or statuses.get("consolidate") != "completed":
        run_consolidation(delete_sources=getattr(args, "delete_sources", False))
        mark_stage(current, "consolidate", "completed")
    only = args.only or None
    exclude = args.exclude or None
    if not args.resume or statuses.get("load") != "completed":
        run_loader(only=only, exclude=exclude)
        mark_stage(current, "load", "completed")
    if getattr(args, "run_queries", False):
        run_queries_in_dir(settings.queries_dir)
    if date:
        update_local_version(date)
    end = time.time()
    logger.info(f"Tempo total: {end - start:.2f}s")


def cmd_status(args):
    print_status(args.date or None)


def build_parser():
    parser = argparse.ArgumentParser(prog="cnpj", description="ETL CNPJ")
    sub = parser.add_subparsers(dest="command")
    p_check = sub.add_parser("check")
    p_check.set_defaults(func=cmd_check)
    p_download = sub.add_parser("download")
    p_download.set_defaults(func=cmd_download)
    p_extract = sub.add_parser("extract")
    p_extract.set_defaults(func=cmd_extract)
    p_consolidate = sub.add_parser("consolidate")
    p_consolidate.set_defaults(func=cmd_consolidate)
    p_consolidate.add_argument("--delete-sources", action="store_true")
    p_load = sub.add_parser("load")
    p_load.add_argument("--only", nargs="*")
    p_load.add_argument("--exclude", nargs="*")
    p_load.add_argument("--run-queries", action="store_true")
    p_load.set_defaults(func=cmd_load)
    p_full = sub.add_parser("full")
    p_full.add_argument("--resume", action="store_true")
    p_full.add_argument("--dry-run", action="store_true")
    p_full.add_argument("--only", nargs="*")
    p_full.add_argument("--exclude", nargs="*")
    p_full.add_argument("--run-queries", action="store_true")
    p_full.add_argument("--delete-sources", action="store_true")
    p_full.set_defaults(func=cmd_full)
    p_queries = sub.add_parser("queries")
    p_queries.set_defaults(func=lambda a: run_queries_in_dir(settings.queries_dir))
    p_status = sub.add_parser("status")
    p_status.add_argument("--date")
    p_status.set_defaults(func=cmd_status)
    return parser


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()
    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()
