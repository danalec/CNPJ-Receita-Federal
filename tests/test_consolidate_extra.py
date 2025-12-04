from pathlib import Path

from src.settings import settings
from src.consolidate_csv import get_subdirectories, concatenate_files_in_directory, run_consolidation


def test_get_subdirectories_handles_missing(tmp_path):
    base = tmp_path / "missing"
    subs = list(get_subdirectories(base))
    assert subs == []


def test_concatenate_files_basic_bom_and_crlf(tmp_path):
    d = tmp_path / "dir"
    d.mkdir()
    (d / "x1.bin").write_bytes(b"\xEF\xBB\xBFa,b\r\n1,2\r\n")
    (d / "x2.bin").write_bytes(b"c,d\r\n3,4\r\n")
    concatenate_files_in_directory(d, delete_sources=False)
    out = (d / "dir.csv").read_bytes()
    assert b"\xEF\xBB\xBF" not in out
    assert out.count(b"\r\n") == 0
    assert out.count(b"\n") >= 3


def test_concatenate_files_delete_sources(tmp_path):
    d = tmp_path / "alpha"
    d.mkdir()
    (d / "a.bin").write_bytes(b"1\n")
    (d / "b.bin").write_bytes(b"2\n")
    concatenate_files_in_directory(d, delete_sources=True)
    assert (d / "alpha.csv").exists()
    assert not (d / "a.bin").exists()
    assert not (d / "b.bin").exists()


def test_run_consolidation_missing_extracted_dir(tmp_path):
    settings.project_root = Path(tmp_path)
    extracted = settings.project_root / "data" / "extracted_files"
    if extracted.exists():
        for p in extracted.glob("**/*"):
            if p.is_file():
                p.unlink()
            else:
                p.rmdir()
        extracted.rmdir()
    try:
        run_consolidation()
        raised = False
    except FileNotFoundError:
        raised = True
    assert raised


def test_run_consolidation_happy_path(tmp_path):
    settings.project_root = Path(tmp_path)
    settings.create_dirs()
    sub = settings.extracted_dir / "empresas"
    sub.mkdir(parents=True, exist_ok=True)
    (sub / "f1.bin").write_bytes(b"a\r\nb\r\n")
    (sub / "f2.bin").write_bytes(b"c\r\nd\r\n")
    run_consolidation(delete_sources=True)
    outp = sub / "empresas.csv"
    assert outp.exists()
    assert not (sub / "f1.bin").exists()
    assert b"\r\n" not in outp.read_bytes()
