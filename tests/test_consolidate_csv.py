from pathlib import Path
from src.consolidate_csv import get_source_csv_files


def test_get_source_csv_files_excludes_output_and_filters_csv(tmp_path: Path):
    a = tmp_path / "a.csv"
    b = tmp_path / "b.CSV"
    c = tmp_path / "c.txt"
    out = tmp_path / "dir.csv"
    a.write_text("1\n")
    b.write_text("2\n")
    c.write_text("x\n")
    out.write_text("")

    result = sorted(p.name for p in get_source_csv_files(tmp_path, "dir.csv"))
    assert result == ["a.csv", "b.CSV"]
