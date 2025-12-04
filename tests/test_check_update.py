from pathlib import Path

import src.check_update as cu
from src.settings import settings


def test_get_latest_remote_date_html(monkeypatch):
    html = """
    <html><body>
    <a href="2025-10/">2025-10/</a>
    <a href="2025-11/">2025-11/</a>
    <a href="2024-12/">2024-12/</a>
    </body></html>
    """
    class FakeResp:
        text = html
        def raise_for_status(self):
            return None
    monkeypatch.setattr(cu.requests, "get", lambda url, timeout=30: FakeResp())
    d = cu.get_latest_remote_date()
    assert d == "2025-11"


def test_get_latest_remote_date_none(monkeypatch):
    html = "<html><body><a href=\"foo\">foo</a></body></html>"
    class FakeResp:
        text = html
        def raise_for_status(self):
            return None
    monkeypatch.setattr(cu.requests, "get", lambda url, timeout=30: FakeResp())
    d = cu.get_latest_remote_date()
    assert d is None


def test_clean_data_dirs(tmp_path, monkeypatch):
    settings.project_root = Path(tmp_path)
    settings.create_dirs()
    (settings.compressed_dir / "a.zip").write_text("x")
    (settings.extracted_dir / "dir1").mkdir(parents=True, exist_ok=True)
    (settings.extracted_dir / "dir1" / "f.txt").write_text("y")
    (settings.extracted_dir / "b.txt").write_text("z")
    cu.clean_data_dirs()
    assert list(settings.compressed_dir.glob("*")) == []
    assert list(settings.extracted_dir.glob("*")) == []


def test_check_updates_updates_target_date(tmp_path, monkeypatch):
    settings.project_root = Path(tmp_path)
    settings.create_dirs()
    (settings.state_file).write_text("2025-09")
    html = """
    <a href="2025-10/">2025-10/</a>
    <a href="2025-11/">2025-11/</a>
    """
    class FakeResp:
        text = html
        def raise_for_status(self):
            return None
    monkeypatch.setattr(cu.requests, "get", lambda url, timeout=30: FakeResp())
    d = cu.check_updates(skip_clean=True)
    assert d == "2025-11"
    assert settings.target_date == "2025-11"
