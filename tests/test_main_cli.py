import sys
from types import SimpleNamespace

import src.__main__ as m


def test_main_step_check(monkeypatch):
    called = []
    monkeypatch.setattr("src.check_update.check_updates", lambda: called.append("check"))
    argv = ["prog", "--step", "check"]
    monkeypatch.setattr(sys, "argv", argv)
    m.main()
    assert called == ["check"]


def test_main_step_download(monkeypatch):
    called = []
    monkeypatch.setattr("src.check_update.check_updates", lambda: None)
    monkeypatch.setattr("src.downloader.run_download", lambda: called.append("download"))
    argv = ["prog", "--step", "download"]
    monkeypatch.setattr(sys, "argv", argv)
    m.main()
    assert called == ["download"]


def test_main_full_pipeline_force(monkeypatch):
    calls = []
    monkeypatch.setattr("src.check_update.check_updates", lambda: None)
    monkeypatch.setattr("src.downloader.run_download", lambda: calls.append("download"))
    monkeypatch.setattr("src.extract_files.run_extraction", lambda: calls.append("extract"))
    monkeypatch.setattr("src.consolidate_csv.run_consolidation", lambda: calls.append("consolidate"))
    monkeypatch.setattr("src.database_loader.run_loader", lambda: calls.append("load"))
    monkeypatch.setattr("src.database_loader.run_constraints", lambda: calls.append("constraints"))
    dummy_state = SimpleNamespace(
        should_skip=lambda step: False,
        update=lambda step, status: None,
    )
    monkeypatch.setattr(m, "state", dummy_state)
    argv = ["prog", "--force"]
    monkeypatch.setattr(sys, "argv", argv)
    m.main()
    assert calls == ["download", "extract", "consolidate", "load", "constraints"]
