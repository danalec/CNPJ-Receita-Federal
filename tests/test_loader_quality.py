from pathlib import Path
import pandas as pd

import src.database_loader as dl
from src.settings import settings


class _Recorder:
    def __init__(self):
        self.calls = []
    def __call__(self, kind, config_name, record):
        self.calls.append((kind, config_name, record))


class FakeConn:
    def __init__(self):
        self.autocommit = False
        self.cursors = []
        self.commits = 0
        self.rollbacks = 0
    def cursor(self):
        return self
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc, tb):
        return False
    def execute(self, stmt, params=None):
        return None
    def fetchone(self):
        return (0,)
    def commit(self):
        self.commits += 1
    def rollback(self):
        self.rollbacks += 1
    def copy_expert(self, sql_stmt, file_obj):
        return None


class _FakeSQL:
    class _S:
        def __init__(self, s):
            self.s = s
        def join(self, parts):
            return _FakeSQL._S(", ".join(parts))
        def format(self, **kwargs):
            return _FakeSQL._S(self.s)
        def as_string(self, _conn):
            return self.s
    def SQL(self, s):
        return _FakeSQL._S(s)
    def Identifier(self, s):
        return s


def _make_csv(tmp_path: Path, sub: str, name: str, text: str, encoding: str = "utf-8"):
    settings.project_root = Path(tmp_path)
    settings.create_dirs()
    d = settings.extracted_dir / sub
    d.mkdir(parents=True, exist_ok=True)
    p = d / name
    p.write_text(text, encoding=encoding)
    return p


def test_quality_gate_writes_telemetry_and_quarantine(tmp_path, monkeypatch):
    _make_csv(tmp_path, "empresas", "empresas.csv", "a;b\n1;2\n3;4\n")
    rec = _Recorder()
    monkeypatch.setattr(dl, "_write_jsonl", rec)
    settings.gate_min_rows = 1
    settings.gate_max_changed_ratio = 0.1
    settings.gate_max_null_delta_ratio = 0.1
    def fake_validate(name, chunk):
        rows = int(len(chunk))
        tel = {
            "changed_counts": {"a": rows},
            "null_deltas": {"b": rows},
        }
        return chunk, tel, {}
    monkeypatch.setattr(dl, "schema_validate", fake_validate)
    conn = FakeConn()
    dl.process_and_load_file(conn, "empresas")
    kinds = [k for k, _, _ in rec.calls]
    assert "telemetry" in kinds
    assert any((k == "quarantine" and r.get("reason") == "quality_gate") for k, _, r in rec.calls)


def test_fk_subset_quarantine_counts(tmp_path, monkeypatch):
    settings.project_root = Path(tmp_path)
    settings.create_dirs()
    d = settings.extracted_dir / "estabelecimentos"
    d.mkdir(parents=True, exist_ok=True)
    cols = dl.ETL_CONFIG["estabelecimentos"]["column_names"]
    rows = [
        {"pais_codigo": "999", "municipio_codigo": "888", "cnpj_basico": "00000001", "cnpj_ordem": "0001", "cnpj_dv": "00"},
        {"pais_codigo": "150", "municipio_codigo": "1234", "cnpj_basico": "00000002", "cnpj_ordem": "0002", "cnpj_dv": "11"},
    ]
    def mk(r):
        return ";".join(str(r.get(c, "")) for c in cols)
    content = "\n".join(mk(r) for r in rows)
    (d / "estabelecimentos.csv").write_text(content + "\n", encoding="utf-8")
    monkeypatch.setattr(dl, "schema_validate", lambda n, c: (c, {"changed_counts": {}, "null_deltas": {}}, {}))
    def fake_validate_chunk(name, chunk, conn):
        m1 = chunk.index == 0
        m2 = chunk.index == 0
        return {"pais_codigo": m1, "municipio_codigo": m2}
    monkeypatch.setattr(dl, "validate_chunk", fake_validate_chunk)
    rec = _Recorder()
    monkeypatch.setattr(dl, "_write_jsonl", rec)
    monkeypatch.setattr(dl, "sql", _FakeSQL())
    conn = FakeConn()
    dl.process_and_load_file(conn, "estabelecimentos")
    violations = [r for k, _, r in rec.calls if k == "quarantine" and r.get("reason") == "fk_violation"]
    labels = [tuple(r.get("fields", [])) for r in violations]
    assert (("pais_codigo",) in labels) and (("municipio_codigo",) in labels)


def test_critical_fields_quarantine(tmp_path, monkeypatch):
    # cnpj_basico critical nulls should be quarantined
    _make_csv(tmp_path, "empresas", "empresas.csv", "cnpj_basico;razao_social\n;X\n")
    rec = _Recorder()
    monkeypatch.setattr(dl, "_write_jsonl", rec)
    monkeypatch.setattr(dl, "schema_validate", lambda n, c: (c, {}, {}))
    conn = FakeConn()
    monkeypatch.setattr(dl, "sql", _FakeSQL())
    dl.process_and_load_file(conn, "empresas")
    assert any(r.get("reason") == "critical_fields_null" for _, _, r in rec.calls)


def test_invalid_cnpj_masks_skip_and_quarantine(tmp_path, monkeypatch):
    _make_csv(tmp_path, "estabelecimentos", "estabelecimentos.csv", "cnpj_basico;cnpj_ordem;cnpj_dv\n12345678;0001;00\n87654321;0002;11\n")
    rec = _Recorder()
    monkeypatch.setattr(dl, "_write_jsonl", rec)
    def fake_validate(name, c):
        m = pd.Series([True] + [False] * (len(c) - 1), index=c.index, dtype=bool)
        return c, {"changed_counts": {}, "null_deltas": {}}, {"invalid_cnpj": m}
    monkeypatch.setattr(dl, "schema_validate", fake_validate)
    settings.skip_invalid_estabelecimentos_cnpj = True
    called = {"rows": []}
    def fake_load(conn, df, table_name):
        called["rows"].append(int(len(df)))
    monkeypatch.setattr(dl, "fast_load_chunk", fake_load)
    conn = FakeConn()
    dl.process_and_load_file(conn, "estabelecimentos")
    assert any(r.get("reason") == "invalid_cnpj" for _, _, r in rec.calls)


def test_prometheus_metrics_and_push(tmp_path, monkeypatch):
    _make_csv(tmp_path, "empresas", "empresas.csv", "a;b\n1;2\n")
    out_prom = Path(tmp_path) / "metrics.prom"
    settings.enable_quality_gates = False
    settings.enable_metrics_prometheus = True
    settings.prometheus_metrics_path = str(out_prom)
    settings.enable_prometheus_push = True
    settings.prometheus_push_url = "http://example.com"
    def fake_validate(name, chunk):
        tel = {"changed_counts": {"a": len(chunk)}, "null_deltas": {"b": len(chunk)}}
        return chunk, tel, {}
    monkeypatch.setattr(dl, "schema_validate", fake_validate)
    posted = {"n": 0, "last": None}
    def fake_post(url, data=None, headers=None, timeout=None):
        posted["n"] += 1
        posted["last"] = (url, data)
    monkeypatch.setattr(dl.requests, "post", fake_post)
    conn = FakeConn()
    monkeypatch.setattr(dl, "sql", _FakeSQL())
    dl.process_and_load_file(conn, "empresas")
    assert out_prom.exists()
    content = out_prom.read_text(encoding="utf-8")
    assert "cnpj_auto_repair_rows_total" in content
    assert posted["n"] >= 1


def test_otlp_summary_push(tmp_path, monkeypatch):
    _make_csv(tmp_path, "empresas", "empresas.csv", "a;b\n1;2\n")
    settings.enable_otlp_push = True
    settings.otlp_endpoint = "http://otel.local"
    monkeypatch.setattr(dl, "schema_validate", lambda n, c: (c, {}, {}))
    posted = {"n": 0, "payload": None}
    def fake_post(url, payload=None, timeout=None):
        posted["n"] += 1
        posted["payload"] = payload
    monkeypatch.setattr(dl.requests, "post", fake_post)
    conn = FakeConn()
    monkeypatch.setattr(dl, "sql", _FakeSQL())
    dl.process_and_load_file(conn, "empresas")
    dir_path = dl._jsonl_dir("telemetry")
    summ = (dir_path / "empresas_summary.json").read_text(encoding="utf-8")
    import json as _json
    payload = _json.loads(summ)
    assert int(payload.get("rows_total", 0)) >= 1


def test_execute_sql_file_autocommit_constraints(monkeypatch):
    conn = FakeConn()
    # Use real constraints.sql that exists in src directory; ensure no commit in autocommit path
    dl.execute_sql_file(conn, "constraints.sql")
    assert conn.autocommit is False
    assert conn.commits == 0
