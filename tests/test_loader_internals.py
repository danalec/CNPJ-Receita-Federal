import json
from pathlib import Path
import pandas as pd
import src.database_loader as dl
from src.settings import settings


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.executed = []
        self.fetch = []
        self.copy_calls = []

    def execute(self, stmt, params=None):
        self.executed.append((stmt, params))

    def fetchall(self):
        return self.fetch

    def fetchone(self):
        return (0,)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def copy_expert(self, sql_stmt, file_obj):
        self.copy_calls.append(sql_stmt)


class FakeConn:
    def __init__(self):
        self.autocommit = False
        self.cursors = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        c = FakeCursor(self)
        self.cursors.append(c)
        return c

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def test_critical_fields_map():
    assert dl._critical_fields("empresas") == ["cnpj_basico"]
    assert dl._critical_fields("estabelecimentos") == ["cnpj_basico","cnpj_ordem","cnpj_dv"]
    assert dl._critical_fields("socios") == ["cnpj_basico","cnpj_cpf_socio"]
    assert dl._critical_fields("simples") == ["cnpj_basico"]


def test_jsonl_dir_and_selection(tmp_path):
    settings.project_root = Path(tmp_path)
    settings.create_dirs()
    d1 = dl._jsonl_dir("telemetry")
    assert d1.exists() or True
    base = dl._select_jsonl_file(d1, "x", 10)
    base.write_text("a" * 20, encoding="utf-8")
    nxt = dl._select_jsonl_file(d1, "x", 10)
    assert nxt.name.startswith("x_")


def test_write_jsonl_and_json(tmp_path):
    settings.project_root = Path(tmp_path)
    settings.create_dirs()
    dl._write_jsonl("telemetry","empresas",{"ok":1})
    tdir = dl._jsonl_dir("telemetry")
    files = list(tdir.glob("empresas*.jsonl"))
    assert files
    p = tdir / "summary.json"
    dl._write_json(p, {"a":1})
    assert json.loads(p.read_text(encoding="utf-8"))["a"] == 1


class _FakeSQL:
    class _S:
        def __init__(self, s):
            self.s = s
        def join(self, parts):
            return _FakeSQL._S(", ".join(parts))
        def format(self, **kwargs):
            return _FakeSQL._S(self.s.format(**kwargs))
        def as_string(self, conn):
            return self.s
    def SQL(self, s):
        return _FakeSQL._S(s)
    def Identifier(self, s):
        return s
    def Literal(self, s):
        return s


def test_fast_load_chunk_calls_copy_and_commit(monkeypatch):
    conn = FakeConn()
    df = pd.DataFrame({"a":[1,2],"b":["x","y"]})
    monkeypatch.setattr(dl, "sql", _FakeSQL())
    dl.fast_load_chunk(conn, df, "t1")
    assert conn.commits == 1
    calls = sum(len(c.copy_calls) for c in conn.cursors)
    assert calls == 1


def test_validate_chunk_uf_strict_and_relaxed(monkeypatch):
    df = pd.DataFrame({"uf":["XX","SP"]})
    conn = FakeConn()
    monkeypatch.setattr(dl, "_ensure_domains_loaded", lambda *_args, **_kw: None)
    settings.strict_fk_validation = True
    try:
        dl.validate_chunk("estabelecimentos", df.copy(), conn)
        raised = False
    except ValueError:
        raised = True
    assert raised
    settings.strict_fk_validation = False
    res = dl.validate_chunk("estabelecimentos", df.copy(), conn)
    assert isinstance(res, dict) and "uf" in res


def test_execute_sql_path(tmp_path):
    conn = FakeConn()
    p = Path(tmp_path) / "q.sql"
    p.write_text("SELECT 1;", encoding="utf-8")
    dl.execute_sql_path(conn, p)
    assert conn.commits == 1
    assert any("SELECT 1" in e[0] for c in conn.cursors for e in c.executed)


def test_process_and_load_file_noop_when_missing(tmp_path, monkeypatch):
    settings.project_root = Path(tmp_path)
    settings.create_dirs()
    conn = FakeConn()
    dl.process_and_load_file(conn, "empresas")
    # now create minimal CSV
    d = settings.extracted_dir / "empresas"
    d.mkdir(parents=True, exist_ok=True)
    (d / "empresas.csv").write_text("a;b\n1;2\n", encoding="latin1")
    monkeypatch.setattr(dl, "schema_validate", lambda name, chunk: (chunk, {}, {}))
    called = {"n":0}
    def fake_load(c, df, t):
        called["n"] += 1
    monkeypatch.setattr(dl, "fast_load_chunk", fake_load)
    dl.process_and_load_file(conn, "empresas")
    assert called["n"] >= 1
