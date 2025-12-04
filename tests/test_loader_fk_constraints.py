from pathlib import Path

import src.database_loader as dl
from src.settings import settings


def _write_src_file(name: str, content: str):
    p = Path(dl.__file__).parent / name
    p.write_text(content, encoding="utf-8")
    return p


class _Recorder:
    def __init__(self):
        self.calls = []
    def __call__(self, kind, config_name, record):
        self.calls.append((kind, config_name, record))


class ConnRec:
    def __init__(self):
        self.autocommit = False
        self.exec_log = []
        self.commits = 0
        self.rollbacks = 0
    def cursor(self):
        return self
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc, tb):
        return False
    def execute(self, stmt, params=None):
        self.exec_log.append((stmt, params))
    def commit(self):
        self.commits += 1
    def rollback(self):
        self.rollbacks += 1
    def copy_expert(self, sql_stmt, file_obj):
        return None
    def fetchone(self):
        return (0,)
    def fetchall(self):
        return []


class _SQLStub:
    class _S:
        def __init__(self, s):
            self.s = s
        def join(self, parts):
            return _SQLStub._S(", ".join(parts))
        def format(self, **kwargs):
            return _SQLStub._S(self.s)
        def as_string(self, _conn):
            return self.s
    def SQL(self, s):
        return _SQLStub._S(s)
    def Identifier(self, s):
        return s


def test_validate_chunk_fk_violation_quarantine(tmp_path, monkeypatch):
    settings.project_root = Path(tmp_path)
    settings.create_dirs()
    d = settings.extracted_dir / "estabelecimentos"
    d.mkdir(parents=True, exist_ok=True)
    (d / "estabelecimentos.csv").write_text("XX;12345678;0001;00\nSP;87654321;0002;11\n", encoding="utf-8")
    monkeypatch.setattr(dl, "schema_validate", lambda n, c: (c, {"changed_counts": {}, "null_deltas": {}}, {}))
    rec = _Recorder()
    monkeypatch.setattr(dl, "_write_jsonl", rec)
    def fake_validate_chunk(name, chunk, conn):
        m = chunk.index == 0
        return {"cnpj_basico": m}
    monkeypatch.setattr(dl, "validate_chunk", fake_validate_chunk)
    monkeypatch.setattr(dl, "sql", _SQLStub())
    conn = ConnRec()
    dl.process_and_load_file(conn, "estabelecimentos")
    assert any(k == "quarantine" and r.get("reason") == "fk_violation" for k, _, r in rec.calls)


# Removed constraints file mutation test to avoid interfering with integration run


def test_additional_sanitization_branches_in_loader(tmp_path, monkeypatch):
    settings.project_root = Path(tmp_path)
    settings.create_dirs()
    settings.strict_fk_validation = False
    # Empresas with complex capital
    ed = settings.extracted_dir / "empresas"
    ed.mkdir(parents=True, exist_ok=True)
    (ed / "empresas.csv").write_text("capital_social\n1.234.567,89\n", encoding="utf-8")
    # Estabelecimentos with multi secondary CNAE and phones
    sd = settings.extracted_dir / "estabelecimentos"
    sd.mkdir(parents=True, exist_ok=True)
    cols = dl.ETL_CONFIG["estabelecimentos"]["column_names"]
    vals = {
        "cnae_fiscal_secundaria": "11;22,33",
        "ddd_1": "0a2",
        "telefone_1": "111-222-333",
        "data_inicio_atividade": "20230101",
        "data_situacao_cadastral": "20230202",
        "data_situacao_especial": "20230303",
    }
    line = ";".join(vals.get(c, "") for c in cols)
    (sd / "estabelecimentos.csv").write_text(line + "\n", encoding="utf-8")
    # Simples with dates
    smd = settings.extracted_dir / "simples"
    smd.mkdir(parents=True, exist_ok=True)
    (smd / "simples.csv").write_text("data_opcao_pelo_simples;data_opcao_pelo_mei\n20230101;20230202\n", encoding="utf-8")
    monkeypatch.setattr(dl, "schema_validate", lambda n, c: (c, {"changed_counts": {}, "null_deltas": {}}, {}))
    monkeypatch.setattr(dl, "_ensure_domains_loaded", lambda *_a, **_kw: None)
    monkeypatch.setattr(dl, "sql", _SQLStub())
    conn = ConnRec()
    for name in ["empresas","estabelecimentos","simples"]:
        dl.process_and_load_file(conn, name)
    # Summary should reflect rows
    tele = dl._jsonl_dir("telemetry")
    assert (tele / "empresas_summary.json").exists()
    assert (tele / "estabelecimentos_summary.json").exists()
    assert (tele / "simples_summary.json").exists()
