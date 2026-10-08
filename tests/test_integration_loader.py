import os
from pathlib import Path
import pytest

from src.settings import settings
from src.database_loader import run_loader, run_constraints

def _db_ready() -> bool:
    try:
        import psycopg2
        with psycopg2.connect(settings.database_uri, connect_timeout=2):
            return True
    except Exception:
        return False

def _should_run_integration() -> bool:
    return bool(settings.database_uri) and os.environ.get("PG_INTEGRATION") == "1" and _db_ready()

@pytest.mark.integration
@pytest.mark.skipif(not _should_run_integration(), reason="Integration tests disabled")
def test_loader_and_constraints_with_sample_domain_data(tmp_path: Path):
    settings.project_root = tmp_path
    settings.allow_drop = True
    settings.set_logged_after_copy = True
    settings.skip_constraints = False
    settings.postgres_user = os.environ.get("POSTGRES_USER", "cnpj")
    settings.postgres_password = os.environ.get("POSTGRES_PASSWORD", "cnpj")
    settings.postgres_host = os.environ.get("POSTGRES_HOST", "localhost")
    settings.postgres_port = int(os.environ.get("POSTGRES_PORT", "5432"))
    settings.postgres_database = os.environ.get("POSTGRES_DATABASE", "cnpj")
    settings.create_dirs()

    base = settings.extracted_dir

    def write(name: str, lines: list[str]):
        d = base / name
        d.mkdir(parents=True, exist_ok=True)
        (d / f"{name}.csv").write_text("\n".join(lines), encoding=settings.file_encoding)

    write("paises", ["150;Brasil"])
    write("municipios", ["1234;Cidade"])
    write("qualificacoes", ["99;Qualificação"])
    write("naturezas", ["9999;Natureza"])
    write("cnaes", ["1234567;Atividade"])

    write(
        "empresas",
        [
            "00000001;Empresa;9999;99;1000,00;1;",
        ],
    )

    write(
        "estabelecimentos",
        [
            ";".join(
                [
                    "00000001",  # cnpj_basico
                    "0001",       # cnpj_ordem
                    "00",         # cnpj_dv
                    "1",          # identificador_matriz_filial
                    "Fantasia",   # nome_fantasia
                    "2",          # situacao_cadastral
                    "20240101",   # data_situacao_cadastral
                    "0",          # motivo_situacao_cadastral
                    "",           # nome_cidade_exterior
                    "150",        # pais_codigo
                    "20240102",   # data_inicio_atividade
                    "1234567",    # cnae_fiscal_principal_codigo
                    "2345678,3456789",  # cnae_fiscal_secundaria
                    "Rua",        # tipo_logradouro
                    "Exemplo",    # logradouro
                    "123",        # numero
                    "",           # complemento
                    "Centro",     # bairro
                    "12345678",   # cep
                    "SP",         # uf
                    "1234",       # municipio_codigo
                    "11",         # ddd_1
                    "123456789",  # telefone_1
                    "",           # ddd_2
                    "",           # telefone_2
                    "",           # ddd_fax
                    "",           # fax
                    "contato@example.com",  # correio_eletronico
                    "",           # situacao_especial
                    "20240103",   # data_situacao_especial
                ]
            )
        ],
    )

    run_loader()
    run_constraints()

    import psycopg2
    conn = psycopg2.connect(settings.database_uri)
    cur = conn.cursor()

    cur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name='rfb'")
    assert cur.fetchone() is not None

    cur.execute("SELECT COUNT(*) FROM rfb.paises")
    assert cur.fetchone()[0] == 1
    cur.execute("SELECT COUNT(*) FROM rfb.empresas")
    assert cur.fetchone()[0] == 1
    cur.execute("SELECT COUNT(*) FROM rfb.estabelecimentos")
    assert cur.fetchone()[0] == 1

    cur.execute("SELECT relpersistence FROM pg_class WHERE relname='paises'")
    assert cur.fetchone()[0] == "p"

    for cname in [
        'paises_pkey',
        'fk_empresas_natureza',
        'fk_empresas_qualificacao',
        'fk_estabelecimentos_empresa',
        'fk_estabelecimentos_pais',
        'fk_estabelecimentos_municipio',
        'fk_estabelecimentos_cnae',
    ]:
        cur.execute("SELECT 1 FROM pg_constraint WHERE conname=%s", (cname,))
        assert cur.fetchone() is not None

    cur.execute(
        "SELECT COUNT(*) FROM rfb.estabelecimentos e JOIN rfb.empresas emp ON emp.cnpj_basico = e.cnpj_basico"
    )
    assert cur.fetchone()[0] == 1

    cur.execute(
        "SELECT count(*) FROM pg_constraint WHERE connamespace='rfb'::regnamespace"
    )
    assert cur.fetchone()[0] == 18, "expected 8 primary keys and 10 foreign keys"

    # The three trigram indexes are the reason pg_trgm is created at all; none of
    # them were asserted, so a regression that dropped every index still passed.
    for iname in [
        "idx_empresas_razao_social",
        "idx_estabelecimentos_nome_fantasia",
        "idx_socios_nome",
    ]:
        cur.execute(
            "SELECT 1 FROM pg_indexes WHERE schemaname='rfb' AND indexname=%s",
            (iname,),
        )
        assert cur.fetchone() is not None, f"missing trigram index {iname}"

    cur.execute("SELECT count(*) FROM pg_indexes WHERE schemaname='rfb'")
    assert cur.fetchone()[0] >= 10

    cur.execute("SELECT 1 FROM pg_extension WHERE extname='pg_trgm'")
    assert cur.fetchone() is not None, "pg_trgm was not created"

    conn.close()


@pytest.mark.integration
@pytest.mark.skipif(not _should_run_integration(), reason="Integration tests disabled")
def test_constraints_work_when_pg_trgm_lives_in_public(tmp_path: Path):
    """The trigram opclass is only reachable if public is on the search_path.

    CREATE EXTENSION without a SCHEMA clause is database-wide: when pg_trgm is
    already installed in public -- the default on RDS, Cloud SQL, Azure and
    Supabase -- CREATE EXTENSION IF NOT EXISTS is a silent no-op, and an opclass
    lookup only walks the search_path. The stock postgres image used by CI has
    no preinstalled pg_trgm, so this case is invisible there.
    """
    settings.project_root = tmp_path
    settings.allow_drop = True
    settings.set_logged_after_copy = True
    settings.skip_constraints = False
    settings.create_dirs()

    import psycopg2

    conn = psycopg2.connect(settings.database_uri)
    conn.autocommit = True
    cur = conn.cursor()

    try:
        cur.execute("DROP SCHEMA IF EXISTS rfb CASCADE")
        # Make the precondition explicit rather than depending on what another
        # test happened to leave behind.
        cur.execute("DROP EXTENSION IF EXISTS pg_trgm")
        cur.execute("CREATE EXTENSION pg_trgm")
        cur.execute(
            "SELECT n.nspname FROM pg_extension e JOIN pg_namespace n"
            " ON n.oid = e.extnamespace WHERE e.extname='pg_trgm'"
        )
        assert cur.fetchone()[0] == "public", "expected pg_trgm to already be in public"

        src = Path(__file__).resolve().parents[1] / "src"
        cur.execute((src / "schema.sql").read_text(encoding="utf-8"))
        cur.execute("SET app.enable_backfill = '1';")
        cur.execute(
            "SET search_path TO rfb;\n"
            + (src / "constraints.sql").read_text(encoding="utf-8")
        )

        cur.execute("SELECT count(*) FROM pg_indexes WHERE schemaname='rfb'")
        assert cur.fetchone()[0] >= 10, "constraints.sql did not create the indexes"
    finally:
        cur.execute("DROP SCHEMA IF EXISTS rfb CASCADE")
        cur.close()
        conn.close()
