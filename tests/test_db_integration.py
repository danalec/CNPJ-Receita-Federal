import os
import pytest
import pandas as pd

from src.settings import settings


pytestmark = pytest.mark.integration


def _should_run_integration() -> bool:
    return bool(settings.database_uri) and os.environ.get("PG_INTEGRATION") == "1"


@pytest.mark.skipif(not _should_run_integration(), reason="Integration tests disabled")
def test_psycopg_copy_into_temp_table():
    import psycopg2
    from src.database_loader import fast_load_chunk

    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

    with psycopg2.connect(settings.database_uri) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS etl_test")
            cur.execute("CREATE UNLOGGED TABLE etl_test (a INT, b INT)")
        fast_load_chunk(conn, df, "etl_test")
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM etl_test")
            rows = cur.fetchone()[0]
        assert rows == len(df)
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS etl_test")
