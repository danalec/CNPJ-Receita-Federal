import io
import types
import pandas as pd

from src.database_loader import fast_load_chunk


class DummyCursor:
    def __init__(self):
        self.last_sql = None
        self.last_len = 0

    def copy(self, sql, source):
        self.last_sql = sql
        self.last_len = len(source.read())

    def execute(self, sql):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class DummyConn:
    def __init__(self):
        self.autocommit = False
        self.cursor_obj = DummyCursor()

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        pass

    def rollback(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_fast_load_chunk_builds_copy_sql_and_bytes():
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    conn = DummyConn()
    fast_load_chunk(conn, df, "tabela")
    cur = conn.cursor_obj
    assert cur.last_sql.startswith("COPY tabela (a, b) FROM STDIN")
    assert cur.last_len > 0
