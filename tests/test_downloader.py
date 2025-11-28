import io
import zipfile
from pathlib import Path

from src.settings import settings
from src.downloader import download_file


class FakeHead:
    def __init__(self, size: int):
        self.headers = {"content-length": str(size)}

    def raise_for_status(self):
        return None


class FakeResponse:
    def __init__(self, data: bytes, content_type: str = "application/octet-stream"):
        self._bio = io.BytesIO(data)
        self.headers = {"content-length": str(len(data)), "content-type": content_type}

    def raise_for_status(self):
        return None

    def iter_content(self, chunk_size: int):
        while True:
            b = self._bio.read(chunk_size)
            if not b:
                break
            yield b

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeSession:
    def __init__(self, data: bytes):
        self._data = data

    def head(self, url, timeout=30, allow_redirects=True):
        return FakeHead(len(self._data))

    def get(self, url, stream=True, timeout=60, headers=None):
        return FakeResponse(self._data)


def test_download_file_zip_integrity_ok(tmp_path, monkeypatch):
    bio = io.BytesIO()
    with zipfile.ZipFile(bio, mode="w") as z:
        z.writestr("x.txt", "hello")
    data = bio.getvalue()
    monkeypatch.setattr("src.downloader.get_session", lambda: FakeSession(data))
    settings.verify_zip_integrity = True
    settings.rate_limit_per_sec = 0
    url = "http://example.com/file.zip"
    ok = download_file(url, Path(tmp_path))
    assert ok
    assert (Path(tmp_path) / "file.zip").exists()


def test_download_file_zip_integrity_bad(tmp_path, monkeypatch):
    data = b"not-a-zip"
    monkeypatch.setattr("src.downloader.get_session", lambda: FakeSession(data))
    settings.verify_zip_integrity = True
    settings.rate_limit_per_sec = 0
    url = "http://example.com/file.zip"
    ok = download_file(url, Path(tmp_path))
    assert not ok
    assert not (Path(tmp_path) / "file.zip").exists()


def test_download_rate_limit_sleep(tmp_path, monkeypatch):
    data = b"x" * (8192 * 2 + 100)
    monkeypatch.setattr("src.downloader.get_session", lambda: FakeSession(data))
    settings.verify_zip_integrity = False
    settings.rate_limit_per_sec = 1024
    calls = []

    def fake_sleep(s):
        calls.append(s)

    import src.downloader as dl
    monkeypatch.setattr(dl.time, "sleep", fake_sleep)
    url = "http://example.com/file.bin"
    ok = download_file(url, Path(tmp_path))
    assert ok
    assert (Path(tmp_path) / "file.bin").exists()
    assert len(calls) > 0
    assert sum(calls) > 0
