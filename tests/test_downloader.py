import pytest
import io
import zipfile
from unittest.mock import AsyncMock, patch
from curl_cffi.requests import AsyncSession
from src.downloader import AsyncDownloader
from src.settings import settings

# --- Mocks ---

class MockResponse:
    def __init__(self, content=b"", status_code=200, headers=None):
        self.content = content
        self.status_code = status_code
        self.headers = headers or {}
        self.text = content.decode("utf-8", errors="ignore")

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP Error {self.status_code}")

    async def aiter_content(self, chunk_size=None):
        yield self.content

class MockStreamContext:
    def __init__(self, response):
        self.response = response

    async def __aenter__(self):
        return self.response

    async def __aexit__(self, exc_type, exc_value, traceback):
        pass

# --- Tests ---

@pytest.mark.asyncio
async def test_fetch_file_list():
    html_content = """
    <html>
        <body>
            <a href="file1.zip">File 1</a>
            <a href="other.txt">Text File</a>
            <a href="http://external.com/file2.zip">External Zip</a>
        </body>
    </html>
    """
    
    mock_session = AsyncMock(spec=AsyncSession)
    mock_session.get = AsyncMock(return_value=MockResponse(content=html_content.encode()))
    
    # Mock context manager for session
    mock_session.__aenter__.return_value = mock_session
    mock_session.__aexit__.return_value = None

    with patch("src.downloader.AsyncDownloader._get_session", return_value=mock_session):
        downloader = AsyncDownloader()
        downloader.base_url = "http://test.com/"
        links = await downloader.fetch_file_list()
        
        assert len(links) == 2
        assert "http://test.com/file1.zip" in links
        assert "http://external.com/file2.zip" in links

@pytest.mark.asyncio
async def test_download_file_success(tmp_path):
    # Create a valid zip file in memory
    bio = io.BytesIO()
    with zipfile.ZipFile(bio, mode="w") as z:
        z.writestr("test.txt", "hello world")
    zip_data = bio.getvalue()

    mock_session = AsyncMock(spec=AsyncSession)
    
    # Mock HEAD request
    mock_session.head = AsyncMock(return_value=MockResponse(headers={"content-length": str(len(zip_data))}))
    
    # Mock GET request (stream)
    mock_response = MockResponse(content=zip_data, headers={"content-length": str(len(zip_data))})
    mock_session.get = AsyncMock(return_value=mock_response)

    mock_session.__aenter__.return_value = mock_session
    mock_session.__aexit__.return_value = None

    with patch("src.downloader.AsyncDownloader._get_session", return_value=mock_session):
        settings.verify_zip_integrity = True
        
        downloader = AsyncDownloader()
        
        # Monkey-patch the retry policy on the bound method to stop immediately
        import tenacity
        downloader.download_file.retry.stop = tenacity.stop_after_attempt(1)
        downloader.download_file.retry.wait = tenacity.wait_none()
        
        downloader.dest_dir = tmp_path
        
        success = await downloader.download_file("http://test.com/test.zip")
        
        assert success
        assert (tmp_path / "test.zip").exists()

@pytest.mark.asyncio
async def test_download_file_integrity_failure(tmp_path):
    # Create invalid data
    zip_data = b"this is not a zip file"

    mock_session = AsyncMock(spec=AsyncSession)
    mock_session.head = AsyncMock(return_value=MockResponse(headers={"content-length": str(len(zip_data))}))
    mock_response = MockResponse(content=zip_data)
    mock_session.get = AsyncMock(return_value=mock_response)
    mock_session.__aenter__.return_value = mock_session
    mock_session.__aexit__.return_value = None

    with patch("src.downloader.AsyncDownloader._get_session", return_value=mock_session):
        settings.verify_zip_integrity = True
        
        downloader = AsyncDownloader()
        
        # Monkey-patch the retry policy on the bound method to stop immediately
        import tenacity
        downloader.download_file.retry.stop = tenacity.stop_after_attempt(1)
        downloader.download_file.retry.wait = tenacity.wait_none()
        
        downloader.dest_dir = tmp_path
        
        # Since we are using tenacity, the original exception is wrapped in RetryError
        # after max retries are exhausted.
        try:
            await downloader.download_file("http://test.com/bad.zip")
        except Exception as e:
            # Check if it's a RetryError (which wraps the underlying exception)
            # or if tenacity was configured to reraise the original exception
            import tenacity
            if isinstance(e, tenacity.RetryError):
                # If wrapped, we can inspect the last attempt's exception if needed,
                # but simply catching it confirms that retries failed as expected.
                pass
            elif "Integrity check failed" in str(e):
                pass
            else:
                pytest.fail(f"Unexpected exception raised: {e}")
        
        assert not (tmp_path / "bad.zip").exists()
