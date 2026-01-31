import asyncio
import logging
import random
import time
from pathlib import Path
from typing import Optional, List
import zipfile

from curl_cffi.requests import AsyncSession
import aiofiles
from bs4 import BeautifulSoup
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)
from tqdm.asyncio import tqdm

from .settings import settings

logger = logging.getLogger(__name__)

# --- Helper Functions ---

def verify_zip_sync(file_path: Path) -> bool:
    """Synchronous zip verification to be run in an executor."""
    try:
        if not file_path.exists():
            return False
        with zipfile.ZipFile(file_path) as z:
            bad = z.testzip()
            if bad is not None:
                logger.error(f"Corrupted file inside zip {file_path.name}: {bad}")
                return False
        return True
    except zipfile.BadZipFile:
        logger.error(f"Invalid zip file: {file_path.name}")
        return False
    except Exception as e:
        logger.error(f"Error verifying zip {file_path.name}: {e}")
        return False

# --- Async Downloader Class ---

class AsyncDownloader:
    def __init__(self):
        self.max_concurrent = settings.max_concurrent_requests or (settings.max_workers * 2)
        self.semaphore = asyncio.Semaphore(self.max_concurrent)
        self.base_url = settings.download_url
        self.dest_dir = settings.compressed_dir
        
        # Token bucket for global rate limiting
        self.rate_limit = settings.rate_limit_per_sec
        self.tokens = self.rate_limit if self.rate_limit > 0 else float('inf')
        self.last_token_update = time.monotonic()
        
        # Proxy Management
        self.proxies = settings.proxies or []
        self.proxy_index = 0
        
        # Circuit Breaker
        self.consecutive_errors = 0
        self.circuit_open = False
        self.circuit_break_threshold = 5  # Number of consecutive errors to trigger break
        self.circuit_cool_down = 30.0     # Seconds to wait when circuit is open

    def _get_proxy(self) -> Optional[str]:
        if not self.proxies:
            return None
        
        if settings.proxy_rotation_strategy == "random":
            return random.choice(self.proxies)
        
        # Round robin
        proxy = self.proxies[self.proxy_index]
        self.proxy_index = (self.proxy_index + 1) % len(self.proxies)
        return proxy

    async def _wait_for_token(self, chunk_size: int):
        """Global token bucket rate limiter."""
        if self.rate_limit <= 0:
            return

        while True:
            now = time.monotonic()
            elapsed = now - self.last_token_update
            self.tokens = min(self.rate_limit, self.tokens + elapsed * self.rate_limit)
            self.last_token_update = now

            if self.tokens >= chunk_size:
                self.tokens -= chunk_size
                return
            
            # Wait enough time to generate enough tokens for this chunk
            needed = chunk_size - self.tokens
            wait_time = needed / self.rate_limit
            await asyncio.sleep(wait_time)

    async def _check_circuit_breaker(self):
        """Checks if circuit is open and waits if necessary."""
        if self.circuit_open:
            logger.warning(f"Circuit breaker OPEN. Waiting {self.circuit_cool_down}s...")
            await asyncio.sleep(self.circuit_cool_down)
            self.circuit_open = False
            self.consecutive_errors = 0
            logger.info("Circuit breaker CLOSED. Resuming...")

    def _get_session(self) -> AsyncSession:
        return AsyncSession(
            impersonate=settings.impersonate,
            proxy=self._get_proxy(),
            verify=False,
            timeout=60.0
        )

    @retry(
        stop=stop_after_attempt(settings.retry_max_attempts),
        wait=wait_exponential(multiplier=settings.retry_backoff_factor, min=1, max=10),
        retry=retry_if_exception_type((Exception, asyncio.TimeoutError)),
        before_sleep=before_sleep_log(logger, logging.WARNING)
    )
    async def fetch_file_list(self) -> List[str]:
        """Fetches the list of .zip files from the RFB directory."""
        logger.info(f"Fetching file list from {self.base_url}")
        
        async with self._get_session() as session:
            response = await session.get(self.base_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, "html.parser")
            zip_links = []
            for a in soup.find_all("a"):
                href = a.get("href")
                if href and href.lower().endswith(".zip"):
                    if href.startswith("http"):
                        zip_links.append(href)
                    else:
                        zip_links.append(f"{self.base_url.rstrip('/')}/{href}")
            
            logger.info(f"Found {len(zip_links)} files.")
            return zip_links

    @retry(
        stop=stop_after_attempt(settings.retry_max_attempts),
        wait=wait_exponential(multiplier=settings.retry_backoff_factor, min=2, max=30),
        retry=retry_if_exception_type((Exception, asyncio.TimeoutError)),
        before_sleep=before_sleep_log(logger, logging.WARNING)
    )
    async def download_file(self, url: str, pbar_pos: int = 0) -> bool:
        await self._check_circuit_breaker()
        
        filename = url.split("/")[-1]
        dest_path = self.dest_dir / filename
        
        try:
            async with self.semaphore:  # Limit concurrency
                async with self._get_session() as session:
                    # 1. Check existing file size for resume
                    initial_size = 0
                    total_size = 0
                    headers = {}
                    
                    try:
                        head_resp = await session.head(url)
                        # curl_cffi might return 404/403 directly, raise for status
                        if head_resp.status_code >= 400:
                             head_resp.raise_for_status()
                        total_size = int(head_resp.headers.get("content-length", 0))
                    except Exception as e:
                        logger.warning(f"Could not get size for {filename}: {e}")

                    if dest_path.exists():
                        initial_size = dest_path.stat().st_size
                        if total_size > 0 and initial_size >= total_size:
                            # Validate integrity if needed before skipping
                            if settings.verify_zip_integrity and filename.lower().endswith(".zip"):
                                loop = asyncio.get_running_loop()
                                # Use lighter check first if possible, but keeping verify_zip_sync for safety
                                is_valid = await loop.run_in_executor(None, verify_zip_sync, dest_path)
                                if is_valid:
                                    logger.info(f"Skipping {filename} (already downloaded and valid)")
                                    self.consecutive_errors = 0 # Reset on success
                                    return True
                                else:
                                    logger.warning(f"Redownloading {filename} (corrupted)")
                                    initial_size = 0
                                    dest_path.unlink()
                            else:
                                logger.info(f"Skipping {filename} (already downloaded)")
                                self.consecutive_errors = 0 # Reset on success
                                return True

                        if initial_size > 0:
                            headers["Range"] = f"bytes={initial_size}-"
                            logger.info(f"Resuming {filename} from {initial_size} bytes")

                    # 2. Start Download
                    mode = "ab" if "Range" in headers else "wb"
                    
                    # curl_cffi stream handling
                    response = await session.get(url, headers=headers, stream=True)
                    
                    if response.status_code == 416: # Range Not Satisfiable
                        logger.warning(f"Range not satisfiable for {filename}, restarting.")
                        initial_size = 0
                        mode = "wb"
                        headers = {}
                        # Restart request without range
                        response = await session.get(url, stream=True)

                    response.raise_for_status()
                    
                    # If server ignores Range header, reset
                    if response.status_code == 200 and "Range" in headers:
                            initial_size = 0
                            mode = "wb"

                    # Progress bar
                    pbar = tqdm(
                        total=total_size,
                        initial=initial_size,
                        unit="iB",
                        unit_scale=True,
                        desc=filename,
                        leave=False,
                        position=pbar_pos
                    )
                    
                    async with aiofiles.open(dest_path, mode) as f:  # type: ignore
                        async for chunk in response.aiter_content(chunk_size=settings.download_chunk_size):
                            if not chunk:
                                break
                            
                            # Rate limiting
                            await self._wait_for_token(len(chunk))
                            
                            await f.write(chunk)
                            pbar.update(len(chunk))
                    
                    pbar.close()

            # 3. Post-download Verification
            if settings.verify_zip_integrity and filename.lower().endswith(".zip"):
                loop = asyncio.get_running_loop()
                is_valid = await loop.run_in_executor(None, verify_zip_sync, dest_path)
                if not is_valid:
                    dest_path.unlink(missing_ok=True)
                    raise Exception(f"Integrity check failed for {filename}")

            logger.info(f"Finished {filename}")
            self.consecutive_errors = 0 # Reset on success
            return True
            
        except Exception:
            self.consecutive_errors += 1
            if self.consecutive_errors >= self.circuit_break_threshold:
                self.circuit_open = True
            raise

    async def run(self):
        logger.info(f"Starting Async Downloader (Impersonate={settings.impersonate}, Workers={self.max_concurrent})")
        if self.proxies:
            logger.info(f"Using {len(self.proxies)} proxies with strategy {settings.proxy_rotation_strategy}")
        
        try:
            links = await self.fetch_file_list()
            if not links:
                logger.error("No files found to download.")
                return

            tasks = []
            for i, link in enumerate(links):
                tasks.append(self.download_file(link, pbar_pos=i))
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            errors = 0
            for res in results:
                if isinstance(res, Exception):
                    logger.error(f"Download error: {res}")
                    errors += 1
            
            logger.info("=" * 40)
            logger.info(f"Total files: {len(links)}")
            logger.info(f"Errors: {errors}")
            logger.info("=" * 40)
            
            if errors > 0:
                raise RuntimeError(f"{errors} downloads failed.")

        except Exception as e:
            logger.critical(f"Critical pipeline error: {e}")
            raise

def run_download():
    """Entry point compatible with the existing pipeline."""
    downloader = AsyncDownloader()
    try:
        asyncio.run(downloader.run())
    except KeyboardInterrupt:
        logger.info("Download interrupted by user.")
    except Exception as e:
        logger.error(f"Fatal error in download process: {e}")
        raise
