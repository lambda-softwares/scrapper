"""
Kenya Law Worker - Processes a single year/month combination.
Designed to be run in parallel by the coordinator.

Environment variables:
  SCRAPER_YEAR: Year to process (e.g., "2024")
  SCRAPER_MONTH: Month number to process (1-12)
  SCRAPER_START_LETTER: Letter to start from (a-z), defaults to 'a'
"""

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import time
import re
import requests
import os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import logging
from boto3.s3.transfer import TransferConfig
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import psutil
import signal
import sys
import tempfile
import shutil
from io import BytesIO
import fcntl
import json
import uuid
import gc

# Environment configuration
YEAR = os.environ.get('SCRAPER_YEAR')
MONTH = os.environ.get('SCRAPER_MONTH')
START_LETTER = os.environ.get('SCRAPER_START_LETTER', 'a').lower()

if not YEAR or not MONTH:
    print("ERROR: SCRAPER_YEAR and SCRAPER_MONTH environment variables are required")
    sys.exit(1)

MONTH = int(MONTH)
MONTH_NAMES = {
    1: 'January', 2: 'February', 3: 'March', 4: 'April',
    5: 'May', 6: 'June', 7: 'July', 8: 'August',
    9: 'September', 10: 'October', 11: 'November', 12: 'December'
}
MONTH_NAME = MONTH_NAMES.get(MONTH, str(MONTH))

# Setup logging - per worker log file
os.makedirs('logs', exist_ok=True)
log_file = f"logs/worker_{YEAR}_{MONTH}.log"
logging.basicConfig(
    level=logging.INFO,
    format=f"[{YEAR}/{MONTH_NAME}] %(levelname)s: %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

# Disable verbose logs
logging.getLogger('selenium').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)

# S3 configuration
bucket_name = "denning-kenya-law"
s3 = boto3.client('s3')
TRANSFER_MAX_CONCURRENCY = 8
transfer_config = TransferConfig(
    max_concurrency=TRANSFER_MAX_CONCURRENCY,
    multipart_threshold=8 * 1024 * 1024,
    multipart_chunksize=8 * 1024 * 1024
)

# File paths
PROGRESS_FILE = "processed_urls.txt"
CHECKPOINT_FILE = "checkpoint.json"

# Processed URLs tracking (thread-safe within this worker)
processed_urls = set()
processed_urls_lock = threading.Lock()

# Batch URL persistence to reduce file lock contention
_pending_urls_buffer = []
_pending_urls_buffer_lock = threading.Lock()
BATCH_FLUSH_SIZE = 25

# Memory monitoring
WORKER_MEMORY_LIMIT_MB = 1500
_last_memory_check = 0
MEMORY_CHECK_INTERVAL = 30  # seconds

# Download workers for parallel document processing
DOWNLOAD_WORKERS = 2
download_executor = ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS)

# HTTP session configuration
DEFAULT_REQUEST_TIMEOUT = 30
DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}
_thread_local = threading.local()

base_url = "https://new.kenyalaw.org"
target_url = "https://new.kenyalaw.org/judgments/court-class/superior-courts/"

# Cleanup flag
cleanup_initiated = False

# Rate-limit tracking
_consecutive_failures = 0
_base_delay = 5  # Base delay between letters (seconds)
_max_delay = 60  # Maximum delay after repeated failures


def get_adaptive_delay():
    """Calculate delay based on consecutive failures (exponential backoff)."""
    global _consecutive_failures
    delay = min(_base_delay * (2 ** _consecutive_failures), _max_delay)
    return delay


def record_success():
    """Reset failure counter on successful fetch."""
    global _consecutive_failures
    if _consecutive_failures > 0:
        logging.info(f"Rate limit recovered, resetting failure counter")
    _consecutive_failures = 0


def record_failure():
    """Increment failure counter and log backoff."""
    global _consecutive_failures
    _consecutive_failures += 1
    delay = get_adaptive_delay()
    logging.warning(f"Rate limit hit (consecutive failures: {_consecutive_failures}), next delay: {delay}s")


def get_requests_session():
    """Return a thread-local session with retry/backoff configured."""
    if not hasattr(_thread_local, "session"):
        session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retries, pool_connections=16, pool_maxsize=16)
        session.mount('https://', adapter)
        session.mount('http://', adapter)
        session.headers.update(DEFAULT_HEADERS)
        _thread_local.session = session
    return _thread_local.session


def ensure_absolute_url(url):
    """Normalize relative Kenya Law URLs to absolute form."""
    if not url:
        return url
    if url.lower().startswith(("http://", "https://")):
        return url
    return urljoin(base_url, url)


def fetch_page_via_http(url, timeout=20):
    """Fetch static page using HTTP instead of Selenium."""
    try:
        session = get_requests_session()
        response = session.get(url, timeout=timeout)
        if response.status_code == 200:
            return BeautifulSoup(response.text, 'html.parser')
        logging.warning(f"HTTP fetch failed ({response.status_code}): {url}")
        return None
    except Exception as e:
        logging.warning(f"HTTP fetch error for {url}: {e}")
        return None


def fetch_alphabet_page(url, fallback_driver=None):
    """Fetch alphabet page. Returns (soup, rate_limited) tuple."""
    soup = fetch_page_via_http(url)
    if soup and (soup.find("tr") or soup.find("ul", class_="pagination")):
        return soup, False

    if fallback_driver:
        logging.info(f"Falling back to Selenium for {url}")
        delay = get_adaptive_delay()
        time.sleep(delay)
        soup = scrape_page_with_escalation(fallback_driver, url)
        if soup and (soup.find("tr") or soup.find("ul", class_="pagination")):
            return soup, False
        logging.warning(f"Selenium returned empty/blocked page for {url}")
        return None, True  # Rate limited
    return None, True


def fetch_pagination_page(url, fallback_driver=None):
    """Fetch pagination page. Returns (soup, rate_limited) tuple."""
    soup = fetch_page_via_http(url)
    if soup and soup.find("td", class_="cell-title"):
        return soup, False

    if fallback_driver:
        logging.info(f"Falling back to Selenium for {url}")
        delay = get_adaptive_delay()
        time.sleep(delay)
        soup = scrape_page_with_escalation(fallback_driver, url)
        if soup and soup.find("td", class_="cell-title"):
            return soup, False
        logging.warning(f"Selenium returned empty/blocked page for {url}")
        return None, True
    return None, True


# File-locked operations for multi-process safety
def load_processed_urls():
    """Load previously processed document URLs from disk with file locking."""
    if not os.path.exists(PROGRESS_FILE):
        return
    try:
        with open(PROGRESS_FILE, "r") as f:
            fcntl.flock(f, fcntl.LOCK_SH)  # Shared lock for reading
            for line in f:
                url = line.strip()
                if url:
                    processed_urls.add(ensure_absolute_url(url))
            fcntl.flock(f, fcntl.LOCK_UN)
        logging.info(f"Loaded {len(processed_urls)} processed URLs")
    except Exception as e:
        logging.warning(f"Failed to load processed URLs: {e}")


def _flush_pending_urls():
    """Write buffered URLs with single lock acquisition."""
    global _pending_urls_buffer
    with _pending_urls_buffer_lock:
        if not _pending_urls_buffer:
            return
        urls_to_write = _pending_urls_buffer.copy()
        _pending_urls_buffer = []

    try:
        with open(PROGRESS_FILE, "a") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            for url in urls_to_write:
                f.write(f"{url}\n")
            fcntl.flock(f, fcntl.LOCK_UN)
    except Exception as e:
        logging.warning(f"Failed to flush {len(urls_to_write)} URLs: {e}")
        # Re-add failed URLs to buffer
        with _pending_urls_buffer_lock:
            _pending_urls_buffer.extend(urls_to_write)


def persist_processed_url(url):
    """Buffer URL and batch-write to reduce lock contention."""
    normalized_url = ensure_absolute_url(url)
    with _pending_urls_buffer_lock:
        _pending_urls_buffer.append(normalized_url)
        should_flush = len(_pending_urls_buffer) >= BATCH_FLUSH_SIZE

    if should_flush:
        _flush_pending_urls()


def flush_on_shutdown():
    """Flush any remaining buffered URLs - call before exit."""
    _flush_pending_urls()
    logging.info("Flushed pending URLs on shutdown")


def check_worker_memory():
    """Check worker memory usage and trigger GC if high. Returns True if under limit."""
    global _last_memory_check

    current_time = time.time()
    if current_time - _last_memory_check < MEMORY_CHECK_INTERVAL:
        return True  # Skip check if too recent

    _last_memory_check = current_time

    try:
        process = psutil.Process()
        memory_mb = process.memory_info().rss / (1024 * 1024)

        if memory_mb > WORKER_MEMORY_LIMIT_MB * 0.8:  # 80% of limit
            logging.warning(f"Memory usage high: {memory_mb:.0f}MB (limit: {WORKER_MEMORY_LIMIT_MB}MB), triggering GC")
            gc.collect()
            # Re-check after GC
            memory_mb = process.memory_info().rss / (1024 * 1024)
            logging.info(f"Memory after GC: {memory_mb:.0f}MB")

        return memory_mb < WORKER_MEMORY_LIMIT_MB
    except Exception as e:
        logging.warning(f"Memory check failed: {e}")
        return True


def update_checkpoint(letter, status):
    """Update checkpoint.json with current progress."""
    key = f"{YEAR}_{MONTH}"
    try:
        # Ensure file exists
        if not os.path.exists(CHECKPOINT_FILE):
            with open(CHECKPOINT_FILE, "w") as f:
                json.dump({}, f)

        with open(CHECKPOINT_FILE, "r+") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                data = {}
            data[key] = {"letter": letter, "status": status}
            f.seek(0)
            json.dump(data, f, indent=2)
            f.truncate()
            fcntl.flock(f, fcntl.LOCK_UN)
    except Exception as e:
        logging.warning(f"Failed to update checkpoint: {e}")


def get_checkpoint():
    """Get the current checkpoint for this year/month."""
    key = f"{YEAR}_{MONTH}"
    try:
        if not os.path.exists(CHECKPOINT_FILE):
            return None
        with open(CHECKPOINT_FILE, "r") as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            data = json.load(f)
            fcntl.flock(f, fcntl.LOCK_UN)
            return data.get(key)
    except Exception:
        return None


def signal_handler(sig, frame):
    global cleanup_initiated
    if not cleanup_initiated:
        cleanup_initiated = True
        logging.info("Shutting down gracefully...")
        flush_on_shutdown()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def initialize_driver():
    """Initialize a single WebDriver."""
    temp_dir = None
    try:
        temp_dir = tempfile.mkdtemp(
            prefix=f'chrome_worker_{YEAR}_{MONTH}_{uuid.uuid4().hex[:8]}_',
            dir='/tmp'
        )

        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-background-timer-throttling")
        options.add_argument("--disable-backgrounding-occluded-windows")
        options.add_argument("--disable-renderer-backgrounding")
        options.add_argument("--disable-default-apps")
        options.add_argument("--no-first-run")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument(f"--user-data-dir={temp_dir}")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36")

        # Chrome stability flags for high concurrency
        options.add_argument("--disable-sync")
        options.add_argument("--disable-breakpad")
        options.add_argument("--metrics-recording-only")
        options.add_argument("--disable-features=TranslateUI")
        options.add_argument("--disable-hang-monitor")
        options.add_argument("--disable-ipc-flooding-protection")
        options.add_argument("--disable-domain-reliability")
        options.add_argument("--no-pings")

        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.default_content_settings.popups": 0,
            "profile.managed_default_content_settings.media_stream": 2,
        }
        options.add_experimental_option("prefs", prefs)
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(30)
        driver.implicitly_wait(5)

        driver.get("data:text/html,<html><body>Test</body></html>")
        time.sleep(0.5)

        driver._temp_dir = temp_dir
        logging.info("Chrome driver initialized successfully")
        return driver

    except Exception as e:
        logging.error(f"Failed to initialize driver: {e}")
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
            except Exception:
                pass
        return None


def cleanup_driver(driver):
    """Safely cleanup driver."""
    if driver:
        try:
            temp_dir = getattr(driver, '_temp_dir', None)
            driver.quit()
            if temp_dir and os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
        except Exception as e:
            logging.warning(f"Error cleaning up driver: {e}")


def scrape_page_with_escalation(driver, url):
    """Scrape a page with escalating timeouts for stability."""
    timeouts = [20, 30, 45]  # Escalating timeouts
    backoffs = [2, 5, 10]    # Progressive backoff between retries

    for attempt, (timeout, backoff) in enumerate(zip(timeouts, backoffs)):
        try:
            driver.set_page_load_timeout(timeout)
            driver.get(url)
            WebDriverWait(driver, min(timeout, 15)).until(
                EC.presence_of_element_located((By.TAG_NAME, 'body'))
            )
            time.sleep(0.5)
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            return soup
        except Exception as e:
            error_msg = str(e).lower()
            is_timeout = 'timeout' in error_msg or 'timed out' in error_msg

            if attempt < len(timeouts) - 1:
                if is_timeout:
                    logging.warning(f"Timeout ({timeout}s) for {url}, escalating to {timeouts[attempt + 1]}s")
                else:
                    logging.warning(f"Retry {attempt + 1} for {url}: {e}")
                time.sleep(backoff)
            else:
                logging.error(f"Failed to scrape {url} after {len(timeouts)} attempts: {e}")
                return None
    return None


def scrape_page(driver, url, retries=2):
    """Legacy wrapper - delegates to escalating timeout version."""
    return scrape_page_with_escalation(driver, url)


def extract_page_numbers_links(url, page):
    """Extract pagination links."""
    try:
        ul_element = page.find("ul", class_="pagination flex-wrap")
        if not ul_element:
            return [url]
        page_numbers = []
        for li in ul_element.find_all("li"):
            a_tag = li.find("a")
            if a_tag and "href" in a_tag.attrs:
                page_numbers.append(f"{url}&{a_tag['href'][12:]}")
        return list(set(page_numbers))
    except Exception as e:
        logging.error(f"Error extracting page numbers: {e}")
        return [url]


def extract_alphabetical_links(url):
    """Generate alphabetical filter links."""
    return [f"{url}?alphabet={chr(i)}" for i in range(ord('a'), ord('z') + 1)]


def pdf_links(page):
    """Extract case links from page."""
    try:
        tr_elements = page.find_all("tr")
        links = []
        for tr in tr_elements:
            td_title = tr.find("td", class_="cell-title")
            if td_title:
                a_tag = td_title.find("a")
                if a_tag and "href" in a_tag.attrs:
                    links.append(base_url + a_tag["href"])
        return links
    except Exception as e:
        logging.error(f"Error extracting case links: {e}")
        return []


def is_document_size_greater_than_zero(text):
    if not text:
        return True
    match = re.search(r'(\d+(\.\d+)?)\s*(KB|MB)', text, re.IGNORECASE)
    if match:
        size = float(match.group(1))
        unit = match.group(3).upper()
        return size > 0 if unit == "KB" else size > 0.001
    return True


def extract_document_link_via_requests(url):
    """Extract document link using HTTP request."""
    try:
        session = get_requests_session()
        response = session.get(url, timeout=DEFAULT_REQUEST_TIMEOUT)
        if response.status_code != 200:
            return None

        soup = BeautifulSoup(response.text, 'html.parser')

        a_element = soup.select_one("a.btn.btn-primary.btn-shrink-sm")
        if a_element:
            href = a_element.get("href")
            if href and (href.lower().endswith(('.pdf', '.doc', '.docx')) or '/source' in href.lower()) and is_document_size_greater_than_zero(a_element.get_text(strip=True)):
                return ensure_absolute_url(href)

        dd_elements = soup.find_all("dd")
        if dd_elements:
            a_tag = dd_elements[-1].find("a")
            if a_tag:
                href = a_tag.get("href")
                if href and (href.lower().endswith(('.pdf', '.doc', '.docx')) or '/source' in href.lower()):
                    return ensure_absolute_url(href)

        return None
    except Exception as e:
        logging.error(f"Error extracting document link from {url}: {e}")
        return None


def extract_document_links_parallel(case_links, max_workers=8):
    """Extract document links from multiple case pages in parallel using HTTP."""
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(extract_document_link_via_requests, link): link
            for link in case_links
        }
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    results.append(result)
            except Exception as e:
                logging.error(f"Parallel extraction failed: {e}")
    return results


def download_document_to_s3(url):
    """Download document to S3."""
    try:
        url = ensure_absolute_url(url)

        with processed_urls_lock:
            if url in processed_urls:
                return None
            processed_urls.add(url)

        persist_processed_url(url)

        parsed_url = urlparse(url)

        if '/source' in url and 'kenyalaw.org' in url:
            path_parts = parsed_url.path.split('/')
            if len(path_parts) >= 6:
                court = path_parts[4]
                url_year = path_parts[5]
                case_id = path_parts[6]
                filename = f"{court}_{url_year}_{case_id}.pdf"
            else:
                filename = f"document_{int(time.time())}.pdf"
        else:
            filename = os.path.basename(parsed_url.path) or f"document_{int(time.time())}"
            if not filename.lower().endswith(('.pdf', '.doc', '.docx')):
                filename += ".pdf"

        s3_key = f"documents/{YEAR}/{MONTH_NAME}/{filename}"

        try:
            s3.head_object(Bucket=bucket_name, Key=s3_key)
            return f"s3://{bucket_name}/{s3_key}"
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code not in ['404', '403']:
                logging.error(f"S3 error: {e}")
                return None

        session = get_requests_session()
        response = session.get(url, timeout=DEFAULT_REQUEST_TIMEOUT)

        if response.status_code == 200:
            if len(response.content) == 0:
                logging.warning(f"Empty file: {url}")
                return None

            body_stream = BytesIO(response.content)
            body_stream.seek(0)
            s3.upload_fileobj(body_stream, bucket_name, s3_key, Config=transfer_config)
            logging.info(f"Uploaded: {YEAR}/{MONTH_NAME}/{filename} ({len(response.content)} bytes)")
            return f"s3://{bucket_name}/{s3_key}"
        else:
            logging.error(f"Download failed ({response.status_code}): {url}")
            return None

    except Exception as e:
        logging.error(f"S3 upload error for {url}: {e}")
        return None


def process_single_document(link):
    """Process a single document link."""
    try:
        time.sleep(1)  # Delay before fetching case page
        document_link = extract_document_link_via_requests(link)
        if document_link:
            time.sleep(1)  # Delay before downloading PDF
            return download_document_to_s3(document_link)
        return None
    except Exception as e:
        logging.error(f"Error processing {link}: {e}")
        return None


def process_letter(driver, alphabet_link, letter):
    """Process a single letter. Returns (documents, success, rate_limited)."""
    global cleanup_initiated

    check_worker_memory()
    logging.info(f"Processing letter '{letter}'...")
    update_checkpoint(letter, "in_progress")

    # Fetch with rate-limit awareness
    page_1, rate_limited = fetch_alphabet_page(alphabet_link, fallback_driver=driver)
    if rate_limited:
        record_failure()
        update_checkpoint(letter, "rate_limited")
        return [], False, True

    if not page_1:
        update_checkpoint(letter, "failed")
        return [], False, False

    record_success()  # Reset backoff on success

    pages_links = extract_page_numbers_links(alphabet_link, page_1)

    pdf_download_page_links = []
    pagination_rate_limited = False

    for page_link in pages_links:
        if cleanup_initiated:
            break
        time.sleep(2)  # Delay between pagination page fetches
        page_2, rate_limited = fetch_pagination_page(page_link, fallback_driver=driver)
        if rate_limited:
            record_failure()
            pagination_rate_limited = True
            continue
        if page_2:
            record_success()
            pdf_download_page_links.extend(pdf_links(page_2))

    # Process documents...
    documents = []
    if pdf_download_page_links:
        logging.info(f"Found {len(pdf_download_page_links)} documents for letter '{letter}'")
        futures = []
        for link in pdf_download_page_links:
            if cleanup_initiated:
                break
            futures.append(download_executor.submit(process_single_document, link))

        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    documents.append(result)
            except Exception as e:
                logging.error(f"Download worker failed: {e}")

    check_worker_memory()

    # Add cooling delay between letters
    delay = get_adaptive_delay()
    logging.info(f"Cooling off for {delay}s before next letter")
    time.sleep(delay)

    if not cleanup_initiated:
        update_checkpoint(letter, "complete")

    return documents, True, pagination_rate_limited


def process_month(driver):
    """Process all cases for the configured year/month."""
    global cleanup_initiated, _consecutive_failures

    month_url = f"{target_url}{YEAR}/{MONTH}/"
    logging.info(f"Starting processing for {YEAR}/{MONTH_NAME}")

    all_documents = []
    failed_letters = []
    rate_limited_letters = []
    all_alphabets_links = extract_alphabetical_links(month_url)

    start_idx = ord(START_LETTER) - ord('a')

    # First pass
    for i, alphabet_link in enumerate(all_alphabets_links):
        if cleanup_initiated:
            break

        letter = chr(ord('a') + i)
        if i < start_idx:
            continue

        logging.info(f"Processing letter '{letter}' ({i+1}/26)...")
        documents, success, rate_limited = process_letter(driver, alphabet_link, letter)
        all_documents.extend(documents)

        if rate_limited:
            rate_limited_letters.append((i, alphabet_link, letter))
            logging.warning(f"Letter '{letter}' rate-limited, will retry with backoff")
        elif not success:
            failed_letters.append((i, alphabet_link, letter))

    # Retry rate-limited letters with exponential backoff (up to 3 attempts)
    for attempt in range(3):
        if not rate_limited_letters or cleanup_initiated:
            break

        backoff_time = 30 * (2 ** attempt)  # 30s, 60s, 120s
        logging.info(f"Retry attempt {attempt + 1}/3: waiting {backoff_time}s for {len(rate_limited_letters)} rate-limited letters...")
        time.sleep(backoff_time)

        _consecutive_failures = 0  # Reset for retry
        still_limited = []

        for i, alphabet_link, letter in rate_limited_letters:
            if cleanup_initiated:
                break
            logging.info(f"Retrying letter '{letter}'...")
            documents, success, rate_limited = process_letter(driver, alphabet_link, letter)
            all_documents.extend(documents)

            if rate_limited:
                still_limited.append((i, alphabet_link, letter))
            elif success:
                logging.info(f"Letter '{letter}' succeeded on retry")

        rate_limited_letters = still_limited

    # Retry other failed letters once
    if failed_letters and not cleanup_initiated:
        logging.info(f"Retrying {len(failed_letters)} failed letter(s)...")
        time.sleep(10)
        for i, alphabet_link, letter in failed_letters:
            if cleanup_initiated:
                break
            documents, success, _ = process_letter(driver, alphabet_link, letter)
            all_documents.extend(documents)

    if not cleanup_initiated:
        update_checkpoint('z', "complete")

    return all_documents


def main():
    """Main entry point."""
    logging.info(f"Worker starting for {YEAR}/{MONTH_NAME} (starting from letter '{START_LETTER}')")

    # Check AWS credentials
    try:
        s3.list_buckets()
    except NoCredentialsError:
        logging.error("AWS credentials not found")
        sys.exit(1)

    # Load processed URLs
    load_processed_urls()

    driver = None
    try:
        driver = initialize_driver()
        if not driver:
            logging.error("Failed to initialize WebDriver")
            sys.exit(1)

        results = process_month(driver)
        logging.info(f"Completed: {len(results)} documents processed")

    except KeyboardInterrupt:
        logging.warning("Interrupted by user")
    except Exception as e:
        logging.error(f"Worker failed: {e}")
        sys.exit(1)
    finally:
        flush_on_shutdown()
        if driver:
            cleanup_driver(driver)
        download_executor.shutdown(wait=False)


if __name__ == "__main__":
    main()
