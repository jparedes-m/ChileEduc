"""Step 1: download the raw archives from datosabiertos.mineduc.cl.

Every .zip/.rar linked from a dataset page is saved as raw/<dataset>/<dataset>_<period>.<ext>.
Files already on disk are skipped, so the step can be rerun to fetch only new years.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipeline.common import parse_period, period_stem

log = logging.getLogger(__name__)

ARCHIVE_SUFFIXES = (".zip", ".rar")
CHUNK = 1 << 20


def _session() -> requests.Session:
    retry = Retry(total=5, backoff_factor=2, status_forcelist=(429, 500, 502, 503, 504))
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers["User-Agent"] = "ChileEduc research pipeline"
    return session


def _archive_links(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        url = urljoin(base_url, a["href"])
        if urlparse(url).path.lower().endswith(ARCHIVE_SUFFIXES) and url not in links:
            links.append(url)
    return links


def _render_with_selenium(url: str) -> str:
    """Fallback for when the page builds its links with JavaScript."""
    try:
        from selenium import webdriver
    except ImportError as err:
        raise RuntimeError(
            f"No archive links in the static HTML of {url}, and selenium is not "
            "installed to render the page. Run: pip install selenium"
        ) from err
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    driver = webdriver.Chrome(options=options)
    try:
        driver.get(url)
        driver.implicitly_wait(10)
        return driver.page_source
    finally:
        driver.quit()


def find_archive_links(session: requests.Session, url: str) -> list[str]:
    response = session.get(url, timeout=60)
    response.raise_for_status()
    links = _archive_links(response.text, url)
    if not links:
        log.info("No links in static HTML of %s, rendering with selenium", url)
        links = _archive_links(_render_with_selenium(url), url)
    return links


def plan_downloads(links: list[str], dataset: str, monthly: bool, out_dir: Path) -> dict[str, Path]:
    """Map each link to its standardized target file."""
    plan = {}
    for link in links:
        original = unquote(Path(urlparse(link).path).name)
        suffix = Path(original).suffix.lower()
        period = parse_period(original, monthly)
        if period is None:
            log.warning("[%s] no period in %s, keeping original name", dataset, original)
            target = out_dir / original
        else:
            target = out_dir / f"{period_stem(dataset, *period)}{suffix}"
        if target in plan.values():
            log.warning("[%s] %s collides with another link, keeping original name", dataset, original)
            target = out_dir / original
        plan[link] = target
    return plan


def download(session: requests.Session, url: str, target: Path) -> None:
    partial = target.with_name(target.name + ".part")
    with session.get(url, stream=True, timeout=120) as response:
        response.raise_for_status()
        with open(partial, "wb") as f:
            for chunk in response.iter_content(CHUNK):
                f.write(chunk)
    partial.replace(target)


def run(datasets: dict, raw_dir: Path, overwrite: bool = False, max_workers: int = 4) -> None:
    session = _session()
    for dataset, cfg in datasets.items():
        out_dir = raw_dir / dataset
        out_dir.mkdir(parents=True, exist_ok=True)

        links = find_archive_links(session, cfg["url"])
        log.info("[%s] %d archives listed", dataset, len(links))
        plan = plan_downloads(links, dataset, cfg["monthly"], out_dir)
        todo = {url: t for url, t in plan.items() if overwrite or not t.exists()}
        log.info("[%s] %d to download, %d already on disk", dataset, len(todo), len(plan) - len(todo))

        failures = 0
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(download, session, url, t): t for url, t in todo.items()}
            for future in as_completed(futures):
                try:
                    future.result()
                    log.info("[%s] downloaded %s", dataset, futures[future].name)
                except Exception:
                    failures += 1
                    log.exception("[%s] failed %s", dataset, futures[future].name)
        if failures:
            log.error("[%s] %d downloads failed; rerun the step to retry them", dataset, failures)
