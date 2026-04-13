"""
Dual proxy pools for scraping (residential) and downloading (datacenter).

Scraping proxies use sticky IPs — each worker keeps the same exit IP for the
entire job and only rotates on failure.  Download proxies rotate randomly
since each download is a standalone CDN request.
"""

import logging
import os
import random
from threading import Lock

from dotenv import load_dotenv

load_dotenv()

# ── Residential pool (scraping) ─────────────────────────────────────────
SCRAPE_PROXY_ENABLED = os.getenv("SCRAPE_PROXY_ENABLED", "true").strip().lower() in ("1", "true", "yes")
SCRAPE_PROXY_USER = os.getenv("SCRAPE_PROXY_USER", "")
SCRAPE_PROXY_PASS = os.getenv("SCRAPE_PROXY_PASS", "")
SCRAPE_PROXY_HOST = os.getenv("SCRAPE_PROXY_HOST", "isp.decodo.com")
SCRAPE_PROXY_PORT_MIN = int(os.getenv("SCRAPE_PROXY_PORT_MIN", "10001"))
SCRAPE_PROXY_PORT_MAX = int(os.getenv("SCRAPE_PROXY_PORT_MAX", "10100"))

# ── Datacenter pool (downloading) ───────────────────────────────────────
DL_PROXY_ENABLED = os.getenv("DL_PROXY_ENABLED", "true").strip().lower() in ("1", "true", "yes")
DL_PROXY_USER = os.getenv("DL_PROXY_USER", "")
DL_PROXY_PASS = os.getenv("DL_PROXY_PASS", "")
DL_PROXY_HOST = os.getenv("DL_PROXY_HOST", "dc.decodo.com")
DL_PROXY_PORT_MIN = int(os.getenv("DL_PROXY_PORT_MIN", "10001"))
DL_PROXY_PORT_MAX = int(os.getenv("DL_PROXY_PORT_MAX", "10100"))

# ── Sticky IP state for scraping workers ────────────────────────────────
# Port 0 is a sentinel meaning "direct / no proxy".
DIRECT_PORT = 0
SCRAPE_PROXY_INCLUDE_DIRECT = os.getenv("SCRAPE_PROXY_INCLUDE_DIRECT", "false").strip().lower() in ("1", "true", "yes")

_sticky: dict[str, int] = {}
_sticky_lock = Lock()


def _build_url(user: str, passwd: str, host: str, port: int) -> str:
    return f"https://{user}:{passwd}@{host}:{port}"


# ── Scraping (residential, sticky per worker) ───────────────────────────

def _random_scrape_port(exclude: int | None = None) -> int:
    """Pick a random port from the proxy range, or DIRECT_PORT if direct is enabled."""
    if SCRAPE_PROXY_INCLUDE_DIRECT:
        total_proxy_ports = SCRAPE_PROXY_PORT_MAX - SCRAPE_PROXY_PORT_MIN + 1
        # ~25% chance of direct on each rotation
        pick = random.randint(0, total_proxy_ports + max(total_proxy_ports // 3, 1))
        if pick >= total_proxy_ports:
            return DIRECT_PORT
    port = random.randint(SCRAPE_PROXY_PORT_MIN, SCRAPE_PROXY_PORT_MAX)
    while port == exclude and SCRAPE_PROXY_PORT_MAX > SCRAPE_PROXY_PORT_MIN:
        port = random.randint(SCRAPE_PROXY_PORT_MIN, SCRAPE_PROXY_PORT_MAX)
    return port


def _assign_scrape_port(worker_id: str) -> int:
    """Return the sticky port for *worker_id*, assigning a new random one if needed."""
    with _sticky_lock:
        if worker_id not in _sticky:
            _sticky[worker_id] = _random_scrape_port()
        return _sticky[worker_id]


def rotate_scrape_proxy(worker_id: str) -> str | None:
    """Force a new residential IP for *worker_id*. Returns the new proxy URL or None for direct."""
    if not SCRAPE_PROXY_ENABLED or not SCRAPE_PROXY_USER:
        return None
    with _sticky_lock:
        old = _sticky.get(worker_id)
        new_port = _random_scrape_port(exclude=old)
        _sticky[worker_id] = new_port
        if new_port == DIRECT_PORT:
            logging.info("[Proxy] Rotated scraping IP for %s: port %s → DIRECT (no proxy)", worker_id, old)
        else:
            logging.info("[Proxy] Rotated scraping IP for %s: port %s → %s", worker_id, old, new_port)
    if new_port == DIRECT_PORT:
        return None
    return _build_url(SCRAPE_PROXY_USER, SCRAPE_PROXY_PASS, SCRAPE_PROXY_HOST, new_port)


def get_scrape_proxy(worker_id: str | None = None) -> str | None:
    """Return a residential proxy URL. Sticky if *worker_id* is given. None means direct."""
    if not SCRAPE_PROXY_ENABLED or not SCRAPE_PROXY_USER or not SCRAPE_PROXY_PASS:
        return None
    if worker_id:
        port = _assign_scrape_port(worker_id)
    else:
        port = _random_scrape_port()
    if port == DIRECT_PORT:
        return None
    return _build_url(SCRAPE_PROXY_USER, SCRAPE_PROXY_PASS, SCRAPE_PROXY_HOST, port)


def apply_scrape_proxy(session, worker_id: str | None = None) -> str | None:
    """Set the residential proxy on a requests/curl_cffi session, or clear it for direct."""
    url = get_scrape_proxy(worker_id)
    if not url:
        session.proxies = {}
        return None
    session.proxies = {"http": url, "https": url}
    return url


def get_scrape_proxy_for_ytdlp(worker_id: str | None = None) -> str | None:
    """Proxy URL for yt-dlp scraping calls (TikTok metadata extraction)."""
    return get_scrape_proxy(worker_id)


# ── Downloading (datacenter, random rotation) ───────────────────────────

def get_download_proxy() -> str | None:
    """Return a datacenter proxy URL with a random port, or None."""
    if not DL_PROXY_ENABLED or not DL_PROXY_USER or not DL_PROXY_PASS:
        return None
    port = random.randint(DL_PROXY_PORT_MIN, DL_PROXY_PORT_MAX)
    return _build_url(DL_PROXY_USER, DL_PROXY_PASS, DL_PROXY_HOST, port)


def apply_download_proxy(session) -> str | None:
    """Set the datacenter proxy on a requests session used for downloading."""
    url = get_download_proxy()
    if not url:
        return None
    session.proxies = {"http": url, "https": url}
    return url


def get_download_proxy_for_ytdlp() -> str | None:
    """Proxy URL for yt-dlp download calls."""
    return get_download_proxy()
