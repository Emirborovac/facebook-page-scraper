"""
Media downloader for scraped Facebook posts.

Images  : direct HTTP GET to fbcdn.net CDN URLs (with cookies + 3 retries)
Videos  : yt_dlp Python library with Facebook cookies (same technique as fallback.py)

Up to MAX_WORKERS concurrent downloads run via a bounded ThreadPoolExecutor queue.
After each download the file is uploaded to S3 and deleted locally.
"""

import json
import logging
import os
import time
import uuid
from threading import Event, Lock
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path
from urllib.parse import urlparse

import requests
import yt_dlp
from dotenv import load_dotenv

from models.operations import (
    apply_job_control_action,
    find_downloaded_media_for_post_link,
    get_download_progress,
    get_job_control_state,
    get_pending_download_posts,
    update_job_progress,
    update_job_status,
    update_post_download,
)
from models.cookie_pool import CookiePool
from models.proxy import apply_download_proxy, get_download_proxy_for_ytdlp
from models.s3_upload import delete_local_file, upload_file_to_s3

load_dotenv()

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./downloads")
FACEBOOK_COOKIE_DIR = Path(os.getenv("FACEBOOK_COOKIE_DIR", "./cookies/facebook"))
INSTAGRAM_COOKIE_DIR = Path(os.getenv("INSTAGRAM_COOKIE_DIR", "./cookies/instagram"))
MAX_WORKERS = int(os.getenv("DOWNLOAD_MAX_WORKERS", "5"))
# Per-download-worker concurrency. Each download worker spins up this many
# concurrent media-download threads. Total system parallelism =
# (download_workers_per_platform × DOWNLOAD_CONCURRENCY_PER_WORKER × platforms).
DOWNLOAD_CONCURRENCY_PER_WORKER = max(int(os.getenv("DOWNLOAD_CONCURRENCY_PER_WORKER", "5")), 1)
# A scraping job becomes eligible for drip-feed downloads once it has this many
# pending posts. Lower = more responsive but more overhead.
DOWNLOAD_BATCH_TRIGGER = max(int(os.getenv("DOWNLOAD_BATCH_TRIGGER", "5")), 1)
MAX_RETRIES = 3
RETRY_DELAY_SEC = 2
IG_DOWNLOAD_USE_COOKIES = os.getenv("IG_DOWNLOAD_USE_COOKIES", "false").strip().lower() in ("1", "true", "yes")

# How many distinct cookies to try before declaring a single download "failed".
# Each retry uses a fresh cookie + fresh datacenter proxy IP.
INSTAGRAM_DOWNLOAD_RETRIES = max(int(os.getenv("INSTAGRAM_DOWNLOAD_RETRIES", "3")), 1)
FACEBOOK_DOWNLOAD_RETRIES = max(int(os.getenv("FACEBOOK_DOWNLOAD_RETRIES", "3")), 1)
DOWNLOAD_THROTTLE_COOLDOWN = float(os.getenv("DOWNLOAD_THROTTLE_COOLDOWN", "300"))


# ──────────────────────────────────────────────────────────────────────────────
# Download error classifiers (different from scraper classifiers — yt-dlp wraps
# everything in a generic DownloadError so we look at the message)
# ──────────────────────────────────────────────────────────────────────────────

def _is_download_throttle_error(exc: Exception) -> bool:
    """Errors that mean 'this cookie just got rate-limited, give it a cooldown'."""
    msg = str(exc).lower()
    return (
        'rate-limit' in msg
        or 'rate limit' in msg
        or 'please wait' in msg
        or 'too many requests' in msg
        or 'http error 429' in msg
        or 'status=429' in msg
        # yt-dlp's catch-all for IG rejection — usually a throttled cookie
        or ('not available' in msg and 'login required' in msg)
    )


def _is_download_burnt_error(exc: Exception) -> bool:
    """Errors that mean 'this cookie is permanently dead — trash it'."""
    msg = str(exc).lower()
    return (
        'challenge_required' in msg
        or 'checkpoint_required' in msg
        or 'consent_required' in msg
        or 'http error 401' in msg
        or 'http error 403' in msg
        or 'status=401' in msg
        or 'status=403' in msg
    )


def _is_dead_content_error(exc: Exception) -> bool:
    """Errors where the post itself is unreachable — not a cookie problem.
    Don't penalize the cookie or retry, just give up immediately.
    """
    msg = str(exc).lower()
    return (
        'http error 404' in msg
        or 'no video formats found' in msg
        or "this content isn't available to everyone" in msg
        or 'video unavailable' in msg
    )


def _video_source_platform(url: str) -> str:
    host = (urlparse(url or '').netloc or '').split(':', 1)[0].lower()
    if 'tiktok.com' in host:
        return 'tiktok'
    if 'instagram.com' in host:
        return 'instagram'
    return 'facebook'


def _ensure_output_dir():
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)


def _load_cookies_into_session(session: requests.Session, platform: str = 'facebook', cookie_file: str | Path | None = None) -> int:
    """Load Netscape cookies into a requests session for the target platform domains."""
    if cookie_file is not None:
        path = Path(cookie_file)
    else:
        # No specific file requested — pick any available cookie from the download pool.
        if platform == 'instagram':
            picked = _next_instagram_cookie_file() if IG_DOWNLOAD_USE_COOKIES else None
        else:
            picked = _next_facebook_cookie_file()
        if picked is None:
            return 0
        path = Path(picked)
    if not path.exists():
        return 0
    if platform == 'instagram':
        domains = ['www.instagram.com', '.instagram.com', 'instagram.com']
        host_markers = ('instagram',)
    else:
        domains = ['www.facebook.com', '.facebook.com', 'facebook.com']
        host_markers = ('facebook', 'fb.com')

    count = 0
    with open(path, encoding='utf-8', errors='replace') as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = line.split("	")
            if len(parts) < 7:
                continue
            domain_hint = parts[0].lower()
            if not any(marker in domain_hint for marker in host_markers):
                continue
            name = parts[5]
            value = "	".join(parts[6:])
            for domain in domains:
                session.cookies.set(name, value, domain=domain)
            count += 1
    return count



def _download_cookie_files(platform_dir: Path) -> list[Path]:
    """Cookie files reserved for the downloader.

    Preferred:  cookies/<platform>/download/*.txt   (dedicated, isolated from scraper cookies)
    Fallback 1: any cookies/<platform>/worker_N/*.txt (borrows from scraper pool)
    Fallback 2: cookies/<platform>/*.txt (legacy flat layout)

    Subfolders named cookie_trash are always skipped.
    """
    download_dir = platform_dir / 'download'
    if download_dir.is_dir():
        files = sorted(p for p in download_dir.glob('*.txt') if p.is_file())
        if files:
            return files

    if platform_dir.is_dir():
        worker_files: list[Path] = []
        for sub in sorted(platform_dir.iterdir()):
            if sub.is_dir() and sub.name.startswith('worker_'):
                worker_files.extend(sorted(p for p in sub.glob('*.txt') if p.is_file()))
        if worker_files:
            return worker_files

        flat_files = sorted(p for p in platform_dir.glob('*.txt') if p.is_file())
        if flat_files:
            return flat_files

    return []


def _facebook_cookie_files() -> list[Path]:
    return _download_cookie_files(FACEBOOK_COOKIE_DIR)


def _instagram_cookie_files() -> list[Path]:
    return _download_cookie_files(INSTAGRAM_COOKIE_DIR)


# ──────────────────────────────────────────────────────────────────────────────
# Smart download cookie pools (thread-safe, with cooldown + burnt detection).
#
# The system supports two pool keys:
#   - per-worker lane (preferred): "instagram-download-3" → cookies/instagram/download_3/
#   - per-platform shared (fallback): "instagram" → cookies/instagram/download/
#
# Per-worker lanes give isolation: when scraper-3 produces posts, download
# worker-3 downloads them using only its own cookies. If a lane has no cookies,
# the system falls back to the shared download/ pool, then to the worker_N/
# scraping cookies.
# Lazy-initialized and refreshed periodically so cookies uploaded via the
# dashboard get picked up without a service restart.
# ──────────────────────────────────────────────────────────────────────────────

_POOL_LOCK = Lock()
_POOLS: dict[str, CookiePool] = {}
_POOL_LAST_REFRESH: dict[str, float] = {}
_POOL_REFRESH_INTERVAL = 60.0  # seconds


def _parse_download_worker_name(worker_name: str | None) -> tuple[str, int] | None:
    """('instagram-download-3') -> ('instagram', 3). Returns None if not a lane."""
    if not worker_name:
        return None
    parts = worker_name.rsplit('-', 1)
    if len(parts) != 2 or not parts[1].isdigit():
        return None
    base, idx = parts[0], int(parts[1])
    if not base.endswith('-download'):
        return None
    return base[: -len('-download')], idx


def _lane_cookie_files(worker_name: str | None) -> list[Path]:
    """Cookie files for a specific download lane, with fallbacks.

    Order:
      1. cookies/<platform>/download_N/    (the worker's dedicated lane)
      2. cookies/<platform>/download/      (shared download pool)
      3. cookies/<platform>/worker_N/      (the matching scraping worker's pool)
      4. cookies/<platform>/*.txt + sub worker_*/ (legacy, via _download_cookie_files)
    """
    parsed = _parse_download_worker_name(worker_name)
    if parsed is None:
        return []
    platform, idx = parsed
    base = FACEBOOK_COOKIE_DIR if platform == 'facebook' else (
        INSTAGRAM_COOKIE_DIR if platform == 'instagram' else None
    )
    if base is None:
        return []

    lane_dir = base / f'download_{idx}'
    if lane_dir.is_dir():
        files = sorted(p for p in lane_dir.glob('*.txt') if p.is_file())
        if files:
            return files

    shared_dir = base / 'download'
    if shared_dir.is_dir():
        files = sorted(p for p in shared_dir.glob('*.txt') if p.is_file())
        if files:
            return files

    scraper_dir = base / f'worker_{idx}'
    if scraper_dir.is_dir():
        files = sorted(
            p for p in scraper_dir.glob('*.txt')
            if p.is_file() and p.parent.name != 'cookie_trash'
        )
        if files:
            return files

    return _download_cookie_files(base)


def _platform_files_fn(platform: str):
    if platform == 'facebook':
        return _facebook_cookie_files
    if platform == 'instagram':
        return _instagram_cookie_files
    return None


def _get_or_make_pool(key: str, files_fn) -> CookiePool | None:
    """Lazy-create + periodically refresh a pool keyed by *key*."""
    now = time.time()
    with _POOL_LOCK:
        pool = _POOLS.get(key)
        last = _POOL_LAST_REFRESH.get(key, 0.0)
        if pool is None:
            files = files_fn()
            if not files:
                return None
            pool = CookiePool(
                files,
                is_burnt_fn=_is_download_burnt_error,
                is_throttle_fn=_is_download_throttle_error,
                throttle_cooldown=DOWNLOAD_THROTTLE_COOLDOWN,
            )
            _POOLS[key] = pool
            _POOL_LAST_REFRESH[key] = now
        elif (now - last) > _POOL_REFRESH_INTERVAL:
            files = files_fn()
            if files:
                pool.replace_files(files)
            _POOL_LAST_REFRESH[key] = now
    return pool


def _get_download_pool_for_worker(worker_name: str) -> CookiePool | None:
    """Per-lane pool. Falls back to shared platform pool if lane has nothing."""
    pool = _get_or_make_pool(worker_name, lambda: _lane_cookie_files(worker_name))
    if pool is not None:
        return pool
    parsed = _parse_download_worker_name(worker_name)
    if parsed is None:
        return None
    return _get_download_pool(parsed[0])


def _get_download_pool(platform: str) -> CookiePool | None:
    """Legacy platform-wide pool — used when no worker_name is available."""
    files_fn = _platform_files_fn(platform)
    if files_fn is None:
        return None
    return _get_or_make_pool(platform, files_fn)


def _resolve_pool(platform: str, worker_name: str | None) -> CookiePool | None:
    if worker_name:
        pool = _get_download_pool_for_worker(worker_name)
        if pool is not None:
            return pool
    return _get_download_pool(platform)


def _next_facebook_cookie_file(current: Path | None = None, worker_name: str | None = None) -> Path | None:
    pool = _resolve_pool('facebook', worker_name)
    if pool is None:
        return None
    return pool.next(current)


def _next_instagram_cookie_file(current: Path | None = None, worker_name: str | None = None) -> Path | None:
    pool = _resolve_pool('instagram', worker_name)
    if pool is None:
        return None
    return pool.next(current)


def _mark_download_cookie_outcome(platform: str, cookie_file: Path | None, exc: Exception,
                                  worker_name: str | None = None) -> str:
    """Classify and record a download error against the cookie that handled it.

    Returns 'burnt', 'throttle', 'dead_content', or 'unknown'.
    """
    if cookie_file is None:
        return 'unknown'
    if _is_dead_content_error(exc):
        return 'dead_content'
    pool = _resolve_pool(platform, worker_name)
    if pool is None:
        return 'unknown'
    if _is_download_burnt_error(exc):
        pool.mark_burnt(cookie_file)
        return 'burnt'
    if _is_download_throttle_error(exc):
        pool.mark_throttled(cookie_file)
        return 'throttle'
    return 'unknown'


# ──────────────────────────────────────────────────────────────────────────────
# Image downloader — HTTP GET with cookies and up to 3 retries
# ──────────────────────────────────────────────────────────────────────────────

MIN_IMAGE_BYTES = 500


def _direct_binary_download(url: str, dest_path: str, platform: str,
                            worker_name: str | None = None) -> bool:
    referer = 'https://www.instagram.com/' if platform == 'instagram' else 'https://www.facebook.com/'
    accept = '*/*' if platform == 'instagram' else 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8'

    if platform == 'tiktok':
        max_attempts = MAX_RETRIES
    elif platform == 'instagram':
        max_attempts = INSTAGRAM_DOWNLOAD_RETRIES
    else:
        max_attempts = FACEBOOK_DOWNLOAD_RETRIES

    last_cookie: Path | None = None
    for attempt in range(1, max_attempts + 1):
        # Fresh session per attempt → fresh datacenter IP + fresh cookie.
        session = requests.Session()
        apply_download_proxy(session)
        if platform == 'instagram':
            cookie_file = _next_instagram_cookie_file(last_cookie, worker_name=worker_name) if IG_DOWNLOAD_USE_COOKIES else None
        elif platform == 'facebook':
            cookie_file = _next_facebook_cookie_file(last_cookie, worker_name=worker_name)
        else:
            cookie_file = None
        last_cookie = cookie_file

        session.headers.update({
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/124.0.0.0 Safari/537.36'
            ),
            'Referer': referer,
            'Accept': accept,
        })
        if cookie_file:
            _load_cookies_into_session(session, platform=platform, cookie_file=cookie_file)

        try:
            response = session.get(url, stream=True, timeout=30)
            response.raise_for_status()
            with open(dest_path, 'wb') as handle:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        handle.write(chunk)
            size = os.path.getsize(dest_path)
            if size >= MIN_IMAGE_BYTES or platform == 'instagram':
                return True
            logging.warning(
                f'[Downloader] Binary too small ({size}B) attempt {attempt}/{max_attempts}: {url[:80]}'
            )
        except Exception as exc:
            verdict = _mark_download_cookie_outcome(platform, cookie_file, exc, worker_name=worker_name)
            cookie_label = cookie_file.name if cookie_file else 'no-cookie'
            logging.warning(
                f'[Downloader] Binary attempt {attempt}/{max_attempts} failed '
                f'({verdict}, cookie={cookie_label}): {url[:80]} → {exc}'
            )
            if verdict == 'dead_content':
                return False
        if attempt < max_attempts:
            time.sleep(RETRY_DELAY_SEC)

    logging.warning(f'[Downloader] Binary download gave up after {max_attempts} attempt(s): {url[:80]}')
    return False


def _ytdlp_image_fallback(post_link: str, dest_path: str, platform: str = 'instagram',
                          playlist_item: int | None = None,
                          worker_name: str | None = None) -> bool:
    """Use yt-dlp to download an image when the CDN URL has expired."""
    referer = "https://www.instagram.com/" if platform == 'instagram' else "https://www.facebook.com/"

    if platform == 'instagram':
        max_attempts = INSTAGRAM_DOWNLOAD_RETRIES
    elif platform == 'facebook':
        max_attempts = FACEBOOK_DOWNLOAD_RETRIES
    else:
        max_attempts = MAX_RETRIES

    last_cookie: Path | None = None
    for attempt in range(1, max_attempts + 1):
        if platform == 'instagram':
            cookie_file = _next_instagram_cookie_file(last_cookie, worker_name=worker_name) if IG_DOWNLOAD_USE_COOKIES else None
        elif platform == 'facebook':
            cookie_file = _next_facebook_cookie_file(last_cookie, worker_name=worker_name)
        else:
            cookie_file = None
        last_cookie = cookie_file

        ydl_opts = {
            "outtmpl": dest_path,
            "quiet": True,
            "no_warnings": True,
            "socket_timeout": 30,
            "retries": 2,
            "skip_download": False,
            "http_headers": {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Referer": referer,
            },
        }
        if cookie_file and os.path.exists(cookie_file):
            ydl_opts["cookiefile"] = str(cookie_file)
        dl_proxy = get_download_proxy_for_ytdlp()
        if dl_proxy:
            ydl_opts["proxy"] = dl_proxy
        if playlist_item is not None:
            ydl_opts["playlist_items"] = str(playlist_item)

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([post_link])

            if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
                return True

            parent = Path(dest_path).parent
            stem = Path(dest_path).stem
            for candidate in parent.iterdir():
                if candidate.stem.startswith(stem) and candidate.is_file() and candidate.stat().st_size > 0:
                    os.rename(str(candidate), dest_path)
                    return True

            logging.warning(
                "[Downloader] yt-dlp image fallback attempt %s/%s produced no file for %s",
                attempt, max_attempts, post_link[:80],
            )
        except Exception as exc:
            verdict = _mark_download_cookie_outcome(platform, cookie_file, exc, worker_name=worker_name)
            cookie_label = cookie_file.name if cookie_file else 'no-cookie'
            logging.warning(
                "[Downloader] Image fallback attempt %s/%s failed (%s, cookie=%s): %s",
                attempt, max_attempts, verdict, cookie_label, str(exc)[:200],
            )
            if verdict == 'dead_content':
                return False
        if attempt < max_attempts:
            time.sleep(RETRY_DELAY_SEC)

    logging.warning(f"[Downloader] yt-dlp image fallback gave up after {max_attempts} attempt(s): {post_link[:80]}")
    return False


def _download_image(url: str, dest_path: str, platform: str = 'facebook',
                    fallback_url: str | None = None, fallback_playlist_item: int | None = None,
                    worker_name: str | None = None) -> bool:
    if _direct_binary_download(url, dest_path, platform, worker_name=worker_name):
        return True
    if fallback_url:
        logging.info("[Downloader] CDN failed, trying yt-dlp fallback for %s image: %s", platform, fallback_url[:80])
        return _ytdlp_image_fallback(fallback_url, dest_path, platform=platform,
                                     playlist_item=fallback_playlist_item,
                                     worker_name=worker_name)
    return False



# ──────────────────────────────────────────────────────────────────────────────
# Video downloader — yt_dlp Python library with Facebook cookies
# ──────────────────────────────────────────────────────────────────────────────

def _download_video(url: str, dest_path: str, platform: str | None = None,
                    worker_name: str | None = None) -> bool:
    platform = platform or _video_source_platform(url)
    referers = {"tiktok": "https://www.tiktok.com/", "instagram": "https://www.instagram.com/"}
    referer = referers.get(platform, "https://www.facebook.com/")

    # Per-platform retry budget. Each attempt rotates to a fresh cookie + IP.
    if platform == 'tiktok':
        max_attempts = MAX_RETRIES
    elif platform == 'instagram':
        max_attempts = INSTAGRAM_DOWNLOAD_RETRIES
    else:
        max_attempts = FACEBOOK_DOWNLOAD_RETRIES

    last_cookie: Path | None = None
    for attempt in range(1, max_attempts + 1):
        # Pick the next AVAILABLE cookie (skips ones currently cooling down).
        if platform == 'facebook':
            cookie_file = _next_facebook_cookie_file(last_cookie, worker_name=worker_name)
        elif platform == 'instagram':
            cookie_file = _next_instagram_cookie_file(last_cookie, worker_name=worker_name) if IG_DOWNLOAD_USE_COOKIES else None
        else:
            cookie_file = None
        last_cookie = cookie_file

        ydl_opts = {
            "outtmpl": dest_path,
            "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
            "merge_output_format": "mp4",
            "noplaylist": True,
            "quiet": True,
            "no_warnings": True,
            "socket_timeout": 30,
            "retries": 3,
            "fragment_retries": 3,
            "http_headers": {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Referer": referer,
            },
        }
        if platform in ("facebook", "instagram"):
            if cookie_file and os.path.exists(cookie_file):
                ydl_opts["cookiefile"] = str(cookie_file)
            elif platform == 'instagram' and not IG_DOWNLOAD_USE_COOKIES:
                pass  # IG cookies disabled by env, expected
            else:
                logging.warning("[Downloader] No %s cookie file available for yt-dlp", platform)

        dl_proxy = get_download_proxy_for_ytdlp()
        if dl_proxy:
            ydl_opts["proxy"] = dl_proxy

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])

            if not os.path.exists(dest_path):
                for ext in (".mp4", ".mkv", ".webm"):
                    candidate = dest_path if dest_path.endswith(ext) else dest_path + ext
                    if os.path.exists(candidate):
                        os.rename(candidate, dest_path)
                        break

            if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
                return True

            logging.warning(
                f"[Downloader] Video file missing after attempt {attempt}/{max_attempts}: {url[:80]}"
            )
        except (yt_dlp.utils.DownloadError, Exception) as exc:
            verdict = _mark_download_cookie_outcome(platform, cookie_file, exc, worker_name=worker_name)
            cookie_label = cookie_file.name if cookie_file else 'no-cookie'
            logging.warning(
                "[Downloader] Video attempt %s/%s failed (%s, cookie=%s): %s",
                attempt, max_attempts, verdict, cookie_label, str(exc)[:200],
            )
            # Dead content — no amount of cookie rotation will help. Bail early.
            if verdict == 'dead_content':
                logging.warning(f"[Downloader] Dead content, skipping further retries: {url[:80]}")
                return False

        if attempt < max_attempts:
            time.sleep(RETRY_DELAY_SEC)

    logging.warning(f"[Downloader] Video download gave up after {max_attempts} attempt(s): {url[:80]}")
    return False


# ──────────────────────────────────────────────────────────────────────────────
# Video cleanup — remove merged file + yt-dlp intermediate streams
# ──────────────────────────────────────────────────────────────────────────────

def _cleanup_video_files(dest_path: str):
    from pathlib import Path as _Path

    path = _Path(dest_path)
    directory = path.parent
    stem = path.name.split(".")[0]
    removed = 0
    try:
        for file_path in directory.iterdir():
            if file_path.name.startswith(stem + ".") or file_path.name == path.name:
                try:
                    file_path.unlink()
                    removed += 1
                except Exception as exc:
                    logging.warning(f"[Downloader] Could not delete temp file {file_path}: {exc}")
    except Exception as exc:
        logging.warning(f"[Downloader] Video cleanup error for {dest_path}: {exc}")
    if removed:
        logging.debug(f"[Downloader] Cleaned up {removed} file(s) for {path.name}")


# ──────────────────────────────────────────────────────────────────────────────
# Per-post download task
# ──────────────────────────────────────────────────────────────────────────────

def _process_post(post: dict, job_id: str, cancel: Event | None = None,
                  worker_name: str | None = None) -> bool:
    post_id = post["id"]
    post_link = post.get("post_link") or post.get("video_url") or ""
    platform = _video_source_platform(post_link)

    if cancel and cancel.is_set():
        update_post_download(post_id, "pending")
        return False

    update_post_download(post_id, "downloading")

    if platform == 'tiktok':
        existing = find_downloaded_media_for_post_link(post_link, platform='tiktok', exclude_job_id=job_id)
        if existing and existing.get('video_s3_url'):
            image_s3_urls = existing.get('image_s3_urls')
            image_s3_keys = existing.get('image_s3_keys')
            if isinstance(image_s3_urls, str):
                try:
                    image_s3_urls = json.loads(image_s3_urls)
                except Exception:
                    image_s3_urls = None
            if isinstance(image_s3_keys, str):
                try:
                    image_s3_keys = json.loads(image_s3_keys)
                except Exception:
                    image_s3_keys = None
            update_post_download(
                post_id,
                'completed',
                error=None,
                video_s3_url=existing.get('video_s3_url'),
                video_s3_key=existing.get('video_s3_key'),
                image_s3_urls=image_s3_urls,
                image_s3_keys=image_s3_keys,
            )
            logging.info(
                '[Downloader] [%s] Reused downloaded TikTok media for post_id=%s from job=%s',
                job_id,
                post_id,
                existing.get('job_id'),
            )
            return True

    image_urls = []
    raw_images = post.get("image_urls")
    if raw_images:
        try:
            image_urls = json.loads(raw_images) if isinstance(raw_images, str) else raw_images
        except Exception:
            image_urls = []

    video_url = post.get("video_url") or ""
    has_video = bool(post.get("has_video")) and bool(video_url)
    has_image = bool(post.get("has_image")) and bool(image_urls)

    s3_image_urls, s3_image_keys = [], []
    s3_video_url, s3_video_key = None, None
    errors = []
    uid = str(uuid.uuid4())[:8]

    if has_image:
        fallback_url = post_link if post_link and platform in ('instagram', 'facebook') else None
        num_images = len(image_urls)
        for idx, image_url in enumerate(image_urls):
            if cancel and cancel.is_set():
                update_post_download(post_id, "pending")
                return False
            local_path = os.path.join(OUTPUT_DIR, f"{uid}_img_{idx}.jpg")
            s3_name = f"{job_id}/{uid}_img_{idx}.jpg"
            fb_item = (idx + 1) if num_images > 1 else None
            try:
                if _download_image(image_url, local_path, platform=platform,
                                   fallback_url=fallback_url, fallback_playlist_item=fb_item,
                                   worker_name=worker_name):
                    url_out, key = upload_file_to_s3(local_path, s3_name, content_type="image/jpeg")
                    s3_image_urls.append(url_out)
                    s3_image_keys.append(key)
                else:
                    errors.append(f"img_dl_failed:{image_url[:60]}")
            except Exception as exc:
                errors.append(f"img_s3_failed:{str(exc)[:80]}")
            finally:
                delete_local_file(local_path)

    if cancel and cancel.is_set():
        update_post_download(post_id, "pending")
        return False

    if has_video:
        local_path = os.path.join(OUTPUT_DIR, f"{uid}_video.mp4")
        s3_name = f"{job_id}/{uid}_video.mp4"
        dl_url = post_link if platform == "instagram" and post_link else video_url
        try:
            if _download_video(dl_url, local_path, platform=platform, worker_name=worker_name):
                url_out, key = upload_file_to_s3(local_path, s3_name, content_type="video/mp4")
                s3_video_url = url_out
                s3_video_key = key
            else:
                errors.append(f"video_dl_failed:{video_url[:60]}")
        except Exception as exc:
            errors.append(f"video_s3_failed:{str(exc)[:80]}")
        finally:
            _cleanup_video_files(local_path)

    final_status = "completed" if not errors else "failed"
    update_post_download(
        post_id,
        final_status,
        error="; ".join(errors) if errors else None,
        video_s3_url=s3_video_url,
        video_s3_key=s3_video_key,
        image_s3_urls=s3_image_urls if s3_image_urls else None,
        image_s3_keys=s3_image_keys if s3_image_keys else None,
    )
    return final_status == "completed"


# ──────────────────────────────────────────────────────────────────────────────
# Main download entry point
# ──────────────────────────────────────────────────────────────────────────────

def download_job_media(job: dict, worker_name: str | None = None, concurrency: int | None = None):
    """Drain pending media downloads for a job.

    Behaviour depends on the job's current status:
      - 'downloading_content': drain all + finalize (mark completed/failed)
      - 'scraping' (drip-feed mode): drain currently-pending only; don't finalize
        (the scraper hasn't finished producing posts yet)
    """
    job_id = job["job_id"]
    _ensure_output_dir()
    job_status_at_start = job.get("status") or "downloading_content"
    is_drip_mode = job_status_at_start == "scraping"
    concurrency = max(int(concurrency or MAX_WORKERS), 1)
    label = worker_name or 'download'
    logging.info(
        f"[Downloader:{label}] [{job_id}] Starting media downloads "
        f"(mode={'drip' if is_drip_mode else 'finalize'}, concurrency={concurrency})"
    )

    try:
        progress = get_download_progress(job_id)
        total = progress["total"]
        completed_count = progress["completed"]
        posts = get_pending_download_posts(job_id)

        if total == 0 and not is_drip_mode:
            update_job_status(job_id, "completed")
            logging.info(f"[Downloader:{label}] [{job_id}] No media posts — marking completed")
            return
        if not posts and is_drip_mode:
            logging.info(f"[Downloader:{label}] [{job_id}] No pending posts in drip cycle — yielding")
            return

        update_job_progress(job_id, total_media_count=total, total_media_downloaded=completed_count)

        state = get_job_control_state(job_id)
        if state and state.get("control_action"):
            action = apply_job_control_action(job_id, "downloading_content")
            logging.info(f"[Downloader:{label}] [{job_id}] Control action applied before start: {action}")
            return

        pending_action = None
        post_iter = iter(posts)
        cancel = Event()

        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            in_flight = {}

            def fill_pool():
                nonlocal pending_action
                while not pending_action and len(in_flight) < concurrency:
                    try:
                        post = next(post_iter)
                    except StopIteration:
                        break
                    future = pool.submit(_process_post, post, job_id, cancel, worker_name)
                    in_flight[future] = post

            fill_pool()
            if not in_flight:
                if get_pending_download_posts(job_id):
                    logging.info(f"[Downloader:{label}] [{job_id}] Waiting for download queue to refill")
                elif is_drip_mode:
                    logging.info(f"[Downloader:{label}] [{job_id}] Drip cycle finished — yielding")
                    return
                else:
                    progress = get_download_progress(job_id)
                    update_job_progress(job_id, total_media_count=progress["total"], total_media_downloaded=progress["completed"])
                    if progress["remaining"]:
                        # Real unfinished work — mark failed so the user can retry.
                        msg = f"Media incomplete: completed={progress['completed']} failed={progress['failed']} remaining={progress['remaining']}"
                        update_job_status(job_id, "failed", error_message=msg, extra={"resume_stage": "downloading_content"})
                        logging.warning(f"[Downloader:{label}] [{job_id}] {msg}")
                    else:
                        # Nothing pending. Old failures don't make this run a failure
                        # — the user can click "Retry Downloads" to attempt them.
                        update_job_status(job_id, "completed")
                        if progress["failed"]:
                            logging.info(f"[Downloader:{label}] [{job_id}] All available downloads done ({progress['completed']} ok, {progress['failed']} previously failed and not retried) — marking completed")
                        else:
                            logging.info(f"[Downloader:{label}] [{job_id}] Nothing left to download")
                    return

            while in_flight:
                done_set, _ = wait(list(in_flight.keys()), return_when=FIRST_COMPLETED, timeout=2)

                if not pending_action:
                    state = get_job_control_state(job_id)
                    if not state:
                        pending_action = "delete"
                    elif state.get("control_action"):
                        pending_action = state["control_action"]

                if pending_action and not cancel.is_set():
                    cancel.set()
                    logging.info(f"[Downloader:{label}] [{job_id}] Cancel signal sent — draining {len(in_flight)} in-flight tasks")

                for future in done_set:
                    in_flight.pop(future, None)
                    try:
                        future.result()
                    except Exception as exc:
                        logging.error(f"[Downloader:{label}] [{job_id}] Future error: {exc}")

                if not cancel.is_set():
                    progress = get_download_progress(job_id)
                    update_job_progress(job_id, total_media_count=progress["total"], total_media_downloaded=progress["completed"])
                    logging.info(f"[Downloader:{label}] [{job_id}] Media progress: {progress['completed']}/{progress['total']} (failed={progress['failed']} remaining={progress['remaining']})")
                    fill_pool()

        if pending_action:
            action = apply_job_control_action(job_id, "downloading_content")
            logging.info(f"[Downloader:{label}] [{job_id}] Control action applied: {action}")
            return

        progress = get_download_progress(job_id)
        update_job_progress(job_id, total_media_count=progress["total"], total_media_downloaded=progress["completed"])

        if is_drip_mode:
            # Don't terminate the job; scraper is still active. Just log progress and return.
            logging.info(
                f"[Downloader:{label}] [{job_id}] Drip cycle done "
                f"(completed={progress['completed']}/{progress['total']} failed={progress['failed']} remaining={progress['remaining']})"
            )
            return

        if progress["remaining"]:
            # Real unfinished work — mark failed so the user can retry.
            msg = f"Media incomplete: completed={progress['completed']} failed={progress['failed']} remaining={progress['remaining']}"
            update_job_status(job_id, "failed", error_message=msg, extra={"resume_stage": "downloading_content"})
            logging.warning(f"[Downloader:{label}] [{job_id}] {msg}")
        elif progress["failed"]:
            # Some old failures, but nothing pending — mark completed so
            # clicking Continue doesn't infinite-loop the job through 'failed'.
            # User can click Retry Downloads to attempt the failed ones again.
            update_job_status(job_id, "completed")
            logging.info(f"[Downloader:{label}] [{job_id}] All downloads done ({progress['completed']} ok, {progress['failed']} previously failed and not retried) — marking completed")
        else:
            update_job_status(job_id, "completed")
            logging.info(f"[Downloader:{label}] [{job_id}] All downloads complete ({progress['completed']}/{progress['total']})")

    except Exception as exc:
        logging.error(f"[Downloader:{label}] [{job_id}] FAILED: {exc}", exc_info=True)
        if not is_drip_mode:
            update_job_status(job_id, "failed", error_message=str(exc), extra={"resume_stage": "downloading_content"})
