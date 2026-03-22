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
from threading import Lock
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
from models.s3_upload import delete_local_file, upload_file_to_s3

load_dotenv()

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./downloads")
COOKIES_FILE = os.getenv("COOKIES_FILE", "./cookies.txt")
INSTAGRAM_COOKIES_FILE = os.getenv("INSTAGRAM_COOKIES_FILE", "./instagram_cookies.txt")
FACEBOOK_COOKIE_POOL_DIR = Path(os.getenv("FACEBOOK_COOKIE_IMPORT_DIR", "./cookie_refresh_20260319/fb_active_last_bundle"))
FACEBOOK_PREFERRED_COOKIE_FILES = [
    name.strip()
    for name in os.getenv(
        "FACEBOOK_PREFERRED_COOKIE_FILES",
        "07.txt,08.txt,10.txt,02.txt,09.txt,01.txt,03.txt,04.txt,11.txt,12.txt,13.txt,14.txt,15.txt,16.txt,17.txt,18.txt,19.txt,20.txt",
    ).split(",")
    if name.strip()
]
INSTAGRAM_COOKIE_POOL_DIR = Path(os.getenv("INSTAGRAM_COOKIE_POOL_DIR", "./cookie_refresh_20260319/ig_active"))
INSTAGRAM_PREFERRED_COOKIE_FILES = [
    name.strip()
    for name in os.getenv(
        "INSTAGRAM_PREFERRED_COOKIE_FILES",
        "01.txt,02.txt,03.txt,04.txt,05.txt,06.txt,07.txt,08.txt,09.txt,10.txt,11.txt,12.txt,13.txt,14.txt,15.txt,16.txt,17.txt,18.txt,19.txt,20.txt,21.txt,22.txt,23.txt,24.txt,25.txt,26.txt,27.txt,28.txt,29.txt,30.txt,31.txt,32.txt,33.txt,34.txt,35.txt,36.txt,37.txt,38.txt,39.txt",
    ).split(",")
    if name.strip()
]
_FB_COOKIE_LOCK = Lock()
_FB_COOKIE_INDEX = 0
_IG_COOKIE_LOCK = Lock()
_IG_COOKIE_INDEX = 0
MAX_WORKERS = 5
MAX_RETRIES = 3
RETRY_DELAY_SEC = 2


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
        path = Path(INSTAGRAM_COOKIES_FILE if platform == 'instagram' else COOKIES_FILE)
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



def _facebook_cookie_files() -> list[Path]:
    if FACEBOOK_COOKIE_POOL_DIR.exists():
        preferred_map = {name: FACEBOOK_COOKIE_POOL_DIR / name for name in FACEBOOK_PREFERRED_COOKIE_FILES}
        ordered = [preferred_map[name] for name in FACEBOOK_PREFERRED_COOKIE_FILES if preferred_map.get(name) and preferred_map[name].exists()]
        discovered = [p for p in sorted(FACEBOOK_COOKIE_POOL_DIR.glob('*.txt')) if p.is_file()]
        seen = {p.resolve() for p in ordered}
        ordered.extend([p for p in discovered if p.resolve() not in seen])
        if ordered:
            return ordered
    fallback = Path(COOKIES_FILE)
    return [fallback] if fallback.exists() else []


def _next_facebook_cookie_file() -> Path | None:
    global _FB_COOKIE_INDEX
    files = _facebook_cookie_files()
    if not files:
        return None
    with _FB_COOKIE_LOCK:
        path = files[_FB_COOKIE_INDEX % len(files)]
        _FB_COOKIE_INDEX = (_FB_COOKIE_INDEX + 1) % len(files)
    return path


def _instagram_cookie_files() -> list[Path]:
    if INSTAGRAM_COOKIE_POOL_DIR.exists():
        preferred_map = {name: INSTAGRAM_COOKIE_POOL_DIR / name for name in INSTAGRAM_PREFERRED_COOKIE_FILES}
        ordered = [preferred_map[name] for name in INSTAGRAM_PREFERRED_COOKIE_FILES if preferred_map.get(name) and preferred_map[name].exists()]
        if ordered:
            return ordered
    return [Path(INSTAGRAM_COOKIES_FILE)]


def _next_instagram_cookie_file() -> Path:
    global _IG_COOKIE_INDEX
    files = _instagram_cookie_files()
    with _IG_COOKIE_LOCK:
        path = files[_IG_COOKIE_INDEX % len(files)]
        _IG_COOKIE_INDEX = (_IG_COOKIE_INDEX + 1) % len(files)
    return path


# ──────────────────────────────────────────────────────────────────────────────
# Image downloader — HTTP GET with cookies and up to 3 retries
# ──────────────────────────────────────────────────────────────────────────────

MIN_IMAGE_BYTES = 500


def _direct_binary_download(url: str, dest_path: str, platform: str) -> bool:
    referer = 'https://www.instagram.com/' if platform == 'instagram' else 'https://www.facebook.com/'
    accept = '*/*' if platform == 'instagram' else 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8'
    session = requests.Session()
    cookie_file = _next_instagram_cookie_file() if platform == 'instagram' else _next_facebook_cookie_file()
    session.headers.update({
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/124.0.0.0 Safari/537.36'
        ),
        'Referer': referer,
        'Accept': accept,
    })
    _load_cookies_into_session(session, platform=platform, cookie_file=cookie_file)
    if cookie_file:
        logging.debug(f'[Downloader] Using {platform} cookie file {Path(cookie_file).name} for binary download')

    for attempt in range(1, MAX_RETRIES + 1):
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
            logging.warning(f'[Downloader] Binary too small ({size}B) attempt {attempt}/{MAX_RETRIES}: {url[:80]}')
        except Exception as exc:
            logging.warning(f'[Downloader] Binary download failed attempt {attempt}/{MAX_RETRIES} ({url[:80]}): {exc}')
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY_SEC)
    logging.warning(f'[Downloader] Binary download gave up after {MAX_RETRIES} attempts: {url[:80]}')
    return False


def _download_image(url: str, dest_path: str, platform: str = 'facebook') -> bool:
    return _direct_binary_download(url, dest_path, platform)



# ──────────────────────────────────────────────────────────────────────────────
# Video downloader — yt_dlp Python library with Facebook cookies
# ──────────────────────────────────────────────────────────────────────────────

def _download_video(url: str, dest_path: str, platform: str | None = None) -> bool:
    platform = platform or _video_source_platform(url)
    if platform == "instagram":
        return _direct_binary_download(url, dest_path, platform)
    referer = "https://www.tiktok.com/" if platform == "tiktok" else "https://www.facebook.com/"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            cookie_file = _next_facebook_cookie_file() if platform == "facebook" else None
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
            if platform == "facebook":
                if cookie_file and os.path.exists(cookie_file):
                    ydl_opts["cookiefile"] = str(cookie_file)
                elif os.path.exists(COOKIES_FILE):
                    ydl_opts["cookiefile"] = COOKIES_FILE
                else:
                    logging.warning(f"[Downloader] Cookies file not found: {COOKIES_FILE}")

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

            logging.warning(f"[Downloader] Video file missing after download attempt {attempt}/{MAX_RETRIES}: {url[:80]}")
        except yt_dlp.utils.DownloadError as exc:
            logging.warning(f"[Downloader] yt_dlp DownloadError attempt {attempt}/{MAX_RETRIES} ({url[:80]}): {exc}")
        except Exception as exc:
            logging.warning(f"[Downloader] Video download error attempt {attempt}/{MAX_RETRIES} ({url[:80]}): {exc}")

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY_SEC)

    logging.warning(f"[Downloader] Video download gave up after {MAX_RETRIES} attempts: {url[:80]}")
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

def _process_post(post: dict, job_id: str) -> bool:
    post_id = post["id"]
    post_link = post.get("post_link") or post.get("video_url") or ""
    platform = _video_source_platform(post_link)
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
        for idx, image_url in enumerate(image_urls):
            local_path = os.path.join(OUTPUT_DIR, f"{uid}_img_{idx}.jpg")
            s3_name = f"{job_id}/{uid}_img_{idx}.jpg"
            try:
                if _download_image(image_url, local_path, platform=platform):
                    url_out, key = upload_file_to_s3(local_path, s3_name, content_type="image/jpeg")
                    s3_image_urls.append(url_out)
                    s3_image_keys.append(key)
                else:
                    errors.append(f"img_dl_failed:{image_url[:60]}")
            except Exception as exc:
                errors.append(f"img_s3_failed:{str(exc)[:80]}")
            finally:
                delete_local_file(local_path)

    if has_video:
        local_path = os.path.join(OUTPUT_DIR, f"{uid}_video.mp4")
        s3_name = f"{job_id}/{uid}_video.mp4"
        try:
            if _download_video(video_url, local_path, platform=platform):
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

def download_job_media(job: dict):
    """Download all media for a job in downloading_content status."""
    job_id = job["job_id"]
    _ensure_output_dir()
    logging.info(f"[Downloader] [{job_id}] Starting media downloads")

    try:
        progress = get_download_progress(job_id)
        total = progress["total"]
        completed_count = progress["completed"]
        posts = get_pending_download_posts(job_id)

        if total == 0:
            update_job_status(job_id, "completed")
            logging.info(f"[Downloader] [{job_id}] No media posts — marking completed")
            return

        update_job_progress(job_id, total_media_count=total, total_media_downloaded=completed_count)

        state = get_job_control_state(job_id)
        if state and state.get("control_action"):
            action = apply_job_control_action(job_id, "downloading_content")
            logging.info(f"[Downloader] [{job_id}] Control action applied before start: {action}")
            return

        pending_action = None
        post_iter = iter(posts)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            in_flight = {}

            def fill_pool():
                nonlocal pending_action
                while not pending_action and len(in_flight) < MAX_WORKERS:
                    try:
                        post = next(post_iter)
                    except StopIteration:
                        break
                    future = pool.submit(_process_post, post, job_id)
                    in_flight[future] = post

            fill_pool()
            if not in_flight:
                if get_pending_download_posts(job_id):
                    logging.info(f"[Downloader] [{job_id}] Waiting for download queue to refill")
                else:
                    progress = get_download_progress(job_id)
                    update_job_progress(job_id, total_media_count=progress["total"], total_media_downloaded=progress["completed"])
                    if progress["failed"] or progress["remaining"]:
                        msg = f"Media incomplete: completed={progress['completed']} failed={progress['failed']} remaining={progress['remaining']}"
                        update_job_status(job_id, "failed", error_message=msg, extra={"resume_stage": "downloading_content"})
                        logging.warning(f"[Downloader] [{job_id}] {msg}")
                    else:
                        update_job_status(job_id, "completed")
                        logging.info(f"[Downloader] [{job_id}] Nothing left to download")
                    return

            while in_flight:
                done_set, _ = wait(list(in_flight.keys()), return_when=FIRST_COMPLETED)
                for future in done_set:
                    in_flight.pop(future, None)
                    try:
                        future.result()
                    except Exception as exc:
                        logging.error(f"[Downloader] [{job_id}] Future error: {exc}")
                    progress = get_download_progress(job_id)
                    update_job_progress(job_id, total_media_count=progress["total"], total_media_downloaded=progress["completed"])
                    logging.info(f"[Downloader] [{job_id}] Media progress: {progress['completed']}/{progress['total']} (failed={progress['failed']} remaining={progress['remaining']})")

                state = get_job_control_state(job_id)
                if not state:
                    pending_action = pending_action or "delete"
                elif not pending_action and state.get("control_action"):
                    pending_action = state["control_action"]
                    logging.info(f"[Downloader] [{job_id}] Control action requested: {pending_action}")

                if not pending_action:
                    fill_pool()

        if pending_action:
            action = apply_job_control_action(job_id, "downloading_content")
            logging.info(f"[Downloader] [{job_id}] Control action applied after in-flight work: {action}")
            return

        progress = get_download_progress(job_id)
        update_job_progress(job_id, total_media_count=progress["total"], total_media_downloaded=progress["completed"])
        if progress["failed"] or progress["remaining"]:
            msg = f"Media incomplete: completed={progress['completed']} failed={progress['failed']} remaining={progress['remaining']}"
            update_job_status(job_id, "failed", error_message=msg, extra={"resume_stage": "downloading_content"})
            logging.warning(f"[Downloader] [{job_id}] {msg}")
        else:
            update_job_status(job_id, "completed")
            logging.info(f"[Downloader] [{job_id}] All downloads complete ({progress['completed']}/{progress['total']})")

    except Exception as exc:
        logging.error(f"[Downloader] [{job_id}] FAILED: {exc}", exc_info=True)
        update_job_status(job_id, "failed", error_message=str(exc), extra={"resume_stage": "downloading_content"})
