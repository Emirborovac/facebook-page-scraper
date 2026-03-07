"""
Media downloader for scraped Facebook posts.

Images  : direct HTTP GET to fbcdn.net CDN URLs (with cookies + 3 retries)
Videos  : yt_dlp Python library with Facebook cookies (same technique as fallback.py)

Up to MAX_WORKERS concurrent downloads run via ThreadPoolExecutor.
After each download the file is uploaded to S3 and deleted locally.
"""

import json
import logging
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
import yt_dlp
from dotenv import load_dotenv

from models.operations import (
    get_pending_download_posts,
    update_job_progress,
    update_job_status,
    update_post_download,
)
from models.s3_upload import delete_local_file, upload_file_to_s3

load_dotenv()

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./downloads")
COOKIES_FILE = os.getenv("COOKIES_FILE", "./cookies.txt")
MAX_WORKERS = 5
MAX_RETRIES = 3
RETRY_DELAY_SEC = 2


def _ensure_output_dir():
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)


def _load_cookies_into_session(session: requests.Session) -> int:
    """Load Netscape cookies.txt into a requests session for facebook.com domains."""
    path = Path(COOKIES_FILE)
    if not path.exists():
        return 0
    count = 0
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) < 7:
                continue
            if "facebook" not in parts[0].lower() and "fb.com" not in parts[0].lower():
                continue
            name = parts[5]
            value = "\t".join(parts[6:])
            for d in ["www.facebook.com", ".facebook.com", "facebook.com"]:
                session.cookies.set(name, value, domain=d)
            count += 1
    return count


# ──────────────────────────────────────────────────────────────────────────────
# Image downloader — HTTP GET with cookies and up to 3 retries
# ──────────────────────────────────────────────────────────────────────────────

# Minimum bytes to accept as valid image (40x40 thumbnails can be ~500–900 bytes)
MIN_IMAGE_BYTES = 500


def _download_image(url: str, dest_path: str) -> bool:
    """Download image from Facebook CDN. Retries up to MAX_RETRIES on failure.
    Accepts small thumbnails (e.g. 40x40) — s960x960 substitution causes 403 from CDN."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Referer": "https://www.facebook.com/",
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    }
    session = requests.Session()
    session.headers.update(headers)
    _load_cookies_into_session(session)

    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, stream=True, timeout=30)
            r.raise_for_status()
            with open(dest_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            size = os.path.getsize(dest_path)
            if size < MIN_IMAGE_BYTES:
                logging.warning(f"[Downloader] Image too small ({size}B) attempt {attempt}/{MAX_RETRIES}: {url[:80]}")
                last_exc = ValueError(f"File too small ({size}B)")
            else:
                if size < 1000:
                    logging.info(f"[Downloader] Accepted small thumbnail ({size}B): {url[:60]}...")
                return True
        except Exception as exc:
            last_exc = exc
            logging.warning(f"[Downloader] Image download failed attempt {attempt}/{MAX_RETRIES} ({url[:80]}): {exc}")
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY_SEC)
    logging.warning(f"[Downloader] Image download gave up after {MAX_RETRIES} attempts: {url[:80]}")
    return False


# ──────────────────────────────────────────────────────────────────────────────
# Video downloader — yt_dlp Python library with Facebook cookies
# ──────────────────────────────────────────────────────────────────────────────

def _download_video(url: str, dest_path: str) -> bool:
    """Download a Facebook video using yt_dlp with cookie authentication. Retries up to MAX_RETRIES on failure."""
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
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
                    "Referer": "https://www.facebook.com/",
                },
            }
            if os.path.exists(COOKIES_FILE):
                ydl_opts["cookiefile"] = COOKIES_FILE
            else:
                logging.warning(f"[Downloader] Cookies file not found: {COOKIES_FILE}")

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])

            # yt_dlp may write e.g. dest_path.mp4 if dest_path has no extension
            if not os.path.exists(dest_path):
                for ext in (".mp4", ".mkv", ".webm"):
                    candidate = dest_path if dest_path.endswith(ext) else dest_path + ext
                    if os.path.exists(candidate):
                        os.rename(candidate, dest_path)
                        break

            if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
                return True

            last_exc = ValueError("Video file missing or empty after download")
            logging.warning(f"[Downloader] Video file missing after download attempt {attempt}/{MAX_RETRIES}: {url[:80]}")
        except yt_dlp.utils.DownloadError as exc:
            last_exc = exc
            logging.warning(f"[Downloader] yt_dlp DownloadError attempt {attempt}/{MAX_RETRIES} ({url[:80]}): {exc}")
        except Exception as exc:
            last_exc = exc
            logging.warning(f"[Downloader] Video download error attempt {attempt}/{MAX_RETRIES} ({url[:80]}): {exc}")

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY_SEC)

    logging.warning(f"[Downloader] Video download gave up after {MAX_RETRIES} attempts: {url[:80]}")
    return False


# ──────────────────────────────────────────────────────────────────────────────
# Video cleanup — remove merged file + yt-dlp intermediate streams
# ──────────────────────────────────────────────────────────────────────────────

def _cleanup_video_files(dest_path: str):
    """Delete the merged video file and any yt-dlp intermediate files sharing the same stem.

    yt-dlp writes separate audio/video streams before merging, e.g.:
      uid_video.f908065465168373v.mp4
      uid_video.f1195255629094736a.m4a
    These are not cleaned up by a simple os.remove(dest_path).
    """
    from pathlib import Path as _Path
    p = _Path(dest_path)
    directory = p.parent
    # The stem is everything before the first dot, e.g. "uid_video" from "uid_video.mp4"
    stem = p.name.split(".")[0]
    removed = 0
    try:
        for f in directory.iterdir():
            if f.name.startswith(stem + ".") or f.name == p.name:
                try:
                    f.unlink()
                    removed += 1
                except Exception as exc:
                    logging.warning(f"[Downloader] Could not delete temp file {f}: {exc}")
    except Exception as exc:
        logging.warning(f"[Downloader] Video cleanup error for {dest_path}: {exc}")
    if removed:
        logging.debug(f"[Downloader] Cleaned up {removed} file(s) for {p.name}")


# ──────────────────────────────────────────────────────────────────────────────
# Per-post download task
# ──────────────────────────────────────────────────────────────────────────────

def _process_post(post: dict, job_id: str) -> bool:
    post_id = post["id"]
    update_post_download(post_id, "downloading")

    image_urls = []
    raw_img = post.get("image_urls")
    if raw_img:
        try:
            image_urls = json.loads(raw_img) if isinstance(raw_img, str) else raw_img
        except Exception:
            image_urls = []

    video_url = post.get("video_url") or ""
    has_video = bool(post.get("has_video")) and bool(video_url)
    has_image = bool(post.get("has_image")) and bool(image_urls)

    s3_image_urls, s3_image_keys = [], []
    s3_video_url, s3_video_key = None, None
    errors = []
    uid = str(uuid.uuid4())[:8]

    # ── Images ──────────────────────────────────────────────────────────────
    if has_image:
        for idx, img_url in enumerate(image_urls):
            local = os.path.join(OUTPUT_DIR, f"{uid}_img_{idx}.jpg")
            s3_fname = f"{job_id}/{uid}_img_{idx}.jpg"
            try:
                if _download_image(img_url, local):
                    url_out, key = upload_file_to_s3(local, s3_fname, content_type="image/jpeg")
                    s3_image_urls.append(url_out)
                    s3_image_keys.append(key)
                else:
                    errors.append(f"img_dl_failed:{img_url[:60]}")
            except Exception as exc:
                errors.append(f"img_s3_failed:{str(exc)[:80]}")
            finally:
                delete_local_file(local)

    # ── Video ────────────────────────────────────────────────────────────────
    if has_video:
        local = os.path.join(OUTPUT_DIR, f"{uid}_video.mp4")
        s3_fname = f"{job_id}/{uid}_video.mp4"
        try:
            if _download_video(video_url, local):
                url_out, key = upload_file_to_s3(local, s3_fname, content_type="video/mp4")
                s3_video_url = url_out
                s3_video_key = key
            else:
                errors.append(f"video_dl_failed:{video_url[:60]}")
        except Exception as exc:
            errors.append(f"video_s3_failed:{str(exc)[:80]}")
        finally:
            # Delete merged file and any yt-dlp intermediate streams (e.g. uid_video.f*.mp4 / .m4a)
            _cleanup_video_files(local)

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
    """
    Download all media for a job in 'downloading_content' status.
    Blocking — call from a thread-pool executor.
    """
    job_id = job["job_id"]
    _ensure_output_dir()
    logging.info(f"[Downloader] [{job_id}] Starting media downloads")

    try:
        posts = get_pending_download_posts(job_id)
        total = len(posts)

        if total == 0:
            update_job_status(job_id, "completed")
            logging.info(f"[Downloader] [{job_id}] No media posts — marking completed")
            return

        update_job_progress(job_id, total_media_count=total, total_media_downloaded=0)

        done = 0
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(_process_post, post, job_id): post for post in posts}
            for fut in as_completed(futures):
                try:
                    fut.result()
                except Exception as exc:
                    logging.error(f"[Downloader] [{job_id}] Future error: {exc}")
                done += 1
                update_job_progress(job_id, total_media_downloaded=done)
                logging.info(f"[Downloader] [{job_id}] Media progress: {done}/{total}")

        update_job_status(job_id, "completed")
        logging.info(f"[Downloader] [{job_id}] All downloads complete ({done}/{total})")

    except Exception as exc:
        logging.error(f"[Downloader] [{job_id}] FAILED: {exc}", exc_info=True)
        update_job_status(job_id, "failed", error_message=str(exc))
