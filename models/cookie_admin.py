"""
Cookie folder administration helpers used by the dashboard's Cookies tab.

Layout (per platform):
    cookies/<platform>/                 — shared pool (legacy fallback)
    cookies/<platform>/worker_<N>/      — dedicated per-worker pool
    cookies/<platform>/cookie_trash/    — burnt cookies (also per-worker subfolder)
    cookies/<platform>/download/        — separate cookies for the downloader

A single helper `inventory()` returns a snapshot of every cookie file on disk;
upload/move/delete helpers manipulate the folders safely.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import time
from pathlib import Path

from models.queue_worker import (
    FACEBOOK_SCRAPING_WORKER_COUNT,
    INSTAGRAM_SCRAPING_WORKER_COUNT,
)

# Platforms that actually use cookies for scraping. TikTok currently doesn't.
SCRAPER_PLATFORMS: dict[str, dict] = {
    "facebook": {
        "label": "Facebook",
        "cookie_dir": Path(os.getenv("FACEBOOK_COOKIE_DIR", "./cookies/facebook")),
        "workers": FACEBOOK_SCRAPING_WORKER_COUNT,
    },
    "instagram": {
        "label": "Instagram",
        "cookie_dir": Path(os.getenv("INSTAGRAM_COOKIE_DIR", "./cookies/instagram")),
        "workers": INSTAGRAM_SCRAPING_WORKER_COUNT,
    },
}

VALID_FILENAME_RE = re.compile(r"^[A-Za-z0-9._-]+\.txt$")
NETSCAPE_HEADER = "# netscape http cookie file"


def _platform_dir(platform: str) -> Path:
    info = SCRAPER_PLATFORMS.get(platform)
    if not info:
        raise ValueError(f"Unknown platform: {platform}")
    return info["cookie_dir"]


def _safe_filename(name: str) -> str:
    """Return *name* if it looks safe; raise otherwise.

    Prevents path traversal (no ``../``, no slashes, must end in .txt).
    """
    base = os.path.basename(name or "")
    if not base or not VALID_FILENAME_RE.match(base):
        raise ValueError(f"Invalid cookie filename: {name!r}")
    return base


def _safe_location(platform: str, location: str) -> Path:
    """Resolve a destination folder string into an absolute path inside the platform dir.

    Accepted forms:
        "shared"              → cookies/<platform>/
        "worker_3"            → cookies/<platform>/worker_3/
        "download"            → cookies/<platform>/download/
        "trash"               → cookies/<platform>/cookie_trash/
        "worker_3/trash"      → cookies/<platform>/worker_3/cookie_trash/
    """
    base = _platform_dir(platform)
    location = (location or "").strip().strip("/").lower()

    if not location or location == "shared":
        return base

    if location == "trash":
        return base / "cookie_trash"

    if location == "download":
        return base / "download"

    parts = location.split("/")
    safe_parts: list[str] = []
    for part in parts:
        if not re.match(r"^(worker_\d+|cookie_trash|download)$", part):
            raise ValueError(f"Invalid location segment: {part!r}")
        safe_parts.append(part)
    return base.joinpath(*safe_parts)


def _cookie_files_in(folder: Path) -> list[dict]:
    if not folder.is_dir():
        return []
    out: list[dict] = []
    for path in sorted(folder.glob("*.txt")):
        if not path.is_file():
            continue
        try:
            stat = path.stat()
            out.append({
                "name": path.name,
                "size": stat.st_size,
                "modified": int(stat.st_mtime),
            })
        except OSError:
            continue
    return out


def inventory() -> dict:
    """Return a snapshot of every cookie file across all scraper platforms."""
    result = {"platforms": []}
    for platform_id, info in SCRAPER_PLATFORMS.items():
        base = info["cookie_dir"]
        base.mkdir(parents=True, exist_ok=True)

        workers: list[dict] = []
        for n in range(1, info["workers"] + 1):
            worker_dir = base / f"worker_{n}"
            cookies = _cookie_files_in(worker_dir)
            trash = _cookie_files_in(worker_dir / "cookie_trash")
            workers.append({
                "name": f"worker_{n}",
                "cookies": cookies,
                "cookie_count": len(cookies),
                "trash": trash,
                "trash_count": len(trash),
            })

        # Cookies sitting in the platform root (legacy / shared pool)
        shared_cookies: list[dict] = []
        if base.is_dir():
            for path in sorted(base.glob("*.txt")):
                if path.is_file():
                    try:
                        stat = path.stat()
                        shared_cookies.append({
                            "name": path.name,
                            "size": stat.st_size,
                            "modified": int(stat.st_mtime),
                        })
                    except OSError:
                        continue

        platform_trash = _cookie_files_in(base / "cookie_trash")
        download_cookies = _cookie_files_in(base / "download")

        result["platforms"].append({
            "id": platform_id,
            "label": info["label"],
            "worker_count": info["workers"],
            "workers": workers,
            "shared_cookies": shared_cookies,
            "shared_count": len(shared_cookies),
            "trash": platform_trash,
            "trash_count": len(platform_trash),
            "download_cookies": download_cookies,
            "download_count": len(download_cookies),
        })
    return result


def validate_netscape_cookie(content: bytes) -> bool:
    """Loose check that *content* looks like a Netscape cookie file.

    Accepts files where the first non-blank line starts with the standard
    header, OR where at least one valid 7+-column tab-separated row exists.
    """
    try:
        text = content.decode("utf-8", errors="replace")
    except Exception:
        return False
    head = text[:200].lower().strip()
    if head.startswith("# netscape") or head.startswith("# http cookie file"):
        return True
    valid_lines = 0
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if len(line.split("\t")) >= 7:
            valid_lines += 1
            if valid_lines >= 1:
                return True
    return False


def upload_cookie(platform: str, location: str, filename: str, content: bytes) -> dict:
    """Validate and write *content* to <platform>/<location>/<filename>.

    Returns a dict describing the saved file.
    """
    if not content:
        raise ValueError("Empty file uploaded")
    if not validate_netscape_cookie(content):
        raise ValueError("File does not look like a Netscape cookie file")

    safe_name = _safe_filename(filename)
    target_dir = _safe_location(platform, location)
    target_dir.mkdir(parents=True, exist_ok=True)

    dest = target_dir / safe_name
    if dest.exists():
        # Stamp the new file so we don't blow away an existing cookie silently.
        dest = target_dir / f"{dest.stem}_{int(time.time())}{dest.suffix}"

    dest.write_bytes(content)
    logging.info("[CookieAdmin] Uploaded %s → %s (%d bytes)", safe_name, dest, len(content))
    return {
        "name": dest.name,
        "path": str(dest),
        "size": len(content),
    }


def auto_distribute_uploads(platform: str, files: list[tuple[str, bytes]]) -> list[dict]:
    """Spread uploaded files across this platform's worker_N folders.

    Picks the worker with the fewest cookies, breaking ties by lowest worker
    number. Returns a list of saved file descriptors.
    """
    info = SCRAPER_PLATFORMS.get(platform)
    if not info:
        raise ValueError(f"Unknown platform: {platform}")
    if info["workers"] < 1:
        raise ValueError(f"No workers configured for {platform}")

    saved: list[dict] = []
    for filename, content in files:
        # Recount each round so each file lands on the (currently) emptiest worker.
        counts = []
        for n in range(1, info["workers"] + 1):
            worker_dir = info["cookie_dir"] / f"worker_{n}"
            count = len(_cookie_files_in(worker_dir))
            counts.append((count, n))
        counts.sort()
        target_worker = counts[0][1]
        result = upload_cookie(platform, f"worker_{target_worker}", filename, content)
        result["assigned_to"] = f"worker_{target_worker}"
        saved.append(result)
    return saved


def delete_cookie(platform: str, location: str, filename: str) -> None:
    safe_name = _safe_filename(filename)
    target_dir = _safe_location(platform, location)
    target = target_dir / safe_name
    if not target.is_file():
        raise FileNotFoundError(f"Cookie not found: {platform}/{location}/{filename}")
    target.unlink()
    logging.info("[CookieAdmin] Deleted %s", target)


def move_cookie(platform: str, source_location: str, dest_location: str, filename: str) -> dict:
    safe_name = _safe_filename(filename)
    source_dir = _safe_location(platform, source_location)
    dest_dir = _safe_location(platform, dest_location)
    src = source_dir / safe_name
    if not src.is_file():
        raise FileNotFoundError(f"Cookie not found at source: {platform}/{source_location}/{filename}")
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / safe_name
    if dest.exists():
        dest = dest_dir / f"{dest.stem}_{int(time.time())}{dest.suffix}"
    shutil.move(str(src), str(dest))
    logging.info("[CookieAdmin] Moved %s → %s", src, dest)
    return {"name": dest.name, "path": str(dest)}


def restore_from_trash(platform: str, source_location: str, filename: str, target_worker: int | None = None) -> dict:
    """Move a cookie out of cookie_trash back into a worker folder."""
    info = SCRAPER_PLATFORMS.get(platform)
    if not info:
        raise ValueError(f"Unknown platform: {platform}")
    target = target_worker or 1
    if target < 1 or target > info["workers"]:
        raise ValueError(f"Worker {target} is out of range for {platform}")
    return move_cookie(platform, source_location, f"worker_{target}", filename)
