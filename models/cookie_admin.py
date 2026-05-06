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
        "shared"                 → cookies/<platform>/
        "worker_3"               → cookies/<platform>/worker_3/
        "download"               → cookies/<platform>/download/         (legacy shared download pool)
        "download_3"             → cookies/<platform>/download_3/        (per-lane download pool)
        "trash"                  → cookies/<platform>/cookie_trash/
        "worker_3/trash"         → cookies/<platform>/worker_3/cookie_trash/
        "download_3/cookie_trash" → cookies/<platform>/download_3/cookie_trash/
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
        if not re.match(r"^(worker_\d+|download_\d+|cookie_trash|download)$", part):
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

        # Per-lane download workers (mirror of scraping workers, isolated cookies).
        download_workers: list[dict] = []
        for n in range(1, info["workers"] + 1):
            lane_dir = base / f"download_{n}"
            cookies = _cookie_files_in(lane_dir)
            trash = _cookie_files_in(lane_dir / "cookie_trash")
            download_workers.append({
                "name": f"download_{n}",
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
        # Legacy shared download pool (still used as fallback if a lane is empty).
        download_cookies = _cookie_files_in(base / "download")

        result["platforms"].append({
            "id": platform_id,
            "label": info["label"],
            "worker_count": info["workers"],
            "workers": workers,
            "download_workers": download_workers,
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


def auto_distribute_uploads(platform: str, files: list[tuple[str, bytes]],
                            lane_kind: str = "scraping") -> list[dict]:
    """Spread uploaded files across this platform's worker folders.

    *lane_kind* selects the target lane series:
      - "scraping" → ``worker_N``     (paired with scraping workers)
      - "download" → ``download_N``    (paired with download workers)

    Picks the lane with the fewest cookies, breaking ties by lowest index.
    """
    info = SCRAPER_PLATFORMS.get(platform)
    if not info:
        raise ValueError(f"Unknown platform: {platform}")
    if info["workers"] < 1:
        raise ValueError(f"No workers configured for {platform}")

    if lane_kind == "scraping":
        prefix = "worker"
    elif lane_kind == "download":
        prefix = "download"
    else:
        raise ValueError(f"Unknown lane_kind: {lane_kind!r}")

    saved: list[dict] = []
    for filename, content in files:
        # Recount each round so each file lands on the (currently) emptiest lane.
        counts = []
        for n in range(1, info["workers"] + 1):
            lane_dir = info["cookie_dir"] / f"{prefix}_{n}"
            count = len(_cookie_files_in(lane_dir))
            counts.append((count, n))
        counts.sort()
        target_idx = counts[0][1]
        target_location = f"{prefix}_{target_idx}"
        result = upload_cookie(platform, target_location, filename, content)
        result["assigned_to"] = target_location
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


def redistribute_legacy_download_pool(platform: str) -> dict:
    """Move every cookie from cookies/<platform>/download/ into download_N/ lanes.

    Spreads them across worker_count lanes round-robin (lowest count first).
    Returns a summary describing how many moved into each lane.
    """
    info = SCRAPER_PLATFORMS.get(platform)
    if not info:
        raise ValueError(f"Unknown platform: {platform}")
    if info["workers"] < 1:
        raise ValueError(f"No download workers configured for {platform}")

    legacy_dir = info["cookie_dir"] / "download"
    if not legacy_dir.is_dir():
        return {"moved": 0, "lanes": {}}

    files = sorted(p for p in legacy_dir.glob("*.txt") if p.is_file())
    moves: list[dict] = []
    lane_totals: dict[str, int] = {}
    for path in files:
        # Re-count each round so each cookie lands on the (currently) emptiest lane.
        counts = []
        for n in range(1, info["workers"] + 1):
            lane = info["cookie_dir"] / f"download_{n}"
            counts.append((len(_cookie_files_in(lane)), n))
        counts.sort()
        target_idx = counts[0][1]
        target_location = f"download_{target_idx}"
        try:
            result = move_cookie(platform, "download", target_location, path.name)
            moves.append({"file": path.name, "lane": target_location, "new_name": result["name"]})
            lane_totals[target_location] = lane_totals.get(target_location, 0) + 1
        except Exception as exc:
            logging.error("[CookieAdmin] Failed to redistribute %s: %s", path.name, exc)

    logging.info(
        "[CookieAdmin] Redistributed %d %s cookies from legacy download pool: %s",
        len(moves), platform, lane_totals,
    )
    return {"moved": len(moves), "lanes": lane_totals, "details": moves}


def restore_from_trash(platform: str, source_location: str, filename: str,
                       target_worker: int | None = None, lane_kind: str = "scraping") -> dict:
    """Move a cookie out of cookie_trash back into a worker folder.

    *lane_kind*:
      - "scraping" → restore to ``worker_N``
      - "download" → restore to ``download_N``
    """
    info = SCRAPER_PLATFORMS.get(platform)
    if not info:
        raise ValueError(f"Unknown platform: {platform}")
    target = target_worker or 1
    if target < 1 or target > info["workers"]:
        raise ValueError(f"Worker {target} is out of range for {platform}")
    if lane_kind == "download":
        prefix = "download"
    elif lane_kind == "scraping":
        prefix = "worker"
    else:
        raise ValueError(f"Unknown lane_kind: {lane_kind!r}")
    return move_cookie(platform, source_location, f"{prefix}_{target}", filename)
