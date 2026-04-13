"""
Facebook GraphQL scraper – adapted from references/test_graphql_direct.py.
Runs synchronously inside a thread-pool executor so it doesn't block the
asyncio event loop.
"""

import json
import logging
import os
import re
import time
from datetime import datetime, date
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests
from dotenv import load_dotenv

from models.operations import (
    apply_job_control_action,
    get_job_post_count,
    record_job_good_checkpoint,
    save_post,
    update_job_progress,
    update_job_scrape_checkpoint,
    update_job_status,
)
from models.instagram_scraper import run_instagram_scraper
from models.proxy import apply_scrape_proxy, rotate_scrape_proxy
from models.tiktok_scraper import run_tiktok_scraper

load_dotenv()

FACEBOOK_COOKIE_DIR = Path(os.getenv("FACEBOOK_COOKIE_DIR", "./cookies/facebook"))
FACEBOOK_SCRAPING_WORKER_COUNT = max(1, int(os.getenv("FACEBOOK_SCRAPING_WORKER_COUNT", "6")))

# Facebook internal query ID for the scroll-triggered pagination query.
# This ID is embedded in Facebook's JS bundle and may change after major deploys.
REFETCH_QUERY_ID = "25997057593319516"

POSTS_PER_PAGE = int(os.getenv("FACEBOOK_POSTS_PER_PAGE", "5"))
REQUEST_DELAY = float(os.getenv("FACEBOOK_REQUEST_DELAY", "2.0"))
MAX_STALE_PAGES = 5
MAX_CYCLE_REBOOTSTRAP_RETRIES = 5
GOOD_CHECKPOINT_HISTORY_LIMIT = 20
REBOOTSTRAP_EVERY = int(os.getenv("FACEBOOK_REBOOTSTRAP_EVERY", "150"))
REST_EVERY = int(os.getenv("FACEBOOK_REST_EVERY", "100"))
REST_DURATION = float(os.getenv("FACEBOOK_REST_DURATION", "20"))

GET_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}


# ──────────────────────────────────────────────────────────────────────────────
# Cookie loading
# ──────────────────────────────────────────────────────────────────────────────

def load_cookies(session: requests.Session, cookie_file=None) -> int:
    path = Path(cookie_file) if cookie_file else (FACEBOOK_COOKIE_DIR / "01.txt")
    if not path.exists():
        logging.warning(f"[Scraper] Cookies file not found: {path}")
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
            name = parts[5]
            value = "\t".join(parts[6:])
            if "facebook" not in parts[0] and "fb.com" not in parts[0]:
                continue
            for d in ["www.facebook.com", ".facebook.com", "facebook.com"]:
                session.cookies.set(name, value, domain=d)
            count += 1
    return count


class RecoverableScrapePause(RuntimeError):
    """Raised when a job should pause and wait for a later continue."""


def _cookie_label(cookie_file) -> str:
    return Path(cookie_file).name


def _numeric_sort_key(path: Path):
    stem = path.stem
    return (0, int(stem)) if stem.isdigit() else (1, stem)


def _worker_cookie_pool(worker_name: str = None) -> list[Path]:
    worker_num = 1
    if worker_name:
        match = re.search(r"(\d+)$", worker_name)
        if match:
            worker_num = max(int(match.group(1)), 1)

    if FACEBOOK_COOKIE_DIR.exists():
        files = [p for p in FACEBOOK_COOKIE_DIR.glob('*.txt') if p.is_file()]
        files.sort(key=_numeric_sort_key)
        if files:
            primary_count = min(FACEBOOK_SCRAPING_WORKER_COUNT, len(files))
            primaries = files[:primary_count]
            reserves = files[primary_count:]
            primary = primaries[(worker_num - 1) % len(primaries)]
            ordered = [primary]
            if reserves:
                reserve_offset = (worker_num - 1) % len(reserves)
                ordered.extend(reserves[reserve_offset:] + reserves[:reserve_offset])
            for path in primaries:
                if path != primary:
                    ordered.append(path)
            deduped = []
            seen = set()
            for path in ordered:
                key = str(path)
                if key in seen:
                    continue
                seen.add(key)
                deduped.append(path)
            return deduped

    logging.warning("[Scraper] No Facebook cookie files found in %s", FACEBOOK_COOKIE_DIR)
    return []


def _open_cookie_context(cookie_pool: list, start_index: int, target_page: str, job_id: str, reason: str, worker_name: str = None):
    if not cookie_pool:
        raise RecoverableScrapePause("No cookie files are configured for this worker")

    last_error = None
    for cookie_index in range(max(int(start_index or 0), 0), len(cookie_pool)):
        cookie_file = cookie_pool[cookie_index]
        session = requests.Session()
        apply_scrape_proxy(session, worker_id=worker_name)
        try:
            n_cookies = load_cookies(session, cookie_file)
            if n_cookies == 0:
                raise RuntimeError(f"No Facebook cookies loaded from {_cookie_label(cookie_file)}")

            resp = session.get(target_page, headers=GET_HEADERS, timeout=30)
            if resp.status_code != 200:
                raise RuntimeError(
                    f"Page GET failed with {_cookie_label(cookie_file)}: status={resp.status_code}"
                )

            bootstrap = extract_bootstrap(resp.text)
            if not bootstrap.get("user_id"):
                raise RuntimeError(
                    f"Could not extract userID from page HTML using {_cookie_label(cookie_file)}"
                )

            logging.info(
                f"[Scraper] [{job_id}] Cookie {_cookie_label(cookie_file)} ready — "
                f"query_id={bootstrap.get('query_id')} user_id={bootstrap.get('user_id')}"
            )
            return cookie_index, session, bootstrap, cookie_file
        except Exception as exc:
            session.close()
            last_error = str(exc)
            logging.warning(
                f"[Scraper] [{job_id}] Cookie {_cookie_label(cookie_file)} unusable during {reason}: {exc}"
            )

    raise RecoverableScrapePause(
        f"All assigned cookies were exhausted during {reason}. Last error: {last_error or 'unknown'}"
    )


# ──────────────────────────────────────────────────────────────────────────────
# Bootstrap extraction
# ──────────────────────────────────────────────────────────────────────────────

def _first_match(html: str, *patterns: str):
    for pat in patterns:
        m = re.search(pat, html)
        if m:
            return m.group(1)
    return None


def extract_bootstrap(html: str) -> dict:
    result = {}

    m = re.search(
        r'ProfileCometTimelineFeedQuery[^\]]*?"queryID"\s*:\s*"(\d+)"', html
    )
    if m:
        result["query_id"] = m.group(1)
    else:
        m2 = re.search(
            r'ProfileCometTimelineFeedQuery[^\]]*?"doc_id"\s*:\s*"(\d+)"', html
        )
        if m2:
            result["query_id"] = m2.group(1)

    if result.get("query_id"):
        idx = html.find(result["query_id"])
        if idx != -1:
            sub = html[idx:]
            var_start = sub.find('"variables":')
            if var_start != -1:
                brace = sub.find("{", var_start)
                depth, i = 0, brace
                while i < len(sub):
                    if sub[i] == "{":
                        depth += 1
                    elif sub[i] == "}":
                        depth -= 1
                        if depth == 0:
                            break
                    i += 1
                try:
                    result["variables"] = json.loads(sub[brace: i + 1])
                    result["user_id"] = result["variables"].get("userID")
                except Exception:
                    pass

    result["lsd"] = _first_match(
        html,
        r'"LSD",\[\],\{"token":"([^"]+)"',
        r'"lsd"\s*:\s*"([^"]+)"',
    )
    result["fb_dtsg"] = _first_match(
        html,
        r'"DTSGInitialData",\[\],\{"token":"([^"]+)"',
        r'"DTSGInitData",\[\],\{"token":"([^"]+)"',
        r'"fb_dtsg"\s*:\s*"([^"]+)"',
    )

    if not result.get("user_id"):
        result["user_id"] = _first_match(
            html,
            r'"actorID"\s*:\s*"(\d{10,})"',
            r'"pageID"\s*:\s*"(\d{10,})"',
            r'"profile_id"\s*:\s*"(\d{10,})"',
        )

    # Prefer og:title when available; plain <title> is often just "Facebook".
    og_title_match = re.search(
        r'<meta[^>]+property="og:title"[^>]+content="([^"]+)"', html
    ) or re.search(
        r'<meta[^>]+content="([^"]+)"[^>]+property="og:title"', html
    )
    if og_title_match:
        result["page_name"] = og_title_match.group(1).strip()
    else:
        title_match = re.search(r"<title[^>]*>([^<]+)</title>", html)
        if title_match:
            result["page_name"] = title_match.group(1).strip().split("|")[0].strip()

    return result


def _preferred_scrape_url(url: str) -> str:
    parsed = urlsplit(url)
    if "facebook.com" not in parsed.netloc:
        return url

    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query.setdefault("sk", "posts")
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query), parsed.fragment))


def _merge_bootstrap(bootstrap: dict, fresh: dict) -> dict:
    merged = dict(bootstrap)
    for key in ("query_id", "variables", "user_id", "lsd", "fb_dtsg", "page_name"):
        value = fresh.get(key)
        if value:
            merged[key] = value
    return merged


def _refresh_bootstrap(session: requests.Session, target_page: str, bootstrap: dict) -> dict:
    rb = session.get(target_page, headers=GET_HEADERS, timeout=30)
    if rb.status_code != 200:
        raise RuntimeError(f"Rebootstrap GET failed: status={rb.status_code}")

    fresh = extract_bootstrap(rb.text)
    if not fresh.get("query_id") or not fresh.get("user_id"):
        raise RuntimeError("Rebootstrap did not return a valid query/user id")

    return _merge_bootstrap(bootstrap, fresh)


# ──────────────────────────────────────────────────────────────────────────────
# GraphQL POST
# ──────────────────────────────────────────────────────────────────────────────

def _merge_timeline_chunks(chunks: list) -> dict:
    main = chunks[0]
    d = main.get("data") or {}
    tl_parent_key = "node" if "node" in d else "user"
    tl_parent = d.get(tl_parent_key) or {}
    tl = tl_parent.get("timeline_list_feed_units")

    if not isinstance(tl, dict):
        return main

    if "edges" not in tl:
        tl["edges"] = []

    for chunk in chunks[1:]:
        path = chunk.get("path") or []
        chunk_data = chunk.get("data") or {}
        if (
            len(path) == 4
            and path[0] in ("node", "user")
            and path[1] == "timeline_list_feed_units"
            and path[2] == "edges"
            and isinstance(path[3], int)
        ):
            edge_idx = path[3]
            while len(tl["edges"]) <= edge_idx:
                tl["edges"].append({})
            tl["edges"][edge_idx] = chunk_data
        elif (
            len(path) == 2
            and path[0] in ("node", "user")
            and path[1] == "timeline_list_feed_units"
        ):
            pi = chunk_data.get("page_info")
            if pi:
                tl["page_info"] = pi

    return main


def _execute_timeline_query(
    session: requests.Session,
    bootstrap: dict,
    target_page: str,
    doc_id: str,
    variables: dict,
    friendly_name: str,
) -> dict:
    payload = {
        "doc_id": doc_id,
        "variables": json.dumps(variables, separators=(",", ":")),
        "server_timestamps": "true",
        "lsd": bootstrap.get("lsd", ""),
        "fb_dtsg": bootstrap.get("fb_dtsg", ""),
        "fb_api_req_friendly_name": friendly_name,
    }

    post_headers = {
        "User-Agent": GET_HEADERS["User-Agent"],
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": "https://www.facebook.com",
        "Referer": target_page,
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "X-FB-Friendly-Name": friendly_name,
        "X-FB-LSD": bootstrap.get("lsd", ""),
    }

    r = session.post(
        "https://www.facebook.com/api/graphql/",
        headers=post_headers,
        data=payload,
        timeout=30,
    )

    if r.status_code != 200:
        logging.warning(
            f"[Scraper] GQL POST failed for {friendly_name}: status={r.status_code}"
        )
        return {}

    chunks = []
    for raw in r.text.splitlines():
        raw = raw.strip()
        if not raw:
            continue
        try:
            chunks.append(json.loads(raw))
        except Exception:
            continue

    if not chunks:
        return {}

    return _merge_timeline_chunks(chunks)


def _build_primary_query(bootstrap: dict, cursor=None):
    query_id = bootstrap.get("query_id")
    if not query_id:
        return None

    variables = dict(bootstrap.get("variables", {}))
    user_id = bootstrap.get("user_id", "")
    if user_id and "userID" not in variables:
        variables["userID"] = user_id
    variables.pop("id", None)
    variables["count"] = POSTS_PER_PAGE
    if cursor:
        variables["cursor"] = cursor
    else:
        variables.pop("cursor", None)

    return query_id, variables, "ProfileCometTimelineFeedQuery"


def _build_refetch_fallback_query(bootstrap: dict, cursor=None):
    variables = dict(bootstrap.get("variables", {}))
    user_id = bootstrap.get("user_id", "")
    variables.pop("userID", None)
    if user_id:
        variables["id"] = user_id
    variables["count"] = POSTS_PER_PAGE
    if cursor:
        variables["cursor"] = cursor
    else:
        variables.pop("cursor", None)

    return REFETCH_QUERY_ID, variables, "ProfileCometTimelineFeedRefetchQuery"


def _has_timeline_payload(data: dict) -> bool:
    timeline = _get_timeline(data)
    return bool(timeline.get("edges") or timeline.get("page_info"))


def gql_post(session: requests.Session, bootstrap: dict, target_page: str, cursor=None) -> dict:
    # The bootstrap feed query is reliable for the first page, but on some pages
    # its cursor pagination loops the same feed units. Switch to the refetch
    # query once a cursor exists, and keep the other query as a fallback.
    if cursor:
        query_builders = [
            (_build_refetch_fallback_query, "Refetch timeline query returned no timeline edges; falling back to primary feed query"),
            (_build_primary_query, "Primary timeline query returned no timeline edges after refetch fallback"),
        ]
    else:
        query_builders = [
            (_build_primary_query, "Primary timeline query returned no timeline edges; falling back to refetch query"),
            (_build_refetch_fallback_query, "Refetch timeline query returned no timeline edges after primary fallback"),
        ]

    for build_query, empty_message in query_builders:
        query = build_query(bootstrap, cursor=cursor)
        if not query:
            continue
        data = _execute_timeline_query(session, bootstrap, target_page, *query)
        if _has_timeline_payload(data):
            return data
        logging.info(f"[Scraper] {empty_message}")

    return {}


# ──────────────────────────────────────────────────────────────────────────────
# Parsing helpers
# ──────────────────────────────────────────────────────────────────────────────

def _find_first(obj, *keys):
    if isinstance(obj, dict):
        for k in keys:
            if k in obj:
                return obj[k]
        for v in obj.values():
            r = _find_first(v, *keys)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for item in obj:
            r = _find_first(item, *keys)
            if r is not None:
                return r
    return None


def _get_timeline(data: dict) -> dict:
    d = data.get("data") or {}
    feed = d.get("node") or d.get("user") or {}
    return feed.get("timeline_list_feed_units") or {}


def _is_generic_page_name(name: str) -> bool:
    normalized = (name or "").strip().lower()
    return normalized in {"", "facebook", "meta"}


def extract_page_name_from_feed(data: dict, page_id: str = None):
    expected_page_id = str(page_id or "")
    candidates = []

    for edge in _get_timeline(data).get("edges", []):
        node = edge.get("node") or {}
        title_story = ((node.get("comet_sections") or {}).get("title") or {}).get("story") or {}
        for actor in title_story.get("actors") or []:
            actor_id = str(actor.get("id") or "")
            actor_name = (actor.get("name") or "").strip()
            if not actor_name or _is_generic_page_name(actor_name):
                continue
            if expected_page_id and actor_id == expected_page_id:
                return actor_name
            candidates.append(actor_name)

    if candidates:
        return candidates[0]

    fallback_name = (_find_first(data, "name") or "").strip()
    if fallback_name and not _is_generic_page_name(fallback_name):
        return fallback_name

    fallback_short_name = (_find_first(data, "short_name") or "").strip()
    if fallback_short_name and not _is_generic_page_name(fallback_short_name):
        return fallback_short_name

    return None


def extract_pagination(data: dict):
    tl = _get_timeline(data)
    pi = tl.get("page_info")
    if isinstance(pi, dict):
        return pi.get("end_cursor"), bool(pi.get("has_next_page", False))
    pi2 = _find_first(data, "page_info")
    if isinstance(pi2, dict):
        return pi2.get("end_cursor"), bool(pi2.get("has_next_page", False))
    return None, False


def parse_posts(data: dict, page_url: str) -> list:
    out = []
    edges = _get_timeline(data).get("edges", [])

    for edge in edges:
        node = edge.get("node") or {}
        post_id = str(node.get("post_id", ""))

        post_link = (node.get("permalink_url") or "").replace("\\/", "/").rstrip("/")
        if not post_link and post_id:
            slug = page_url.rstrip("/").split("/")[-1]
            post_link = f"https://www.facebook.com/{slug}/posts/{post_id}"

        ts = (
            (node.get("comet_sections") or {})
            .get("timestamp", {})
            .get("story", {})
            .get("creation_time")
        )
        if not ts:
            ts = _find_first(node, "creation_time")

        date_str = (
            datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S UTC")
            if ts else "Unknown"
        )

        content_story = (
            (node.get("comet_sections") or {})
            .get("content", {})
            .get("story", {})
        )
        msg = content_story.get("message") or _find_first(node, "message")
        description = (msg or {}).get("text") if isinstance(msg, dict) else None

        has_video = False
        has_image = False
        image_links = []
        video_url = ""

        all_attachments = list(node.get("attachments") or [])
        comet_atts = content_story.get("attachments") or []
        all_attachments.extend(comet_atts)

        for att in all_attachments:
            media = (att.get("styles") or {}).get("attachment", {}).get("media") or {}
            typename = media.get("__typename", "")

            if typename == "Video":
                has_video = True
                raw = (media.get("permalink_url") or media.get("url") or "").replace("\\/", "/")
                if raw and not video_url:
                    video_url = raw.rstrip("/")
                    if not post_link or post_link == page_url.rstrip("/"):
                        post_link = video_url

            elif typename == "Photo":
                has_image = True
                img = media.get("image") or {}
                uri = img.get("uri") or img.get("src") or ""
                if uri and "fbcdn" in uri and uri not in image_links:
                    image_links.append(uri)

        if not image_links:
            all_uris = _find_first(node, "uri")
            if isinstance(all_uris, str) and "fbcdn" in all_uris:
                # Skip profile/avatar thumbnails — they appear inside mentions/shares
                # and are NOT actual post images. Profile pics always use /t39.30808-1/.
                if "/t39.30808-1/" not in all_uris:
                    image_links.append(all_uris)

        if not has_video and not has_image:
            has_image = bool(image_links)

        out.append({
            "post_link": post_link,
            "published_date": date_str,
            "published_timestamp": int(ts) if ts else None,
            "description": description or "N/A",
            "has_video": has_video,
            "has_image": has_image,
            "image_links": image_links,
            "video_url": video_url,
        })

    return out


# ──────────────────────────────────────────────────────────────────────────────
# Date filter helpers
# ──────────────────────────────────────────────────────────────────────────────

def _ts_from_date(d: date, end_of_day=False) -> int:
    """Convert a date object to Unix timestamp (start or end of day, UTC)."""
    if end_of_day:
        return int(datetime(d.year, d.month, d.day, 23, 59, 59).timestamp())
    return int(datetime(d.year, d.month, d.day, 0, 0, 0).timestamp())


def _load_good_checkpoints(raw_value) -> list:
    if not raw_value:
        return []
    if isinstance(raw_value, str):
        try:
            raw_value = json.loads(raw_value)
        except Exception:
            return []
    if not isinstance(raw_value, list):
        return []

    checkpoints = []
    for item in raw_value:
        if not isinstance(item, dict):
            continue
        page_num = int(item.get('resume_page_num') or 0)
        if page_num <= 0:
            continue
        checkpoints.append({
            'resume_cursor': item.get('resume_cursor'),
            'resume_page_num': page_num,
            'resume_skip_posts': int(item.get('resume_skip_posts') or 0),
            'total_posts_scraped': int(item.get('total_posts_scraped') or 0),
        })
    return checkpoints[-GOOD_CHECKPOINT_HISTORY_LIMIT:]


def _checkpoint_signature(checkpoint: dict):
    return (
        int(checkpoint.get('resume_page_num') or 0),
        int(checkpoint.get('resume_skip_posts') or 0),
        checkpoint.get('resume_cursor') or '',
    )


def _remember_good_checkpoint(history: list, checkpoint: dict) -> list:
    signature = _checkpoint_signature(checkpoint)
    history = [item for item in history if _checkpoint_signature(item) != signature]
    history.append({
        'resume_cursor': checkpoint.get('resume_cursor'),
        'resume_page_num': int(checkpoint.get('resume_page_num') or 0),
        'resume_skip_posts': int(checkpoint.get('resume_skip_posts') or 0),
        'total_posts_scraped': int(checkpoint.get('total_posts_scraped') or 0),
    })
    return history[-GOOD_CHECKPOINT_HISTORY_LIMIT:]


def _pick_rollback_checkpoint(history: list, attempted: set, current_page_num: int):
    for checkpoint in reversed(history):
        if int(checkpoint.get('resume_page_num') or 0) > int(current_page_num or 0):
            continue
        signature = _checkpoint_signature(checkpoint)
        if signature in attempted:
            continue
        attempted.add(signature)
        return checkpoint
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Main scraping entry point
# ──────────────────────────────────────────────────────────────────────────────

def run_scraper(job: dict, worker_name: str = None):
    """
    Execute a full scrape for the given job dict.
    This function is blocking and should be called from a thread-pool executor.
    """
    job_id = job["job_id"]
    source_page = job["facebook_url"]
    normalized_source = (source_page or '').lower()
    if 'instagram.com' in normalized_source:
        return run_instagram_scraper(job, worker_name=worker_name)
    if 'tiktok.com' in normalized_source:
        return run_tiktok_scraper(job, worker_name=worker_name)
    target_page = source_page
    date_from = job.get("date_from")
    date_to = job.get("date_to")
    max_posts = job.get("max_posts")

    ts_from = _ts_from_date(date_from, end_of_day=False) if date_from else None
    ts_to = _ts_from_date(date_to, end_of_day=True) if date_to else None

    total_saved = get_job_post_count(job_id)
    resume_page_num = int(job.get("scrape_resume_page_num") or 0)
    exact_resume = resume_page_num > 0
    legacy_resume = total_saved > 0 and not exact_resume
    next_page_num = resume_page_num or 1
    next_request_cursor = job.get("scrape_resume_cursor") if exact_resume else None
    skip_posts_for_page = int(job.get("scrape_resume_skip_posts") or 0) if exact_resume else 0
    good_checkpoints = _load_good_checkpoints(job.get("scrape_good_checkpoints"))
    worker_token = job.get("active_worker_token")
    cookie_pool = _worker_cookie_pool(worker_name)
    cookie_index = 0
    cookie_file = None
    session = None

    logging.info(
        f"[Scraper] [{job_id}] Starting — url={source_page} scrape_url={target_page} "
        f"worker={worker_name or 'n/a'} cookies={[_cookie_label(path) for path in cookie_pool]}"
    )

    try:
        update_job_status(job_id, "scraping")

        cookie_index, session, bootstrap, cookie_file = _open_cookie_context(
            cookie_pool,
            0,
            target_page,
            job_id,
            "initial bootstrap",
            worker_name=worker_name,
        )

        page_name = bootstrap.get("page_name") or source_page
        page_id = bootstrap.get("user_id")
        update_job_progress(job_id, page_name=page_name, page_id=page_id, total_posts_scraped=total_saved)

        logging.info(
            f"[Scraper] [{job_id}] Bootstrap OK — "
            f"page_id={page_id} page_name={page_name!r} cookie={_cookie_label(cookie_file)}"
        )
        if exact_resume:
            logging.info(
                f"[Scraper] [{job_id}] Exact resume — "
                f"page={next_page_num:04d} skip={skip_posts_for_page} total={total_saved}"
            )
        elif legacy_resume:
            logging.warning(
                f"[Scraper] [{job_id}] Legacy resume — no saved cursor; "
                f"replaying from start until the saved frontier (total={total_saved})"
            )

        stale_pages = 0
        cycle_rebootstrap_retries = 0
        rollback_attempted_checkpoints = set()
        new_saved_this_run = 0

        def rotate_cookie(reason: str, resume_cursor, resume_page_num: int, resume_skip_posts: int = 0):
            nonlocal cookie_index, session, bootstrap, cookie_file, page_name, page_id
            nonlocal stale_pages, cycle_rebootstrap_retries, next_page_num, next_request_cursor, skip_posts_for_page

            next_cookie_index = cookie_index + 1
            if next_cookie_index >= len(cookie_pool):
                raise RecoverableScrapePause(
                    f"{reason}; all assigned cookies are exhausted at page {int(resume_page_num or 0)}"
                )

            if session is not None:
                try:
                    session.close()
                except Exception:
                    pass

            rotate_scrape_proxy(worker_name or job_id)
            cookie_index, session, bootstrap, cookie_file = _open_cookie_context(
                cookie_pool,
                next_cookie_index,
                target_page,
                job_id,
                reason,
                worker_name=worker_name,
            )
            page_name = bootstrap.get("page_name") or page_name
            page_id = bootstrap.get("user_id") or page_id
            update_job_progress(
                job_id,
                page_name=page_name,
                page_id=page_id,
                total_posts_scraped=total_saved,
            )
            next_page_num = max(int(resume_page_num or 1), 1)
            next_request_cursor = resume_cursor
            skip_posts_for_page = max(int(resume_skip_posts or 0), 0)
            update_job_scrape_checkpoint(
                job_id,
                next_request_cursor,
                next_page_num,
                skip_posts_for_page,
                total_saved,
            )
            stale_pages = 0
            cycle_rebootstrap_retries = 0
            rollback_attempted_checkpoints.clear()

            logging.info(
                f"[Scraper] [{job_id}] Switched to cookie {_cookie_label(cookie_file)} "
                f"at page={next_page_num:04d} after {reason}"
            )

        while True:
            action = apply_job_control_action(job_id, "scraping", worker_token=worker_token)
            if action:
                logging.info(f"[Scraper] [{job_id}] Control action applied before page fetch: {action}")
                return

            current_page_num = max(next_page_num, 1)
            request_cursor = next_request_cursor
            page_skip_posts = max(skip_posts_for_page, 0)
            skip_posts_for_page = 0

            if current_page_num > 1 and current_page_num % REST_EVERY == 0:
                logging.info(f"[Scraper] [{job_id}] Resting {REST_DURATION}s ...")
                time.sleep(REST_DURATION)

            if current_page_num > 1 and current_page_num % REBOOTSTRAP_EVERY == 0:
                try:
                    rb = session.get(target_page, headers=GET_HEADERS, timeout=30)
                    if rb.status_code == 200:
                        bootstrap = _refresh_bootstrap(session, target_page, bootstrap)
                except Exception as exc:
                    logging.warning(f"[Scraper] [{job_id}] Rebootstrap failed: {exc}")
                    rotate_cookie(f"periodic rebootstrap failure: {exc}", request_cursor, current_page_num, page_skip_posts)
                    time.sleep(REQUEST_DELAY)
                    continue

            update_job_scrape_checkpoint(
                job_id,
                request_cursor,
                current_page_num,
                page_skip_posts,
                total_saved,
            )

            data = None
            for _gql_attempt in range(1, 4):
                try:
                    data = gql_post(session, bootstrap, target_page, cursor=request_cursor)
                    break
                except Exception as gql_exc:
                    logging.warning(
                        f"[Scraper] [{job_id}] GraphQL request failed attempt {_gql_attempt}/3: {gql_exc}"
                    )
                    if _gql_attempt < 3:
                        time.sleep(REQUEST_DELAY * 2)
                        rotate_cookie(f"GraphQL error: {gql_exc}", request_cursor, current_page_num, page_skip_posts)
            if not data:
                rotate_cookie("empty GraphQL response", request_cursor, current_page_num, page_skip_posts)
                time.sleep(REQUEST_DELAY)
                continue

            if _is_generic_page_name(page_name):
                resolved_page_name = extract_page_name_from_feed(data, page_id)
                if resolved_page_name and resolved_page_name != page_name:
                    page_name = resolved_page_name
                    update_job_progress(job_id, page_name=page_name)
                    logging.info(f"[Scraper] [{job_id}] Resolved page name from feed: {page_name!r}")

            posts = parse_posts(data, source_page)
            next_request_cursor, has_more = extract_pagination(data)
            if page_skip_posts > len(posts):
                logging.warning(
                    f"[Scraper] [{job_id}] Stored skip offset {page_skip_posts} exceeds page size {len(posts)}; clamping"
                )
                page_skip_posts = len(posts)

            page_saved = 0
            stop_reason = None

            for idx, post in enumerate(posts):
                action = apply_job_control_action(job_id, "scraping", worker_token=worker_token)
                if action:
                    logging.info(f"[Scraper] [{job_id}] Control action applied during page processing: {action}")
                    return

                if idx < page_skip_posts:
                    continue

                ts = post.get("published_timestamp")

                if ts_to and ts and ts > ts_to:
                    update_job_scrape_checkpoint(job_id, request_cursor, current_page_num, idx + 1, total_saved)
                    continue

                if ts_from and ts and ts < ts_from:
                    stop_reason = "date_from_reached"
                    break

                if save_post(job_id, post):
                    total_saved += 1
                    page_saved += 1
                    new_saved_this_run += 1

                update_job_scrape_checkpoint(job_id, request_cursor, current_page_num, idx + 1, total_saved)

                if max_posts and total_saved >= max_posts:
                    stop_reason = "max_posts_reached"
                    break

            update_job_progress(job_id, total_posts_scraped=total_saved)

            if page_saved > 0:
                productive_checkpoint = {
                    'resume_cursor': request_cursor,
                    'resume_page_num': current_page_num,
                    'resume_skip_posts': page_skip_posts,
                    'total_posts_scraped': total_saved,
                }
                good_checkpoints = _remember_good_checkpoint(good_checkpoints, productive_checkpoint)
                record_job_good_checkpoint(
                    job_id,
                    request_cursor,
                    current_page_num,
                    page_skip_posts,
                    total_saved,
                )

            next_page_num = current_page_num + 1
            update_job_scrape_checkpoint(job_id, next_request_cursor, next_page_num, 0, total_saved)

            if legacy_resume and new_saved_this_run == 0:
                stale_pages = 0
                cycle_rebootstrap_retries = 0
                rollback_attempted_checkpoints.clear()
            elif page_saved > 0:
                stale_pages = 0
                cycle_rebootstrap_retries = 0
                rollback_attempted_checkpoints.clear()
            else:
                stale_pages += 1

            logging.info(
                f"[Scraper] [{job_id}] page={current_page_num:04d} "
                f"posts={len(posts)} new={page_saved} total={total_saved}"
            )

            if stop_reason:
                logging.info(f"[Scraper] [{job_id}] Stop: {stop_reason}")
                break

            if stale_pages >= MAX_STALE_PAGES:
                rollback_checkpoint = None
                if cycle_rebootstrap_retries < MAX_CYCLE_REBOOTSTRAP_RETRIES:
                    rollback_checkpoint = _pick_rollback_checkpoint(
                        good_checkpoints,
                        rollback_attempted_checkpoints,
                        current_page_num,
                    )

                if rollback_checkpoint:
                    logging.info(
                        f"[Scraper] [{job_id}] Feed cycling detected — rollback to good checkpoint "
                        f"page={int(rollback_checkpoint.get('resume_page_num') or 0):04d} "
                        f"attempt {cycle_rebootstrap_retries + 1}/{MAX_CYCLE_REBOOTSTRAP_RETRIES}"
                    )
                    try:
                        bootstrap = _refresh_bootstrap(session, target_page, bootstrap)
                    except Exception as exc:
                        logging.warning(f"[Scraper] [{job_id}] Rebootstrap after rollback failed: {exc}")
                        rotate_cookie(
                            f"rollback rebootstrap failure: {exc}",
                            rollback_checkpoint.get('resume_cursor'),
                            int(rollback_checkpoint.get('resume_page_num') or current_page_num),
                            int(rollback_checkpoint.get('resume_skip_posts') or 0),
                        )
                        time.sleep(REQUEST_DELAY)
                        continue

                    cycle_rebootstrap_retries += 1
                    stale_pages = 0
                    next_page_num = int(rollback_checkpoint.get('resume_page_num') or current_page_num)
                    next_request_cursor = rollback_checkpoint.get('resume_cursor')
                    skip_posts_for_page = int(rollback_checkpoint.get('resume_skip_posts') or 0)
                    update_job_scrape_checkpoint(
                        job_id,
                        next_request_cursor,
                        next_page_num,
                        skip_posts_for_page,
                        total_saved,
                    )
                    time.sleep(REQUEST_DELAY)
                    continue

                if request_cursor and cycle_rebootstrap_retries < MAX_CYCLE_REBOOTSTRAP_RETRIES:
                    logging.info(
                        f"[Scraper] [{job_id}] Feed cycling detected — rebootstrap retry "
                        f"{cycle_rebootstrap_retries + 1}/{MAX_CYCLE_REBOOTSTRAP_RETRIES}"
                    )
                    try:
                        bootstrap = _refresh_bootstrap(session, target_page, bootstrap)
                    except Exception as exc:
                        logging.warning(f"[Scraper] [{job_id}] Rebootstrap after feed cycle failed: {exc}")
                        rotate_cookie(f"feed-cycle rebootstrap failure: {exc}", request_cursor, current_page_num, 0)
                        time.sleep(REQUEST_DELAY)
                        continue

                    cycle_rebootstrap_retries += 1
                    stale_pages = 0
                    next_page_num = current_page_num
                    next_request_cursor = request_cursor
                    update_job_scrape_checkpoint(job_id, request_cursor, current_page_num, 0, total_saved)
                    time.sleep(REQUEST_DELAY)
                    continue

                if cookie_index + 1 < len(cookie_pool):
                    rotate_cookie("feed cycling detected", request_cursor, current_page_num, 0)
                    time.sleep(REQUEST_DELAY)
                    continue

                logging.info(f"[Scraper] [{job_id}] Feed cycling detected — stopping")
                break

            if not has_more or not next_request_cursor:
                if ts_from is not None and cookie_index + 1 < len(cookie_pool):
                    logging.info(
                        f"[Scraper] [{job_id}] Feed exhausted before date_from boundary — trying next cookie"
                    )
                    rotate_cookie("feed exhausted before date_from boundary", request_cursor, current_page_num, 0)
                    time.sleep(REQUEST_DELAY)
                    continue
                logging.info(f"[Scraper] [{job_id}] Feed exhausted")
                break

            time.sleep(REQUEST_DELAY)

        action = apply_job_control_action(job_id, "scraping", worker_token=worker_token)
        if action:
            logging.info(f"[Scraper] [{job_id}] Control action applied at scrape completion boundary: {action}")
            return

        logging.info(f"[Scraper] [{job_id}] Done — {total_saved} posts saved")
        update_job_status(job_id, "downloading_content", clear_scrape_checkpoint=True)

    except RecoverableScrapePause as exc:
        logging.warning(f"[Scraper] [{job_id}] PAUSED: {exc}")
        update_job_status(job_id, "paused", error_message=str(exc), extra={"resume_stage": "scraping"})
    except Exception as exc:
        logging.error(f"[Scraper] [{job_id}] FAILED: {exc}", exc_info=True)
        update_job_status(job_id, "failed", error_message=str(exc), extra={"resume_stage": "scraping"})
    finally:
        if session is not None:
            try:
                session.close()
            except Exception:
                pass
