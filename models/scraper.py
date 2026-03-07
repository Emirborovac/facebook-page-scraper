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

import requests
from dotenv import load_dotenv

from models.operations import (
    save_post,
    update_job_status,
    update_job_progress,
)

load_dotenv()

COOKIES_FILE = os.getenv("COOKIES_FILE", "./cookies.txt")

# Facebook internal query ID for the scroll-triggered pagination query.
# This ID is embedded in Facebook's JS bundle and may change after major deploys.
REFETCH_QUERY_ID = "25997057593319516"

POSTS_PER_PAGE = 5
REQUEST_DELAY = 2.0
MAX_STALE_PAGES = 5
REBOOTSTRAP_EVERY = 150
REST_EVERY = 100
REST_DURATION = 20

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

def load_cookies(session: requests.Session) -> int:
    path = Path(COOKIES_FILE)
    if not path.exists():
        logging.warning(f"[Scraper] Cookies file not found: {COOKIES_FILE}")
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

    # Extract page name from <title> or og:title
    title_match = re.search(r"<title[^>]*>([^<]+)</title>", html)
    if title_match:
        result["page_name"] = title_match.group(1).strip().split("|")[0].strip()

    return result


# ──────────────────────────────────────────────────────────────────────────────
# GraphQL POST
# ──────────────────────────────────────────────────────────────────────────────

def gql_post(session: requests.Session, bootstrap: dict, target_page: str, cursor=None) -> dict:
    lsd = bootstrap.get("lsd", "")
    fb_dtsg = bootstrap.get("fb_dtsg", "")
    user_id = bootstrap.get("user_id", "")

    variables = dict(bootstrap.get("variables", {}))
    variables.pop("userID", None)
    variables["id"] = user_id
    variables["count"] = POSTS_PER_PAGE
    if cursor:
        variables["cursor"] = cursor
    else:
        variables.pop("cursor", None)

    payload = {
        "doc_id": REFETCH_QUERY_ID,
        "variables": json.dumps(variables, separators=(",", ":")),
        "server_timestamps": "true",
        "lsd": lsd,
        "fb_dtsg": fb_dtsg,
        "fb_api_req_friendly_name": "ProfileCometTimelineFeedRefetchQuery",
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
        "X-FB-Friendly-Name": "ProfileCometTimelineFeedRefetchQuery",
        "X-FB-LSD": lsd,
    }

    r = session.post(
        "https://www.facebook.com/api/graphql/",
        headers=post_headers,
        data=payload,
        timeout=30,
    )

    if r.status_code != 200:
        logging.warning(f"[Scraper] GQL POST failed: status={r.status_code}")
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


# ──────────────────────────────────────────────────────────────────────────────
# Main scraping entry point
# ──────────────────────────────────────────────────────────────────────────────

def run_scraper(job: dict):
    """
    Execute a full scrape for the given job dict.
    This function is blocking and should be called from a thread-pool executor.
    """
    job_id = job["job_id"]
    target_page = job["facebook_url"]
    date_from = job.get("date_from")   # datetime.date or None
    date_to = job.get("date_to")       # datetime.date or None
    max_posts = job.get("max_posts")   # int or None

    ts_from = _ts_from_date(date_from, end_of_day=False) if date_from else None
    ts_to = _ts_from_date(date_to, end_of_day=True) if date_to else None

    logging.info(f"[Scraper] [{job_id}] Starting — url={target_page}")

    try:
        update_job_status(job_id, "scraping")

        session = requests.Session()
        n_cookies = load_cookies(session)
        if n_cookies == 0:
            raise RuntimeError("No Facebook cookies loaded — check cookies.txt")

        # Bootstrap
        resp = session.get(target_page, headers=GET_HEADERS, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f"Page GET failed: status={resp.status_code}")

        bootstrap = extract_bootstrap(resp.text)

        if not bootstrap.get("query_id"):
            raise RuntimeError("Could not extract queryID from page HTML")
        if not bootstrap.get("user_id"):
            raise RuntimeError("Could not extract userID from page HTML")

        page_name = bootstrap.get("page_name") or target_page
        page_id = bootstrap.get("user_id")
        update_job_progress(job_id, page_name=page_name, page_id=page_id)

        logging.info(
            f"[Scraper] [{job_id}] Bootstrap OK — "
            f"page_id={page_id} page_name={page_name!r}"
        )

        cursor = None
        page_num = 0
        total_saved = 0
        stale_pages = 0
        new_saved_this_run = 0
        last_productive_cursor = None

        while True:
            page_num += 1

            if page_num > 1 and page_num % REST_EVERY == 0:
                logging.info(f"[Scraper] [{job_id}] Resting {REST_DURATION}s ...")
                time.sleep(REST_DURATION)

            if page_num > 1 and page_num % REBOOTSTRAP_EVERY == 0:
                try:
                    rb = session.get(target_page, headers=GET_HEADERS, timeout=30)
                    if rb.status_code == 200:
                        fresh = extract_bootstrap(rb.text)
                        if fresh.get("lsd") and fresh.get("fb_dtsg"):
                            bootstrap["lsd"] = fresh["lsd"]
                            bootstrap["fb_dtsg"] = fresh["fb_dtsg"]
                except Exception as e:
                    logging.warning(f"[Scraper] [{job_id}] Rebootstrap failed: {e}")

            data = gql_post(session, bootstrap, target_page, cursor=cursor)
            if not data:
                logging.warning(f"[Scraper] [{job_id}] Empty response — stopping")
                break

            posts = parse_posts(data, target_page)
            cursor, has_more = extract_pagination(data)

            page_saved = 0
            stop_reason = None

            for post in posts:
                ts = post.get("published_timestamp")

                # Date ceiling: skip posts newer than date_to
                if ts_to and ts and ts > ts_to:
                    continue

                # Date floor: stop when posts are older than date_from
                if ts_from and ts and ts < ts_from:
                    stop_reason = "date_from_reached"
                    break

                if save_post(job_id, post):
                    total_saved += 1
                    page_saved += 1
                    new_saved_this_run += 1

                if max_posts and total_saved >= max_posts:
                    stop_reason = "max_posts_reached"
                    break

            update_job_progress(job_id, total_posts_scraped=total_saved)

            if new_saved_this_run > 0:
                stale_pages = 0 if page_saved > 0 else stale_pages + 1
            if page_saved > 0:
                last_productive_cursor = cursor

            logging.info(
                f"[Scraper] [{job_id}] page={page_num:04d} "
                f"posts={len(posts)} new={page_saved} total={total_saved}"
            )

            if stop_reason:
                logging.info(f"[Scraper] [{job_id}] Stop: {stop_reason}")
                break

            if stale_pages >= MAX_STALE_PAGES:
                logging.info(f"[Scraper] [{job_id}] Feed cycling detected — stopping")
                break

            if not has_more or not cursor:
                logging.info(f"[Scraper] [{job_id}] Feed exhausted")
                break

            time.sleep(REQUEST_DELAY)

        logging.info(f"[Scraper] [{job_id}] Done — {total_saved} posts saved")
        update_job_status(job_id, "downloading_content")

    except Exception as exc:
        logging.error(f"[Scraper] [{job_id}] FAILED: {exc}", exc_info=True)
        update_job_status(job_id, "failed", error_message=str(exc))
