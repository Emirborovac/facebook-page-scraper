"""
Facebook Page Scraper — FastAPI application
"""

import csv
import io
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import date, datetime
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlparse, urlunparse

from dotenv import load_dotenv
from fastapi import FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    RedirectResponse,
    StreamingResponse,
)
from starlette.middleware.sessions import SessionMiddleware

from models.database import init_db
from models.operations import (
    continue_job as continue_job_operation,
    create_job,
    get_all_jobs,
    get_all_posts_deduped,
    get_all_posts_for_job,
    get_all_posts_summary,
    get_job,
    get_posts_for_all_jobs,
    get_posts_for_job,
    get_global_stats,
    mark_job_completed,
    stream_all_s3_content,
    prepare_jobs_for_worker_startup,
    request_job_delete,
    request_job_pause,
    request_job_stop,
    rerun_job,
    retry_failed_downloads,
    retry_single_post_download,
    start_downloads,
)
from models.queue_worker import (
    FACEBOOK_SCRAPING_WORKER_COUNT,
    TIKTOK_SCRAPING_WORKER_COUNT,
    INSTAGRAM_SCRAPING_WORKER_COUNT,
    SCRAPING_WORKER_COUNT,
    download_worker,
    scraping_worker,
    shutdown_worker_executors,
)

load_dotenv()
logging.basicConfig(level=logging.INFO)

ADMIN_USERNAME = os.getenv("FB_ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("FB_ADMIN_PASSWORD", "password")
SESSION_SECRET = os.getenv("SESSION_SECRET_KEY", "change_me_in_production")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./downloads")


# ──────────────────────────────────────────────────────────────────────────────
# Lifespan
# ──────────────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    startup_state = prepare_jobs_for_worker_startup({
        "facebook": FACEBOOK_SCRAPING_WORKER_COUNT,
        "tiktok": TIKTOK_SCRAPING_WORKER_COUNT,
        "instagram": INSTAGRAM_SCRAPING_WORKER_COUNT,
    })
    logging.info(
        "[App] Startup job normalization — resumable_scraping_jobs=%s requeued_scraping_jobs=%s released_download_jobs=%s resumable_by_platform=%s",
        startup_state["resumable_scraping_jobs"],
        startup_state["requeued_scraping_jobs"],
        startup_state["released_download_jobs"],
        startup_state.get("resumable_by_platform"),
    )

    import asyncio
    scrape_tasks = [
        asyncio.create_task(scraping_worker(platform="facebook", worker_name=f"facebook-{index + 1}"))
        for index in range(FACEBOOK_SCRAPING_WORKER_COUNT)
    ] + [
        asyncio.create_task(scraping_worker(platform="tiktok", worker_name=f"tiktok-{index + 1}"))
        for index in range(TIKTOK_SCRAPING_WORKER_COUNT)
    ] + [
        asyncio.create_task(scraping_worker(platform="instagram", worker_name=f"instagram-{index + 1}"))
        for index in range(INSTAGRAM_SCRAPING_WORKER_COUNT)
    ]
    download_task = asyncio.create_task(download_worker())
    logging.info(
        f"[App] {FACEBOOK_SCRAPING_WORKER_COUNT} Facebook scraping workers, "
        f"{TIKTOK_SCRAPING_WORKER_COUNT} TikTok scraping workers, "
        f"{INSTAGRAM_SCRAPING_WORKER_COUNT} Instagram scraping workers, and 1 download worker started."
    )

    loop = asyncio.get_event_loop()
    loop.run_in_executor(None, get_all_jobs, 500, 0)
    loop.run_in_executor(None, get_global_stats)

    yield

    for task in scrape_tasks:
        task.cancel()
    download_task.cancel()
    await asyncio.gather(*scrape_tasks, download_task, return_exceptions=True)
    shutdown_worker_executors()
    logging.info("[App] Workers stopped.")


# ──────────────────────────────────────────────────────────────────────────────
# App
# ──────────────────────────────────────────────────────────────────────────────

app = FastAPI(title="Facebook Page Scraper", lifespan=lifespan)
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET)


# ──────────────────────────────────────────────────────────────────────────────
# Auth helpers
# ──────────────────────────────────────────────────────────────────────────────

def is_authenticated(request: Request) -> bool:
    return request.session.get("logged_in") is True


def require_auth(request: Request):
    if not is_authenticated(request):
        raise HTTPException(status_code=401, detail="Unauthorized")


def _read_template(name: str) -> str:
    path = os.path.join(os.path.dirname(__file__), "templates", name)
    with open(path, encoding="utf-8") as f:
        return f.read()


_FACEBOOK_ALLOWED_HOSTS = {
    "facebook.com",
    "www.facebook.com",
    "m.facebook.com",
    "mbasic.facebook.com",
    "lm.facebook.com",
    "l.facebook.com",
    "fb.watch",
    "www.fb.watch",
}

_FACEBOOK_ALLOWED_QUERY_KEYS = {
    "v",
    "story_fbid",
    "id",
    "comment_id",
    "reply_comment_id",
    "substory_index",
    "fbid",
    "set",
}

_TIKTOK_ALLOWED_HOSTS = {
    "tiktok.com",
    "www.tiktok.com",
    "m.tiktok.com",
}

_INSTAGRAM_ALLOWED_HOSTS = {
    "instagram.com",
    "www.instagram.com",
    "m.instagram.com",
}


def _is_allowed_facebook_host(host: str) -> bool:
    host = (host or "").split(":", 1)[0].lower()
    return host in _FACEBOOK_ALLOWED_HOSTS or host.endswith(".facebook.com")


def _unwrap_facebook_link(raw_url: str) -> str:
    current = raw_url
    for _ in range(3):
        parsed = urlparse(current)
        host = (parsed.netloc or "").split(":", 1)[0].lower()
        if host not in {"l.facebook.com", "lm.facebook.com"} or parsed.path != "/l.php":
            return current
        target = dict(parse_qsl(parsed.query, keep_blank_values=True)).get("u")
        if not target:
            return current
        current = unquote(target)
    return current


def _normalize_facebook_url(raw_url: str) -> str | None:
    if not raw_url:
        return None

    candidate = raw_url.strip()
    if not candidate:
        return None

    if candidate.startswith("//"):
        candidate = f"https:{candidate}"
    elif not candidate.startswith(("http://", "https://")):
        candidate = f"https://{candidate.lstrip('/')}"

    candidate = _unwrap_facebook_link(candidate)
    parsed = urlparse(candidate)
    host = (parsed.netloc or "").split(":", 1)[0].lower()

    if not _is_allowed_facebook_host(host):
        return None

    raw_path = parsed.path or "/"
    segments = [segment for segment in raw_path.split("/") if segment]
    normalized_path = f"/{'/'.join(segments)}" if segments else "/"
    query_items = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key in _FACEBOOK_ALLOWED_QUERY_KEYS
    ]

    if host.endswith("fb.watch"):
        normalized_query = urlencode(query_items, doseq=True)
        return urlunparse(("https", "fb.watch", normalized_path, "", normalized_query, ""))

    if raw_path in {"/story.php", "/permalink.php"}:
        normalized_path = "/story.php"
        query_items = [
            (key, value)
            for key, value in query_items
            if key in {"story_fbid", "id", "comment_id", "reply_comment_id", "substory_index"}
        ]
    elif raw_path == "/watch":
        query_items = [(key, value) for key, value in query_items if key == "v"]

    normalized_query = urlencode(query_items, doseq=True)
    return urlunparse(("https", "www.facebook.com", normalized_path, "", normalized_query, ""))


def _build_facebook_redirect_url(raw_url: str) -> str | None:
    normalized = _normalize_facebook_url(raw_url)
    if not normalized:
        return None
    return f"/out/facebook?url={quote(normalized, safe='')}"


def _is_allowed_tiktok_host(host: str) -> bool:
    host = (host or '').split(':', 1)[0].lower()
    return host in _TIKTOK_ALLOWED_HOSTS or host.endswith('.tiktok.com')


def _is_allowed_instagram_host(host: str) -> bool:
    host = (host or '').split(':', 1)[0].lower()
    return host in _INSTAGRAM_ALLOWED_HOSTS or host.endswith('.instagram.com')


def _detect_source_platform(raw_url: str) -> str | None:
    if not raw_url:
        return None
    candidate = raw_url.strip()
    if not candidate:
        return None
    if candidate.startswith('//'):
        candidate = f'https:{candidate}'
    elif not candidate.startswith(('http://', 'https://')):
        candidate = f'https://{candidate.lstrip('/')}'
    parsed = urlparse(candidate)
    host = (parsed.netloc or '').split(':', 1)[0].lower()
    if _is_allowed_facebook_host(host):
        return 'facebook'
    if _is_allowed_tiktok_host(host):
        return 'tiktok'
    if _is_allowed_instagram_host(host):
        return 'instagram'
    return None


def _normalize_tiktok_url(raw_url: str) -> str | None:
    if not raw_url:
        return None
    candidate = raw_url.strip()
    if not candidate:
        return None
    if candidate.startswith('//'):
        candidate = f'https:{candidate}'
    elif not candidate.startswith(('http://', 'https://')):
        candidate = f'https://{candidate.lstrip('/')}'
    parsed = urlparse(candidate)
    host = (parsed.netloc or '').split(':', 1)[0].lower()
    if not _is_allowed_tiktok_host(host):
        return None

    segments = [segment for segment in (parsed.path or '/').split('/') if segment]
    normalized_path = '/'
    if segments:
        normalized_path = f'/{segments[0]}'
        if len(segments) >= 3 and segments[0].startswith('@') and segments[1] == 'video':
            normalized_path = f'/{segments[0]}/video/{segments[2]}'
    return urlunparse(('https', 'www.tiktok.com', normalized_path, '', '', ''))


def _normalize_instagram_url(raw_url: str) -> str | None:
    if not raw_url:
        return None
    candidate = raw_url.strip()
    if not candidate:
        return None
    if candidate.startswith('//'):
        candidate = f'https:{candidate}'
    elif not candidate.startswith(('http://', 'https://')):
        candidate = f"https://{candidate.lstrip('/')}"
    parsed = urlparse(candidate)
    host = (parsed.netloc or '').split(':', 1)[0].lower()
    if not _is_allowed_instagram_host(host):
        return None

    segments = [segment for segment in (parsed.path or '/').split('/') if segment]
    if not segments:
        return 'https://www.instagram.com/'

    first = segments[0]
    if first in {'p', 'reel', 'tv'} and len(segments) >= 2:
        normalized_path = f'/{first}/{segments[1]}/'
    else:
        normalized_path = f'/{first}/'
    return urlunparse(('https', 'www.instagram.com', normalized_path, '', '', ''))


def _normalize_submit_url(raw_url: str) -> str | None:
    platform = _detect_source_platform(raw_url)
    if platform == 'facebook':
        return _normalize_facebook_url(raw_url)
    if platform == 'tiktok':
        return _normalize_tiktok_url(raw_url)
    if platform == 'instagram':
        return _normalize_instagram_url(raw_url)
    return None


def _build_external_link(raw_url: str) -> str | None:
    platform = _detect_source_platform(raw_url)
    if platform == 'facebook':
        return _build_facebook_redirect_url(raw_url)
    if platform == 'tiktok':
        return _normalize_tiktok_url(raw_url)
    if platform == 'instagram':
        return _normalize_instagram_url(raw_url)
    return raw_url or None


def _serialize_post_record(post: dict) -> dict:
    p = dict(post)
    if p.get("scraped_at") and hasattr(p["scraped_at"], "isoformat"):
        p["scraped_at"] = p["scraped_at"].isoformat()
    if p.get("job_created_at") and hasattr(p["job_created_at"], "isoformat"):
        p["job_created_at"] = p["job_created_at"].isoformat()

    for field in ("image_urls", "image_s3_urls", "image_s3_keys"):
        if p.get(field) and isinstance(p[field], str):
            try:
                p[field] = json.loads(p[field])
            except Exception:
                p[field] = []

    if not p.get("image_s3_urls"):
        p["image_s3_urls"] = p.get("image_urls") or []
    if not p.get("video_s3_url"):
        p["video_s3_url"] = None
    platform = _detect_source_platform(p.get("post_link") or p.get("facebook_url") or "")
    p["source_platform"] = platform
    p["source_category"] = (p.get("source_category") or "").strip() or None
    p["facebook_redirect_url"] = _build_facebook_redirect_url(p.get("post_link")) if platform == "facebook" else None
    p["external_redirect_url"] = _build_external_link(p.get("post_link"))
    if platform == 'tiktok':
        p["external_link_label"] = "View on TikTok"
    elif platform == 'instagram':
        p["external_link_label"] = "View on Instagram"
    elif platform == 'facebook':
        p["external_link_label"] = "View on Facebook"
    else:
        p["external_link_label"] = "View Original"
    return p


def _csv_field(val):
    if not val:
        return ""
    if isinstance(val, str):
        try:
            return " | ".join(json.loads(val))
        except Exception:
            return val
    if isinstance(val, list):
        return " | ".join(val)
    return str(val)


def _serialize_job(job: dict) -> dict:
    serialized = dict(job)
    for key in (
        "created_at",
        "started_scraping_at",
        "scraping_completed_at",
        "completed_at",
        "scrape_last_progress_at",
    ):
        if serialized.get(key) and hasattr(serialized[key], "isoformat"):
            serialized[key] = serialized[key].isoformat()
        elif serialized.get(key) and isinstance(serialized[key], str):
            pass
    for key in ("date_from", "date_to"):
        if serialized.get(key) and hasattr(serialized[key], "isoformat"):
            serialized[key] = serialized[key].isoformat()

    status = serialized.get("status")
    resume_page_num = int(serialized.get("scrape_resume_page_num") or 0)
    total_posts = int(serialized.get("total_posts_scraped") or 0)
    total_media = int(serialized.get("total_media_count") or 0)
    resume_stage = serialized.get("resume_stage") or (
        "downloading_content"
        if status == "downloading_content" or serialized.get("scraping_completed_at")
        else "scraping"
    )

    can_continue = False
    if status in {"paused", "stopped"}:
        can_continue = True
    elif status == "scraping":
        can_continue = resume_page_num > 0 or bool(total_posts)
    elif status == "failed":
        can_continue = (
            resume_stage == "downloading_content"
            or resume_page_num > 0
            or bool(total_posts)
            or bool(total_media)
        )

    if resume_stage == "downloading_content":
        continue_mode = "download" if can_continue else None
    elif resume_page_num > 0:
        continue_mode = "exact"
    elif can_continue and total_posts == 0:
        continue_mode = "fresh"
    elif can_continue:
        continue_mode = "legacy"
    else:
        continue_mode = None

    reached_ts = serialized.get("oldest_published_timestamp")
    try:
        reached_ts = int(reached_ts) if reached_ts is not None else None
    except Exception:
        reached_ts = None

    serialized["oldest_published_timestamp"] = reached_ts
    serialized["reached_date"] = datetime.utcfromtimestamp(reached_ts).isoformat() + "Z" if reached_ts else None
    serialized["source_category"] = (serialized.get("source_category") or "").strip() or None
    serialized["resume_stage"] = resume_stage
    serialized["can_continue"] = can_continue
    serialized["continue_mode"] = continue_mode
    serialized["source_platform"] = _detect_source_platform(serialized.get("facebook_url") or "")

    # Live media download breakdown from fb_posts (may differ slightly from cached job counters)
    dc = int(serialized.get("download_completed_count") or 0)
    df = int(serialized.get("download_failed_count") or 0)
    dp = int(serialized.get("download_pending_count") or 0)
    sum_status = dc + df + dp
    tm_job = int(serialized.get("total_media_count") or 0)
    serialized["media_download_total"] = max(tm_job, sum_status) if sum_status else tm_job
    serialized["download_completed_count"] = dc
    serialized["download_failed_count"] = df
    serialized["download_pending_count"] = dp

    return serialized



# ──────────────────────────────────────────────────────────────────────────────
# Auth routes
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    if is_authenticated(request):
        return RedirectResponse("/dashboard", status_code=302)
    return RedirectResponse("/login", status_code=302)


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    if is_authenticated(request):
        return RedirectResponse("/dashboard", status_code=302)
    return HTMLResponse(_read_template("login.html"))


@app.post("/login")
async def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
):
    if username == ADMIN_USERNAME and password == ADMIN_PASSWORD:
        request.session["logged_in"] = True
        request.session["username"] = username
        return RedirectResponse("/dashboard", status_code=302)
    return HTMLResponse(
        _read_template("login.html").replace(
            "<!--ERROR-->",
            '<p class="login-error">Invalid credentials. Please try again.</p>',
        )
    )


@app.post("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=302)


# ──────────────────────────────────────────────────────────────────────────────
# Dashboard
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    if not is_authenticated(request):
        return RedirectResponse("/login", status_code=302)
    return HTMLResponse(_read_template("index.html"))


@app.get("/gallery/{job_id}", response_class=HTMLResponse)
async def gallery_page(job_id: str, request: Request):
    if not is_authenticated(request):
        return RedirectResponse("/login", status_code=302)
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Operation not found")
    return HTMLResponse(_read_template("gallery.html"))


@app.get("/content", response_class=HTMLResponse)
async def all_content_page(request: Request):
    if not is_authenticated(request):
        return RedirectResponse("/login", status_code=302)
    return HTMLResponse(_read_template("all_content.html"))


@app.get("/out/facebook")
async def redirect_facebook_link(request: Request, url: str = Query(...)):
    require_auth(request)
    normalized = _normalize_facebook_url(url)
    if not normalized:
        raise HTTPException(status_code=400, detail="Invalid Facebook URL")
    return RedirectResponse(normalized, status_code=302)


# ──────────────────────────────────────────────────────────────────────────────
# API — Jobs
# ──────────────────────────────────────────────────────────────────────────────

@app.post("/api/submit")
async def submit_job(
    request: Request,
    facebook_url: str = Form(...),
    date_from: str = Form(None),
    date_to: str = Form(None),
    max_posts: str = Form(None),
    source_category: str = Form(None),
):
    require_auth(request)

    raw_url = facebook_url.strip()
    url = _normalize_submit_url(raw_url)
    if not url:
        return JSONResponse({"success": False, "error": "Invalid Facebook, TikTok, or Instagram URL"}, status_code=400)

    df = None
    dt = None
    mp = None
    category = (source_category or '').strip() or None

    try:
        if date_from and date_from.strip():
            df = date.fromisoformat(date_from.strip())
        if date_to and date_to.strip():
            dt = date.fromisoformat(date_to.strip())
        if max_posts and max_posts.strip():
            mp = int(max_posts.strip())
            if mp <= 0:
                raise ValueError("max_posts must be positive")
        if category and len(category) > 120:
            raise ValueError("source_category must be 120 characters or fewer")
    except (ValueError, TypeError) as exc:
        return JSONResponse({"success": False, "error": str(exc)}, status_code=400)

    job_id = str(uuid.uuid4())
    create_job(job_id, url, date_from=df, date_to=dt, max_posts=mp, source_category=category)

    return JSONResponse({"success": True, "job_id": job_id, "status": "pending"})


@app.get("/api/jobs")
def list_jobs(
    request: Request,
    limit: int = Query(100),
    offset: int = Query(0),
):
    require_auth(request)
    result = get_all_jobs(limit, offset)
    jobs = [_serialize_job(job) for job in result["jobs"]]
    return JSONResponse({
        "success": True,
        "jobs": jobs,
        "total": result["total"],
        "stats": result["stats"],
    })


@app.get("/api/global-stats")
def global_stats_endpoint(request: Request):
    require_auth(request)
    return JSONResponse({"success": True, **get_global_stats()})


@app.get("/api/jobs/{job_id}")
def get_job_detail(job_id: str, request: Request):
    require_auth(request)
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return JSONResponse({"success": True, "job": _serialize_job(job)})


@app.post("/api/jobs/{job_id}/pause")
async def pause_existing_job(job_id: str, request: Request):
    require_auth(request)
    try:
        job = request_job_pause(job_id)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return JSONResponse({"success": True, "job": _serialize_job(job)})


@app.post("/api/jobs/{job_id}/stop")
async def stop_existing_job(job_id: str, request: Request):
    require_auth(request)
    try:
        job = request_job_stop(job_id)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return JSONResponse({"success": True, "job": _serialize_job(job)})


@app.post("/api/jobs/{job_id}/continue")
async def continue_existing_job(job_id: str, request: Request):
    require_auth(request)
    try:
        job = continue_job_operation(job_id)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return JSONResponse({"success": True, "job": _serialize_job(job)})


@app.post("/api/jobs/{job_id}/rerun")
async def rerun_existing_job(job_id: str, request: Request):
    require_auth(request)
    try:
        job = rerun_job(job_id)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return JSONResponse({"success": True, "job": _serialize_job(job)})


@app.post("/api/jobs/{job_id}/retry-downloads")
async def retry_downloads_existing_job(job_id: str, request: Request):
    require_auth(request)
    try:
        job = retry_failed_downloads(job_id)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return JSONResponse({"success": True, "job": _serialize_job(job)})


@app.post("/api/jobs/{job_id}/start-downloads")
async def start_downloads_endpoint(job_id: str, request: Request):
    require_auth(request)
    try:
        job = start_downloads(job_id)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return JSONResponse({"success": True, "job": _serialize_job(job)})


@app.post("/api/jobs/{job_id}/mark-completed")
async def mark_job_completed_endpoint(job_id: str, request: Request):
    require_auth(request)
    try:
        job = mark_job_completed(job_id)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return JSONResponse({"success": True, "job": _serialize_job(job)})


@app.post("/api/posts/{post_id}/retry-download")
async def retry_single_post_download_endpoint(post_id: int, request: Request):
    require_auth(request)
    try:
        result = retry_single_post_download(post_id)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    if not result:
        raise HTTPException(status_code=404, detail="Post not found")
    return JSONResponse({"success": True, **result})


@app.delete("/api/jobs/{job_id}")
async def delete_existing_job(job_id: str, request: Request):
    require_auth(request)
    try:
        result = request_job_delete(job_id)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    if not result or (not result.get("deleted") and not result.get("job")):
        raise HTTPException(status_code=404, detail="Job not found")

    payload = {"success": True, "job_id": job_id, "deleted": bool(result.get("deleted"))}
    if result.get("job"):
        payload["job"] = _serialize_job(result["job"])
    return JSONResponse(payload)


# ──────────────────────────────────────────────────────────────────────────────
# API — Posts
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/api/jobs/{job_id}/posts")
def get_job_posts(
    job_id: str,
    request: Request,
    post_type: str = Query("all"),
    limit: int = Query(50),
    offset: int = Query(0),
):
    require_auth(request)
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    result = get_posts_for_job(job_id, post_type=post_type, limit=limit, offset=offset)
    posts = [_serialize_post_record(p) for p in result["posts"]]
    return JSONResponse({"success": True, "posts": posts, "total": result["total"]})


@app.get("/api/content/summary")
async def get_all_content_summary(
    request: Request,
    source_category: str = Query("all"),
):
    require_auth(request)
    return JSONResponse({"success": True, **get_all_posts_summary(source_category=source_category)})


@app.get("/api/content/posts")
def get_all_content_posts(
    request: Request,
    post_type: str = Query("all"),
    limit: int = Query(50),
    offset: int = Query(0),
    source_category: str = Query("all"),
):
    require_auth(request)
    result = get_posts_for_all_jobs(post_type=post_type, limit=limit, offset=offset, source_category=source_category)
    posts = [_serialize_post_record(p) for p in result["posts"]]
    return JSONResponse({"success": True, "posts": posts, "total": result["total"]})


# ──────────────────────────────────────────────────────────────────────────────
# API — CSV Export
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/api/jobs/{job_id}/export")
async def export_job_csv(job_id: str, request: Request):
    require_auth(request)
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    posts = get_all_posts_for_job(job_id)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "Post Link",
        "Published Date",
        "Description",
        "Has Video",
        "Has Image",
        "Video URL (Original)",
        "Video S3 URL",
        "Image URLs (Original)",
        "Image S3 URLs",
        "Download Status",
        "Scraped At",
    ])

    for p in posts:
        writer.writerow([
            p.get("post_link", ""),
            p.get("published_date", ""),
            (p.get("description") or "").replace("\n", " "),
            "Yes" if p.get("has_video") else "No",
            "Yes" if p.get("has_image") else "No",
            p.get("video_url", ""),
            p.get("video_s3_url", ""),
            _csv_field(p.get("image_urls")),
            _csv_field(p.get("image_s3_urls")),
            p.get("download_status", ""),
            str(p.get("scraped_at", "")),
        ])

    output.seek(0)
    page_slug = (job.get("page_name") or job_id).replace(" ", "_")[:40]
    filename = f"fb_scrape_{page_slug}_{job_id[:8]}.csv"

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/content/export")
async def export_all_content_csv(request: Request):
    require_auth(request)
    posts = get_all_posts_deduped()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "Source Page",
        "Source URL",
        "Post Link",
        "Published Date",
        "Description",
        "Has Video",
        "Has Image",
        "Video URL (Original)",
        "Video S3 URL",
        "Image URLs (Original)",
        "Image S3 URLs",
        "Download Status",
        "Scraped At",
    ])

    for p in posts:
        writer.writerow([
            p.get("page_name") or "",
            p.get("facebook_url") or "",
            p.get("post_link") or "",
            p.get("published_date") or "",
            (p.get("description") or "").replace("\n", " "),
            "Yes" if p.get("has_video") else "No",
            "Yes" if p.get("has_image") else "No",
            p.get("video_url") or "",
            p.get("video_s3_url") or "",
            _csv_field(p.get("image_urls")),
            _csv_field(p.get("image_s3_urls")),
            p.get("download_status") or "",
            str(p.get("scraped_at") or ""),
        ])

    output.seek(0)
    filename = f"fb_scrape_all_content_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/content/export-s3")
async def export_s3_content_csv(request: Request):
    require_auth(request)
    filename = f"s3_content_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"

    def generate():
        yield "Source Page,Source URL,S3 URL,Type\r\n"
        buf = io.StringIO()
        writer = csv.writer(buf)
        for row in stream_all_s3_content():
            writer.writerow(row)
            yield buf.getvalue()
            buf.seek(0)
            buf.truncate(0)

    return StreamingResponse(
        generate(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ──────────────────────────────────────────────────────────────────────────────
# Health
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)
