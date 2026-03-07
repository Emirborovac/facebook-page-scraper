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
    create_job,
    get_all_jobs,
    get_all_posts_for_job,
    get_job,
    get_posts_for_job,
)
from models.queue_worker import download_worker, scraping_worker

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

    import asyncio
    t1 = asyncio.create_task(scraping_worker())
    t2 = asyncio.create_task(download_worker())
    logging.info("[App] Scraping and download workers started.")

    yield

    t1.cancel()
    t2.cancel()
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
):
    require_auth(request)

    url = facebook_url.strip()
    if not url.startswith("https://www.facebook.com") and not url.startswith("https://facebook.com"):
        return JSONResponse({"success": False, "error": "Invalid Facebook URL"}, status_code=400)

    df = None
    dt = None
    mp = None

    try:
        if date_from and date_from.strip():
            df = date.fromisoformat(date_from.strip())
        if date_to and date_to.strip():
            dt = date.fromisoformat(date_to.strip())
        if max_posts and max_posts.strip():
            mp = int(max_posts.strip())
            if mp <= 0:
                raise ValueError("max_posts must be positive")
    except (ValueError, TypeError) as exc:
        return JSONResponse({"success": False, "error": str(exc)}, status_code=400)

    job_id = str(uuid.uuid4())
    create_job(job_id, url, date_from=df, date_to=dt, max_posts=mp)

    return JSONResponse({"success": True, "job_id": job_id, "status": "pending"})


@app.get("/api/jobs")
async def list_jobs(
    request: Request,
    limit: int = Query(100),
    offset: int = Query(0),
):
    require_auth(request)
    result = get_all_jobs(limit, offset)
    jobs = []
    for j in result["jobs"]:
        j = dict(j)
        for key in ("created_at", "started_scraping_at", "scraping_completed_at", "completed_at"):
            if j.get(key) and hasattr(j[key], "isoformat"):
                j[key] = j[key].isoformat()
            elif j.get(key) and isinstance(j[key], str):
                pass
        for key in ("date_from", "date_to"):
            if j.get(key) and hasattr(j[key], "isoformat"):
                j[key] = j[key].isoformat()
        jobs.append(j)
    return JSONResponse({
        "success": True,
        "jobs": jobs,
        "total": result["total"],
        "stats": result["stats"],
    })


@app.get("/api/jobs/{job_id}")
async def get_job_detail(job_id: str, request: Request):
    require_auth(request)
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    j = dict(job)
    for key in ("created_at", "started_scraping_at", "scraping_completed_at", "completed_at"):
        if j.get(key) and hasattr(j[key], "isoformat"):
            j[key] = j[key].isoformat()
    for key in ("date_from", "date_to"):
        if j.get(key) and hasattr(j[key], "isoformat"):
            j[key] = j[key].isoformat()
    return JSONResponse({"success": True, "job": j})


# ──────────────────────────────────────────────────────────────────────────────
# API — Posts
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/api/jobs/{job_id}/posts")
async def get_job_posts(
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
    posts = []
    for p in result["posts"]:
        p = dict(p)
        if p.get("scraped_at") and hasattr(p["scraped_at"], "isoformat"):
            p["scraped_at"] = p["scraped_at"].isoformat()

        # Parse JSON fields
        for field in ("image_urls", "image_s3_urls", "image_s3_keys"):
            if p.get(field) and isinstance(p[field], str):
                try:
                    p[field] = json.loads(p[field])
                except Exception:
                    p[field] = []

        # For posts where S3 download failed, fall back to original CDN URLs for preview
        if not p.get("image_s3_urls"):
            p["image_s3_urls"] = p.get("image_urls") or []
        if not p.get("video_s3_url"):
            p["video_s3_url"] = None

        posts.append(p)
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
        def _load_json_field(val):
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

        writer.writerow([
            p.get("post_link", ""),
            p.get("published_date", ""),
            (p.get("description") or "").replace("\n", " "),
            "Yes" if p.get("has_video") else "No",
            "Yes" if p.get("has_image") else "No",
            p.get("video_url", ""),
            p.get("video_s3_url", ""),
            _load_json_field(p.get("image_urls")),
            _load_json_field(p.get("image_s3_urls")),
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


# ──────────────────────────────────────────────────────────────────────────────
# Health
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)
