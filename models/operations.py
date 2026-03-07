import hashlib
import json
import logging
from datetime import datetime
from models.database import get_connection


# ──────────────────────────────────────────────────────────────────────────────
# Job operations
# ──────────────────────────────────────────────────────────────────────────────

def create_job(job_id: str, facebook_url: str, date_from=None, date_to=None, max_posts=None):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO fb_scrape_jobs
                   (job_id, facebook_url, date_from, date_to, max_posts, status)
                   VALUES (%s, %s, %s, %s, %s, 'pending')""",
                (job_id, facebook_url, date_from, date_to, max_posts),
            )
        conn.commit()
    finally:
        conn.close()


def get_job(job_id: str):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM fb_scrape_jobs WHERE job_id = %s", (job_id,))
            return cur.fetchone()
    finally:
        conn.close()


def get_all_jobs(limit=100, offset=0):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM fb_scrape_jobs ORDER BY created_at DESC LIMIT %s OFFSET %s",
                (limit, offset),
            )
            jobs = cur.fetchall()
            cur.execute("SELECT COUNT(*) AS total FROM fb_scrape_jobs")
            total = cur.fetchone()["total"]
            cur.execute(
                "SELECT status, COUNT(*) AS cnt FROM fb_scrape_jobs GROUP BY status"
            )
            stats = {row["status"]: row["cnt"] for row in cur.fetchall()}
            return {"jobs": jobs, "total": total, "stats": stats}
    finally:
        conn.close()


def get_jobs_by_status(status: str):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM fb_scrape_jobs WHERE status = %s ORDER BY created_at ASC",
                (status,),
            )
            return cur.fetchall()
    finally:
        conn.close()


def claim_next_pending_job():
    """Atomically claim the oldest pending job → sets it to 'scraping'."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT job_id FROM fb_scrape_jobs
                   WHERE status = 'pending'
                   ORDER BY created_at ASC LIMIT 1 FOR UPDATE"""
            )
            row = cur.fetchone()
            if not row:
                conn.rollback()
                return None
            job_id = row["job_id"]
            cur.execute(
                """UPDATE fb_scrape_jobs
                   SET status = 'scraping', started_scraping_at = NOW()
                   WHERE job_id = %s AND status = 'pending'""",
                (job_id,),
            )
            conn.commit()
            cur.execute("SELECT * FROM fb_scrape_jobs WHERE job_id = %s", (job_id,))
            return cur.fetchone()
    except Exception:
        conn.rollback()
        return None
    finally:
        conn.close()


def claim_next_downloading_job():
    """Claim the oldest job in 'downloading_content' that hasn't been picked yet."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT * FROM fb_scrape_jobs
                   WHERE status = 'downloading_content'
                   ORDER BY scraping_completed_at ASC LIMIT 1 FOR UPDATE"""
            )
            row = cur.fetchone()
            if not row:
                conn.rollback()
                return None
            conn.commit()
            return row
    except Exception:
        conn.rollback()
        return None
    finally:
        conn.close()


def update_job_status(job_id: str, status: str, error_message: str = None, extra: dict = None):
    conn = get_connection()
    try:
        fields = ["status = %s"]
        values = [status]

        if status == "scraping":
            fields.append("started_scraping_at = NOW()")
        elif status == "downloading_content":
            fields.append("scraping_completed_at = NOW()")
        elif status == "completed":
            fields.append("completed_at = NOW()")

        if error_message is not None:
            fields.append("error_message = %s")
            values.append(error_message)

        if extra:
            for k, v in extra.items():
                fields.append(f"{k} = %s")
                values.append(v)

        values.append(job_id)
        sql = f"UPDATE fb_scrape_jobs SET {', '.join(fields)} WHERE job_id = %s"
        with conn.cursor() as cur:
            cur.execute(sql, values)
        conn.commit()
    finally:
        conn.close()


def update_job_progress(job_id: str, total_posts_scraped: int = None,
                        page_name: str = None, page_id: str = None,
                        total_media_count: int = None, total_media_downloaded: int = None):
    conn = get_connection()
    try:
        fields, values = [], []
        if total_posts_scraped is not None:
            fields.append("total_posts_scraped = %s"); values.append(total_posts_scraped)
        if page_name is not None:
            fields.append("page_name = %s"); values.append(page_name)
        if page_id is not None:
            fields.append("page_id = %s"); values.append(page_id)
        if total_media_count is not None:
            fields.append("total_media_count = %s"); values.append(total_media_count)
        if total_media_downloaded is not None:
            fields.append("total_media_downloaded = %s"); values.append(total_media_downloaded)
        if not fields:
            return
        values.append(job_id)
        sql = f"UPDATE fb_scrape_jobs SET {', '.join(fields)} WHERE job_id = %s"
        with conn.cursor() as cur:
            cur.execute(sql, values)
        conn.commit()
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# Post operations
# ──────────────────────────────────────────────────────────────────────────────

def _hash_url(url: str) -> str:
    return hashlib.md5(url.encode("utf-8")).hexdigest()


def save_post(job_id: str, post: dict) -> bool:
    """Insert a post; silently ignore if already exists for this job."""
    link = post.get("post_link", "")
    if not link:
        return False
    link_hash = _hash_url(link)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT IGNORE INTO fb_posts
                   (job_id, post_link, post_link_hash,
                    published_date, published_timestamp, description,
                    has_video, has_image, video_url, image_urls)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    job_id,
                    link,
                    link_hash,
                    post.get("published_date"),
                    post.get("published_timestamp"),
                    post.get("description"),
                    1 if post.get("has_video") else 0,
                    1 if post.get("has_image") else 0,
                    post.get("video_url"),
                    json.dumps(post.get("image_links", [])),
                ),
            )
            inserted = cur.rowcount > 0
        conn.commit()
        return inserted
    except Exception as exc:
        logging.error(f"[DB] save_post error: {exc}")
        return False
    finally:
        conn.close()


def get_posts_for_job(job_id: str, post_type: str = "all", limit: int = 50, offset: int = 0):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            where = "job_id = %s"
            params = [job_id]
            if post_type == "video":
                where += " AND has_video = 1"
            elif post_type == "image":
                where += " AND has_image = 1 AND has_video = 0"
            elif post_type == "text":
                where += " AND has_video = 0 AND has_image = 0"

            cur.execute(
                f"SELECT COUNT(*) AS total FROM fb_posts WHERE {where}", params
            )
            total = cur.fetchone()["total"]

            cur.execute(
                f"""SELECT * FROM fb_posts WHERE {where}
                    ORDER BY published_timestamp DESC
                    LIMIT %s OFFSET %s""",
                params + [limit, offset],
            )
            posts = cur.fetchall()
            return {"posts": posts, "total": total}
    finally:
        conn.close()


def get_all_posts_for_job(job_id: str):
    """Return all posts for a job (used by CSV export and download worker)."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM fb_posts WHERE job_id = %s ORDER BY published_timestamp DESC",
                (job_id,),
            )
            return cur.fetchall()
    finally:
        conn.close()


def get_pending_download_posts(job_id: str):
    """Return posts whose media hasn't been downloaded yet (includes text posts so they get auto-completed)."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT * FROM fb_posts
                   WHERE job_id = %s AND download_status = 'pending'""",
                (job_id,),
            )
            return cur.fetchall()
    finally:
        conn.close()


def update_post_download(post_id: int, status: str, error: str = None,
                         video_s3_url: str = None, video_s3_key: str = None,
                         image_s3_urls: list = None, image_s3_keys: list = None):
    conn = get_connection()
    try:
        fields, values = ["download_status = %s"], [status]
        if error is not None:
            fields.append("download_error = %s"); values.append(error)
        if video_s3_url is not None:
            fields.append("video_s3_url = %s"); values.append(video_s3_url)
        if video_s3_key is not None:
            fields.append("video_s3_key = %s"); values.append(video_s3_key)
        if image_s3_urls is not None:
            fields.append("image_s3_urls = %s"); values.append(json.dumps(image_s3_urls))
        if image_s3_keys is not None:
            fields.append("image_s3_keys = %s"); values.append(json.dumps(image_s3_keys))
        values.append(post_id)
        sql = f"UPDATE fb_posts SET {', '.join(fields)} WHERE id = %s"
        with conn.cursor() as cur:
            cur.execute(sql, values)
        conn.commit()
    finally:
        conn.close()
