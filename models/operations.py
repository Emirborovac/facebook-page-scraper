import hashlib
import json
import logging
import time
import uuid
from datetime import datetime, timedelta
from threading import Lock
from urllib.parse import urlparse

from models.database import get_connection
from models.s3_upload import delete_s3_objects


# ── Simple TTL cache for expensive aggregation queries ──────────────────
_cache: dict[str, tuple[float, object]] = {}
_cache_lock = Lock()


def _cache_get(key: str, ttl: float):
    with _cache_lock:
        entry = _cache.get(key)
        if entry and (time.monotonic() - entry[0]) < ttl:
            return entry[1]
    return None


def _cache_set(key: str, value):
    with _cache_lock:
        _cache[key] = (time.monotonic(), value)


def invalidate_cache(*keys):
    with _cache_lock:
        for k in keys:
            _cache.pop(k, None)

RUNNING_STATUSES = {"scraping", "downloading_content"}
IDLE_STATUSES = {"pending", "paused", "stopped", "completed", "failed"}

# ── Schema-tolerance helpers ────────────────────────────────────────────
# The app user may lack ALTER privilege, in which case some optional columns
# (normalized_url, last_scraped_*, assigned_download_worker, active_worker_name)
# may not exist on fb_scrape_jobs. These helpers let queries detect what's
# available so they can degrade gracefully without crashing.
_SCHEMA_LOCK = Lock()
_TABLE_COLUMNS_CACHE: dict[str, set[str]] = {}


def _table_columns(table: str) -> set[str]:
    with _SCHEMA_LOCK:
        cached = _TABLE_COLUMNS_CACHE.get(table)
        if cached is not None:
            return cached
    cols: set[str] = set()
    try:
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f"SHOW COLUMNS FROM {table}")
                for row in cur.fetchall() or []:
                    name = row.get("Field") if isinstance(row, dict) else (row[0] if row else None)
                    if name:
                        cols.add(name)
        finally:
            conn.close()
    except Exception as exc:
        logging.warning("[DB] Could not introspect columns for %s: %s", table, exc)
    with _SCHEMA_LOCK:
        _TABLE_COLUMNS_CACHE[table] = cols
    return cols


def _has_column(table: str, column: str) -> bool:
    cols = _table_columns(table)
    if not cols:
        # If introspection failed entirely, assume the column exists and let
        # queries surface the real error rather than silently dropping it.
        return True
    return column in cols


def invalidate_schema_cache():
    """Re-read column list (call after a manual ALTER)."""
    with _SCHEMA_LOCK:
        _TABLE_COLUMNS_CACHE.clear()
GOOD_CHECKPOINT_HISTORY_LIMIT = 20
SCRAPING_WORKER_STAGE = "scraping"
DOWNLOADING_WORKER_STAGE = "downloading_content"
FACEBOOK_PLATFORM = "facebook"
TIKTOK_PLATFORM = "tiktok"
INSTAGRAM_PLATFORM = "instagram"
WORKER_LEASE_TIMEOUT_SECONDS = 300
CLAIM_RETRY_LIMIT = 20


# ──────────────────────────────────────────────────────────────────────────────
# Job operations
# ──────────────────────────────────────────────────────────────────────────────

def create_job(job_id: str, facebook_url: str, date_from=None, date_to=None, max_posts=None,
               source_category=None, normalized_url: str = None):
    """Insert a new job row.

    Resilient to a missing ``normalized_url`` column — if the schema doesn't
    have it (because the app user can't ALTER), the column is dropped from
    the INSERT and the job is created without duplicate-detection metadata.
    """
    has_norm = _has_column("fb_scrape_jobs", "normalized_url")
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            if has_norm:
                cur.execute(
                    """INSERT INTO fb_scrape_jobs
                       (job_id, facebook_url, normalized_url, source_category, date_from, date_to,
                        max_posts, status, resume_stage)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending', 'scraping')""",
                    (job_id, facebook_url, normalized_url, source_category, date_from, date_to, max_posts),
                )
            else:
                cur.execute(
                    """INSERT INTO fb_scrape_jobs
                       (job_id, facebook_url, source_category, date_from, date_to,
                        max_posts, status, resume_stage)
                       VALUES (%s, %s, %s, %s, %s, %s, 'pending', 'scraping')""",
                    (job_id, facebook_url, source_category, date_from, date_to, max_posts),
                )
        conn.commit()
    finally:
        conn.close()


def find_jobs_by_normalized_url(normalized_url: str, limit: int = 20) -> list[dict]:
    """Return existing jobs that target the same normalized URL.

    Used by the duplicate-detection endpoint at submit time. Resilient to a
    missing ``normalized_url`` column: falls back to a facebook_url match.
    Optional last_scraped_* columns are also dropped from the SELECT when
    the schema doesn't have them yet.
    """
    if not normalized_url:
        return []
    cols = _table_columns("fb_scrape_jobs")
    has_norm = (not cols) or "normalized_url" in cols
    has_last = (not cols) or "last_scraped_page_num" in cols

    base_cols = [
        "job_id", "facebook_url", "status", "source_category",
        "page_name", "page_id", "total_posts_scraped", "total_media_count",
        "total_media_downloaded", "scrape_resume_page_num", "scrape_resume_cursor",
        "started_scraping_at", "scraping_completed_at", "completed_at", "created_at",
        "resume_stage", "error_message",
    ]
    if has_norm:
        base_cols.insert(2, "normalized_url")
    if has_last:
        base_cols.extend(["last_scraped_page_num", "last_scraped_cursor", "last_scraped_at"])
    select_list = ", ".join(base_cols)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            if has_norm:
                where_sql = "WHERE normalized_url = %s OR facebook_url = %s"
                params = (normalized_url, normalized_url, limit)
            else:
                # No normalized_url column → match by exact facebook_url only.
                where_sql = "WHERE facebook_url = %s"
                params = (normalized_url, limit)
            cur.execute(
                f"SELECT {select_list} FROM fb_scrape_jobs {where_sql} "
                f"ORDER BY created_at DESC LIMIT %s",
                params,
            )
            return cur.fetchall() or []
    finally:
        conn.close()


def get_newest_post_timestamp_for_url(normalized_url: str) -> int | None:
    """Newest post timestamp (seconds) across all jobs for this URL.

    Used by "Scan for new" — the new job will set date_from to this+1 so the
    scraper stops as soon as it reaches a post we already have. Resilient to
    a missing normalized_url column (matches on facebook_url only).
    """
    if not normalized_url:
        return None
    has_norm = _has_column("fb_scrape_jobs", "normalized_url")
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            if has_norm:
                cur.execute(
                    """SELECT MAX(p.published_timestamp) AS newest
                       FROM fb_posts p
                       JOIN fb_scrape_jobs j ON j.job_id = p.job_id
                       WHERE (j.normalized_url = %s OR j.facebook_url = %s)
                         AND p.published_timestamp IS NOT NULL""",
                    (normalized_url, normalized_url),
                )
            else:
                cur.execute(
                    """SELECT MAX(p.published_timestamp) AS newest
                       FROM fb_posts p
                       JOIN fb_scrape_jobs j ON j.job_id = p.job_id
                       WHERE j.facebook_url = %s
                         AND p.published_timestamp IS NOT NULL""",
                    (normalized_url,),
                )
            row = cur.fetchone()
            if not row or row.get("newest") is None:
                return None
            try:
                return int(row["newest"])
            except (TypeError, ValueError):
                return None
    finally:
        conn.close()


def backfill_normalized_url(job_id: str, normalized_url: str) -> None:
    """Set normalized_url on a legacy job row that was missing it.

    No-op if the column doesn't exist on the table.
    """
    if not job_id or not normalized_url:
        return
    if not _has_column("fb_scrape_jobs", "normalized_url"):
        return
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE fb_scrape_jobs SET normalized_url = %s WHERE job_id = %s AND (normalized_url IS NULL OR normalized_url = '')",
                (normalized_url, job_id),
            )
        conn.commit()
    finally:
        conn.close()


def stamp_last_scraped(job_id: str, cursor: str | None, page_num: int, total_posts: int = None) -> None:
    """Persist 'last position scraped' so we can resume / scan-for-new later.

    No-op when the optional last_scraped_* columns don't exist on the schema
    (because the app user couldn't ALTER and the columns were never added).
    """
    if not job_id:
        return
    if not _has_column("fb_scrape_jobs", "last_scraped_page_num"):
        return
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE fb_scrape_jobs
                   SET last_scraped_cursor = %s,
                       last_scraped_page_num = %s,
                       last_scraped_at = NOW()
                   WHERE job_id = %s""",
                (cursor, max(int(page_num or 0), 0), job_id),
            )
        conn.commit()
    finally:
        conn.close()


_POST_DOWNLOAD_STATS_SUBQUERY = """(
    SELECT job_id,
           SUM(CASE WHEN download_status = 'completed' THEN 1 ELSE 0 END) AS download_completed_count,
           SUM(CASE WHEN download_status = 'failed' THEN 1 ELSE 0 END) AS download_failed_count,
           SUM(CASE WHEN download_status IN ('pending', 'downloading') THEN 1 ELSE 0 END) AS download_pending_count
    FROM fb_posts
    GROUP BY job_id
) AS dlstats"""


def get_job(job_id: str):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT j.*,
                          COALESCE(dlstats.download_completed_count, 0) AS download_completed_count,
                          COALESCE(dlstats.download_failed_count, 0) AS download_failed_count,
                          COALESCE(dlstats.download_pending_count, 0) AS download_pending_count
                   FROM fb_scrape_jobs j
                   LEFT JOIN (
                       SELECT job_id,
                              SUM(CASE WHEN download_status = 'completed' THEN 1 ELSE 0 END) AS download_completed_count,
                              SUM(CASE WHEN download_status = 'failed' THEN 1 ELSE 0 END) AS download_failed_count,
                              SUM(CASE WHEN download_status IN ('pending', 'downloading') THEN 1 ELSE 0 END) AS download_pending_count
                       FROM fb_posts
                       WHERE job_id = %s
                   ) AS dlstats ON dlstats.job_id = j.job_id
                   WHERE j.job_id = %s""",
                (job_id, job_id),
            )
            return cur.fetchone()
    finally:
        conn.close()


def get_job_control_state(job_id: str):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT job_id, status, resume_stage, control_action,
                          active_worker_stage, active_worker_token,
                          total_posts_scraped, total_media_count, total_media_downloaded,
                          scrape_resume_page_num, scrape_resume_skip_posts,
                          scrape_good_checkpoints,
                          scrape_last_progress_at, started_scraping_at, created_at,
                          scraping_completed_at, completed_at, error_message
                   FROM fb_scrape_jobs
                   WHERE job_id = %s""",
                (job_id,),
            )
            return cur.fetchone()
    finally:
        conn.close()


def _parse_json_list(value):
    if not value:
        return []
    if isinstance(value, list):
        return [item for item in value if item]
    if isinstance(value, str):
        try:
            loaded = json.loads(value)
        except Exception:
            return []
        if isinstance(loaded, list):
            return [item for item in loaded if item]
    return []


def _load_good_checkpoints(value):
    if not value:
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            return []
    if not isinstance(value, list):
        return []

    checkpoints = []
    for item in value:
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


def _collect_job_s3_keys(cur, job_id: str):
    cur.execute(
        """SELECT video_s3_key, image_s3_keys
           FROM fb_posts
           WHERE job_id = %s""",
        (job_id,),
    )
    keys = []
    for row in cur.fetchall():
        if row.get("video_s3_key"):
            keys.append(row["video_s3_key"])
        keys.extend(_parse_json_list(row.get("image_s3_keys")))
    return list(dict.fromkeys(keys))


def _delete_job_now(job_id: str) -> bool:
    conn = get_connection()
    s3_keys = []
    deleted = False
    try:
        conn.begin()
        with conn.cursor() as cur:
            cur.execute("SELECT job_id FROM fb_scrape_jobs WHERE job_id = %s", (job_id,))
            job = cur.fetchone()
            if not job:
                conn.rollback()
                return False

            s3_keys = _collect_job_s3_keys(cur, job_id)
            cur.execute("DELETE FROM fb_posts WHERE job_id = %s", (job_id,))
            cur.execute("DELETE FROM fb_scrape_jobs WHERE job_id = %s", (job_id,))
            deleted = cur.rowcount > 0
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    if deleted and s3_keys:
        delete_s3_objects(s3_keys)
    return deleted


def _update_job_fields(job_id: str, assignments: dict):
    if not assignments:
        return
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            fields = []
            values = []
            for key, value in assignments.items():
                fields.append(f"{key} = %s")
                values.append(value)
            values.append(job_id)
            cur.execute(
                f"UPDATE fb_scrape_jobs SET {', '.join(fields)} WHERE job_id = %s",
                values,
            )
        conn.commit()
    finally:
        conn.close()


def _can_apply_download_control_immediately(job: dict) -> bool:
    return (
        job.get("status") == "downloading_content"
        and not int(job.get("total_media_count") or 0)
        and not int(job.get("total_media_downloaded") or 0)
    )


def _worker_lease_cutoff() -> datetime:
    return datetime.utcnow() - timedelta(seconds=WORKER_LEASE_TIMEOUT_SECONDS)


def prepare_jobs_for_worker_startup(scraping_worker_limits) -> dict:
    """Normalize interrupted jobs before workers begin polling after a process restart."""
    if isinstance(scraping_worker_limits, dict):
        platform_limits = {
            FACEBOOK_PLATFORM: max(int(scraping_worker_limits.get(FACEBOOK_PLATFORM, 0) or 0), 0),
            TIKTOK_PLATFORM: max(int(scraping_worker_limits.get(TIKTOK_PLATFORM, 0) or 0), 0),
            INSTAGRAM_PLATFORM: max(int(scraping_worker_limits.get(INSTAGRAM_PLATFORM, 0) or 0), 0),
        }
    else:
        limit = max(int(scraping_worker_limits or 0), 0)
        platform_limits = {FACEBOOK_PLATFORM: limit, TIKTOK_PLATFORM: limit, INSTAGRAM_PLATFORM: limit}

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT job_id, facebook_url FROM fb_scrape_jobs
                   WHERE status = 'scraping'
                   ORDER BY COALESCE(started_scraping_at, created_at) ASC, created_at ASC"""
            )
            scraping_job_rows = cur.fetchall()

            keep_ids = []
            requeue_ids = []
            kept_counts = {FACEBOOK_PLATFORM: 0, TIKTOK_PLATFORM: 0, INSTAGRAM_PLATFORM: 0}

            for row in scraping_job_rows:
                platform = _job_source_platform(row.get('facebook_url'))
                if kept_counts[platform] < platform_limits.get(platform, 0):
                    keep_ids.append(row['job_id'])
                    kept_counts[platform] += 1
                else:
                    requeue_ids.append(row['job_id'])

            if keep_ids:
                placeholders = ", ".join(["%s"] * len(keep_ids))
                cur.execute(
                    f"""UPDATE fb_scrape_jobs
                           SET control_action = NULL,
                               active_worker_stage = NULL,
                               active_worker_token = NULL,
                               active_worker_name = NULL,
                               completed_at = NULL
                           WHERE status = 'scraping'
                             AND job_id IN ({placeholders})""",
                    keep_ids,
                )

            if requeue_ids:
                placeholders = ", ".join(["%s"] * len(requeue_ids))
                cur.execute(
                    f"""UPDATE fb_scrape_jobs
                           SET status = 'pending',
                               resume_stage = 'scraping',
                               control_action = NULL,
                               active_worker_stage = NULL,
                               active_worker_token = NULL,
                               active_worker_name = NULL,
                               completed_at = NULL
                           WHERE status = 'scraping'
                             AND job_id IN ({placeholders})""",
                    requeue_ids,
                )

            cur.execute(
                """UPDATE fb_scrape_jobs
                   SET control_action = NULL,
                       active_worker_stage = NULL,
                       active_worker_token = NULL,
                       active_worker_name = NULL,
                       completed_at = NULL
                   WHERE status = 'downloading_content'"""
            )
            released_download_jobs = cur.rowcount

        conn.commit()
        return {
            "resumable_scraping_jobs": len(keep_ids),
            "requeued_scraping_jobs": len(requeue_ids),
            "released_download_jobs": int(released_download_jobs or 0),
            "resumable_job_ids": keep_ids,
            "requeued_job_ids": requeue_ids,
            "resumable_by_platform": kept_counts,
        }
    finally:
        conn.close()


def request_job_pause(job_id: str):
    job = get_job(job_id)
    if not job:
        return None

    status = job["status"]
    stage = job.get("resume_stage") or ("downloading_content" if status == "downloading_content" else "scraping")

    if status == "paused":
        return job
    if status == "pending":
        update_job_status(job_id, "paused", extra={"resume_stage": "scraping", "error_message": None})
        return get_job(job_id)
    if _can_apply_download_control_immediately(job):
        update_job_status(job_id, "paused", extra={"resume_stage": stage, "error_message": None})
        return get_job(job_id)
    if status in RUNNING_STATUSES:
        _update_job_fields(job_id, {"control_action": "pause", "resume_stage": stage})
        return get_job(job_id)
    raise ValueError("Only queued or active jobs can be paused")


def request_job_stop(job_id: str):
    job = get_job(job_id)
    if not job:
        return None

    status = job["status"]
    stage = job.get("resume_stage") or ("downloading_content" if status == "downloading_content" else "scraping")

    if status == "stopped":
        return job
    if status in {"pending", "paused"}:
        update_job_status(job_id, "stopped", extra={"resume_stage": stage, "error_message": None})
        return get_job(job_id)
    if _can_apply_download_control_immediately(job):
        update_job_status(job_id, "stopped", extra={"resume_stage": stage, "error_message": None})
        return get_job(job_id)
    if status in RUNNING_STATUSES:
        _update_job_fields(job_id, {"control_action": "stop", "resume_stage": stage})
        return get_job(job_id)
    raise ValueError("Only queued or active jobs can be stopped")


def request_job_delete(job_id: str):
    job = get_job(job_id)
    if not job:
        return {"deleted": False, "job": None}

    status = job["status"]
    stage = job.get("resume_stage") or ("downloading_content" if status == "downloading_content" else "scraping")

    if _can_apply_download_control_immediately(job):
        _delete_job_now(job_id)
        return {"deleted": True, "job": None}

    if status in RUNNING_STATUSES:
        _update_job_fields(job_id, {"control_action": "delete", "resume_stage": stage})
        return {"deleted": False, "job": get_job(job_id)}

    _delete_job_now(job_id)
    return {"deleted": True, "job": None}


def continue_job(job_id: str):
    """Move a job back into the workable queue from a paused/stopped/failed/completed state.

    For ``completed`` jobs this is the "early-stop resume" path — when scraping
    finished (or thought it finished) but did not reach the user's requested
    date_from, the cursor was cleared from scrape_resume_* but preserved in
    last_scraped_*. Copy it back so the worker picks up where it left off.
    """
    job = get_job(job_id)
    if not job:
        return None

    status = job["status"]
    stage = job.get("resume_stage") or "scraping"
    if status not in {"paused", "stopped", "failed", "scraping", "completed"}:
        raise ValueError("Only paused, stopped, failed, completed, or interrupted jobs can be continued")

    # For completed jobs we always resume scraping (downloads of new posts will
    # follow naturally once they're saved). Restore the position the scraper
    # left off at, if we have it preserved.
    if status == "completed":
        stage = "scraping"
        # Prefer the new last_scraped_* columns (preserved across completion),
        # but fall back to the legacy scrape_resume_* values which are still
        # populated when the new columns aren't on the schema yet.
        last_page = int(job.get("last_scraped_page_num") or 0)
        last_cursor = job.get("last_scraped_cursor")
        if last_page <= 0:
            last_page = int(job.get("scrape_resume_page_num") or 0)
            last_cursor = job.get("scrape_resume_cursor") or last_cursor
        if last_page > 0:
            # Copy the preserved position back into the active checkpoint cols.
            try:
                conn = get_connection()
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            """UPDATE fb_scrape_jobs
                               SET scrape_resume_cursor = %s,
                                   scrape_resume_page_num = %s,
                                   scrape_resume_skip_posts = 0,
                                   scraping_completed_at = NULL
                               WHERE job_id = %s""",
                            (last_cursor, last_page, job_id),
                        )
                    conn.commit()
                    logging.info(
                        "[continue_job] Restored position for completed job %s: page=%s cursor=%s",
                        job_id, last_page, "set" if last_cursor else "none",
                    )
                finally:
                    conn.close()
            except Exception as exc:
                logging.warning("[continue_job] Could not restore last_scraped position for %s: %s", job_id, exc)
        else:
            logging.warning(
                "[continue_job] Resuming completed job %s with no preserved checkpoint — will restart from page 1",
                job_id,
            )

    target_status = "downloading_content" if stage == "downloading_content" else "pending"
    update_job_status(
        job_id,
        target_status,
        extra={
            "resume_stage": stage,
            "control_action": None,
            "error_message": None,
        },
    )
    return get_job(job_id)


def start_downloads(job_id: str):
    """Queue a stopped/failed/paused job for downloading its pending posts."""
    job = get_job(job_id)
    if not job:
        return None
    if job["status"] not in ("stopped", "failed", "paused", "completed"):
        raise ValueError(f"Cannot start downloads for a job with status '{job['status']}'")

    conn = get_connection()
    try:
        conn.begin()
        with conn.cursor() as cur:
            cur.execute(
                """SELECT COUNT(*) AS total,
                          SUM(CASE WHEN download_status = 'completed' THEN 1 ELSE 0 END) AS downloaded,
                          SUM(CASE WHEN download_status IN ('pending', 'downloading') THEN 1 ELSE 0 END) AS pending
                   FROM fb_posts WHERE job_id = %s""",
                (job_id,),
            )
            counts = cur.fetchone() or {}
            total_media = int(counts.get("total") or 0)
            downloaded_media = int(counts.get("downloaded") or 0)
            pending = int(counts.get("pending") or 0)
            if total_media <= 0:
                raise ValueError("This job has no posts to download")
            if pending <= 0:
                raise ValueError("This job has no pending downloads — use Retry Downloads if there are failures")

            cur.execute(
                """UPDATE fb_scrape_jobs
                   SET status = 'downloading_content',
                       resume_stage = 'downloading_content',
                       control_action = NULL,
                       error_message = NULL,
                       active_worker_stage = NULL,
                       active_worker_token = NULL,
                       active_worker_name = NULL,
                       total_media_count = %s,
                       total_media_downloaded = %s,
                       completed_at = NULL
                   WHERE job_id = %s""",
                (total_media, downloaded_media, job_id),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    invalidate_cache("global_stats")
    return get_job(job_id)


def retry_failed_downloads(job_id: str):
    job = get_job(job_id)
    if not job:
        return None

    conn = get_connection()
    try:
        conn.begin()
        with conn.cursor() as cur:
            cur.execute(
                """SELECT COUNT(*) AS failed_count
                   FROM fb_posts
                   WHERE job_id = %s AND download_status = 'failed'""",
                (job_id,),
            )
            row = cur.fetchone() or {}
            failed_count = int(row.get("failed_count") or 0)
            if failed_count <= 0:
                raise ValueError("This job has no failed media downloads to retry")

            cur.execute(
                """UPDATE fb_posts
                   SET download_status = 'pending',
                       download_error = NULL
                   WHERE job_id = %s AND download_status = 'failed'""",
                (job_id,),
            )
            cur.execute(
                """SELECT COUNT(*) AS total,
                             SUM(CASE WHEN download_status = 'completed' THEN 1 ELSE 0 END) AS downloaded
                   FROM fb_posts
                   WHERE job_id = %s""",
                (job_id,),
            )
            counts = cur.fetchone() or {}
            total_media = int(counts.get('total') or 0)
            downloaded_media = int(counts.get('downloaded') or 0)
            if total_media <= 0:
                raise ValueError("This job has no saved media rows to retry")

            cur.execute(
                """UPDATE fb_scrape_jobs
                   SET status = 'downloading_content',
                       resume_stage = 'downloading_content',
                       control_action = NULL,
                       error_message = NULL,
                       active_worker_stage = NULL,
                       active_worker_token = NULL,
                       active_worker_name = NULL,
                       total_media_count = %s,
                       total_media_downloaded = %s,
                       completed_at = NULL
                   WHERE job_id = %s""",
                (total_media, downloaded_media, job_id),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return get_job(job_id)


def mark_job_completed(job_id: str):
    job = get_job(job_id)
    if not job:
        return None
    if job["status"] not in {"stopped", "failed", "paused"}:
        raise ValueError("Only stopped, failed, or paused jobs can be marked as completed")
    update_job_status(job_id, "completed", extra={
        "resume_stage": None,
        "control_action": None,
        "error_message": None,
    })
    return get_job(job_id)


def rerun_job(job_id: str):
    job = get_job(job_id)
    if not job:
        return None
    if job["status"] not in {"completed", "failed", "stopped"}:
        raise ValueError("Only completed, failed, or stopped jobs can be restarted")

    platform = _job_source_platform(job.get("facebook_url"))

    if platform == TIKTOK_PLATFORM:
        conn = get_connection()
        try:
            conn.begin()
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE fb_posts
                       SET download_status = CASE WHEN download_status = 'completed' THEN 'completed' ELSE 'pending' END,
                           download_error = NULL,
                           video_s3_url = CASE WHEN download_status = 'completed' THEN video_s3_url ELSE NULL END,
                           video_s3_key = CASE WHEN download_status = 'completed' THEN video_s3_key ELSE NULL END,
                           image_s3_urls = CASE WHEN download_status = 'completed' THEN image_s3_urls ELSE NULL END,
                           image_s3_keys = CASE WHEN download_status = 'completed' THEN image_s3_keys ELSE NULL END
                       WHERE job_id = %s""",
                    (job_id,),
                )
                cur.execute(
                    """SELECT COUNT(*) AS total,
                                  SUM(CASE WHEN download_status = 'completed' THEN 1 ELSE 0 END) AS downloaded
                           FROM fb_posts
                           WHERE job_id = %s""",
                    (job_id,),
                )
                counts = cur.fetchone() or {}
                total_posts = int(counts.get('total') or 0)
                downloaded_posts = int(counts.get('downloaded') or 0)
                cur.execute(
                    """UPDATE fb_scrape_jobs
                       SET status = 'pending',
                           resume_stage = 'scraping',
                           control_action = NULL,
                           total_posts_scraped = %s,
                           total_media_count = %s,
                           total_media_downloaded = %s,
                           error_message = NULL,
                           active_worker_stage = NULL,
                           active_worker_token = NULL,
                           active_worker_name = NULL,
                           scrape_resume_cursor = NULL,
                           scrape_resume_page_num = 0,
                           scrape_resume_skip_posts = 0,
                           scrape_good_checkpoints = NULL,
                           scrape_last_progress_at = NULL,
                           created_at = NOW(),
                           started_scraping_at = NULL,
                           scraping_completed_at = NULL,
                           completed_at = NULL
                       WHERE job_id = %s""",
                    (total_posts, total_posts, downloaded_posts, job_id),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        return get_job(job_id)

    conn = get_connection()
    s3_keys = []
    try:
        conn.begin()
        with conn.cursor() as cur:
            s3_keys = _collect_job_s3_keys(cur, job_id)
            cur.execute("DELETE FROM fb_posts WHERE job_id = %s", (job_id,))
            cur.execute(
                """UPDATE fb_scrape_jobs
                   SET status = 'pending',
                       resume_stage = 'scraping',
                       control_action = NULL,
                       total_posts_scraped = 0,
                       total_media_count = 0,
                       total_media_downloaded = 0,
                       error_message = NULL,
                       active_worker_stage = NULL,
                       active_worker_token = NULL,
                       active_worker_name = NULL,
                       scrape_resume_cursor = NULL,
                       scrape_resume_page_num = 0,
                       scrape_resume_skip_posts = 0,
                       scrape_good_checkpoints = NULL,
                       scrape_last_progress_at = NULL,
                       created_at = NOW(),
                       started_scraping_at = NULL,
                       scraping_completed_at = NULL,
                       completed_at = NULL
                   WHERE job_id = %s""",
                (job_id,),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    if s3_keys:
        delete_s3_objects(s3_keys)
    return get_job(job_id)


def delete_job(job_id: str):
    job = get_job(job_id)
    if not job:
        return False
    if job["status"] in RUNNING_STATUSES:
        raise ValueError("Running jobs must be deleted through the control action flow")
    return _delete_job_now(job_id)


def get_all_jobs(limit=100, offset=0):
    cache_key = f"all_jobs:{limit}:{offset}"
    cached = _cache_get(cache_key, ttl=60)
    if cached is not None:
        return cached

    # Performance: NO fb_posts join here. The dashboard polls this endpoint
    # every 5s — if it joins a multi-million-row fb_posts table, the dashboard
    # crawls (or times out entirely) regardless of caching, because the cache
    # has to be repopulated eventually and the user is stuck waiting when it
    # does. Instead we rely on the denormalized counters already on
    # fb_scrape_jobs (total_posts_scraped, total_media_count,
    # total_media_downloaded) that the scraper + downloader keep updated as
    # work happens. The dashboard's media cell already falls back to these
    # values when the per-status counts are missing (??), so the UX is the
    # same minus the live failed/pending split in the list view — which the
    # user can still see by opening the job's detail page (get_job DOES join
    # fb_posts, but only for one job at a time).
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT j.*
                   FROM fb_scrape_jobs j
                   ORDER BY j.created_at DESC
                   LIMIT %s OFFSET %s""",
                (limit, offset),
            )
            jobs = cur.fetchall()
            cur.execute("SELECT COUNT(*) AS total FROM fb_scrape_jobs")
            total = cur.fetchone()["total"]
            cur.execute("SELECT status, COUNT(*) AS cnt FROM fb_scrape_jobs GROUP BY status")
            stats = {row["status"]: row["cnt"] for row in cur.fetchall()}
            result = {"jobs": jobs, "total": total, "stats": stats}
            _cache_set(cache_key, result)
            return result
    finally:
        conn.close()


def get_global_stats():
    # 60s cache: this query scans the entire fb_posts table for SUM/CASE
    # aggregates. Frequent re-runs on a large dataset are expensive and the
    # numbers don't change meaningfully second-to-second.
    cached = _cache_get("global_stats", ttl=60)
    if cached is not None:
        return cached

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS total_pages, COALESCE(SUM(total_posts_scraped), 0) AS total_posts FROM fb_scrape_jobs")
            jobs_row = cur.fetchone()
            cur.execute("""
                SELECT
                    SUM(CASE WHEN download_status = 'completed' THEN 1 ELSE 0 END) AS total_media_s3,
                    SUM(CASE WHEN download_status = 'failed' THEN 1 ELSE 0 END) AS total_media_failed,
                    SUM(CASE WHEN download_status = 'completed' AND has_video = 1 THEN 1 ELSE 0 END) AS s3_videos,
                    SUM(CASE WHEN download_status = 'completed' AND has_image = 1 AND has_video = 0 THEN 1 ELSE 0 END) AS s3_images,
                    SUM(CASE WHEN download_status = 'completed' AND has_video = 0 AND has_image = 0 THEN 1 ELSE 0 END) AS s3_text
                FROM fb_posts
            """)
            dl_row = cur.fetchone()
            result = {
                "total_pages": int(jobs_row["total_pages"] or 0),
                "total_posts": int(jobs_row["total_posts"] or 0),
                "total_media_s3": int(dl_row["total_media_s3"] or 0),
                "total_media_failed": int(dl_row["total_media_failed"] or 0),
                "s3_videos": int(dl_row["s3_videos"] or 0),
                "s3_images": int(dl_row["s3_images"] or 0),
                "s3_text": int(dl_row["s3_text"] or 0),
            }
            _cache_set("global_stats", result)
            return result
    finally:
        conn.close()


def stream_all_s3_content():
    """Generator that yields rows of (page_name, facebook_url, s3_url, media_type)
    for every S3 link across all completed posts. Uses SSSCursor for streaming."""
    import pymysql.cursors
    conn = get_connection()
    try:
        cur = conn.cursor(pymysql.cursors.SSDictCursor)
        cur.execute("""
            SELECT j.page_name, j.facebook_url AS source_url,
                   p.video_s3_url, p.image_s3_urls, p.has_video, p.has_image
            FROM fb_posts p
            JOIN fb_scrape_jobs j ON j.job_id = p.job_id
            WHERE p.download_status = 'completed'
            ORDER BY j.page_name, p.id
        """)
        while True:
            row = cur.fetchone()
            if row is None:
                break
            page = row["page_name"] or row["source_url"] or ""
            source = row["source_url"] or ""
            if row["video_s3_url"]:
                yield (page, source, row["video_s3_url"], "video")
            img_json = row.get("image_s3_urls")
            if img_json:
                try:
                    urls = json.loads(img_json) if isinstance(img_json, str) else img_json
                    if isinstance(urls, list):
                        for url in urls:
                            if url:
                                yield (page, source, url, "image")
                except (json.JSONDecodeError, TypeError):
                    pass
        cur.close()
    finally:
        conn.close()


def _job_source_platform(source_url: str) -> str:
    host = (urlparse(source_url or "").netloc or "").split(":", 1)[0].lower()
    if "tiktok.com" in host:
        return TIKTOK_PLATFORM
    if "instagram.com" in host:
        return INSTAGRAM_PLATFORM
    return FACEBOOK_PLATFORM


def _normalize_scrape_platform(platform: str | None) -> str:
    if platform == TIKTOK_PLATFORM:
        return TIKTOK_PLATFORM
    if platform == INSTAGRAM_PLATFORM:
        return INSTAGRAM_PLATFORM
    return FACEBOOK_PLATFORM


def _platform_predicate_sql(platform: str) -> str:
    normalized = _normalize_scrape_platform(platform)
    if normalized == TIKTOK_PLATFORM:
        return "facebook_url LIKE '%%tiktok.com%%'"
    if normalized == INSTAGRAM_PLATFORM:
        return "facebook_url LIKE '%%instagram.com%%'"
    return "facebook_url NOT LIKE '%%tiktok.com%%' AND facebook_url NOT LIKE '%%instagram.com%%'"


def _normalize_source_category(source_category: str | None) -> str | None:
    if source_category is None:
        return None
    normalized = str(source_category).strip()
    if not normalized or normalized.lower() == 'all':
        return None
    if normalized.lower() in {'uncategorized', 'none', 'no category'}:
        return '__uncategorized__'
    return normalized


def _append_source_category_filter(where_clauses, params, source_category: str | None, job_alias: str = 'j'):
    normalized = _normalize_source_category(source_category)
    if normalized is None:
        return
    if normalized == '__uncategorized__':
        where_clauses.append(f"COALESCE(NULLIF(TRIM({job_alias}.source_category), ''), '') = ''")
    else:
        where_clauses.append(f"{job_alias}.source_category = %s")
        params.append(normalized)


def _append_post_type_filter(where_clauses, post_type: str, post_alias: str = 'p'):
    if post_type == 'video':
        where_clauses.append(f"{post_alias}.has_video = 1")
    elif post_type == 'image':
        where_clauses.append(f"{post_alias}.has_image = 1 AND {post_alias}.has_video = 0")
    elif post_type == 'text':
        where_clauses.append(f"{post_alias}.has_video = 0 AND {post_alias}.has_image = 0")


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


def _build_claim_set_clause():
    """Build the SET clause for scraping job claims, omitting columns that don't exist.

    Returns (set_sql, value_builder) where value_builder is a function that
    returns the parameter list given (worker_token, worker_name, assigned_dl_worker).
    """
    cols = _table_columns("fb_scrape_jobs")
    has_name = (not cols) or "active_worker_name" in cols
    has_assigned = (not cols) or "assigned_download_worker" in cols

    parts = ["active_worker_stage = %s", "active_worker_token = %s"]
    if has_name:
        parts.append("active_worker_name = %s")
    if has_assigned:
        parts.append("assigned_download_worker = COALESCE(assigned_download_worker, %s)")
    parts.extend(["scrape_last_progress_at = NOW()", "completed_at = NULL"])

    def values(worker_token, worker_name, assigned_dl_worker):
        out = [SCRAPING_WORKER_STAGE, worker_token]
        if has_name:
            out.append(worker_name)
        if has_assigned:
            out.append(assigned_dl_worker)
        return out

    return ", ".join(parts), values


def claim_next_scraping_job(platform: str = FACEBOOK_PLATFORM, worker_name: str = None):
    """Claim the oldest interrupted scraping job for the requested source platform."""
    set_sql, build_values = _build_claim_set_clause()
    conn = get_connection()
    try:
        for _ in range(CLAIM_RETRY_LIMIT):
            stale_before = _worker_lease_cutoff()
            worker_token = str(uuid.uuid4())
            predicate = _platform_predicate_sql(platform)
            with conn.cursor() as cur:
                cur.execute(
                    f"""SELECT job_id FROM fb_scrape_jobs
                       WHERE status = 'scraping'
                         AND {predicate}
                         AND (
                             active_worker_token IS NULL
                             OR active_worker_stage IS NULL
                             OR active_worker_stage <> %s
                             OR scrape_last_progress_at IS NULL
                             OR scrape_last_progress_at < %s
                         )
                       ORDER BY COALESCE(started_scraping_at, created_at) ASC, created_at ASC
                       LIMIT 1""",
                    (SCRAPING_WORKER_STAGE, stale_before),
                )
                row = cur.fetchone()
                if not row:
                    return None

                assigned_dl_worker = derive_download_worker_name(worker_name)
                params = build_values(worker_token, worker_name, assigned_dl_worker)
                params.extend([row["job_id"], SCRAPING_WORKER_STAGE, stale_before])
                cur.execute(
                    f"""UPDATE fb_scrape_jobs
                       SET {set_sql}
                       WHERE job_id = %s
                         AND status = 'scraping'
                         AND {predicate}
                         AND (
                             active_worker_token IS NULL
                             OR active_worker_stage IS NULL
                             OR active_worker_stage <> %s
                             OR scrape_last_progress_at IS NULL
                             OR scrape_last_progress_at < %s
                         )""",
                    params,
                )
                if cur.rowcount != 1:
                    continue

                cur.execute("SELECT * FROM fb_scrape_jobs WHERE job_id = %s", (row["job_id"],))
                return cur.fetchone()
        return None
    finally:
        conn.close()


def derive_download_worker_name(scraping_worker_name: str | None) -> str | None:
    """Map a scraping worker name to its paired download worker name.

    'instagram-3' -> 'instagram-download-3'
    'facebook-1'  -> 'facebook-download-1'
    'tiktok-7'    -> 'tiktok-download-7'

    The download worker that consumes a job is determined at scrape-claim time
    and persisted in fb_scrape_jobs.assigned_download_worker.
    """
    if not scraping_worker_name:
        return None
    parts = scraping_worker_name.rsplit('-', 1)
    if len(parts) != 2 or not parts[1].isdigit():
        return None
    return f"{parts[0]}-download-{parts[1]}"


def claim_next_pending_job(platform: str = FACEBOOK_PLATFORM, worker_name: str = None):
    """Claim the oldest pending job for the requested source platform and move it to scraping."""
    cols = _table_columns("fb_scrape_jobs")
    has_name = (not cols) or "active_worker_name" in cols
    has_assigned = (not cols) or "assigned_download_worker" in cols

    set_parts = [
        "status = 'scraping'",
        "resume_stage = 'scraping'",
        "control_action = NULL",
        "active_worker_stage = %s",
        "active_worker_token = %s",
    ]
    if has_name:
        set_parts.append("active_worker_name = %s")
    if has_assigned:
        set_parts.append("assigned_download_worker = COALESCE(assigned_download_worker, %s)")
    set_parts.extend([
        "started_scraping_at = NOW()",
        "scrape_last_progress_at = NOW()",
        "error_message = NULL",
        "completed_at = NULL",
    ])
    set_sql = ", ".join(set_parts)

    conn = get_connection()
    try:
        for _ in range(CLAIM_RETRY_LIMIT):
            worker_token = str(uuid.uuid4())
            predicate = _platform_predicate_sql(platform)
            assigned_dl_worker = derive_download_worker_name(worker_name)
            with conn.cursor() as cur:
                cur.execute(
                    f"""SELECT job_id FROM fb_scrape_jobs
                       WHERE status = 'pending'
                         AND {predicate}
                       ORDER BY created_at ASC
                       LIMIT 1"""
                )
                row = cur.fetchone()
                if not row:
                    return None
                job_id = row["job_id"]

                params = [SCRAPING_WORKER_STAGE, worker_token]
                if has_name:
                    params.append(worker_name)
                if has_assigned:
                    params.append(assigned_dl_worker)
                params.append(job_id)
                cur.execute(
                    f"UPDATE fb_scrape_jobs SET {set_sql} "
                    f"WHERE job_id = %s AND status = 'pending' AND {predicate}",
                    params,
                )
                if cur.rowcount != 1:
                    continue

                cur.execute("SELECT * FROM fb_scrape_jobs WHERE job_id = %s", (job_id,))
                return cur.fetchone()
        return None
    finally:
        conn.close()


def claim_next_downloading_job(worker_name: str = None):
    """Claim the oldest job in downloading_content (legacy — used when no per-lane assignment).

    Tolerant of missing assigned_download_worker column.

    Note: this does NOT update active_worker_name — that field is reserved for
    the scraping worker. The UI derives the active download worker from the
    job's assigned_download_worker column (or from the scraping worker name
    plus the download lane convention).
    """
    cols = _table_columns("fb_scrape_jobs")
    has_assigned = (not cols) or "assigned_download_worker" in cols

    where_extra = " AND assigned_download_worker IS NULL" if has_assigned else ""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""SELECT * FROM fb_scrape_jobs
                    WHERE status = 'downloading_content'{where_extra}
                    ORDER BY COALESCE(scraping_completed_at, created_at) ASC LIMIT 1 FOR UPDATE"""
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


def claim_download_job_for_worker(download_worker_name: str, batch_trigger: int = 5):
    """Claim a downloadable job assigned to *download_worker_name*.

    Eligibility:
      - status = 'downloading_content' (always — finish any leftovers), OR
      - status = 'scraping' AND has >= batch_trigger pending download posts (drip-feed mode)

    Priority: downloading_content jobs ahead of mid-scrape drip jobs, then by age.
    Returns None when the schema lacks ``assigned_download_worker`` (per-lane
    workers can't filter, so the legacy sweeper handles all jobs instead).
    """
    cols = _table_columns("fb_scrape_jobs")
    has_assigned = (not cols) or "assigned_download_worker" in cols
    if not has_assigned:
        return None

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT j.*, COALESCE(p.pending_count, 0) AS pending_count
                   FROM fb_scrape_jobs j
                   LEFT JOIN (
                       SELECT job_id, COUNT(*) AS pending_count
                       FROM fb_posts
                       WHERE download_status = 'pending'
                       GROUP BY job_id
                   ) p ON p.job_id = j.job_id
                   WHERE j.assigned_download_worker = %s
                     AND (
                         j.status = 'downloading_content'
                         OR (j.status = 'scraping' AND COALESCE(p.pending_count, 0) >= %s)
                     )
                   ORDER BY
                     CASE WHEN j.status = 'downloading_content' THEN 0 ELSE 1 END,
                     COALESCE(j.scraping_completed_at, j.started_scraping_at, j.created_at) ASC
                   LIMIT 1
                   FOR UPDATE""",
                (download_worker_name, batch_trigger),
            )
            row = cur.fetchone()
            if not row:
                conn.rollback()
                return None
            # Do NOT touch active_worker_name here — that column is reserved for
            # the scraping worker (otherwise the download worker would
            # overwrite the scraping worker's name during drip-feed and the
            # UI would lose track of who's actually scraping).
            conn.commit()
            return row
    except Exception:
        conn.rollback()
        return None
    finally:
        conn.close()


def update_job_status(
    job_id: str,
    status: str,
    error_message: str = None,
    extra: dict = None,
    clear_scrape_checkpoint: bool = False,
):
    conn = get_connection()
    try:
        fields = ["status = %s"]
        values = [status]

        if status == "pending":
            fields.extend([
                "control_action = NULL",
                "active_worker_stage = NULL",
                "active_worker_token = NULL",
                "active_worker_name = NULL",
                "completed_at = NULL",
            ])
        elif status == "scraping":
            fields.extend([
                "resume_stage = 'scraping'",
                "control_action = NULL",
                "started_scraping_at = COALESCE(started_scraping_at, NOW())",
                "scrape_last_progress_at = NOW()",
                "completed_at = NULL",
            ])
        elif status == "downloading_content":
            fields.extend([
                "resume_stage = 'downloading_content'",
                "control_action = NULL",
                "active_worker_stage = NULL",
                "active_worker_token = NULL",
                "active_worker_name = NULL",
                "scraping_completed_at = COALESCE(scraping_completed_at, NOW())",
                "scrape_last_progress_at = NOW()",
                "completed_at = NULL",
            ])
            clear_scrape_checkpoint = True
        elif status == "paused":
            fields.extend([
                "control_action = NULL",
                "active_worker_stage = NULL",
                "active_worker_token = NULL",
                "active_worker_name = NULL",
                "scrape_last_progress_at = NOW()",
                "completed_at = NULL",
            ])
        elif status == "stopped":
            fields.extend([
                "control_action = NULL",
                "active_worker_stage = NULL",
                "active_worker_token = NULL",
                "active_worker_name = NULL",
                "scrape_last_progress_at = NOW()",
                "completed_at = NOW()",
            ])
        elif status == "completed":
            fields.extend([
                "control_action = NULL",
                "active_worker_stage = NULL",
                "active_worker_token = NULL",
                "active_worker_name = NULL",
                "completed_at = NOW()",
            ])
        elif status == "failed":
            fields.extend([
                "control_action = NULL",
                "active_worker_stage = NULL",
                "active_worker_token = NULL",
                "active_worker_name = NULL",
                "completed_at = COALESCE(completed_at, NOW())",
            ])

        if error_message is not None:
            fields.append("error_message = %s")
            values.append(error_message)

        if clear_scrape_checkpoint:
            # NOTE: deliberately keep scrape_resume_cursor + scrape_resume_page_num
            # populated on completion. They act as the "last position scraped"
            # fallback when the optional last_scraped_* columns don't exist on
            # the schema (and even when they do, this gives Continue a working
            # path with zero extra DB cost). Clearing them used to be tidiness
            # only — it now actively breaks Continue-on-completed.
            fields.extend([
                "scrape_resume_skip_posts = 0",
                "scrape_good_checkpoints = NULL",
            ])

        if extra:
            for key, value in extra.items():
                fields.append(f"{key} = %s")
                values.append(value)

        values.append(job_id)
        sql = f"UPDATE fb_scrape_jobs SET {', '.join(fields)} WHERE job_id = %s"
        with conn.cursor() as cur:
            cur.execute(sql, values)
        conn.commit()
    finally:
        conn.close()


def update_job_progress(
    job_id: str,
    total_posts_scraped: int = None,
    page_name: str = None,
    page_id: str = None,
    total_media_count: int = None,
    total_media_downloaded: int = None,
):
    conn = get_connection()
    try:
        fields = ["scrape_last_progress_at = NOW()"]
        values = []
        if total_posts_scraped is not None:
            fields.append("total_posts_scraped = %s")
            values.append(total_posts_scraped)
        if page_name is not None:
            fields.append("page_name = %s")
            values.append(page_name)
        if page_id is not None:
            fields.append("page_id = %s")
            values.append(page_id)
        if total_media_count is not None:
            fields.append("total_media_count = %s")
            values.append(total_media_count)
        if total_media_downloaded is not None:
            fields.append("total_media_downloaded = %s")
            values.append(total_media_downloaded)
        values.append(job_id)
        sql = f"UPDATE fb_scrape_jobs SET {', '.join(fields)} WHERE job_id = %s"
        with conn.cursor() as cur:
            cur.execute(sql, values)
        conn.commit()
    finally:
        conn.close()


def update_job_scrape_checkpoint(
    job_id: str,
    resume_cursor,
    resume_page_num: int,
    resume_skip_posts: int = 0,
    total_posts_scraped: int = None,
):
    conn = get_connection()
    try:
        fields = [
            "resume_stage = 'scraping'",
            "scrape_resume_cursor = %s",
            "scrape_resume_page_num = %s",
            "scrape_resume_skip_posts = %s",
            "scrape_last_progress_at = NOW()",
        ]
        values = [resume_cursor, resume_page_num, resume_skip_posts]
        if total_posts_scraped is not None:
            fields.append("total_posts_scraped = %s")
            values.append(total_posts_scraped)
        values.append(job_id)
        sql = f"UPDATE fb_scrape_jobs SET {', '.join(fields)} WHERE job_id = %s"
        with conn.cursor() as cur:
            cur.execute(sql, values)
        conn.commit()
    finally:
        conn.close()


def record_job_good_checkpoint(
    job_id: str,
    resume_cursor,
    resume_page_num: int,
    resume_skip_posts: int = 0,
    total_posts_scraped: int = None,
):
    if int(resume_page_num or 0) <= 0:
        return

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT scrape_good_checkpoints FROM fb_scrape_jobs WHERE job_id = %s",
                (job_id,),
            )
            row = cur.fetchone() or {}
            checkpoints = _load_good_checkpoints(row.get('scrape_good_checkpoints'))
            checkpoint = {
                'resume_cursor': resume_cursor,
                'resume_page_num': int(resume_page_num),
                'resume_skip_posts': int(resume_skip_posts or 0),
                'total_posts_scraped': int(total_posts_scraped or 0),
            }
            signature = _checkpoint_signature(checkpoint)
            checkpoints = [item for item in checkpoints if _checkpoint_signature(item) != signature]
            checkpoints.append(checkpoint)
            checkpoints = checkpoints[-GOOD_CHECKPOINT_HISTORY_LIMIT:]
            cur.execute(
                """UPDATE fb_scrape_jobs
                   SET scrape_good_checkpoints = %s,
                       scrape_last_progress_at = NOW()
                   WHERE job_id = %s""",
                (json.dumps(checkpoints), job_id),
            )
        conn.commit()
    finally:
        conn.close()


def apply_job_control_action(job_id: str, stage: str, worker_token: str = None):
    state = get_job_control_state(job_id)
    if not state:
        return "deleted"

    if worker_token and (
        state.get("active_worker_stage") != stage
        or state.get("active_worker_token") != worker_token
    ):
        return "lost_claim"

    action = state.get("control_action")
    if not action:
        return None

    if action == "pause":
        update_job_status(job_id, "paused", extra={"resume_stage": stage})
        return "paused"
    if action == "stop":
        update_job_status(job_id, "stopped", extra={"resume_stage": stage})
        return "stopped"
    if action == "delete":
        _delete_job_now(job_id)
        return "deleted"
    return None


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


def get_job_post_count(job_id: str) -> int:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS total FROM fb_posts WHERE job_id = %s", (job_id,))
            row = cur.fetchone()
            return int((row or {}).get("total") or 0)
    finally:
        conn.close()


def get_download_progress(job_id: str):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT download_status, COUNT(*) AS cnt
                   FROM fb_posts
                   WHERE job_id = %s
                   GROUP BY download_status""",
                (job_id,),
            )
            counts = {row["download_status"]: row["cnt"] for row in cur.fetchall()}
            total = sum(counts.values())
            pending = counts.get("pending", 0)
            downloading = counts.get("downloading", 0)
            completed = counts.get("completed", 0)
            failed = counts.get("failed", 0)
            remaining = pending + downloading
            processed = total - remaining
            return {
                "total": total,
                "processed": processed,
                "remaining": remaining,
                "pending": pending,
                "downloading": downloading,
                "completed": completed,
                "failed": failed,
            }
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
            elif post_type == "failed":
                where += " AND download_status = 'failed'"

            cur.execute(f"SELECT COUNT(*) AS total FROM fb_posts WHERE {where}", params)
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


def get_posts_for_all_jobs(post_type: str = "all", limit: int = 50, offset: int = 0, source_category: str | None = None):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            where_clauses = ["1=1"]
            params = []
            _append_post_type_filter(where_clauses, post_type, 'p')
            _append_source_category_filter(where_clauses, params, source_category, 'j')
            where = ' AND '.join(where_clauses)

            latest_posts_sql = "(SELECT MAX(id) AS id FROM fb_posts GROUP BY post_link_hash) latest"

            cur.execute(
                f"""SELECT COUNT(*) AS total
                       FROM fb_posts p
                       JOIN {latest_posts_sql} ON latest.id = p.id
                       JOIN fb_scrape_jobs j ON j.job_id = p.job_id
                       WHERE {where}""",
                params,
            )
            total = cur.fetchone()["total"]

            cur.execute(
                f"""SELECT p.*, j.page_name, j.page_id, j.facebook_url, j.source_category,
                                  j.status AS job_status, j.created_at AS job_created_at
                       FROM fb_posts p
                       JOIN {latest_posts_sql} ON latest.id = p.id
                       JOIN fb_scrape_jobs j ON j.job_id = p.job_id
                       WHERE {where}
                       ORDER BY p.published_timestamp DESC, p.id DESC
                       LIMIT %s OFFSET %s""",
                params + [limit, offset],
            )
            posts = cur.fetchall()
            return {"posts": posts, "total": total}
    finally:
        conn.close()


def get_all_posts_summary(source_category: str | None = None):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            latest_posts_sql = "(SELECT MAX(id) AS id FROM fb_posts GROUP BY post_link_hash) latest"
            where_clauses = ["1=1"]
            params = []
            _append_source_category_filter(where_clauses, params, source_category, 'j')
            where = ' AND '.join(where_clauses)

            cur.execute(
                f"""SELECT COUNT(*) AS total,
                              SUM(CASE WHEN p.has_video = 1 THEN 1 ELSE 0 END) AS video_total,
                              SUM(CASE WHEN p.has_image = 1 AND p.has_video = 0 THEN 1 ELSE 0 END) AS image_total,
                              SUM(CASE WHEN p.has_video = 0 AND p.has_image = 0 THEN 1 ELSE 0 END) AS text_total,
                              COUNT(DISTINCT COALESCE(NULLIF(j.page_id, ''), j.facebook_url, p.job_id)) AS source_total
                       FROM fb_posts p
                       JOIN {latest_posts_sql} ON latest.id = p.id
                       JOIN fb_scrape_jobs j ON j.job_id = p.job_id
                       WHERE {where}""",
                params,
            )
            row = cur.fetchone() or {}

            cur.execute(
                f"""SELECT COALESCE(NULLIF(TRIM(j.source_category), ''), '__uncategorized__') AS category_key,
                              COUNT(*) AS total
                       FROM fb_posts p
                       JOIN {latest_posts_sql} ON latest.id = p.id
                       JOIN fb_scrape_jobs j ON j.job_id = p.job_id
                       GROUP BY category_key
                       ORDER BY total DESC, category_key ASC"""
            )
            category_rows = cur.fetchall() or []
            categories = []
            for category_row in category_rows:
                key = category_row.get('category_key') or '__uncategorized__'
                if key == '__uncategorized__':
                    categories.append({
                        'value': 'uncategorized',
                        'label': 'No Category',
                        'total': int(category_row.get('total') or 0),
                    })
                else:
                    categories.append({
                        'value': key,
                        'label': key,
                        'total': int(category_row.get('total') or 0),
                    })

            return {
                "total": int(row.get("total") or 0),
                "video": int(row.get("video_total") or 0),
                "image": int(row.get("image_total") or 0),
                "text": int(row.get("text_total") or 0),
                "sources": int(row.get("source_total") or 0),
                "categories": categories,
            }
    finally:
        conn.close()


def get_all_posts_deduped(post_type: str = "all", source_category: str | None = None):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            latest_posts_sql = "(SELECT MAX(id) AS id FROM fb_posts GROUP BY post_link_hash) latest"
            where_clauses = ["1=1"]
            params = []
            _append_post_type_filter(where_clauses, post_type, 'p')
            _append_source_category_filter(where_clauses, params, source_category, 'j')
            where = ' AND '.join(where_clauses)
            cur.execute(
                f"""SELECT p.*, j.page_name, j.page_id, j.facebook_url, j.source_category,
                              j.status AS job_status, j.created_at AS job_created_at
                       FROM fb_posts p
                       JOIN {latest_posts_sql} ON latest.id = p.id
                       JOIN fb_scrape_jobs j ON j.job_id = p.job_id
                       WHERE {where}
                       ORDER BY p.published_timestamp DESC, p.id DESC""",
                params,
            )
            return cur.fetchall()
    finally:
        conn.close()


def get_pending_download_posts(job_id: str):
    """Return posts whose media still needs work; downloading rows are retried after restarts."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT * FROM fb_posts
                   WHERE job_id = %s AND download_status IN ('pending', 'downloading')
                   ORDER BY id ASC""",
                (job_id,),
            )
            return cur.fetchall()
    finally:
        conn.close()


def find_downloaded_media_for_post_link(post_link: str, platform: str | None = None, exclude_job_id: str | None = None):
    link = (post_link or '').strip()
    if not link:
        return None
    link_hash = _hash_url(link)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            where = [
                "p.post_link_hash = %s",
                "p.download_status = 'completed'",
                "(p.video_s3_url IS NOT NULL OR p.image_s3_urls IS NOT NULL)",
            ]
            params = [link_hash]
            if platform == TIKTOK_PLATFORM:
                where.append("LOWER(j.facebook_url) LIKE %s")
                params.append("%tiktok.com%")
            elif platform == INSTAGRAM_PLATFORM:
                where.append("LOWER(j.facebook_url) LIKE %s")
                params.append("%instagram.com%")
            elif platform == FACEBOOK_PLATFORM:
                where.append("LOWER(j.facebook_url) NOT LIKE %s AND LOWER(j.facebook_url) NOT LIKE %s")
                params.extend(["%tiktok.com%", "%instagram.com%"])
            if exclude_job_id:
                where.append("p.job_id <> %s")
                params.append(exclude_job_id)

            cur.execute(
                f"""SELECT p.job_id, p.post_link, p.video_s3_url, p.video_s3_key,
                              p.image_s3_urls, p.image_s3_keys
                       FROM fb_posts p
                       JOIN fb_scrape_jobs j ON j.job_id = p.job_id
                       WHERE {' AND '.join(where)}
                       ORDER BY p.id DESC
                       LIMIT 1""",
                params,
            )
            return cur.fetchone()
    finally:
        conn.close()


def retry_single_post_download(post_id: int):
    conn = get_connection()
    try:
        conn.begin()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, job_id, download_status FROM fb_posts WHERE id = %s",
                (post_id,),
            )
            post = cur.fetchone()
            if not post:
                return None
            if post["download_status"] != "failed":
                raise ValueError("Only posts with failed downloads can be retried")

            cur.execute(
                """UPDATE fb_posts
                   SET download_status = 'pending', download_error = NULL
                   WHERE id = %s AND download_status = 'failed'""",
                (post_id,),
            )

            job_id = post["job_id"]
            cur.execute(
                "SELECT status FROM fb_scrape_jobs WHERE job_id = %s",
                (job_id,),
            )
            job = cur.fetchone()
            if job and job["status"] not in ("scraping", "downloading_content", "pending"):
                cur.execute(
                    """SELECT COUNT(*) AS total,
                              SUM(CASE WHEN download_status = 'completed' THEN 1 ELSE 0 END) AS downloaded
                       FROM fb_posts WHERE job_id = %s""",
                    (job_id,),
                )
                counts = cur.fetchone() or {}
                cur.execute(
                    """UPDATE fb_scrape_jobs
                       SET status = 'downloading_content',
                           resume_stage = 'downloading_content',
                           control_action = NULL,
                           error_message = NULL,
                           active_worker_stage = NULL,
                           active_worker_token = NULL,
                           active_worker_name = NULL,
                           total_media_count = %s,
                           total_media_downloaded = %s,
                           completed_at = NULL
                       WHERE job_id = %s""",
                    (
                        int(counts.get("total") or 0),
                        int(counts.get("downloaded") or 0),
                        job_id,
                    ),
                )
        conn.commit()
        return {"post_id": post_id, "job_id": job_id}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def update_post_download(
    post_id: int,
    status: str,
    error: str = None,
    video_s3_url: str = None,
    video_s3_key: str = None,
    image_s3_urls: list = None,
    image_s3_keys: list = None,
):
    conn = get_connection()
    try:
        fields = ["download_status = %s"]
        values = [status]
        if error is not None:
            fields.append("download_error = %s")
            values.append(error)
        if video_s3_url is not None:
            fields.append("video_s3_url = %s")
            values.append(video_s3_url)
        if video_s3_key is not None:
            fields.append("video_s3_key = %s")
            values.append(video_s3_key)
        if image_s3_urls is not None:
            fields.append("image_s3_urls = %s")
            values.append(json.dumps(image_s3_urls))
        if image_s3_keys is not None:
            fields.append("image_s3_keys = %s")
            values.append(json.dumps(image_s3_keys))
        values.append(post_id)
        sql = f"UPDATE fb_posts SET {', '.join(fields)} WHERE id = %s"
        with conn.cursor() as cur:
            cur.execute(sql, values)
        conn.commit()
    finally:
        conn.close()
