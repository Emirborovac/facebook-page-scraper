import logging
import os
import re

import pymysql
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Optional admin credentials used ONLY at startup to apply DDL migrations
# (CREATE/ALTER TABLE, CREATE INDEX). Falls back to the regular DB user if
# unset. The admin connection is closed immediately after migrations run, so
# the app does not hold elevated credentials at runtime.
DB_ADMIN_USER = os.getenv("DB_ADMIN_USER") or DB_USER
DB_ADMIN_PASSWORD = os.getenv("DB_ADMIN_PASSWORD") or DB_PASSWORD

JOB_STATUS_VALUES = (
    "pending",
    "paused",
    "scraping",
    "downloading_content",
    "stopped",
    "completed",
    "failed",
)


# ── Schema-tolerant cursor ─────────────────────────────────────────────
# Some optional columns (active_worker_name, assigned_download_worker,
# normalized_url, last_scraped_*) may not exist when the app user can't ALTER.
# When that happens, this cursor automatically strips references to those
# columns from outgoing SET / column-list SQL so existing code keeps working.

# Populated at first init_db() call (or first SHOW COLUMNS).
_MISSING_COLUMNS: set[str] = set()

# Columns that are safe to silently strip when missing. They're all metadata
# the app would like to record but can do without.
_OPTIONAL_COLUMNS = {
    "active_worker_name",
    "assigned_download_worker",
    "normalized_url",
    "last_scraped_cursor",
    "last_scraped_page_num",
    "last_scraped_at",
}


def _set_missing_columns(missing):
    global _MISSING_COLUMNS
    _MISSING_COLUMNS = {c for c in missing if c in _OPTIONAL_COLUMNS}


def _filter_sql_for_missing_columns(sql: str) -> str:
    """Remove SET / INSERT references to columns that don't exist on the table.

    Conservative: only handles two patterns the app uses:
      1. ``<col> = ...,`` inside SET clauses (with optional trailing comma)
      2. column lists in INSERTs are NOT touched here — those funcs are already
         schema-aware via _has_column().
    """
    if not _MISSING_COLUMNS or not sql:
        return sql
    for col in _MISSING_COLUMNS:
        # Strip "<col> = <value>,\n" (with surrounding whitespace) — most common form.
        sql = re.sub(
            rf"\s*{re.escape(col)}\s*=\s*[^,\n)]+,",
            "",
            sql,
        )
        # Strip a trailing-no-comma form: "<col> = <value>" right before WHERE/end.
        sql = re.sub(
            rf",\s*{re.escape(col)}\s*=\s*[^,\n)]+(?=(\s*WHERE|\s*$))",
            "",
            sql,
            flags=re.IGNORECASE,
        )
    return sql


class _SchemaTolerantCursor(pymysql.cursors.DictCursor):
    """DictCursor that auto-strips missing-column references from SQL."""

    def execute(self, query, args=None):
        if _MISSING_COLUMNS and isinstance(query, str):
            query = _filter_sql_for_missing_columns(query)
        return super().execute(query, args)

    def executemany(self, query, args):
        if _MISSING_COLUMNS and isinstance(query, str):
            query = _filter_sql_for_missing_columns(query)
        return super().executemany(query, args)


def get_connection():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset="utf8mb4",
        cursorclass=_SchemaTolerantCursor,
        autocommit=True,
        connect_timeout=10,
    )


def get_admin_connection():
    """Connect with DDL-capable credentials for one-shot schema migrations.

    Falls back to the regular app user when DB_ADMIN_USER is not set. The
    caller is expected to close this connection right after migrations run.
    """
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_ADMIN_USER,
        password=DB_ADMIN_PASSWORD,
        database=DB_NAME,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
        connect_timeout=10,
    )


def _column_exists(cur, table_name: str, column_name: str) -> bool:
    cur.execute(f"SHOW COLUMNS FROM {table_name} LIKE %s", (column_name,))
    return cur.fetchone() is not None


def _is_permission_error(exc: Exception) -> bool:
    """Best-effort detection of MySQL DDL permission errors (1142, 1227, 1044)."""
    code = getattr(exc, "args", [None])[0] if getattr(exc, "args", None) else None
    if code in (1142, 1227, 1044):
        return True
    msg = str(exc).lower()
    return "alter command denied" in msg or "access denied" in msg


def _ensure_job_status_enum(cur):
    enum_sql = ",".join(f"'{value}'" for value in JOB_STATUS_VALUES)
    try:
        cur.execute(
            f"""ALTER TABLE fb_scrape_jobs
                MODIFY COLUMN status ENUM({enum_sql}) DEFAULT 'pending'"""
        )
    except Exception as exc:
        if _is_permission_error(exc):
            logging.warning(
                "[DB] No ALTER permission to normalize status ENUM — assuming it's already correct. "
                "If new statuses were added, run the migration manually with a privileged user."
            )
        else:
            raise


def _ensure_fb_scrape_jobs_columns(cur):
    required_columns = [
        ("source_category", "VARCHAR(120) DEFAULT NULL"),
        ("resume_stage", "VARCHAR(32) DEFAULT 'scraping'"),
        ("control_action", "VARCHAR(16) DEFAULT NULL"),
        ("active_worker_stage", "VARCHAR(32) DEFAULT NULL"),
        ("active_worker_token", "VARCHAR(64) DEFAULT NULL"),
        ("active_worker_name", "VARCHAR(64) DEFAULT NULL"),
        ("assigned_download_worker", "VARCHAR(64) DEFAULT NULL"),
        ("scrape_resume_cursor", "TEXT DEFAULT NULL"),
        ("scrape_resume_page_num", "INT DEFAULT 0"),
        ("scrape_resume_skip_posts", "INT DEFAULT 0"),
        ("scrape_good_checkpoints", "LONGTEXT DEFAULT NULL"),
        ("scrape_last_progress_at", "DATETIME DEFAULT NULL"),
        # Persistent record of the last cursor/page seen — preserved across
        # job completion so "scan for new posts" can pick up where we ended.
        ("last_scraped_cursor", "TEXT DEFAULT NULL"),
        ("last_scraped_page_num", "INT DEFAULT 0"),
        ("last_scraped_at", "DATETIME DEFAULT NULL"),
        # Normalized URL used for duplicate detection. Populated lazily.
        ("normalized_url", "VARCHAR(512) DEFAULT NULL"),
    ]
    missing = []
    for column_name, definition in required_columns:
        if _column_exists(cur, "fb_scrape_jobs", column_name):
            continue
        try:
            cur.execute(f"ALTER TABLE fb_scrape_jobs ADD COLUMN {column_name} {definition}")
        except Exception as exc:
            if _is_permission_error(exc):
                missing.append((column_name, definition))
            else:
                raise

    if missing:
        sql_lines = "\n".join(
            f"  ALTER TABLE fb_scrape_jobs ADD COLUMN {name} {defn};"
            for name, defn in missing
        )
        logging.warning(
            "[DB] No ALTER permission to add %d column(s). The app will start "
            "but features depending on these columns will fail. Run this SQL "
            "manually with a privileged MySQL user:\n%s",
            len(missing), sql_lines,
        )

    # The two backfill UPDATEs below only need DML; they should always succeed
    # for the app user. Wrap defensively anyway.
    try:
        cur.execute(
            """UPDATE fb_scrape_jobs
               SET resume_stage = CASE
                   WHEN status = 'downloading_content' OR scraping_completed_at IS NOT NULL THEN 'downloading_content'
                   ELSE 'scraping'
               END
               WHERE resume_stage IS NULL OR resume_stage = ''"""
        )
    except Exception as exc:
        logging.warning("[DB] resume_stage backfill skipped: %s", exc)
    try:
        cur.execute(
            """UPDATE fb_scrape_jobs
               SET scrape_last_progress_at = COALESCE(started_scraping_at, created_at)
               WHERE scrape_last_progress_at IS NULL AND status IN ('scraping', 'downloading_content')"""
        )
    except Exception as exc:
        logging.warning("[DB] scrape_last_progress_at backfill skipped: %s", exc)


def init_db():
    # Use the admin connection for schema migrations so we can CREATE/ALTER
    # without granting DDL to the regular app user. Falls back to the app
    # user when DB_ADMIN_USER is not set (DDL will then fail gracefully).
    using_admin = bool(os.getenv("DB_ADMIN_USER"))
    if using_admin:
        logging.info("[DB] Running schema migrations as DB_ADMIN_USER=%s", DB_ADMIN_USER)
        conn = get_admin_connection()
    else:
        conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS fb_scrape_jobs (
                    id                       INT AUTO_INCREMENT PRIMARY KEY,
                    job_id                   VARCHAR(36) UNIQUE NOT NULL,
                    facebook_url             TEXT NOT NULL,
                    page_name                VARCHAR(255) DEFAULT NULL,
                    page_id                  VARCHAR(100) DEFAULT NULL,
                    source_category          VARCHAR(120) DEFAULT NULL,
                    date_from                DATE DEFAULT NULL,
                    date_to                  DATE DEFAULT NULL,
                    max_posts                INT DEFAULT NULL,
                    status                   ENUM(
                                                 'pending',
                                                 'paused',
                                                 'scraping',
                                                 'downloading_content',
                                                 'stopped',
                                                 'completed',
                                                 'failed'
                                               ) DEFAULT 'pending',
                    resume_stage             VARCHAR(32) DEFAULT 'scraping',
                    control_action           VARCHAR(16) DEFAULT NULL,
                    active_worker_stage      VARCHAR(32) DEFAULT NULL,
                    active_worker_token      VARCHAR(64) DEFAULT NULL,
                    active_worker_name       VARCHAR(64) DEFAULT NULL,
                    assigned_download_worker VARCHAR(64) DEFAULT NULL,
                    total_posts_scraped      INT DEFAULT 0,
                    total_media_count        INT DEFAULT 0,
                    total_media_downloaded   INT DEFAULT 0,
                    error_message            TEXT DEFAULT NULL,
                    scrape_resume_cursor     TEXT DEFAULT NULL,
                    scrape_resume_page_num   INT DEFAULT 0,
                    scrape_resume_skip_posts INT DEFAULT 0,
                    scrape_good_checkpoints  LONGTEXT DEFAULT NULL,
                    scrape_last_progress_at  DATETIME DEFAULT NULL,
                    last_scraped_cursor      TEXT DEFAULT NULL,
                    last_scraped_page_num    INT DEFAULT 0,
                    last_scraped_at          DATETIME DEFAULT NULL,
                    normalized_url           VARCHAR(512) DEFAULT NULL,
                    created_at               DATETIME DEFAULT CURRENT_TIMESTAMP,
                    started_scraping_at      DATETIME DEFAULT NULL,
                    scraping_completed_at    DATETIME DEFAULT NULL,
                    completed_at             DATETIME DEFAULT NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS fb_posts (
                    id                  INT AUTO_INCREMENT PRIMARY KEY,
                    job_id              VARCHAR(36) NOT NULL,
                    post_link           TEXT NOT NULL,
                    post_link_hash      VARCHAR(32) DEFAULT NULL,
                    published_date      VARCHAR(100) DEFAULT NULL,
                    published_timestamp BIGINT DEFAULT NULL,
                    description         TEXT DEFAULT NULL,
                    has_video           TINYINT(1) DEFAULT 0,
                    has_image           TINYINT(1) DEFAULT 0,
                    video_url           TEXT DEFAULT NULL,
                    video_s3_url        TEXT DEFAULT NULL,
                    video_s3_key        VARCHAR(500) DEFAULT NULL,
                    image_urls          JSON DEFAULT NULL,
                    image_s3_urls       JSON DEFAULT NULL,
                    image_s3_keys       JSON DEFAULT NULL,
                    download_status     ENUM('pending','downloading','completed','failed') DEFAULT 'pending',
                    download_error      TEXT DEFAULT NULL,
                    scraped_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_fb_posts_job_id (job_id),
                    INDEX idx_fb_posts_dl_status (download_status),
                    UNIQUE KEY uq_fb_post_per_job (job_id, post_link_hash)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            _ensure_job_status_enum(cur)
            _ensure_fb_scrape_jobs_columns(cur)

            # Index on normalized_url for fast duplicate-URL lookups.
            cur.execute("SHOW INDEX FROM fb_scrape_jobs WHERE Key_name = 'idx_fb_jobs_normalized_url'")
            if not cur.fetchone():
                try:
                    cur.execute("CREATE INDEX idx_fb_jobs_normalized_url ON fb_scrape_jobs (normalized_url)")
                except Exception as exc:
                    logging.warning(f"[DB] Could not add normalized_url index: {exc}")

            # Compound index for the dashboard's hot path. get_all_jobs runs a
            # GROUP BY job_id with SUM(CASE WHEN download_status = ...) over the
            # entire fb_posts table on every poll; with this index MySQL can
            # service those aggregates purely from the index without touching
            # row data — a big win once fb_posts is millions of rows.
            cur.execute("SHOW INDEX FROM fb_posts WHERE Key_name = 'idx_fb_posts_job_status'")
            if not cur.fetchone():
                try:
                    cur.execute("CREATE INDEX idx_fb_posts_job_status ON fb_posts (job_id, download_status)")
                except Exception as exc:
                    logging.warning(f"[DB] Could not add job_id+download_status index: {exc}")

            # Compound index for the MIN/MAX(published_timestamp) per job_id
            # subquery used on every dashboard refresh.
            cur.execute("SHOW INDEX FROM fb_posts WHERE Key_name = 'idx_fb_posts_job_ts'")
            if not cur.fetchone():
                try:
                    cur.execute("CREATE INDEX idx_fb_posts_job_ts ON fb_posts (job_id, published_timestamp)")
                except Exception as exc:
                    logging.warning(f"[DB] Could not add job_id+published_timestamp index: {exc}")

            # Detect which optional columns made it onto the table. The schema-
            # tolerant cursor uses this list to silently strip references to
            # missing columns from later UPDATE/INSERT statements.
            cur.execute("SHOW COLUMNS FROM fb_scrape_jobs")
            existing_cols = {
                row.get("Field") if isinstance(row, dict) else (row[0] if row else None)
                for row in (cur.fetchall() or [])
            }
            missing_optional = _OPTIONAL_COLUMNS - {c for c in existing_cols if c}
            _set_missing_columns(missing_optional)
            if missing_optional:
                logging.warning(
                    "[DB] Optional columns missing from fb_scrape_jobs: %s. "
                    "References to these columns will be auto-stripped from "
                    "outgoing SQL so the app can keep running. Features that "
                    "depend on them (duplicate detection, scan-for-new, "
                    "per-lane download workers) will degrade gracefully.",
                    sorted(missing_optional),
                )

        logging.info("[DB] Tables fb_scrape_jobs and fb_posts ready.")
    except Exception as exc:
        logging.error(f"[DB] init_db error: {exc}")
        raise
    finally:
        conn.close()
