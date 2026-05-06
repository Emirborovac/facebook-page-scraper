import logging
import os

import pymysql
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

JOB_STATUS_VALUES = (
    "pending",
    "paused",
    "scraping",
    "downloading_content",
    "stopped",
    "completed",
    "failed",
)


def get_connection():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
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

        logging.info("[DB] Tables fb_scrape_jobs and fb_posts ready.")
    except Exception as exc:
        logging.error(f"[DB] init_db error: {exc}")
        raise
    finally:
        conn.close()
