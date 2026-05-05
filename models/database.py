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


def _ensure_job_status_enum(cur):
    enum_sql = ",".join(f"'{value}'" for value in JOB_STATUS_VALUES)
    cur.execute(
        f"""ALTER TABLE fb_scrape_jobs
            MODIFY COLUMN status ENUM({enum_sql}) DEFAULT 'pending'"""
    )


def _ensure_fb_scrape_jobs_columns(cur):
    required_columns = [
        ("source_category", "VARCHAR(120) DEFAULT NULL"),
        ("resume_stage", "VARCHAR(32) DEFAULT 'scraping'"),
        ("control_action", "VARCHAR(16) DEFAULT NULL"),
        ("active_worker_stage", "VARCHAR(32) DEFAULT NULL"),
        ("active_worker_token", "VARCHAR(64) DEFAULT NULL"),
        ("active_worker_name", "VARCHAR(64) DEFAULT NULL"),
        ("scrape_resume_cursor", "TEXT DEFAULT NULL"),
        ("scrape_resume_page_num", "INT DEFAULT 0"),
        ("scrape_resume_skip_posts", "INT DEFAULT 0"),
        ("scrape_good_checkpoints", "LONGTEXT DEFAULT NULL"),
        ("scrape_last_progress_at", "DATETIME DEFAULT NULL"),
    ]
    for column_name, definition in required_columns:
        if not _column_exists(cur, "fb_scrape_jobs", column_name):
            cur.execute(f"ALTER TABLE fb_scrape_jobs ADD COLUMN {column_name} {definition}")

    cur.execute(
        """UPDATE fb_scrape_jobs
           SET resume_stage = CASE
               WHEN status = 'downloading_content' OR scraping_completed_at IS NOT NULL THEN 'downloading_content'
               ELSE 'scraping'
           END
           WHERE resume_stage IS NULL OR resume_stage = ''"""
    )
    cur.execute(
        """UPDATE fb_scrape_jobs
           SET scrape_last_progress_at = COALESCE(started_scraping_at, created_at)
           WHERE scrape_last_progress_at IS NULL AND status IN ('scraping', 'downloading_content')"""
    )


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
                    total_posts_scraped      INT DEFAULT 0,
                    total_media_count        INT DEFAULT 0,
                    total_media_downloaded   INT DEFAULT 0,
                    error_message            TEXT DEFAULT NULL,
                    scrape_resume_cursor     TEXT DEFAULT NULL,
                    scrape_resume_page_num   INT DEFAULT 0,
                    scrape_resume_skip_posts INT DEFAULT 0,
                    scrape_good_checkpoints  LONGTEXT DEFAULT NULL,
                    scrape_last_progress_at  DATETIME DEFAULT NULL,
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

        logging.info("[DB] Tables fb_scrape_jobs and fb_posts ready.")
    except Exception as exc:
        logging.error(f"[DB] init_db error: {exc}")
        raise
    finally:
        conn.close()
