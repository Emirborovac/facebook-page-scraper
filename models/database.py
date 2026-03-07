import os
import logging
import pymysql
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")


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


def init_db():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS fb_scrape_jobs (
                    id                   INT AUTO_INCREMENT PRIMARY KEY,
                    job_id               VARCHAR(36) UNIQUE NOT NULL,
                    facebook_url         TEXT NOT NULL,
                    page_name            VARCHAR(255) DEFAULT NULL,
                    page_id              VARCHAR(100) DEFAULT NULL,
                    date_from            DATE DEFAULT NULL,
                    date_to              DATE DEFAULT NULL,
                    max_posts            INT DEFAULT NULL,
                    status               ENUM(
                                           'pending',
                                           'scraping',
                                           'downloading_content',
                                           'completed',
                                           'failed'
                                         ) DEFAULT 'pending',
                    total_posts_scraped  INT DEFAULT 0,
                    total_media_count    INT DEFAULT 0,
                    total_media_downloaded INT DEFAULT 0,
                    error_message        TEXT DEFAULT NULL,
                    created_at           DATETIME DEFAULT CURRENT_TIMESTAMP,
                    started_scraping_at  DATETIME DEFAULT NULL,
                    scraping_completed_at DATETIME DEFAULT NULL,
                    completed_at         DATETIME DEFAULT NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS fb_posts (
                    id                 INT AUTO_INCREMENT PRIMARY KEY,
                    job_id             VARCHAR(36) NOT NULL,
                    post_link          TEXT NOT NULL,
                    post_link_hash     VARCHAR(32) DEFAULT NULL,
                    published_date     VARCHAR(100) DEFAULT NULL,
                    published_timestamp BIGINT DEFAULT NULL,
                    description        TEXT DEFAULT NULL,
                    has_video          TINYINT(1) DEFAULT 0,
                    has_image          TINYINT(1) DEFAULT 0,
                    video_url          TEXT DEFAULT NULL,
                    video_s3_url       TEXT DEFAULT NULL,
                    video_s3_key       VARCHAR(500) DEFAULT NULL,
                    image_urls         JSON DEFAULT NULL,
                    image_s3_urls      JSON DEFAULT NULL,
                    image_s3_keys      JSON DEFAULT NULL,
                    download_status    ENUM('pending','downloading','completed','failed') DEFAULT 'pending',
                    download_error     TEXT DEFAULT NULL,
                    scraped_at         DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_fb_posts_job_id (job_id),
                    INDEX idx_fb_posts_dl_status (download_status),
                    UNIQUE KEY uq_fb_post_per_job (job_id, post_link_hash)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

        logging.info("[DB] Tables fb_scrape_jobs and fb_posts ready.")
    except Exception as exc:
        logging.error(f"[DB] init_db error: {exc}")
        raise
    finally:
        conn.close()
