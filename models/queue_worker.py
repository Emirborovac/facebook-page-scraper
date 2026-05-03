"""
Background workers that drive the scraping pipeline.

Facebook, TikTok, and Instagram scraping workers run in separate pools so each
platform has its own concurrency cap.
"""

import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from functools import partial

from models.media_downloader import download_job_media
from models.operations import (
    claim_next_downloading_job,
    claim_next_pending_job,
    claim_next_scraping_job,
)
from models.scraper import run_scraper

POLL_INTERVAL = 5
FACEBOOK_SCRAPING_WORKER_COUNT = max(0, int(os.getenv("FACEBOOK_SCRAPING_WORKER_COUNT", "5")))
TIKTOK_SCRAPING_WORKER_COUNT = max(0, int(os.getenv("TIKTOK_SCRAPING_WORKER_COUNT", "10")))
INSTAGRAM_SCRAPING_WORKER_COUNT = max(0, int(os.getenv("INSTAGRAM_SCRAPING_WORKER_COUNT", "5")))
SCRAPING_WORKER_COUNT = (
    FACEBOOK_SCRAPING_WORKER_COUNT
    + TIKTOK_SCRAPING_WORKER_COUNT
    + INSTAGRAM_SCRAPING_WORKER_COUNT
)

_FACEBOOK_EXECUTOR = (
    ThreadPoolExecutor(max_workers=FACEBOOK_SCRAPING_WORKER_COUNT, thread_name_prefix="facebook-scrape")
    if FACEBOOK_SCRAPING_WORKER_COUNT
    else None
)
_TIKTOK_EXECUTOR = (
    ThreadPoolExecutor(max_workers=TIKTOK_SCRAPING_WORKER_COUNT, thread_name_prefix="tiktok-scrape")
    if TIKTOK_SCRAPING_WORKER_COUNT
    else None
)
_INSTAGRAM_EXECUTOR = (
    ThreadPoolExecutor(max_workers=INSTAGRAM_SCRAPING_WORKER_COUNT, thread_name_prefix="instagram-scrape")
    if INSTAGRAM_SCRAPING_WORKER_COUNT
    else None
)
_DOWNLOAD_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="download")


def _scrape_executor(platform: str):
    if platform == "tiktok":
        return _TIKTOK_EXECUTOR
    if platform == "instagram":
        return _INSTAGRAM_EXECUTOR
    return _FACEBOOK_EXECUTOR


def shutdown_worker_executors():
    for executor in (_FACEBOOK_EXECUTOR, _TIKTOK_EXECUTOR, _INSTAGRAM_EXECUTOR, _DOWNLOAD_EXECUTOR):
        if executor is None:
            continue
        executor.shutdown(wait=False, cancel_futures=True)


async def scraping_worker(platform: str = "facebook", worker_name: str = "facebook-1"):
    logging.info(f"[ScrapeWorker:{worker_name}] Started for platform={platform}")
    executor = _scrape_executor(platform)
    if executor is None:
        logging.info(
            f"[ScrapeWorker:{worker_name}] No executor configured for platform={platform}; stopping worker"
        )
        return

    loop = asyncio.get_event_loop()
    while True:
        try:
            job = await loop.run_in_executor(executor, partial(claim_next_scraping_job, platform))
            if job:
                logging.info(f"[ScrapeWorker:{worker_name}] Resuming {platform} job {job['job_id']}")
                await loop.run_in_executor(executor, partial(run_scraper, job, worker_name=worker_name))
                continue

            job = await loop.run_in_executor(executor, partial(claim_next_pending_job, platform))
            if job:
                logging.info(f"[ScrapeWorker:{worker_name}] Claimed {platform} job {job['job_id']}")
                await loop.run_in_executor(executor, partial(run_scraper, job, worker_name=worker_name))
            else:
                await asyncio.sleep(POLL_INTERVAL)
        except asyncio.CancelledError:
            logging.info(f"[ScrapeWorker:{worker_name}] Cancelled")
            break
        except Exception as exc:
            logging.error(f"[ScrapeWorker:{worker_name}] Unhandled error: {exc}", exc_info=True)
            await asyncio.sleep(POLL_INTERVAL)


async def download_worker():
    logging.info("[DownloadWorker] Started")
    loop = asyncio.get_event_loop()
    while True:
        try:
            job = await loop.run_in_executor(_DOWNLOAD_EXECUTOR, claim_next_downloading_job)
            if job:
                logging.info(f"[DownloadWorker] Claimed job {job['job_id']}")
                await loop.run_in_executor(_DOWNLOAD_EXECUTOR, download_job_media, job)
            else:
                await asyncio.sleep(POLL_INTERVAL)
        except asyncio.CancelledError:
            logging.info("[DownloadWorker] Cancelled")
            break
        except Exception as exc:
            logging.error(f"[DownloadWorker] Unhandled error: {exc}", exc_info=True)
            await asyncio.sleep(POLL_INTERVAL)
