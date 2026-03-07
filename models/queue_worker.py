"""
Background workers that drive the scraping pipeline.

scraping_worker  — claims and processes one pending job at a time.
download_worker  — claims and processes one downloading_content job at a time.
                   (within each job, up to 5 concurrent media downloads)

Both workers loop forever and are started as asyncio tasks on app startup.
"""

import asyncio
import logging
from models.operations import claim_next_pending_job, claim_next_downloading_job
from models.scraper import run_scraper
from models.media_downloader import download_job_media

POLL_INTERVAL = 5  # seconds between polls when queue is empty


async def scraping_worker():
    """Processes one scraping job at a time."""
    logging.info("[ScrapeWorker] Started")
    loop = asyncio.get_event_loop()
    while True:
        try:
            job = await loop.run_in_executor(None, claim_next_pending_job)
            if job:
                logging.info(f"[ScrapeWorker] Claimed job {job['job_id']}")
                await loop.run_in_executor(None, run_scraper, job)
            else:
                await asyncio.sleep(POLL_INTERVAL)
        except asyncio.CancelledError:
            logging.info("[ScrapeWorker] Cancelled")
            break
        except Exception as exc:
            logging.error(f"[ScrapeWorker] Unhandled error: {exc}", exc_info=True)
            await asyncio.sleep(POLL_INTERVAL)


async def download_worker():
    """Processes one downloading_content job at a time."""
    logging.info("[DownloadWorker] Started")
    loop = asyncio.get_event_loop()
    while True:
        try:
            job = await loop.run_in_executor(None, claim_next_downloading_job)
            if job:
                logging.info(f"[DownloadWorker] Claimed job {job['job_id']}")
                await loop.run_in_executor(None, download_job_media, job)
            else:
                await asyncio.sleep(POLL_INTERVAL)
        except asyncio.CancelledError:
            logging.info("[DownloadWorker] Cancelled")
            break
        except Exception as exc:
            logging.error(f"[DownloadWorker] Unhandled error: {exc}", exc_info=True)
            await asyncio.sleep(POLL_INTERVAL)
