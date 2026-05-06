"""
Background workers that drive the scraping pipeline.

Facebook, TikTok, and Instagram scraping workers run in separate pools so each
platform has its own concurrency cap. Each scraping worker is paired 1:1 with a
download worker by index — e.g. ``instagram-3`` produces posts that
``instagram-download-3`` then downloads (using its own dedicated cookie pool
in ``cookies/instagram/download_3/``). This isolation keeps cookie burn from
one job from contaminating other lanes.
"""

import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from functools import partial

from models.media_downloader import (
    DOWNLOAD_BATCH_TRIGGER,
    DOWNLOAD_CONCURRENCY_PER_WORKER,
    download_job_media,
)
from models.operations import (
    claim_download_job_for_worker,
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
# Total download workers mirror the scraping pools 1:1.
DOWNLOAD_WORKER_COUNT = SCRAPING_WORKER_COUNT


def _make_executor(count: int, prefix: str) -> ThreadPoolExecutor | None:
    if count <= 0:
        return None
    return ThreadPoolExecutor(max_workers=count, thread_name_prefix=prefix)


_FACEBOOK_EXECUTOR = _make_executor(FACEBOOK_SCRAPING_WORKER_COUNT, "facebook-scrape")
_TIKTOK_EXECUTOR = _make_executor(TIKTOK_SCRAPING_WORKER_COUNT, "tiktok-scrape")
_INSTAGRAM_EXECUTOR = _make_executor(INSTAGRAM_SCRAPING_WORKER_COUNT, "instagram-scrape")

# One executor per download worker (so each lane can run its own thread pool of
# DOWNLOAD_CONCURRENCY_PER_WORKER threads inside it).
_FACEBOOK_DL_EXECUTORS = [
    _make_executor(1, f"facebook-download-{i+1}") for i in range(FACEBOOK_SCRAPING_WORKER_COUNT)
]
_TIKTOK_DL_EXECUTORS = [
    _make_executor(1, f"tiktok-download-{i+1}") for i in range(TIKTOK_SCRAPING_WORKER_COUNT)
]
_INSTAGRAM_DL_EXECUTORS = [
    _make_executor(1, f"instagram-download-{i+1}") for i in range(INSTAGRAM_SCRAPING_WORKER_COUNT)
]
# Backward compat: a legacy unassigned-jobs sweeper also exists.
_LEGACY_DOWNLOAD_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="download-legacy")


def _scrape_executor(platform: str):
    if platform == "tiktok":
        return _TIKTOK_EXECUTOR
    if platform == "instagram":
        return _INSTAGRAM_EXECUTOR
    return _FACEBOOK_EXECUTOR


def _all_executors():
    yield _FACEBOOK_EXECUTOR
    yield _TIKTOK_EXECUTOR
    yield _INSTAGRAM_EXECUTOR
    yield from _FACEBOOK_DL_EXECUTORS
    yield from _TIKTOK_DL_EXECUTORS
    yield from _INSTAGRAM_DL_EXECUTORS
    yield _LEGACY_DOWNLOAD_EXECUTOR


def shutdown_worker_executors():
    for executor in _all_executors():
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
            job = await loop.run_in_executor(executor, partial(claim_next_scraping_job, platform, worker_name=worker_name))
            if job:
                logging.info(f"[ScrapeWorker:{worker_name}] Resuming {platform} job {job['job_id']}")
                await loop.run_in_executor(executor, partial(run_scraper, job, worker_name=worker_name))
                continue

            job = await loop.run_in_executor(executor, partial(claim_next_pending_job, platform, worker_name=worker_name))
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


def _platform_dl_executors(platform: str):
    if platform == "facebook":
        return _FACEBOOK_DL_EXECUTORS
    if platform == "tiktok":
        return _TIKTOK_DL_EXECUTORS
    if platform == "instagram":
        return _INSTAGRAM_DL_EXECUTORS
    return []


async def download_worker(platform: str, worker_index: int):
    """A single per-lane download worker.

    Polls only for jobs whose ``assigned_download_worker`` matches its name
    (``<platform>-download-<idx>``). Drains all currently-pending posts each
    cycle, then yields. Inside a cycle it spins up
    ``DOWNLOAD_CONCURRENCY_PER_WORKER`` threads for the actual media downloads.
    """
    worker_name = f"{platform}-download-{worker_index}"
    executors = _platform_dl_executors(platform)
    if worker_index < 1 or worker_index > len(executors) or executors[worker_index - 1] is None:
        logging.info(f"[DownloadWorker:{worker_name}] No executor configured; stopping")
        return
    executor = executors[worker_index - 1]
    logging.info(
        f"[DownloadWorker:{worker_name}] Started "
        f"(concurrency={DOWNLOAD_CONCURRENCY_PER_WORKER}, batch_trigger={DOWNLOAD_BATCH_TRIGGER})"
    )
    loop = asyncio.get_event_loop()
    while True:
        try:
            job = await loop.run_in_executor(
                executor,
                partial(claim_download_job_for_worker, worker_name, batch_trigger=DOWNLOAD_BATCH_TRIGGER),
            )
            if job:
                logging.info(
                    f"[DownloadWorker:{worker_name}] Claimed job {job['job_id']} "
                    f"(status={job.get('status')}, pending={job.get('pending_count', '?')})"
                )
                await loop.run_in_executor(
                    executor,
                    partial(
                        download_job_media,
                        job,
                        worker_name=worker_name,
                        concurrency=DOWNLOAD_CONCURRENCY_PER_WORKER,
                    ),
                )
            else:
                await asyncio.sleep(POLL_INTERVAL)
        except asyncio.CancelledError:
            logging.info(f"[DownloadWorker:{worker_name}] Cancelled")
            break
        except Exception as exc:
            logging.error(f"[DownloadWorker:{worker_name}] Unhandled error: {exc}", exc_info=True)
            await asyncio.sleep(POLL_INTERVAL)


async def legacy_download_sweeper():
    """Picks up jobs in downloading_content with NO assigned_download_worker.

    Exists for backwards compat — covers jobs that were created before per-lane
    assignment existed (older job rows have NULL assigned_download_worker).
    """
    logging.info("[DownloadWorker:legacy] Started — sweeping unassigned downloading jobs")
    loop = asyncio.get_event_loop()
    while True:
        try:
            job = await loop.run_in_executor(
                _LEGACY_DOWNLOAD_EXECUTOR,
                partial(claim_next_downloading_job, worker_name="download-legacy"),
            )
            if job:
                logging.info(f"[DownloadWorker:legacy] Claimed unassigned job {job['job_id']}")
                await loop.run_in_executor(
                    _LEGACY_DOWNLOAD_EXECUTOR,
                    partial(
                        download_job_media,
                        job,
                        worker_name="download-legacy",
                        concurrency=DOWNLOAD_CONCURRENCY_PER_WORKER,
                    ),
                )
            else:
                await asyncio.sleep(POLL_INTERVAL)
        except asyncio.CancelledError:
            logging.info("[DownloadWorker:legacy] Cancelled")
            break
        except Exception as exc:
            logging.error(f"[DownloadWorker:legacy] Unhandled error: {exc}", exc_info=True)
            await asyncio.sleep(POLL_INTERVAL)
