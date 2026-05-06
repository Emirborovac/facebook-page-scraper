import logging
import os
import time
from datetime import date, datetime
from urllib.parse import urlparse, urlunparse

from yt_dlp import YoutubeDL

from models.proxy import get_scrape_proxy_for_ytdlp
from models.operations import (
    apply_job_control_action,
    get_job_post_count,
    save_post,
    stamp_last_scraped,
    update_job_progress,
    update_job_scrape_checkpoint,
    update_job_status,
)

REQUEST_DELAY = float(os.getenv('TIKTOK_REQUEST_DELAY', '0.75'))
BATCH_DELAY = float(os.getenv('TIKTOK_BATCH_DELAY', '2.0'))
BATCH_RETRY_ATTEMPTS = max(int(os.getenv('TIKTOK_BATCH_RETRY_ATTEMPTS', '3')), 1)
BATCH_RETRY_DELAY = float(os.getenv('TIKTOK_BATCH_RETRY_DELAY', '4.0'))
BATCH_SIZE = max(int(os.getenv('TIKTOK_BATCH_SIZE', '50')), 1)


def _ts_from_date(d: date, end_of_day: bool = False) -> int:
    if end_of_day:
        return int(datetime(d.year, d.month, d.day, 23, 59, 59).timestamp())
    return int(datetime(d.year, d.month, d.day, 0, 0, 0).timestamp())


def _normalize_tiktok_account_url(account_url: str) -> tuple[str, str]:
    candidate = (account_url or '').strip()
    if not candidate:
        raise RuntimeError('TikTok URL is empty')
    if candidate.startswith('//'):
        candidate = f'https:{candidate}'
    elif not candidate.startswith(('http://', 'https://')):
        candidate = f"https://{candidate.lstrip('/')}"

    parsed = urlparse(candidate)
    host = (parsed.netloc or '').split(':', 1)[0].lower()
    if host not in {'tiktok.com', 'www.tiktok.com', 'm.tiktok.com'} and not host.endswith('.tiktok.com'):
        raise RuntimeError('Unsupported TikTok host')

    segments = [segment for segment in (parsed.path or '').split('/') if segment]
    if not segments or not segments[0].startswith('@'):
        raise RuntimeError('TikTok URL must point to an account profile')

    username = segments[0].lstrip('@')
    normalized = urlunparse(('https', 'www.tiktok.com', f'/@{username}', '', '', ''))
    return normalized, username


def _build_ydl_options(max_items: int | None = None, start_index: int = 1, worker_id: str = None) -> dict:
    playlist_start = max(int(start_index or 1), 1)
    opts = {
        'quiet': True,
        'extract_flat': True,
        'skip_download': True,
        'no_warnings': True,
        'ignoreerrors': True,
        'lazy_playlist': True,
        'playliststart': playlist_start,
        'extractor_retries': 3,
        'retries': 3,
        'socket_timeout': 30,
    }
    if max_items and int(max_items) > 0:
        opts['playlistend'] = playlist_start + int(max_items) - 1
    proxy_url = get_scrape_proxy_for_ytdlp(worker_id=worker_id)
    if proxy_url:
        opts['proxy'] = proxy_url
    return opts


def _entry_post_url(entry: dict, username: str) -> str | None:
    if not entry:
        return None
    if entry.get('webpage_url'):
        return entry['webpage_url']
    if entry.get('url') and 'tiktok.com/' in str(entry.get('url')):
        return entry['url']
    if entry.get('id'):
        return f'https://www.tiktok.com/@{username}/video/{entry["id"]}'
    return None


def _entry_timestamp(entry: dict):
    ts = entry.get('timestamp')
    if ts:
        try:
            return int(ts)
        except Exception:
            return None
    upload_date = entry.get('upload_date')
    if upload_date:
        try:
            return int(datetime.strptime(upload_date, '%Y%m%d').timestamp())
        except Exception:
            return None
    return None


def _entry_published_date(entry: dict, timestamp):
    if timestamp:
        return datetime.utcfromtimestamp(int(timestamp)).strftime('%Y-%m-%d %H:%M:%S UTC')
    upload_date = entry.get('upload_date')
    if upload_date and len(upload_date) == 8:
        return f'{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:8]}'
    return 'Unknown'


def _entry_description(entry: dict) -> str:
    return (entry.get('description') or entry.get('title') or 'N/A').strip() or 'N/A'


def _merge_entries(existing: dict | None, incoming: dict) -> dict:
    if existing is None:
        return incoming
    current_ts = existing.get('published_timestamp')
    incoming_ts = incoming.get('published_timestamp')
    if current_ts is None and incoming_ts is not None:
        existing['published_timestamp'] = incoming_ts
        existing['published_date'] = incoming.get('published_date')
    if (not existing.get('description') or existing.get('description') == 'N/A') and incoming.get('description'):
        existing['description'] = incoming['description']
    return existing


def _sort_entries(entries: list[dict]) -> list[dict]:
    enumerated = list(enumerate(entries))
    enumerated.sort(
        key=lambda item: (
            0 if item[1].get('published_timestamp') is not None else 1,
            -(item[1].get('published_timestamp') or 0),
            item[0],
        )
    )
    return [item[1] for item in enumerated]


def _extract_info_once(account_url: str, max_items: int | None = None, start_index: int = 1, worker_id: str = None):
    normalized_url, username = _normalize_tiktok_account_url(account_url)
    with YoutubeDL(_build_ydl_options(max_items=max_items, start_index=start_index, worker_id=worker_id)) as ydl:
        info = ydl.extract_info(normalized_url, download=False)
    if not info:
        raise RuntimeError(f'No info returned from yt-dlp for {normalized_url}')

    page_name = (info.get('channel') or info.get('uploader') or info.get('title') or f'@{username}').strip()
    page_id = str(info.get('channel_id') or info.get('uploader_id') or info.get('id') or username)
    entries = []
    for entry in info.get('entries') or []:
        if not entry:
            continue
        timestamp = _entry_timestamp(entry)
        post_url = _entry_post_url(entry, username)
        if not post_url:
            continue
        entries.append({
            'post_link': post_url,
            'video_url': post_url,
            'published_timestamp': timestamp,
            'published_date': _entry_published_date(entry, timestamp),
            'description': _entry_description(entry),
            'has_video': True,
            'has_image': False,
            'image_links': [],
        })
        if max_items and len(entries) >= int(max_items):
            break

    return {
        'account_url': normalized_url,
        'username': username,
        'page_name': page_name,
        'page_id': page_id,
    }, entries


def extract_tiktok_account_entries(account_url: str, max_items: int | None = None, start_index: int = 1, worker_id: str = None):
    target_items = max(int(max_items), 1) if max_items else None
    best_meta = None
    merged: dict[str, dict] = {}
    last_error = None

    for attempt in range(1, BATCH_RETRY_ATTEMPTS + 1):
        try:
            meta, entries = _extract_info_once(account_url, max_items=max_items, start_index=start_index, worker_id=worker_id)
            if best_meta is None:
                best_meta = meta
            for entry in entries:
                key = entry['post_link']
                merged[key] = _merge_entries(merged.get(key), entry)
            logging.info(
                '[TikTokScraper] batch probe attempt=%s/%s start=%s requested=%s entries=%s unique=%s url=%s',
                attempt,
                BATCH_RETRY_ATTEMPTS,
                start_index,
                target_items or 'all',
                len(entries),
                len(merged),
                account_url,
            )
            if target_items and len(merged) >= target_items:
                break
        except Exception as exc:
            last_error = exc
            logging.warning(
                '[TikTokScraper] batch probe failed attempt=%s/%s start=%s requested=%s url=%s error=%s',
                attempt,
                BATCH_RETRY_ATTEMPTS,
                start_index,
                target_items or 'all',
                account_url,
                exc,
            )
        if attempt < BATCH_RETRY_ATTEMPTS:
            time.sleep(BATCH_RETRY_DELAY)

    if best_meta is None:
        if last_error:
            raise last_error
        raise RuntimeError(f'No info returned from yt-dlp for {account_url}')

    ordered = _sort_entries(list(merged.values()))
    if target_items:
        ordered = ordered[:target_items]
    return best_meta, ordered


def tiktok_scraper_all_ytdlp(account_url: str) -> list:
    _, entries = extract_tiktok_account_entries(account_url)
    return [entry['post_link'] for entry in entries]


def run_tiktok_scraper(job: dict, worker_name: str = None):
    job_id = job['job_id']
    account_url = job['facebook_url']
    date_from = job.get('date_from')
    date_to = job.get('date_to')
    max_posts = job.get('max_posts')
    worker_token = job.get('active_worker_token')

    ts_from = _ts_from_date(date_from, end_of_day=False) if date_from else None
    ts_to = _ts_from_date(date_to, end_of_day=True) if date_to else None

    total_saved = get_job_post_count(job_id)
    next_index = max(int(job.get('scrape_resume_page_num') or 1), 1)

    logging.info(
        f'[TikTokScraper] [{job_id}] Starting - url={account_url} worker={worker_name or "n/a"} resume_index={next_index} total={total_saved}'
    )

    try:
        update_job_status(job_id, 'scraping')
        # Stamp an initial checkpoint so even an immediate failure leaves a
        # resumable position. TikTok uses page_num as the cursor (offset).
        if next_index <= 1:
            update_job_scrape_checkpoint(job_id, None, max(next_index, 1), 0, total_saved)
        normalized_url, _ = _normalize_tiktok_account_url(account_url)
        stop_reason = None

        while True:
            action = apply_job_control_action(job_id, 'scraping', worker_token=worker_token)
            if action:
                logging.info(f'[TikTokScraper] [{job_id}] Control action applied before batch: {action}')
                return

            remaining = None
            if max_posts:
                remaining = max(int(max_posts) - total_saved, 0)
                if remaining <= 0:
                    stop_reason = 'max_posts_reached'
                    break

            batch_limit = min(BATCH_SIZE, remaining) if remaining else BATCH_SIZE
            meta, entries = extract_tiktok_account_entries(
                normalized_url,
                max_items=batch_limit,
                start_index=next_index,
                worker_id=worker_name,
            )
            update_job_progress(
                job_id,
                page_name=meta['page_name'],
                page_id=meta['page_id'],
                total_posts_scraped=total_saved,
            )

            if not entries:
                stop_reason = 'feed_exhausted'
                break

            logging.info(
                '[TikTokScraper] [%s] batch_start=%s batch_size=%s newest=%s oldest=%s',
                job_id,
                next_index,
                len(entries),
                entries[0].get('published_date'),
                entries[-1].get('published_date'),
            )

            for offset, post in enumerate(entries):
                action = apply_job_control_action(job_id, 'scraping', worker_token=worker_token)
                if action:
                    logging.info(f'[TikTokScraper] [{job_id}] Control action applied: {action}')
                    return

                processed_index = next_index + offset
                update_job_scrape_checkpoint(job_id, None, processed_index, 0, total_saved)

                timestamp = post.get('published_timestamp')
                if ts_to and timestamp and timestamp > ts_to:
                    continue
                if ts_from and timestamp and timestamp < ts_from:
                    stop_reason = 'date_from_reached'
                    break

                if save_post(job_id, post):
                    total_saved += 1
                    update_job_progress(job_id, total_posts_scraped=total_saved)

                update_job_scrape_checkpoint(job_id, None, processed_index + 1, 0, total_saved)
                logging.info(
                    f'[TikTokScraper] [{job_id}] item={processed_index:04d} total={total_saved} url={post["post_link"]}'
                )

                if max_posts and total_saved >= int(max_posts):
                    stop_reason = 'max_posts_reached'
                    break

                time.sleep(REQUEST_DELAY)

            if stop_reason:
                break

            next_index += len(entries)
            update_job_scrape_checkpoint(job_id, None, next_index, 0, total_saved)
            if len(entries) < batch_limit:
                stop_reason = 'feed_exhausted'
                break

            time.sleep(BATCH_DELAY)

        action = apply_job_control_action(job_id, 'scraping', worker_token=worker_token)
        if action:
            logging.info(f'[TikTokScraper] [{job_id}] Control action applied at completion boundary: {action}')
            return

        logging.info(
            f'[TikTokScraper] [{job_id}] Done - {total_saved} posts saved stop_reason={stop_reason or "feed_exhausted"}'
        )
        # Persist the final position (page index) so "scan for new" can resume.
        stamp_last_scraped(job_id, None, next_index, total_saved)
        update_job_status(job_id, 'downloading_content', clear_scrape_checkpoint=True)

    except Exception as exc:
        logging.error(f'[TikTokScraper] [{job_id}] FAILED: {exc}', exc_info=True)
        try:
            stamp_last_scraped(job_id, None, next_index, total_saved)
        except Exception:
            pass
        update_job_status(job_id, 'failed', error_message=str(exc), extra={'resume_stage': 'scraping'})
