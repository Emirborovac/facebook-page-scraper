import logging
import os
import time
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import requests

try:
    import curl_cffi.requests as _cffi
    _CFFI = True
except ImportError:
    _CFFI = False

from models.proxy import apply_scrape_proxy, rotate_scrape_proxy
from models.operations import (
    apply_job_control_action,
    get_job_post_count,
    save_post,
    update_job_progress,
    update_job_scrape_checkpoint,
    update_job_status,
)

INSTAGRAM_COOKIE_DIR = Path(os.getenv("INSTAGRAM_COOKIE_DIR", "./cookies/instagram"))
INSTAGRAM_SCRAPING_WORKER_COUNT = max(1, int(os.getenv("INSTAGRAM_SCRAPING_WORKER_COUNT", "3")))
POSTS_PER_PAGE = max(int(os.getenv("INSTAGRAM_POSTS_PER_PAGE", "12")), 1)
REQUEST_DELAY = float(os.getenv("INSTAGRAM_REQUEST_DELAY", "2.5"))
MAX_STALE_PAGES = max(int(os.getenv("INSTAGRAM_MAX_STALE_PAGES", "3")), 1)
REST_EVERY = max(int(os.getenv("INSTAGRAM_REST_EVERY", "25")), 1)
REST_DURATION = float(os.getenv("INSTAGRAM_REST_DURATION", "25"))
APP_ID = os.getenv("INSTAGRAM_APP_ID", "936619743392459")
FETCH_RETRY_ATTEMPTS = max(int(os.getenv("INSTAGRAM_FETCH_RETRY_ATTEMPTS", "4")), 1)
FETCH_RETRY_DELAY = float(os.getenv("INSTAGRAM_FETCH_RETRY_DELAY", "45"))
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


class RecoverableInstagramPause(RuntimeError):
    pass


def _ts_from_date(d: date, end_of_day: bool = False) -> int:
    if end_of_day:
        return int(datetime(d.year, d.month, d.day, 23, 59, 59).timestamp())
    return int(datetime(d.year, d.month, d.day, 0, 0, 0).timestamp())


def _normalize_instagram_account_url(account_url: str) -> tuple[str, str]:
    candidate = (account_url or '').strip()
    if not candidate:
        raise RuntimeError('Instagram URL is empty')
    if candidate.startswith('//'):
        candidate = f'https:{candidate}'
    elif not candidate.startswith(('http://', 'https://')):
        candidate = f"https://{candidate.lstrip('/')}"

    parsed = urlparse(candidate)
    host = (parsed.netloc or '').split(':', 1)[0].lower()
    if host not in {'instagram.com', 'www.instagram.com', 'm.instagram.com'} and not host.endswith('.instagram.com'):
        raise RuntimeError('Unsupported Instagram host')

    segments = [segment for segment in (parsed.path or '').split('/') if segment]
    if not segments or segments[0] in {'p', 'reel', 'tv'}:
        raise RuntimeError('Instagram URL must point to an account profile')

    username = segments[0].lstrip('@')
    normalized = urlunparse(('https', 'www.instagram.com', f'/{username}/', '', '', ''))
    return normalized, username


def _create_session(worker_id: str = None):
    if _CFFI:
        try:
            s = _cffi.Session(impersonate='chrome124')
            apply_scrape_proxy(s, worker_id=worker_id)
            return s
        except Exception:
            pass
    s = requests.Session()
    apply_scrape_proxy(s, worker_id=worker_id)
    return s


def load_cookies(session, cookie_file=None) -> int:
    path = Path(cookie_file) if cookie_file else (INSTAGRAM_COOKIE_DIR / "01.txt")
    if not path.exists():
        logging.warning('[InstagramScraper] Cookies file not found: %s', path)
        return 0
    count = 0
    with open(path, encoding='utf-8', errors='replace') as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line or line.startswith('#'):
                continue
            parts = line.split('\t')
            if len(parts) < 7:
                continue
            name = parts[5]
            value = '\t'.join(parts[6:])
            for domain in ['www.instagram.com', '.instagram.com', 'instagram.com']:
                session.cookies.set(name, value, domain=domain)
            count += 1
    return count


def get_csrf(session) -> str:
    return (
        session.cookies.get('csrftoken', domain='.instagram.com')
        or session.cookies.get('csrftoken')
        or ''
    )


def _headers(csrf: str, referer: str) -> dict:
    return {
        'User-Agent': UA,
        'X-CSRFToken': csrf,
        'X-IG-App-ID': APP_ID,
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': referer,
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
    }


def resolve_user(session, username: str, csrf: str) -> dict:
    response = session.get(
        'https://www.instagram.com/api/v1/users/web_profile_info/',
        params={'username': username},
        headers=_headers(csrf, f'https://www.instagram.com/{username}/'),
        timeout=20,
    )
    if response.status_code != 200:
        raise RuntimeError(f'Instagram profile resolve failed: status={response.status_code} body={response.text[:200]}')
    data = response.json()
    user = data.get('data', {}).get('user', {})
    user_id = user.get('id')
    if not user_id:
        raise RuntimeError('Could not resolve Instagram user ID from profile response')
    total = user.get('edge_owner_to_timeline_media', {}).get('count', 0)
    page_name = (user.get('full_name') or user.get('username') or f'@{username}').strip()
    return {
        'user_id': str(user_id),
        'total_posts': int(total or 0),
        'page_name': page_name,
        'page_id': str(user_id),
        'username': user.get('username') or username,
    }


def fetch_page(session, user_id: str, csrf: str, username: str, max_id: str | None):
    params = {'count': POSTS_PER_PAGE}
    if max_id:
        params['max_id'] = max_id
    response = session.get(
        f'https://www.instagram.com/api/v1/feed/user/{user_id}/',
        params=params,
        headers=_headers(csrf, f'https://www.instagram.com/{username}/'),
        timeout=20,
    )
    if response.status_code != 200:
        raise RuntimeError(f'Instagram feed fetch failed: status={response.status_code} body={response.text[:200]}')
    return response.json()


def _is_retryable_fetch_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        'please wait a few minutes' in message
        or 'require_login' in message
        or 'status=401' in message
        or 'status=429' in message
    )


def _natural_cookie_sort_key(path: Path):
    stem = path.stem
    return (0, int(stem)) if stem.isdigit() else (1, stem)


def _ordered_cookie_files() -> list[Path]:
    if INSTAGRAM_COOKIE_DIR.exists():
        files = sorted([p for p in INSTAGRAM_COOKIE_DIR.glob('*.txt') if p.is_file()])
        if files:
            return files
    return []


def _worker_cookie_pool(worker_name: str | None = None) -> list[Path]:
    """Partition cookies so each worker gets a dedicated slice.

    With 20 cookies and 5 workers each worker owns 4 cookies.
    Other workers' cookies are appended as last-resort fallbacks.
    """
    files = _ordered_cookie_files()
    if not files:
        logging.warning("[InstagramScraper] No cookie files found in %s", INSTAGRAM_COOKIE_DIR)
        return []

    worker_num = 1
    total_workers = INSTAGRAM_SCRAPING_WORKER_COUNT
    if worker_name:
        import re
        match = re.search(r'(\d+)$', worker_name)
        if match:
            worker_num = max(int(match.group(1)), 1)

    if total_workers <= 1 or len(files) <= 1:
        return list(files)

    per_worker = len(files) // total_workers
    remainder = len(files) % total_workers

    slices: list[list[Path]] = []
    idx = 0
    for w in range(total_workers):
        count = per_worker + (1 if w < remainder else 0)
        slices.append(files[idx:idx + count])
        idx += count

    my_slice = slices[(worker_num - 1) % total_workers]
    others = [f for s in slices for f in s if f not in my_slice]

    return my_slice + others


def _open_profile_session(username: str, cookie_file=None, worker_id: str = None):
    session = _create_session(worker_id=worker_id)
    cookie_count = load_cookies(session, cookie_file=cookie_file)
    if cookie_count == 0:
        raise RuntimeError('No Instagram cookies loaded')
    session.get(
        f'https://www.instagram.com/{username}/',
        headers={'User-Agent': UA},
        timeout=20,
        allow_redirects=False,
    )
    csrf = get_csrf(session)
    profile = resolve_user(session, username, csrf)
    return session, csrf, profile


def _cookie_label(cookie_file) -> str:
    return Path(cookie_file).name


def _open_cookie_context(cookie_pool: list[Path], start_index: int, username: str, job_id: str, reason: str, worker_name: str = None):
    last_error = None
    if not cookie_pool:
        raise RecoverableInstagramPause('No Instagram sessions are configured')
    start_index = max(int(start_index or 0), 0) % len(cookie_pool)
    ordered_indices = list(range(start_index, len(cookie_pool))) + list(range(0, start_index))
    for cookie_index in ordered_indices:
        cookie_file = cookie_pool[cookie_index]
        try:
            session, csrf, profile = _open_profile_session(username, cookie_file=cookie_file, worker_id=worker_name)
            logging.info(
                '[InstagramScraper] [%s] Cookie %s ready during %s - page_name=%s user_id=%s',
                job_id,
                _cookie_label(cookie_file),
                reason,
                profile['page_name'],
                profile['user_id'],
            )
            return cookie_index, session, csrf, profile, cookie_file
        except Exception as exc:
            last_error = str(exc)
            logging.warning(
                '[InstagramScraper] [%s] Cookie %s unusable during %s: %s',
                job_id,
                _cookie_label(cookie_file),
                reason,
                exc,
            )
    raise RecoverableInstagramPause(
        f'All assigned Instagram sessions were exhausted during {reason}. Last error: {last_error or "unknown"}'
    )


def _primary_image_url(media: dict) -> str:
    candidates = media.get('image_versions2', {}).get('candidates', []) or []
    if candidates:
        return candidates[0].get('url', '') or ''
    return ''


def _primary_video_url(media: dict) -> str:
    versions = media.get('video_versions') or []
    if versions:
        return versions[0].get('url', '') or ''
    return ''


def parse_item(item: dict) -> dict:
    code = item.get('code', '')
    taken_at = int(item.get('taken_at') or 0)
    media_type = int(item.get('media_type') or 0)

    caption_obj = item.get('caption') or {}
    description = caption_obj.get('text', '') if isinstance(caption_obj, dict) else ''

    image_links = []
    video_url = ''
    has_video = False

    if media_type == 8:
        for child in item.get('carousel_media') or []:
            image_url = _primary_image_url(child)
            if image_url:
                image_links.append(image_url)
            if not video_url:
                candidate_video = _primary_video_url(child)
                if candidate_video:
                    video_url = candidate_video
                    has_video = True
    else:
        image_url = _primary_image_url(item)
        if image_url:
            image_links.append(image_url)
        video_url = _primary_video_url(item)
        has_video = bool(video_url)

    post_link = f'https://www.instagram.com/p/{code}/' if code else ''
    published_date = (
        datetime.utcfromtimestamp(taken_at).strftime('%Y-%m-%d %H:%M:%S UTC')
        if taken_at else 'Unknown'
    )

    return {
        'post_link': post_link,
        'published_date': published_date,
        'published_timestamp': taken_at,
        'description': description or 'N/A',
        'has_video': has_video,
        'has_image': bool(image_links),
        'image_links': image_links,
        'video_url': video_url or None,
    }


def run_instagram_scraper(job: dict, worker_name: str = None):
    job_id = job['job_id']
    account_url = job['facebook_url']
    date_from = job.get('date_from')
    date_to = job.get('date_to')
    max_posts = job.get('max_posts')
    worker_token = job.get('active_worker_token')

    ts_from = _ts_from_date(date_from, end_of_day=False) if date_from else None
    ts_to = _ts_from_date(date_to, end_of_day=True) if date_to else None

    total_saved = get_job_post_count(job_id)
    page_num = int(job.get('scrape_resume_page_num') or 0)
    max_id = job.get('scrape_resume_cursor') or None
    stale_pages = 0
    last_productive_max_id = max_id
    normalized_url, username = _normalize_instagram_account_url(account_url)
    cookie_pool = _worker_cookie_pool(worker_name)
    cookie_index = 0
    cookie_file = None
    session = None
    csrf = None
    profile = None

    logging.info('[InstagramScraper] [%s] Starting - url=%s worker=%s resume_page=%s cookies=%s', job_id, account_url, worker_name or 'n/a', page_num, [_cookie_label(p) for p in cookie_pool])

    try:
        update_job_status(job_id, 'scraping')
        cookie_index, session, csrf, profile, cookie_file = _open_cookie_context(cookie_pool, 0, username, job_id, 'initial session', worker_name=worker_name)
        username = profile['username']
        update_job_progress(
            job_id,
            page_name=profile['page_name'],
            page_id=profile['page_id'],
            total_posts_scraped=total_saved,
        )

        stop_reason = None

        while True:
            action = apply_job_control_action(job_id, 'scraping', worker_token=worker_token)
            if action:
                logging.info('[InstagramScraper] [%s] Control action applied before page fetch: %s', job_id, action)
                return

            if max_posts and total_saved >= int(max_posts):
                stop_reason = 'max_posts_reached'
                break

            page_num += 1
            if page_num > 1 and page_num % REST_EVERY == 0:
                logging.info('[InstagramScraper] [%s] Resting %.1fs at page %s', job_id, REST_DURATION, page_num)
                time.sleep(REST_DURATION)

            data = None
            elapsed_ms = 0
            max_rounds = 3
            for _round in range(max_rounds):
                for attempt in range(1, FETCH_RETRY_ATTEMPTS + 1):
                    action = apply_job_control_action(job_id, 'scraping', worker_token=worker_token)
                    if action:
                        logging.info('[InstagramScraper] [%s] Control action during retry: %s', job_id, action)
                        return
                    try:
                        t0 = time.time()
                        data = fetch_page(session, profile['user_id'], csrf, username, max_id)
                        elapsed_ms = int((time.time() - t0) * 1000)
                        break
                    except Exception as exc:
                        if not _is_retryable_fetch_error(exc):
                            raise
                        wait_seconds = FETCH_RETRY_DELAY * attempt
                        logging.warning(
                            '[InstagramScraper] [%s] Feed attempt %s/%s (round %s/%s) failed at page %s cookie=%s: %s — waiting %.1fs',
                            job_id, attempt, FETCH_RETRY_ATTEMPTS, _round + 1, max_rounds,
                            page_num, _cookie_label(cookie_file) if cookie_file else 'n/a',
                            exc, wait_seconds,
                        )
                        time.sleep(wait_seconds)
                        rotate_scrape_proxy(worker_name or job_id)
                        cookie_index, session, csrf, profile, cookie_file = _open_cookie_context(
                            cookie_pool, cookie_index + 1, username, job_id,
                            f'feed retry page {page_num}', worker_name=worker_name,
                        )
                        username = profile['username']
                if data is not None:
                    break
                cooldown = 30 * (_round + 1)
                logging.warning(
                    '[InstagramScraper] [%s] All %s cookies failed on round %s/%s at page %s — cooling down %ss before retrying',
                    job_id, FETCH_RETRY_ATTEMPTS, _round + 1, max_rounds, page_num, cooldown,
                )
                time.sleep(cooldown)
            if data is None:
                raise RecoverableInstagramPause(f'Instagram feed could not continue at page {page_num} after {max_rounds} full rounds')

            items = data.get('items') or []
            more_available = bool(data.get('more_available'))
            next_max_id = data.get('next_max_id')
            posts = [parse_item(item) for item in items]
            page_saved = 0

            logging.info('[InstagramScraper] [%s] page=%s items=%s more=%s next_max_id=%s %sms cookie=%s', job_id, page_num, len(items), more_available, bool(next_max_id), elapsed_ms, _cookie_label(cookie_file) if cookie_file else 'n/a')

            for post in posts:
                action = apply_job_control_action(job_id, 'scraping', worker_token=worker_token)
                if action:
                    logging.info('[InstagramScraper] [%s] Control action applied during page processing: %s', job_id, action)
                    return

                timestamp = post.get('published_timestamp')
                if ts_to and timestamp and timestamp > ts_to:
                    continue
                if ts_from and timestamp and timestamp < ts_from:
                    stop_reason = 'date_from_reached'
                    break

                if save_post(job_id, post):
                    total_saved += 1
                    page_saved += 1
                    update_job_progress(
                        job_id,
                        page_name=profile['page_name'],
                        page_id=profile['page_id'],
                        total_posts_scraped=total_saved,
                    )

                if max_posts and total_saved >= int(max_posts):
                    stop_reason = 'max_posts_reached'
                    break

            if page_saved > 0:
                stale_pages = 0
                last_productive_max_id = next_max_id
            elif items:
                stale_pages += 1

            checkpoint_cursor = last_productive_max_id if stale_pages > 0 else next_max_id
            update_job_scrape_checkpoint(job_id, checkpoint_cursor, page_num, 0, total_saved)

            if stop_reason:
                break
            if stale_pages >= MAX_STALE_PAGES:
                stop_reason = 'stale_pages_reached'
                break

            if not items and total_saved == 0 and len(cookie_pool) > 1 and (cookie_index + 1) < len(cookie_pool):
                logging.warning(
                    '[InstagramScraper] [%s] Empty feed with 0 posts on cookie %s — trying next cookie',
                    job_id, _cookie_label(cookie_file) if cookie_file else 'n/a',
                )
                rotate_scrape_proxy(worker_name or job_id)
                cookie_index, session, csrf, profile, cookie_file = _open_cookie_context(
                    cookie_pool,
                    cookie_index + 1,
                    username,
                    job_id,
                    'empty feed retry',
                    worker_name=worker_name,
                )
                username = profile['username']
                page_num -= 1
                continue

            if not more_available or not next_max_id:
                stop_reason = 'feed_exhausted'
                break

            max_id = next_max_id
            time.sleep(REQUEST_DELAY)

        action = apply_job_control_action(job_id, 'scraping', worker_token=worker_token)
        if action:
            logging.info('[InstagramScraper] [%s] Control action applied at completion boundary: %s', job_id, action)
            return

        logging.info('[InstagramScraper] [%s] Done - %s posts saved stop_reason=%s url=%s', job_id, total_saved, stop_reason or 'feed_exhausted', normalized_url)
        update_job_status(job_id, 'downloading_content', clear_scrape_checkpoint=True)

    except RecoverableInstagramPause as exc:
        logging.warning('[InstagramScraper] [%s] PAUSED: %s', job_id, exc)
        update_job_status(job_id, 'paused', error_message=str(exc), extra={'resume_stage': 'scraping'})
    except Exception as exc:
        logging.error('[InstagramScraper] [%s] FAILED: %s', job_id, exc, exc_info=True)
        update_job_status(job_id, 'failed', error_message=str(exc), extra={'resume_stage': 'scraping'})
