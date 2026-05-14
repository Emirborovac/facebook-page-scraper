"""
Shared cookie pool management for scrapers.

Each scraping worker gets its own slice of cookies. The pool tracks per-cookie
lifecycle:

    active   — available for use
    cooldown — temporarily throttled, will recover after cooldown_until
    burnt    — permanently dead, file moved to cookie_trash/

Per-worker isolation is enforced via dedicated folders:
    cookies/<platform>/worker_N/    (preferred — no cookie collisions)
    cookies/<platform>/*.txt        (fallback — shared pool, legacy)

Each platform supplies its own error classifiers (`is_burnt_fn`, `is_throttle_fn`)
because Instagram, Facebook, and TikTok return different error shapes.
"""

import logging
import re
import shutil
import threading
import time
from pathlib import Path
from typing import Callable

# Default cooldown for a throttled cookie (seconds).
DEFAULT_THROTTLE_COOLDOWN = 300.0


def trash_cookie(cookie_file: Path) -> Path | None:
    """Move *cookie_file* into a sibling cookie_trash/ folder.

    Returns the new path, or None if the move failed.
    """
    try:
        trash_dir = cookie_file.parent / 'cookie_trash'
        trash_dir.mkdir(parents=True, exist_ok=True)
        dest = trash_dir / cookie_file.name
        if dest.exists():
            dest = trash_dir / f'{cookie_file.stem}_{int(time.time())}{cookie_file.suffix}'
        shutil.move(str(cookie_file), str(dest))
        logging.warning('[CookiePool] Cookie %s is BURNT — moved to %s', cookie_file.name, dest)
        return dest
    except Exception as exc:
        logging.error('[CookiePool] Failed to trash cookie %s: %s', cookie_file.name, exc)
        return None


def load_worker_cookie_pool(platform_dir: Path, worker_name: str | None) -> list[Path]:
    """Load cookies for a worker.

    Looks for ``platform_dir / worker_<N> / *.txt`` first (dedicated mode).
    Falls back to ``platform_dir / *.txt`` (shared mode) when no worker
    folder exists.
    """
    worker_num = 1
    if worker_name:
        match = re.search(r'(\d+)$', worker_name)
        if match:
            worker_num = max(int(match.group(1)), 1)

    worker_dir = platform_dir / f'worker_{worker_num}'
    if worker_dir.is_dir():
        files = sorted(
            p for p in worker_dir.glob('*.txt')
            if p.is_file() and p.parent.name != 'cookie_trash'
        )
        if files:
            logging.info(
                '[CookiePool] %s worker %s using dedicated folder: %s (%d cookies)',
                platform_dir.name, worker_num, worker_dir, len(files),
            )
            return files
        logging.warning(
            '[CookiePool] %s worker %s folder %s exists but has no cookies',
            platform_dir.name, worker_num, worker_dir,
        )

    if platform_dir.is_dir():
        files = sorted(
            p for p in platform_dir.glob('*.txt')
            if p.is_file() and p.parent.name != 'cookie_trash'
        )
        if files:
            logging.info(
                '[CookiePool] %s worker %s using shared cookie pool (%d cookies)',
                platform_dir.name, worker_num, len(files),
            )
            return list(files)

    logging.warning('[CookiePool] No %s cookie files found in %s', platform_dir.name, platform_dir)
    return []


class CookiePool:
    """Round-robin pool with cooldown + burnt-account tracking.

    Callers supply platform-specific error classifiers:
        is_burnt_fn(exc)    → True if the cookie should be retired forever
        is_throttle_fn(exc) → True if the cookie just needs a cooldown
    """

    def __init__(
        self,
        cookie_files: list[Path],
        is_burnt_fn: Callable[[Exception], bool],
        is_throttle_fn: Callable[[Exception], bool],
        throttle_cooldown: float = DEFAULT_THROTTLE_COOLDOWN,
    ):
        self._files = list(cookie_files)
        self._cooldown_until: dict[str, float] = {}
        self._burnt: set[str] = set()
        self._is_burnt = is_burnt_fn
        self._is_throttle = is_throttle_fn
        self._default_cooldown = throttle_cooldown
        # Internal lock so the same pool can be safely shared across threads
        # (e.g. by 25 concurrent download workers).
        self._lock = threading.Lock()

    def replace_files(self, cookie_files: list[Path]) -> None:
        """Swap the underlying cookie file list.

        Used by long-lived pools (like the downloader's) when a user uploads
        new cookies via the dashboard. Existing cooldown / burnt state for
        cookies that are still present is preserved.
        """
        with self._lock:
            new_files = list(cookie_files)
            new_keys = {self._key(f) for f in new_files}
            self._files = new_files
            # Drop state for cookies that no longer exist on disk.
            self._cooldown_until = {k: v for k, v in self._cooldown_until.items() if k in new_keys}
            self._burnt = {k for k in self._burnt if k in new_keys}

    @staticmethod
    def _key(f: Path) -> str:
        return str(f)

    def _is_available(self, f: Path) -> bool:
        k = self._key(f)
        if k in self._burnt:
            return False
        until = self._cooldown_until.get(k)
        return not (until and time.time() < until)

    def next(self, current_file: Path | None = None) -> Path | None:
        """Return next available cookie (round-robin from current). None = none available right now."""
        with self._lock:
            alive = [f for f in self._files if self._key(f) not in self._burnt]
            if not alive:
                return None
            start = 0
            if current_file:
                for i, f in enumerate(alive):
                    if self._key(f) == self._key(current_file):
                        start = (i + 1) % len(alive)
                        break
            for i in range(len(alive)):
                candidate = alive[(start + i) % len(alive)]
                if self._is_available(candidate):
                    return candidate
            return None

    def mark_throttled(self, cookie_file: Path, cooldown_seconds: float | None = None):
        cooldown = cooldown_seconds if cooldown_seconds is not None else self._default_cooldown
        with self._lock:
            self._cooldown_until[self._key(cookie_file)] = time.time() + cooldown
        logging.info(
            '[CookiePool] Cookie %s throttled — cooldown %.0fs',
            cookie_file.name, cooldown,
        )

    def mark_burnt(self, cookie_file: Path):
        with self._lock:
            k = self._key(cookie_file)
            self._burnt.add(k)
            self._cooldown_until.pop(k, None)
        trash_cookie(cookie_file)

    def classify_and_mark(self, cookie_file: Path, exc: Exception) -> str:
        """Convenience: classify *exc* and mark *cookie_file* accordingly.

        Returns 'burnt', 'throttle', or 'unknown' (treated as soft throttle).
        """
        if self._is_burnt(exc):
            self.mark_burnt(cookie_file)
            return 'burnt'
        if self._is_throttle(exc):
            self.mark_throttled(cookie_file)
            return 'throttle'
        # Unknown errors: minimal cooldown so the cookie comes back fast.
        # The previous 60s default was too punishing for transient blips
        # (empty responses, network glitches, etc.) where the cookie is
        # almost certainly fine.
        self.mark_throttled(cookie_file, cooldown_seconds=15)
        return 'unknown'

    def has_active(self) -> bool:
        with self._lock:
            return any(self._key(f) not in self._burnt for f in self._files)

    def active_count(self) -> int:
        with self._lock:
            return sum(1 for f in self._files if self._key(f) not in self._burnt)

    def total_count(self) -> int:
        with self._lock:
            return len(self._files)

    def wait_for_available(
        self,
        job_id: str,
        control_check: Callable[[], bool] | None = None,
    ) -> Path | None:
        """Sleep until earliest cooled-down cookie is ready.

        *control_check* is an optional callable returning True when the worker
        should bail out (pause/stop control action). Returns None when all
        cookies are burnt or when the control_check signals stop.
        """
        if not self.has_active():
            return None
        alive = [f for f in self._files if self._key(f) not in self._burnt]
        while True:
            cooldowns = []
            for f in alive:
                until = self._cooldown_until.get(self._key(f))
                if not until or time.time() >= until:
                    return f
                cooldowns.append((until, f))
            if not cooldowns:
                return None
            cooldowns.sort(key=lambda x: x[0])
            earliest_time, earliest_file = cooldowns[0]
            wait = max(earliest_time - time.time(), 0)
            if wait <= 0:
                return earliest_file
            logging.info(
                '[CookiePool] [%s] All cookies cooling down — waiting %.0fs for %s',
                job_id, wait, earliest_file.name,
            )
            slept = 0.0
            chunk = 5.0
            while slept < wait:
                if control_check and control_check():
                    return None
                step = min(chunk, wait - slept)
                time.sleep(step)
                slept += step
            return earliest_file
