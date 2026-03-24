"""
s.to Backup Tool v1.0
Read-only backup of watched/subscribed/watchlist data from s.to
"""

import atexit
import json
import logging
import os
import queue
import random
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# Add parent directory to path to access config
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config.config2 import SELECTORS_CONFIG, EMAIL, PASSWORD, TIMEOUT, HEADLESS, DATA_DIR

logger = logging.getLogger(__name__)


def _is_pid_alive(pid):
    """Check if a process with the given PID is still running."""
    try:
        result = subprocess.run(
            ['tasklist', '/FI', f'PID eq {pid}', '/NH'],
            capture_output=True, check=False, text=True
        )
        return str(pid) in result.stdout
    except Exception:
        return False


def cleanup_stale_worker_pids():
    """Remove .worker_pids.json if all tracked processes are dead (e.g. after a hard kill)."""
    worker_pids_file = os.path.join(DATA_DIR, '.worker_pids.json')
    if not os.path.exists(worker_pids_file):
        return
    try:
        with open(worker_pids_file, 'r') as f:
            pids = json.load(f)
        if not pids:
            os.remove(worker_pids_file)
            return
        # If any tracked PID is still alive, kill it then remove the file
        any_alive = False
        for worker_id, pid in pids.items():
            if _is_pid_alive(pid):
                any_alive = True
                try:
                    subprocess.run(
                        ['taskkill', '/F', '/PID', str(pid), '/T'],
                        capture_output=True, check=False
                    )
                except Exception:
                    pass
        # All processes are dead (or just killed) — remove stale file
        os.remove(worker_pids_file)
        if any_alive:
            logger.info("Cleaned up orphaned worker processes from previous run")
        else:
            logger.info("Removed stale .worker_pids.json (all processes already dead)")
    except Exception:
        # If anything goes wrong, try to remove the file anyway
        try:
            os.remove(worker_pids_file)
        except Exception:
            pass


# Auto-clean stale PID file on startup (handles hard-killed terminals)
cleanup_stale_worker_pids()


def cleanup_geckodriver_processes():
    """Clean up geckodriver and Firefox processes we spawned (safe for personal Firefox)"""
    worker_pids_file = os.path.join(DATA_DIR, '.worker_pids.json')
    if os.path.exists(worker_pids_file):
        try:
            with open(worker_pids_file, 'r') as f:
                pids = json.load(f)
            
            # Kill all tracked geckodriver PIDs with full process tree (/T kills children too)
            for worker_id, pid in pids.items():
                try:
                    subprocess.run(
                        ['taskkill', '/F', '/PID', str(pid), '/T'],
                        capture_output=True, check=False
                    )
                except Exception:
                    pass
        except Exception:
            pass
        # Always remove the PID file after cleanup attempt
        try:
            os.remove(worker_pids_file)
        except Exception:
            pass


def _signal_handler(signum, frame):
    """Convert termination signals into clean exit so atexit handlers run"""
    sys.exit(0)


# Register cleanup on normal exit
atexit.register(cleanup_geckodriver_processes)

# Register cleanup on termination signals so atexit handlers run
signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

# On Windows, also handle SIGBREAK which is sent when the console window is closed.
# This ensures atexit handlers run and worker processes are cleaned up.
if sys.platform == 'win32':
    signal.signal(signal.SIGBREAK, _signal_handler)

# Performance settings - Allow overriding via environment variable
MAX_WORKERS = 16


# Pre-compiled regex for season label detection
_SEASON_LABEL_RE = re.compile(r'^(staffel|season|s)?\s*\d+$', re.IGNORECASE)
_DOMAIN_STRIP_RE = re.compile(r'^https?://[^/]+')
_SERIE_PATH_RE = re.compile(r'(/serie/[^/]+)')
_SERIE_SLUG_RE = re.compile(r'^/serie/([^/?#]+)/?$')


def is_regular_season(season_label):
    """Check if season label is a regular numbered season (Staffel 1, Season 2, etc.)
    vs special content (Specials, OVA, Movies, Filme, etc.)
    
    Returns True for regular seasons that can use the 'assume unwatched' optimization.
    Returns False for special seasons that should always be checked individually.
    """
    return bool(_SEASON_LABEL_RE.search(season_label.strip()))


class SeasonDetectionError(Exception):
    """Raised when season detection fails after all retries."""
    pass


class SToBackupScraper:
    """
    Read-only backup tool for s.to user data.
    
    Scrapes series, seasons, episodes, watched status, and subscription info.
    Supports both parallel and sequential execution modes.
    """
    
    def __init__(self):
        self.driver = None
        self.config = SELECTORS_CONFIG
        self.email = EMAIL
        self.password = PASSWORD
        self.completed_links = set()
        self.failed_links = []
        self.series_data = []
        self.auth_cookies = []
        self._lock = threading.Lock()
        self._use_parallel = True
        self.checkpoint_file = os.path.join(DATA_DIR, '.scrape_checkpoint.json')
        self.failed_file = os.path.join(DATA_DIR, '.failed_series.json')
        self.pause_file = os.path.join(DATA_DIR, '.pause_scraping')
        self._checkpoint_mode = None  # Tracks which option created the checkpoint

        # Global rate limiter — prevents all workers from hammering the server simultaneously
        self._request_lock = threading.Lock()
        self._last_request_time = 0.0
        self._min_request_interval = self.config.get('timing', {}).get('min_request_interval', 0.2)
        # Adaptive throttle — tracks recent 502/503 errors to slow down globally
        self._server_error_times = []
        self._global_backoff = 0.0

    def _throttle_request(self):
        """Enforce minimum interval between HTTP requests across all workers.
        Also applies adaptive backoff when server is returning many 502/503 errors."""
        with self._request_lock:
            now = time.time()
            # Apply adaptive global backoff if server is struggling
            effective_interval = self._min_request_interval + self._global_backoff
            elapsed = now - self._last_request_time
            if elapsed < effective_interval:
                time.sleep(effective_interval - elapsed)
            self._last_request_time = time.time()

    def _record_server_error(self):
        """Record a 502/503 error and adjust global backoff adaptively."""
        with self._request_lock:
            now = time.time()
            self._server_error_times.append(now)
            # Keep only last 60 seconds of errors
            self._server_error_times = [t for t in self._server_error_times if now - t < 60]
            error_count = len(self._server_error_times)
            if error_count >= 15:
                self._global_backoff = min(5.0, 0.5 * (error_count / 5))
            elif error_count >= 8:
                self._global_backoff = min(3.0, 0.3 * (error_count / 5))
            elif error_count >= 4:
                self._global_backoff = 1.0

    def _decay_global_backoff(self):
        """Gradually reduce global backoff after successful requests."""
        with self._request_lock:
            if self._global_backoff > 0:
                self._global_backoff = max(0.0, self._global_backoff - 0.1)

    def normalize_to_series_url(self, url):
        """Normalize a series URL/slug to full s.to URL.
        
        Handles:
            https://s.to/serie/Series-Name
            https://s.to/serie/Series-Name/staffel-1
            /serie/Series-Name/staffel-1
            Series-Name (bare slug)
        """
        url = _DOMAIN_STRIP_RE.sub("", url)
        m = _SERIE_PATH_RE.match(url)
        if m:
            return f"https://s.to{m.group(1)}"
        # Bare slug
        slug = url.strip().strip('/')
        if slug:
            return f"https://s.to/serie/{slug}"
        return url
    
    def _normalize_failed_item(self, item, series_url=None, display_title=None):
        """Normalize a failed item to a consistent dict format for retry.
        
        Ensures all entries in failed_links are dicts with 'url' and 'title' keys,
        regardless of whether the original item was a dict, URL string, or slug.
        """
        if isinstance(item, dict) and 'url' in item:
            return item
        if isinstance(item, dict):
            return {
                'url': item.get('url', item.get('link', series_url or '')),
                 'title': item.get('title', display_title or '')
            }
        # Bare string (URL or slug)
        url = series_url or str(item)
        if not url.startswith('http'):
            url = f"https://s.to/serie/{url}"
        return {'url': url, 'title': display_title or ''}
    
    def _extract_item_info(self, item):
        """Extract URL, slug, and display title from a work item.
        
        Handles both dict items (with 'url'/'link'/'title' keys) and bare
        URL/slug strings.
        
        Returns:
            tuple: (series_url, series_slug, display_title)
        """
        if isinstance(item, dict):
            series_url = item.get('url', item.get('link', ''))
            display_title = item.get('title', '')
        else:
            series_url = str(item)
            display_title = ''
        series_slug = self.get_series_slug_from_url(series_url)
        display_title = display_title or series_slug
        return series_url, series_slug, display_title

    def _aggregate_season_results(self, series_slug, season_results, missing_seasons, series_data):
        """Build series_data entry from season results and mark completed if successful.

        Caller must hold self._lock when used in parallel mode.

        Returns:
            tuple: (series_watched, series_total_eps, series_had_error, is_subscribed, is_watchlist)
        """
        series_had_error = len(missing_seasons) > 0
        series_watched = 0
        series_total_eps = 0
        is_subscribed = False
        is_watchlist = False

        for season_data in season_results:
            if series_slug not in series_data:
                series_data[series_slug] = {
                    'seasons': [],
                    'url': f"https://s.to/serie/{series_slug}",
                    'link': f"/serie/{series_slug}"
                }
            series_data[series_slug]['seasons'].append(season_data)
            series_watched += season_data.get('watched_episodes', 0)
            series_total_eps += season_data.get('total_episodes', 0)
            if season_data.get('subscribed'):
                is_subscribed = True
            if season_data.get('watchlist'):
                is_watchlist = True

        if not series_had_error and series_slug in series_data and series_total_eps > 0:
            self.completed_links.add(series_slug)

        return series_watched, series_total_eps, series_had_error, is_subscribed, is_watchlist

    @staticmethod
    def _format_progress_line(done, total, start_time, title, watched=None,
                              episode_total=None, empty=False, error=None,
                              worker_id=None, worker_count=None, season_labels=None,
                              subscribed=None, watchlist=None):
        """Build a single-line progress string with bar, ETA, and status.
        
        Used by both parallel and sequential scraping modes.
        """
        elapsed = time.time() - start_time
        avg = elapsed / max(1, done)
        remaining = total - done
        eta_mins = int((avg * remaining) / 60)
        pct = int((done / total) * 100) if total else 0
        bar_len = 30
        filled = int(bar_len * done / total) if total else 0
        bar = '█' * filled + '░' * (bar_len - filled)
        worker_info = f" | W{worker_id}/{worker_count}" if worker_id and worker_count else ""
        season_info = f" [{','.join(str(s) for s in season_labels)}]" if season_labels else ""
        # Subscription status indicators
        sub_parts = []
        if subscribed is not None:
            sub_parts.append(f"Sub:{'✓' if subscribed else '✗'}")
        if watchlist is not None:
            sub_parts.append(f"WL:{'✓' if watchlist else '✗'}")
        sub_info = f" ({' '.join(sub_parts)})" if sub_parts else ""
        if error:
            return f"[{done}/{total}] [{bar}] {pct}% | ETA: {eta_mins}m{worker_info} | ✗ {title}: {error}"
        elif empty:
            return f"[{done}/{total}] [{bar}] {pct}% | ETA: {eta_mins}m{worker_info} | ⚠ {title}{season_info}: No episodes{sub_info}"
        else:
            return f"[{done}/{total}] [{bar}] {pct}% | ETA: {eta_mins}m{worker_info} | ✓ {title}{season_info}: {watched}/{episode_total} watched{sub_info}"
        
    # ==================== FILE I/O HELPERS ====================
    
    @staticmethod
    def _atomic_write_json(filepath, data):
        """
        Write JSON to file atomically via temp file + os.replace.
        
        Prevents corrupted files if the process is killed mid-write.
        os.replace is atomic on both Windows and POSIX.
        
        Args:
            filepath: Target file path
            data: Data to serialize as JSON
            
        Raises:
            Exception: On write errors
        """
        dirpath = os.path.dirname(filepath)
        os.makedirs(dirpath, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=dirpath, suffix='.tmp')
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            os.replace(tmp_path, filepath)
        except Exception:
            try:
                os.remove(tmp_path)
            except OSError:
                logger.warning(f"Failed to clean up temp file: {tmp_path}")
            raise
    
    # ==================== CHECKPOINT SYSTEM ====================
    
    def set_checkpoint_paths(self, data_dir):
        """Set paths for checkpoint, failed series, and pause files"""
        self.checkpoint_file = os.path.join(data_dir, '.scrape_checkpoint.json')
        self.failed_file = os.path.join(data_dir, '.failed_series.json')
        self.pause_file = os.path.join(data_dir, '.pause_scraping')
    
    def save_checkpoint(self, include_data=False):
        """Save scraping checkpoint to resume later.
        
        Args:
            include_data: If True, also save series_data for full state preservation.
                          Used on exit/crash. Periodic saves use False for speed.
        """
        if not self.checkpoint_file:
            return
        try:
            checkpoint_data = {
                'completed_links': list(self.completed_links),
                'mode': self._checkpoint_mode,
                'timestamp': time.time(),
            }
            if include_data and self.series_data:
                checkpoint_data['series_data'] = self.series_data
            self._atomic_write_json(self.checkpoint_file, checkpoint_data)
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")
            print(f"  ⚠ Warning: checkpoint save failed: {e}")
    
    def load_checkpoint(self):
        """Load checkpoint to resume from previous run.
        
        Restores completed_links, mode, and series_data (if saved).
        """
        if not self.checkpoint_file or not os.path.exists(self.checkpoint_file):
            return False
        try:
            with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, dict) and 'completed_links' in data:
                self.completed_links = set(data.get('completed_links', []))
                self._checkpoint_mode = data.get('mode')
                saved_data = data.get('series_data', [])
                if saved_data:
                    self.series_data = saved_data
                return True
            elif isinstance(data, list):
                self.completed_links = set(data)
                return True
            else:
                print(f"✗ Checkpoint file is invalid or corrupted.")
                return False
        except Exception as e:
            print(f"✗ Failed to load checkpoint: {e}")
            return False
    
    @staticmethod
    def get_checkpoint_mode(data_dir):
        """Read the mode from an existing checkpoint file without loading it.
        
        Returns:
            str or None: The mode that created the checkpoint, or None if no checkpoint.
        """
        checkpoint_file = os.path.join(data_dir, '.scrape_checkpoint.json')
        if not os.path.exists(checkpoint_file):
            return None
        try:
            with open(checkpoint_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data.get('mode')
        except (json.JSONDecodeError, OSError) as e:
            logger.debug(f"Could not read checkpoint mode: {e}")
        return None
    
    def clear_checkpoint(self):
        """Clear checkpoint after successful completion"""
        if self.checkpoint_file and os.path.exists(self.checkpoint_file):
            try:
                os.remove(self.checkpoint_file)
            except OSError as e:
                logger.debug(f"Could not remove checkpoint file: {e}")
    
    def save_failed_series(self):
        """Save list of failed series for retry.
        
        Merges with any existing failed series from previous runs so no
        failures are lost across multiple scraping sessions.
        """
        if not self.failed_file or not self.failed_links:
            return
        try:
            existing = self.load_failed_series()
            # Merge: index existing by URL, then overlay with new failures
            merged = {item.get('url', ''): item for item in existing if isinstance(item, dict)}
            for item in self.failed_links:
                key = item.get('url', '') if isinstance(item, dict) else str(item)
                merged[key] = item
            self._atomic_write_json(self.failed_file, list(merged.values()))
        except Exception as e:
            logger.error(f"Failed to save failed series list: {e}")
            print(f"  ⚠ Warning: could not save failed series list: {e}")
    
    def load_failed_series(self):
        """Load previously failed series for retry"""
        if not self.failed_file:
            return []
        try:
            with open(self.failed_file, 'r', encoding='utf-8') as f:
                return json.load(f) or []
        except FileNotFoundError:
            return []
        except json.JSONDecodeError as e:
            logger.warning(f"Failed series file corrupted, ignoring: {e}")
            return []
        except Exception as e:
            logger.warning(f"Could not load failed series: {e}")
            return []
    
    def clear_failed_series(self):
        """Clear failed series list after successful retry"""
        if self.failed_file and os.path.exists(self.failed_file):
            try:
                os.remove(self.failed_file)
            except OSError as e:
                logger.debug(f"Could not remove failed series file: {e}")
    
    def is_pause_requested(self):
        """Check if pause has been requested via pause file"""
        return self.pause_file and os.path.exists(self.pause_file)
    
    def clear_pause_request(self):
        """Clear pause request file"""
        if self.pause_file and os.path.exists(self.pause_file):
            try:
                os.remove(self.pause_file)
            except OSError as e:
                logger.debug(f"Could not remove pause file: {e}")
        
    # ==================== CONFIG HELPERS ====================
    
    def get_selector(self, path):
        """Get selector from config using dot notation"""
        keys = path.split('.')
        value = self.config.get('selectors', {})
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        return value
    
    def get_login_page(self):
        """Get login page URL from config"""
        return self.config.get('login_page', 'https://s.to/login')
    
    def get_site_url(self):
        """Get site URL from config"""
        return self.config.get('site_url', 'https://s.to')
    
    def get_timing(self, key, default=1.0):
        """Get timing delay from config (in seconds)"""
        return self.config.get('timing', {}).get(key, default)
    
    # ==================== ELEMENT FINDING ====================
    
    def convert_selector_to_by(self, selector_type):
        """Convert config selector type to Selenium By"""
        by_map = {
            'id': By.ID,
            'name': By.NAME,
            'css': By.CSS_SELECTOR,
            'xpath': By.XPATH,
            'tag': By.TAG_NAME,
            'class': By.CLASS_NAME
        }
        return by_map.get(selector_type, By.CSS_SELECTOR)
    
    def find_element_from_config(self, driver, config_selectors, timeout=2):
        """Try to find element using list of selectors from config"""
        if not isinstance(config_selectors, list):
            config_selectors = [config_selectors]
        
        for selector_config in config_selectors:
            selector_type = selector_config.get('type', 'css')
            selector_value = selector_config.get('value')
            
            by = self.convert_selector_to_by(selector_type)
            
            try:
                element = WebDriverWait(driver, timeout).until(
                    EC.presence_of_element_located((by, selector_value))
                )
                return element
            except Exception:
                continue
        
        return None
    
    def wait_for_element(self, driver, selector_by, selector_value, timeout=None, silent=False):
        """
        Wait for element to be present.
        
        Args:
            driver: Selenium WebDriver instance
            selector_by: Selenium By selector type
            selector_value: Selector value to find
            timeout: Timeout in seconds (default from config)
            silent: If True, don't print error messages on timeout
            
        Returns:
            bool: True if element found, False on timeout
        """
        if timeout is None:
            timeout = self.get_timing('page_load_delay') or 5
        try:
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((selector_by, selector_value))
            )
            return True
        except Exception as e:
            if not silent:
                print(f"✗ Timeout waiting for element: {selector_value}")
            return False
    
    def wait_for_css_element(self, driver, css_selector, timeout=None, silent=False):
        """Wait for CSS selector element to be present"""
        return self.wait_for_element(driver, By.CSS_SELECTOR, css_selector, timeout, silent)
    
    # ==================== BROWSER SETUP ====================
    
    def _build_firefox_options(self):
        """Build Firefox options with enhanced anti-detection and stealth"""
        firefox_options = Options()
        
        # Core visibility settings - HEADLESS mode for speed
        if HEADLESS:
            firefox_options.add_argument("-headless")  # Firefox uses single dash
        
        # AGGRESSIVE AD & POPUP BLOCKING
        # Performance & Stealth: Disable images to speed up loading and reduce detection vectors
        firefox_options.set_preference("permissions.default.image", 1)
        # Disable autoplay to prevent automatic ads
        firefox_options.set_preference("media.autoplay.default", 1)
        # Block ALL media (video/audio) - major ad blocker
        firefox_options.set_preference("media.autoplay.default.allowed", False)
        # Disable plugins (Flash ads, etc.)
        firefox_options.set_preference("dom.plugins.enabled", False)
        # Disable stylesheets won't load (reduces page bloat)
        # firefox_options.set_preference("permissions.default.stylesheet", 2)  # Don't use - breaks layout
        # Disable Java
        firefox_options.set_preference("security.enable_java", False)
        # Block pop-ups aggressively
        firefox_options.set_preference("dom.disable_beforeunload", True)
        firefox_options.set_preference("dom.popup_allowed_events", "click")
        # CRITICAL: Set highest tracking protection (blocks ads and tracking scripts)
        firefox_options.set_preference("browser.contentblocking.category", "strict")
        # Disable process prelaunch for stealth
        firefox_options.set_preference("dom.ipc.processPrelaunch.enabled", False)
        # Disable speculative connections
        firefox_options.set_preference("network.http.speculative-parallel-limit", 0)
        # Increase privacy: don't keep tracking cookies between requests
        firefox_options.set_preference("network.cookie.lifetimePolicy", 2)
        # Disable network prediction
        firefox_options.set_preference("network.dns.disablePrefetch", True)
        # Disable referrer (reduces tracking)
        firefox_options.set_preference("network.http.sendRefererHeader", 0)
        # Speed up by disabling non-essential features
        firefox_options.set_preference("network.IDN_show_punycode", True)
        # Disable geo-location (ads use this)
        firefox_options.set_preference("geo.enabled", False)
        # Disable WebGL (used by some ad networks)
        firefox_options.set_preference("webgl.disabled", True)
        
        # User agent to appear like real browser
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:110.0) Gecko/20100101 Firefox/110.0',
            'Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0',
        ]
        firefox_options.set_preference('general.useragent.override', random.choice(user_agents))
        
        # Firefox preferences to appear more human-like
        firefox_options.set_preference('dom.webdriver.enabled', False)
        firefox_options.set_preference('useAutomationExtension', False)
        firefox_options.set_preference('dom.webdriver.chromium.enabled', False)
        
        # Network preferences for realistic behavior
        firefox_options.set_preference('network.http.keep-alive.timeout', 300)
        firefox_options.set_preference('network.http.max-persistent-connections-per-server', 6)
        # Reduce HTTP pipelining
        firefox_options.set_preference('network.http.pipelining', False)
        
        return firefox_options
    
    def _get_ublock_xpi(self):
        """Find uBlock Origin .xpi, using local copy or copying from Firefox profile."""
        ublock_id = 'uBlock0@raymondhill.net.xpi'
        addon_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'addons')
        local_xpi = os.path.join(addon_dir, 'ublock_origin.xpi')
        
        # Use local copy if it exists
        if os.path.isfile(local_xpi):
            return local_xpi
        
        # Search Firefox profiles and copy to local addons folder
        profiles_dir = os.path.join(os.environ.get('APPDATA', ''), 'Mozilla', 'Firefox', 'Profiles')
        if os.path.isdir(profiles_dir):
            for profile in os.listdir(profiles_dir):
                xpi_path = os.path.join(profiles_dir, profile, 'extensions', ublock_id)
                if os.path.isfile(xpi_path):
                    os.makedirs(addon_dir, exist_ok=True)
                    shutil.copy2(xpi_path, local_xpi)
                    print(f'✓ Copied uBlock Origin from Firefox profile: {profile}')
                    return local_xpi
        
        # 3. Download from Mozilla Add-ons as last resort
        url = 'https://addons.mozilla.org/firefox/downloads/latest/ublock-origin/latest.xpi'
        print('→ Downloading uBlock Origin from addons.mozilla.org...')
        try:
            os.makedirs(addon_dir, exist_ok=True)
            urllib.request.urlretrieve(url, local_xpi)
            print('✓ uBlock Origin downloaded')
            return local_xpi
        except Exception as e:
            print(f'⚠ Failed to download uBlock Origin: {e}')
            return None
    
    def setup_driver(self):
        """Initialize the Selenium WebDriver with anti-detection measures and aggressive ad-blocking"""
        firefox_options = self._build_firefox_options()
        service = FirefoxService()
        self.driver = webdriver.Firefox(service=service, options=firefox_options)
        
        # Track main driver PID (worker_id=0) so sequential mode is visible
        # in 'show active workers' and cleaned up on hard-kill
        try:
            if hasattr(service, 'process') and service.process:
                geckodriver_pid = service.process.pid
                if geckodriver_pid:
                    self.save_worker_pid(0, geckodriver_pid)
                    # Also track the Firefox child process
                    try:
                        result = subprocess.run(
                            ['wmic', 'process', 'where', f'ParentProcessId={geckodriver_pid}',
                             'get', 'ProcessId', '/value'],
                            capture_output=True, text=True, timeout=2, check=False
                        )
                        for line in result.stdout.split('\n'):
                            if 'ProcessId' in line:
                                firefox_pid = line.split('=')[-1].strip()
                                if firefox_pid.isdigit():
                                    self.save_worker_pid('0_firefox', int(firefox_pid))
                    except Exception:
                        pass
        except Exception:
            pass
        
        # Execute JavaScript to hide webdriver flag and inject stealth properties
        try:
            # Hide webdriver detection
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => false})")
            # Hide plugins to appear more real
            self.driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
            # Set realistic languages
            self.driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']})")
            # Hide chrome property
            self.driver.execute_script("Object.defineProperty(window, 'chrome', {get: () => ({runtime: {}})})")
            # Set realistic screen properties
            self.driver.execute_script("""
                Object.defineProperty(screen, 'availWidth', {get: () => 1920});
                Object.defineProperty(screen, 'availHeight', {get: () => 1040});
                Object.defineProperty(screen, 'width', {get: () => 1920});
                Object.defineProperty(screen, 'height', {get: () => 1080});
                Object.defineProperty(screen, 'colorDepth', {get: () => 24});
            """)
            # Set timezone offset to a realistic European value (fixed per session)
            tz_offset = random.choice([-60, -60, -120, -120, -120, -180])  # UTC+1/+2/+3, weighted toward CET/CEST
            self.driver.execute_script(f"""
                Date.prototype.getTimezoneOffset = () => {tz_offset};
            """)
        except Exception as e:
            print(f"⚠ Warning: Some anti-detection scripts failed: {e}")
        
        # Install uBlock Origin for real ad-blocking
        ublock_xpi = self._get_ublock_xpi()
        if ublock_xpi:
            try:
                self.driver.install_addon(ublock_xpi, temporary=True)
                print('✓ uBlock Origin installed')
                time.sleep(0.5)  # Let extension initialize
            except Exception as e:
                print(f'⚠ Failed to install uBlock Origin: {e}')
        
        # Inject aggressive ad-blocking CSS
        self.inject_aggressive_adblock()
        
        # Setup popup blocker
        self.inject_popup_killer()
    
    # ==================== AD-BLOCKING & POPUP KILLER ====================
    
    def human_delay(self, min_sec=0.5, max_sec=2.0):
        """Add random human-like delay between requests"""
        delay = random.uniform(min_sec, max_sec)
        time.sleep(delay)

    def _wait_for_page_ready(self, driver=None, timeout=10):
        """Wait for a page to be fully loaded before parsing.
        
        Uses document.readyState to ensure the page is complete,
        then waits for <body> to be present. This prevents parsing
        before the DOM (including nav elements like season pills) is ready.
        
        Args:
            driver: WebDriver instance (defaults to self.driver)
            timeout: Max seconds to wait (default 10)
        """
        drv = driver or self.driver
        try:
            WebDriverWait(drv, timeout).until(
                lambda d: d.execute_script('return document.readyState') == 'complete'
            )
        except Exception:
            logger.debug("Timeout waiting for document.readyState == 'complete'")
        try:
            WebDriverWait(drv, timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, 'body'))
            )
        except Exception:
            pass

    def inject_aggressive_adblock(self, driver=None):
        """Inject aggressive CSS to hide ads, popups, and tracking elements"""
        drv = driver or self.driver
        try:
            adblock_css = """
                /* Hide all ad-related elements */
                [class*="ad-"], [id*="ad-"],
                [class*="ads"], [id*="ads"],
                [class*="advertisement"], [id*="advertisement"],
                [class*="advert"], [id*="advert"],
                [class*="banner"], [id*="banner"],
                [class*="popup"], [id*="popup"],
                [class*="modal"], [id*="modal"],
                [class*="overlay"], [id*="overlay"],
                [data-type="ad"], [data-ad-slot],
                .adsense, #adsense,
                .doubleclick, #doubleclick,
                .google-ads, #google-ads,
                iframe[src*="ads"],
                iframe[src*="doubleclick"],
                iframe[src*="google"],
                iframe[src*="adsense"],
                iframe[src*="banner"],
                .sponsor, [class*="sponsor"],
                .commercial, [class*="commercial"],
                .promo, [class*="promo"]
                {
                    display: none !important;
                    visibility: hidden !important;
                    height: 0 !important;
                    margin: 0 !important;
                    padding: 0 !important;
                    border: 0 !important;
                }
                
                /* Hide common popup elements */
                div.popup, div.modal, div.overlay,
                .dialogBox, .notification-box,
                .alert, .alert-box
                {
                    display: none !important;
                }
            """
            drv.execute_script(f"""
                var style = document.createElement('style');
                style.textContent = `{adblock_css}`;
                document.head.appendChild(style);
            """)
        except Exception as e:
            logger.debug(f"Ad-blocking CSS injection failed: {e}")
    
    def inject_popup_killer(self, driver=None):
        """Inject JavaScript to automatically close popups, modals, and blocking iframes"""
        drv = driver or self.driver
        try:
            popup_killer_script = """
                // Close all popups/modals on page load
                function killPopups() {
                    // Remove blocking iframes (common ad containers)
                    document.querySelectorAll('iframe').forEach(iframe => {
                        const classList = iframe.className || '';
                        const id = iframe.id || '';
                        // Remove if it looks like an ad iframe
                        if (classList.includes('container-') || 
                            classList.includes('ad') || 
                            id.includes('container-') ||
                            id.includes('ad')) {
                            iframe.style.display = 'none';
                            iframe.remove();
                        }
                    });
                    
                    // Close Bootstrap modals
                    document.querySelectorAll('.modal').forEach(el => {
                        el.style.display = 'none';
                        el.remove();
                    });
                    
                    // Hide overlays
                    document.querySelectorAll('[class*="overlay"]').forEach(el => {
                        el.style.display = 'none';
                        el.remove();
                    });
                    
                    // Close advertisements
                    document.querySelectorAll('[class*="popup"]').forEach(el => {
                        el.style.display = 'none';
                        el.remove();
                    });
                    
                    // Prevent popups from opening
                    window.open = function() { return null; };
                    window.alert = function() { return null; };
                    window.confirm = function() { return true; };
                    
                    // Remove onbeforeunload listeners (prevent exit popups)
                    window.onbeforeunload = null;
                }
                
                // Run immediately
                killPopups();
                
                // Run on page load
                if (document.readyState !== 'loading') {
                    killPopups();
                } else {
                    document.addEventListener('DOMContentLoaded', killPopups);
                }
                
                // Monitor for new popups being added
                const observer = new MutationObserver(killPopups);
                observer.observe(document.body, { childList: true, subtree: true });
            """
            drv.execute_script(popup_killer_script)
        except Exception as e:
            logger.debug(f"Popup killer injection failed: {e}")
    
    def _is_driver_alive(self, driver=None):
        """Check if a WebDriver session is still usable."""
        drv = driver or self.driver
        if drv is None:
            return False
        try:
            _ = drv.current_url
            return True
        except Exception:
            return False

    def close(self):
        """Close the browser"""
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
            print("✓ Browser closed")
    
    def _has_auth_cookies(self, driver):
        """Lightweight auth check: verify session cookies exist without page navigation.
        
        Much faster than is_logged_in() which loads 2 pages. Use for periodic
        health checks; reserve is_logged_in() for error recovery.
        
        Returns:
            bool: True if session cookies are present
        """
        try:
            cookies = driver.get_cookies()
            cookie_names = {c['name'] for c in cookies}
            # s.to uses a session cookie — check that at least one auth-related cookie exists
            # Common session cookie names for PHP/Laravel sites
            session_indicators = {'session', 'PHPSESSID', 'laravel_session', 'remember_web', 'XSRF-TOKEN'}
            if cookie_names & session_indicators:
                return True
            # Fallback: if we have any cookies on the s.to domain, session is likely alive
            site_domain = urlparse(self.get_site_url()).hostname
            domain_cookies = [c for c in cookies if site_domain in (c.get('domain', '') or '')]
            return len(domain_cookies) >= 2  # At least 2 domain cookies suggests active session
        except Exception:
            return False

    def is_logged_in(self, driver):
        """Verify login by checking redirect behavior"""
        try:
            # Test 1: Try to access account page
            account_url = self.config.get('account_page', 'https://s.to/account')
            self._throttle_request()
            driver.get(account_url)
            self._wait_for_page_ready(driver)
            self.human_delay(0.5, 1.0)
            
            # If we're still on account page (not redirected to login), we're logged in
            current_url = driver.current_url
            login_page = self.get_login_page()
            
            if login_page in current_url or 'login' in current_url:
                logger.debug("Login check: redirected to login page — not logged in")
                return False  # Redirected to login = not logged in
                
            # Test 2: Try to access login page directly 
            self._throttle_request()
            driver.get(login_page)
            self._wait_for_page_ready(driver)
            self.human_delay(0.5, 1.0)
            
            # If we're redirected away from login page, we're logged in
            current_url = driver.current_url
            if current_url != login_page and 'login' not in current_url:
                logger.debug("Login check: redirected from login page — logged in")
                return True  # Redirected from login = logged in
                
            # If we're still on login page, we're not logged in
            logger.debug("Login check: stayed on login page — not logged in")
            return False
            
        except Exception as e:
            logger.warning(f"Login check failed with exception: {e}")
            return False
    
    def login(self, driver=None, retry_count=0, max_retries=2):
        """Login to s.to using email/password with human-like behavior and ad-blocking"""
        drv = driver or self.driver
        try:
            login_config = self.get_selector('login')
            if not login_config:
                raise Exception("Login config not found")
            
            login_page = self.get_login_page()
            self._throttle_request()
            drv.get(login_page)
            self._wait_for_page_ready(drv)
            
            # Check if login page itself returned a server error (502/503)
            page_source = drv.page_source
            server_error = self.check_server_error(page_source)
            if server_error:
                self._record_server_error()
                raise Exception(f"Login page returned {server_error}")
            
            # Kill any popups and remove blocking iframes IMMEDIATELY on page load
            self.inject_popup_killer(drv)
            self.inject_aggressive_adblock(drv)
            
            # Wait for login form to appear
            WebDriverWait(drv, TIMEOUT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='submit'], button[type='submit']"))
            )

            # Email field
            email_field = self.find_element_from_config(
                drv, 
                login_config.get('username_field', []),
                timeout=TIMEOUT
            )
            if not email_field:
                raise Exception("Email field not found")
            
            # Set email instantly via JS
            drv.execute_script(
                "arguments[0].value = arguments[1]; arguments[0].dispatchEvent(new Event('input', {bubbles: true}));",
                email_field, self.email
            )

            # Password field
            password_field = self.find_element_from_config(
                drv,
                login_config.get('password_field', []),
                timeout=TIMEOUT
            )
            if not password_field:
                raise Exception("Password field not found")
            
            # Set password instantly via JS
            drv.execute_script(
                "arguments[0].value = arguments[1]; arguments[0].dispatchEvent(new Event('input', {bubbles: true}));",
                password_field, self.password
            )
            
            # Simulate mouse movement to make activity more human-like
            try:
                action = ActionChains(drv)
                submit_button = self.find_element_from_config(
                    drv,
                    login_config.get('submit_button', []),
                    timeout=TIMEOUT
                )
                if submit_button:
                    action.move_to_element(submit_button)
                    action.perform()
            except Exception:
                pass

            # Find and click submit button
            submit_button = self.find_element_from_config(
                drv,
                login_config.get('submit_button', []),
                timeout=TIMEOUT
            )

            if submit_button:
                # Remove any blocking iframes before clicking
                try:
                    drv.execute_script("""
                        document.querySelectorAll('iframe').forEach(iframe => {
                            iframe.remove();
                        });
                    """)
                except Exception:
                    pass
                
                # Try normal click first
                try:
                    drv.execute_script("arguments[0].scrollIntoView(true);", submit_button)
                    submit_button.click()
                except Exception as e:
                    drv.execute_script("arguments[0].click();", submit_button)
            else:
                # Fallback: press Enter on password field
                password_field = self.find_element_from_config(
                    drv,
                    login_config.get('password_field', []),
                    timeout=TIMEOUT
                )
                if password_field:
                    password_field.send_keys("\n")

            # Wait for redirect or page load
            try:
                WebDriverWait(drv, TIMEOUT).until(
                    lambda d: d.current_url != login_page
                )
            except Exception:
                pass

            # Wait for body to fully load
            WebDriverWait(drv, TIMEOUT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "body"))
            )
            self._wait_for_page_ready(drv)
            
            # Kill any popups and blocking iframes that appear after login
            self.inject_popup_killer(drv)
            self.inject_aggressive_adblock(drv)
            
            # Save auth cookies for worker browsers
            self.auth_cookies = drv.get_cookies()
            
            # Verify login succeeded
            if self.is_logged_in(drv):
                logger.info("Login successful")
                return True

            # Login was not verified — treat as failure
            raise Exception("Login completed but verification failed")

        except Exception as e:
            # Retry logic
            if retry_count < max_retries:
                logger.warning(f"Login attempt {retry_count + 1}/{max_retries} failed: {e}")
                print(f"→ Retrying login ({retry_count + 1}/{max_retries})...")
                drv.delete_all_cookies()
                self.human_delay(2, 5)
                return self.login(drv, retry_count + 1, max_retries)
            else:
                logger.error(f"Login failed after {max_retries} retries: {e}")
                print("✗ Max login retries exceeded")
                raise
    
    # ==================== SERIES DISCOVERY ====================

    def get_all_series(self):
        """
        Get list of all series from the s.to/serien index page.
        
        Equivalent to bs.to's get_all_series() which scrapes /andere-serien.
        
        Returns:
            list[dict]: Series dicts with 'title', 'link', 'url' keys
        """
        try:
            print("→ Fetching list of all series...")
            site_url = self.get_site_url()
            idx_path = (self.get_selector('series_index') or {}).get('path', '/serien')
            all_series_url = f"{site_url}{idx_path}"

            self.driver.get(all_series_url)
            self._wait_for_page_ready()

            soup = BeautifulSoup(self.driver.page_source, 'html.parser')

            series_list = []
            seen = set()
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                m = _SERIE_SLUG_RE.match(href)
                if not m:
                    continue
                slug = m.group(1)
                if slug in seen:
                    continue
                seen.add(slug)
                title = link.get_text(strip=True)
                if title:
                    series_list.append({
                        'title': title,
                        'link': f"/serie/{slug}",
                        'url': f"{site_url}/serie/{slug}"
                    })

            return series_list

        except Exception as e:
            print(f"✗ Failed to fetch series index: {str(e)}")
            logger.error(f"Failed to fetch series index: {e}")
            raise

    def get_account_series(self, source='both'):
        """
        Discover user's subscribed/watchlist series from account pages.
        
        When source is 'both', fetches subscribed and watchlist pages in parallel
        using 2 worker browsers for speed. Otherwise uses the main browser.
        
        Args:
            source: 'subscribed', 'watchlist', or 'both' (default)
        
        Returns:
            list[dict]: Series dicts with 'title', 'link', 'url' keys
        """
        site_url = self.get_site_url()
        acct = self.get_selector('account_pages') or {}
        all_pages = [
            (acct.get('subscribed', '/account/subscribed'), 'Subscriptions'),
            (acct.get('watchlist', '/account/watchlist'),   'Watchlist'),
        ]
        if source == 'subscribed':
            account_pages = [all_pages[0]]
        elif source == 'watchlist':
            account_pages = [all_pages[1]]
        else:
            account_pages = [all_pages[0], all_pages[1]]

        # When fetching both, use 2 parallel workers for speed
        if source == 'both' and len(account_pages) == 2:
            return self._get_account_series_parallel(account_pages, site_url)

        # Single source: use main driver
        return self._get_account_series_sequential(account_pages, site_url, self.driver)

    def _fetch_account_page_series(self, page_path, label, site_url, driver):
        """
        Fetch all series from a single account page (handles pagination).
        
        Returns:
            list[dict]: Series found on this account page
        """
        series_list = []
        page_num = 1
        while True:
            url = f"{site_url}{page_path}" if page_num == 1 else f"{site_url}{page_path}?page={page_num}"
            try:
                driver.get(url)
                self._wait_for_page_ready(driver)

                soup = BeautifulSoup(driver.page_source, 'html.parser')

                if page_num == 1:
                    count_div = soup.find('div', class_='text-muted')
                    if count_div:
                        logger.debug(f"{label}: {count_div.get_text(strip=True)}")

                page_found = 0
                for link in soup.find_all('a', href=True):
                    href = link.get('href', '')
                    m = _SERIE_SLUG_RE.match(href)
                    if not m:
                        continue
                    slug = m.group(1)
                    title = link.get_text(strip=True) or slug
                    series_list.append({
                        'title': title,
                        'link': f"/serie/{slug}",
                        'url': f"{site_url}/serie/{slug}",
                        'slug': slug,
                    })
                    page_found += 1

                has_next = False
                pagination = soup.find('ul', class_='pagination')
                if pagination:
                    next_link = pagination.find('a', attrs={'rel': 'next'})
                    if next_link:
                        has_next = True

                if has_next:
                    logger.debug(f"{label} page {page_num}: found {page_found} series, loading next page...")
                    page_num += 1
                else:
                    logger.debug(f"{label} page {page_num}: found {page_found} series (last page)")
                    break

            except Exception as e:
                logger.warning(f"Could not scan {url}: {e}")
                break

        print(f"  ✓ {label}: {len(series_list)} series found")
        return series_list

    def _get_account_series_parallel(self, account_pages, site_url):
        """
        Fetch subscribed and watchlist pages in parallel using 2 worker browsers.
        Each worker gets its own browser, authenticates, and fetches one source.
        """
        print("→ Fetching subscribed & watchlist in parallel (2 workers)...")

        results = [None, None]
        errors = [None, None]

        def worker(worker_id, page_path, label):
            driver = None
            try:
                driver = self._create_worker_driver(worker_id=f"discovery_{worker_id}")
                self.inject_aggressive_adblock(driver)

                # Authenticate worker
                authenticated = self._authenticate_driver(driver, label=f"Discovery worker {worker_id}")

                if not authenticated:
                    errors[worker_id] = f"Failed to authenticate discovery worker {worker_id}"
                    logger.error(errors[worker_id])
                    return

                results[worker_id] = self._fetch_account_page_series(page_path, label, site_url, driver)

            except Exception as e:
                errors[worker_id] = str(e)
                logger.error(f"Discovery worker {worker_id} failed: {e}")
            finally:
                if driver:
                    try:
                        driver.quit()
                    except Exception:
                        pass

        threads = []
        for idx, (page_path, label) in enumerate(account_pages):
            t = threading.Thread(target=worker, args=(idx, page_path, label))
            threads.append(t)
            t.start()
            time.sleep(1)  # Stagger startup

        for t in threads:
            t.join()

        # If parallel failed for a source, fall back to main driver for that source
        for idx, (page_path, label) in enumerate(account_pages):
            if results[idx] is None:
                if errors[idx]:
                    print(f"  ⚠ {label} parallel fetch failed: {errors[idx]}")
                    print(f"  → Falling back to main browser for {label}...")
                results[idx] = self._fetch_account_page_series(page_path, label, site_url, self.driver)

        # Merge and deduplicate
        seen = set()
        series_list = []
        for source_results in results:
            if source_results:
                for item in source_results:
                    slug = item.get('slug', item['url'].rstrip('/').split('/')[-1])
                    if slug not in seen:
                        seen.add(slug)
                        item.pop('slug', None)
                        series_list.append(item)

        logger.info(f"Account series discovery (both, parallel): found {len(series_list)} unique series")
        print(f"\n  Total unique series discovered: {len(series_list)}")
        return series_list

    def _get_account_series_sequential(self, account_pages, site_url, driver):
        """Fetch account series sequentially using a single driver."""
        seen = set()
        series_list = []

        for page_path, label in account_pages:
            page_series = self._fetch_account_page_series(page_path, label, site_url, driver)
            for item in page_series:
                slug = item.get('slug', item['url'].rstrip('/').split('/')[-1])
                if slug not in seen:
                    seen.add(slug)
                    item.pop('slug', None)
                    series_list.append(item)

        logger.info(f"Account series discovery: found {len(series_list)} unique series")
        print(f"\n  Total unique series discovered: {len(series_list)}")
        return series_list

    # ==================== SERIES & EPISODE SCRAPING ====================
    
    # Patterns that reliably identify HTTP error pages by their <title>.
    # Matches titles like "404", "404 Nicht gefunden", "Error 404", "404 Not Found"
    # but NOT series names that happen to contain digits (e.g. "Apartment404").
    _ERROR_TITLE_RE = re.compile(
        r'^(?:Error\s+)?(?P<code>\d{3})\b|\b(?:Error|Fehler)\s+(?P<code2>\d{3})\b',
        re.IGNORECASE,
    )

    def check_series_not_found_error(self, html):
        """Check if page contains error message for series not found.
        Returns error message if found, None otherwise."""
        soup = BeautifulSoup(html, 'html.parser')
        # Check for inline "messageBox error" div (old-style error)
        error_div = soup.find('div', class_='messageBox error')
        if error_div:
            error_text = error_div.get_text(strip=True)
            if 'nicht gefunden' in error_text.lower():
                return error_text
        # Check for standalone 404 page by <title> – only match if the title
        # starts with "404" or contains "Error 404", not a series name.
        title_tag = soup.find('title')
        if title_tag:
            title_text = title_tag.get_text(strip=True)
            m = self._ERROR_TITLE_RE.search(title_text)
            if m:
                code = m.group('code') or m.group('code2')
                if code == '404':
                    return title_text
        # Check for <h2>404</h2> (exact match, safe from series names)
        h2_tag = soup.find('h2')
        if h2_tag and h2_tag.get_text(strip=True) == '404':
            p_tag = soup.find('p')
            return p_tag.get_text(strip=True) if p_tag else '404 Nicht gefunden'
        return None

    # Map of HTTP error codes to their standard reason phrases.
    _SERVER_ERROR_CODES = {
        '429': '429 Too Many Requests',
        '500': '500 Internal Server Error',
        '502': '502 Bad Gateway',
        '503': '503 Service Unavailable',
        '504': '504 Gateway Timeout',
    }

    def check_server_error(self, html):
        """Check if page contains a server error (429, 500, 502, 503, 504, etc.).
        Returns error message if found, None otherwise."""
        soup = BeautifulSoup(html, 'html.parser')
        # Check <title> for error-page patterns (e.g. "502 Bad Gateway", "Error 503")
        title_tag = soup.find('title')
        if title_tag:
            title_text = title_tag.get_text(strip=True)
            m = self._ERROR_TITLE_RE.search(title_text)
            if m:
                code = m.group('code') or m.group('code2')
                if code in self._SERVER_ERROR_CODES:
                    return self._SERVER_ERROR_CODES[code]
        # Also check for common error patterns in the page body
        # These require BOTH the code AND the reason phrase, so false positives
        # from series names are extremely unlikely.
        body_text = soup.get_text(strip=True) if soup.body else ''
        for code, message in self._SERVER_ERROR_CODES.items():
            reason = message.split(' ', 1)[1]  # e.g. "Too Many Requests"
            if code in body_text and reason in body_text:
                return message
        return None
    
    def scrape_series_detail(self, series_url, driver=None, max_retries=3):
        """
        Scrape all episodes from a single season page with retry logic.
        Returns dict with season number, URL, episodes list, subscription status, and title.
        """
        drv = driver or self.driver
        # Quick sanity check: is the driver still usable?
        if not self._is_driver_alive(drv):
            logger.error(f"Driver is dead before scraping {series_url}")
            return None
        for attempt in range(max_retries):
            try:
                self._throttle_request()
                drv.get(series_url)
                self._wait_for_page_ready(drv)
                self.inject_popup_killer(drv)
                
                page_source = drv.page_source
                
                # Detect browser error pages (about:neterror, about:certerror, DNS failures, etc.)
                try:
                    current_url = drv.current_url or ''
                except Exception:
                    current_url = ''
                if 'neterror' in current_url or 'dnsNotFound' in current_url or current_url.startswith('about:'):
                    raise Exception(f"Browser error page: {current_url}")
                if page_source and ('Die Verbindung mit dem Server' in page_source or 'dnsNotFound' in page_source):
                    raise Exception(f"Network error page for: {series_url}")
                
                # Detect server errors (502, 503) — raise to trigger retry
                server_error = self.check_server_error(page_source)
                if server_error:
                    self._record_server_error()
                    raise Exception(f"{server_error}: {series_url}")

                # Detect "Serie nicht gefunden" error
                error_found = self.check_series_not_found_error(page_source)
                if error_found:
                    logger.warning(f"Series not found: {series_url} — {error_found}")
                    print(f"  ✗ Series not found: {series_url} - {error_found}")
                    return None
                
                soup = BeautifulSoup(page_source, 'html.parser')
                
                # Extract season number from URL
                staffel_match = series_url.split('/staffel-')[-1]
                season = staffel_match.split('/')[0] if staffel_match else '1'
                
                # Find all episode rows
                episodes = []
                sel = self.get_selector('series_detail') or {}
                episode_rows = soup.select(sel.get('episode_rows', '.episode-table tbody tr.episode-row'))
                
                for row in episode_rows:
                    try:
                        # Extract episode number
                        num_cell = row.select_one(sel.get('episode_number', 'th.episode-number-cell'))
                        episode_num = num_cell.get_text(strip=True) if num_cell else '?'
                        
                        # Extract both German and English titles
                        ger_cell = row.select_one(sel.get('title_ger', '.episode-title-ger'))
                        eng_cell = row.select_one(sel.get('title_eng', '.episode-title-eng'))
                        title_ger = ger_cell.get_text(strip=True) if ger_cell else ''
                        title_eng = eng_cell.get_text(strip=True) if eng_cell else ''
                        
                        # Check if watched (look for .seen class)
                        is_watched = sel.get('watched_class', 'seen') in row.get('class', [])
                        
                        episodes.append({
                            'number': episode_num,
                            'title_ger': title_ger,
                            'title_eng': title_eng,
                            'watched': is_watched
                        })
                    except Exception as e:
                        logger.warning(f"Error parsing episode row in {series_url}: {e}")
                        print(f"    ⚠ Error parsing episode row: {str(e)[:50]}")
                        continue
                
                # Detect subscription status and title from this page
                subscribed, watchlist = self.detect_subscription_status(soup)
                title_element = soup.select_one(sel.get('series_title', 'h1.fw-bold'))
                series_title = title_element.get_text(strip=True) if title_element else None
                
                # Extract alternative titles from description brackets [Alt1 | Alt2]
                alt_titles = []
                desc_element = soup.select_one(sel.get('description', '.series-description .description-text'))
                if desc_element:
                    desc_text = desc_element.get_text(strip=True)
                    if desc_text.startswith('['):
                        bracket_end = desc_text.find(']')
                        if bracket_end > 0:
                            bracket_content = desc_text[1:bracket_end]
                            alt_titles = [t.strip() for t in bracket_content.split('|') if t.strip()]
                
                watched_count = sum(1 for ep in episodes if ep['watched'])
                
                logger.debug(f"Scraped {series_url}: {len(episodes)} episodes ({watched_count} watched), title={series_title}")
                if len(episodes) == 0:
                    logger.warning(f"0 episodes found for {series_url} — page may not have loaded correctly")
                
                self._decay_global_backoff()
                return {
                    'season': season,
                    'url': series_url,
                    'episodes': episodes,
                    'watched_episodes': watched_count,
                    'total_episodes': len(episodes),
                    'subscribed': subscribed,
                    'watchlist': watchlist,
                    'title': series_title,
                    'alt_titles': alt_titles
                }
                
            except Exception as e:
                error_msg = str(e)
                if attempt < max_retries - 1:
                    # Use longer backoff for rate limiting (429) and server overload (502/503)
                    if '429' in error_msg or '502' in error_msg or '503' in error_msg:
                        backoff = min(30.0, 10.0 * (2 ** attempt))
                        logger.warning(f"Server overload on {series_url}, backing off {backoff:.0f}s")
                    else:
                        backoff = min(self.get_timing('error_backoff_max') or 15.0,
                                      (self.get_timing('error_backoff_base') or 2.0) * (2 ** attempt))
                    logger.warning(f"Retry {attempt + 2}/{max_retries} for {series_url}: {e}")
                    print(f"  ⚠ Retrying {series_url} (attempt {attempt + 2}/{max_retries}): {str(e)[:50]}")
                    time.sleep(backoff + random.uniform(0.5, 2.0))
                else:
                    logger.error(f"Failed to scrape {series_url} after {max_retries} attempts: {e}")
                    print(f"  ✗ Failed to scrape {series_url} after {max_retries} attempts: {str(e)[:80]}")
                    return None

    def detect_subscription_status(self, soup):
        """
        Detect if user has series subscribed and/or on watchlist.
        
        Checks for active state on subscription buttons using CSS classes.
        
        Returns:
            tuple: (subscribed: bool, watchlist: bool)
        """
        try:
            subscribed = False
            watchlist = False
            
            # Look for subscription buttons
            sub_sel = self.get_selector('subscription') or {}
            buttons = soup.select(sub_sel.get('action_button', '.js-action-btn'))
            active_class = sub_sel.get('active_class', 'btn-glass-primary')
            type_sub = sub_sel.get('type_subscribed', 'favorite')
            type_wl = sub_sel.get('type_watchlist', 'watchlater')
            
            for button in buttons:
                data_type = button.get('data-type', '')
                
                # Check for active state (either active class or data-active="1")
                is_active = active_class in button.get('class', []) or \
                           button.get('data-active') == '1'
                
                if data_type == type_sub and is_active:
                    subscribed = True
                elif data_type == type_wl and is_active:
                    watchlist = True
            
            return (subscribed, watchlist)
            
        except Exception as e:
            print(f"    ⚠ Error detecting subscription status: {str(e)[:50]}")
            return (False, False)

    # ==================== SERIES DISCOVERY ====================
    
    def get_series_slug_from_url(self, url):
        """
        Extract series slug from full URL or relative path.
        
        Handles both /serie/slug and full URLs with host.
        
        Args:
            url: Series URL (full or relative) or slug
            
        Returns:
            str: Series slug (e.g., 'attack-on-titan') or 'unknown' on failure
        """
        try:
            # Remove protocol and domain if present
            if url.startswith('http'):
                # Parse URL and get path
                path = urlparse(url).path
            else:
                path = url
            
            # Extract slug from /serie/{slug} or /serie/{slug}/staffel-{num}
            parts = path.split('/')
            if 'serie' in parts:
                idx = parts.index('serie')
                if idx + 1 < len(parts):
                    return parts[idx + 1]
            
            return 'unknown'
        except Exception as e:
            print(f"    ⚠ Error extracting slug: {str(e)[:50]}")
            return 'unknown'

    def _extract_seasons_from_soup(self, soup, series_slug):
        """
        Extract season numbers from a parsed page that has the season nav.
        
        Uses data-season-pill attributes first, then falls back to href patterns.
        Handles season 0 (Filme/OVAs/Specials) correctly.
        
        Args:
            soup: BeautifulSoup parsed page
            series_slug: Series slug for href matching
            
        Returns:
            list: Season numbers as strings, or empty list if none found
        """
        nav_sel = self.get_selector('season_nav') or {}
        pill_selector = nav_sel.get('pills', '#season-nav a[data-season-pill]')
        pill_attr = nav_sel.get('pill_attribute', 'data-season-pill')
        
        seasons = []
        
        # Primary: use data-season-pill attributes
        seen = set()
        season_links = soup.select(pill_selector)
        for link in season_links:
            try:
                season_num = link.get(pill_attr, '')
                if season_num is not None and season_num != '' and season_num not in seen:
                    seen.add(season_num)
                    seasons.append(season_num)
            except Exception:
                continue
        
        if seasons:
            return seasons
        
        # Fallback: find seasons from href patterns like /serie/{slug}/staffel-{num}
        staffel_pattern = re.compile(
            rf'/serie/{re.escape(series_slug)}/staffel-(\d+)', re.IGNORECASE
        )
        seen = set()
        for a_tag in soup.find_all('a', href=True):
            m = staffel_pattern.search(a_tag['href'])
            if m and m.group(1) not in seen:
                seen.add(m.group(1))
                seasons.append(m.group(1))
        
        return seasons

    def get_all_seasons_for_series(self, series_slug, driver=None, max_retries=None):
        """
        Detect all available seasons for a series from the #season-nav element.
        
        Loads the series main page, waits for #season-nav to appear, extracts
        season numbers from pills or href patterns. Retries on errors or
        partial loads. Never guesses — always relies on the nav element.
        
        Args:
            series_slug: Series slug (e.g., 'attack-on-titan')
            driver: Optional WebDriver instance (defaults to self.driver)
            max_retries: Max retry attempts (default from config)
            
        Returns:
            list: Season numbers as strings (e.g., ['0', '1', '2'])
            
        Raises:
            SeasonDetectionError: If season detection fails after all retries
        """
        drv = driver or self.driver
        if max_retries is None:
            val = self.get_timing('max_retries_season', default=None)
            max_retries = int(val) if val is not None else 3
        
        last_error = None
        for attempt in range(max_retries):
            try:
                base_url = f"https://s.to/serie/{series_slug}"
                self._throttle_request()
                drv.get(base_url)
                self._wait_for_page_ready(drv)
                self.inject_popup_killer(drv)
                
                # Check for browser error pages first (cheapest check)
                try:
                    current_url = drv.current_url or ''
                except Exception:
                    current_url = ''
                if 'neterror' in current_url or 'dnsNotFound' in current_url or current_url.startswith('about:'):
                    last_error = f"Browser error page for {series_slug}: {current_url}"
                    logger.warning(f"Attempt {attempt + 1}/{max_retries}: {last_error}")
                    if attempt < max_retries - 1:
                        time.sleep(2)
                        continue
                    raise SeasonDetectionError(last_error)
                
                # Wait for #season-nav to appear in DOM — polls quickly, returns as soon as found
                nav_wait = self.get_timing('season_nav_wait') or 10
                nav_found = False
                try:
                    WebDriverWait(drv, nav_wait).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, '#season-nav'))
                    )
                    nav_found = True
                except Exception:
                    pass
                
                # Get page source AFTER waiting for nav
                page_source = drv.page_source
                
                # If nav wasn't found, check why before retrying
                if not nav_found:
                    server_error = self.check_server_error(page_source)
                    if server_error:
                        self._record_server_error()
                        last_error = f"{server_error} on season detection for {series_slug}"
                        logger.warning(f"Attempt {attempt + 1}/{max_retries}: {last_error}")
                        if attempt < max_retries - 1:
                            backoff = min(30.0, 10.0 * (2 ** attempt))
                            time.sleep(backoff + random.uniform(0.5, 2.0))
                            continue
                        raise SeasonDetectionError(last_error)
                    
                    # Check for series not found (no point retrying)
                    error_found = self.check_series_not_found_error(page_source)
                    if error_found:
                        logger.warning(f"Series not found during season detection: {series_slug} — {error_found}")
                        raise SeasonDetectionError(f"Series not found: {series_slug} — {error_found}")
                
                # Parse and extract seasons from the nav
                soup = BeautifulSoup(page_source, 'html.parser')
                seasons = self._extract_seasons_from_soup(soup, series_slug)
                
                if seasons:
                    logger.debug(f"Detected {len(seasons)} seasons for {series_slug}: {seasons}")
                    self._decay_global_backoff()
                    return seasons
                
                # Nav not found or empty — page may not have loaded fully, retry
                last_error = f"No season pills found for {series_slug}"
                logger.warning(f"Attempt {attempt + 1}/{max_retries}: {last_error}")
                if attempt < max_retries - 1:
                    time.sleep(1 + random.uniform(0, 0.5))
                    continue
                
                # All retries exhausted — raise error, never default to ['1']
                raise SeasonDetectionError(
                    f"Season detection failed for {series_slug} after {max_retries} attempts: {last_error}"
                )
                
            except SeasonDetectionError:
                raise
            except Exception as e:
                last_error = str(e)
                logger.warning(f"Attempt {attempt + 1}/{max_retries} season detection for {series_slug}: {e}")
                if attempt < max_retries - 1:
                    backoff = min(self.get_timing('error_backoff_max') or 8.0,
                                  (self.get_timing('error_backoff_base') or 1.0) * (2 ** attempt))
                    time.sleep(backoff + random.uniform(0, 0.5))
                    continue
                raise SeasonDetectionError(
                    f"Season detection failed for {series_slug} after {max_retries} attempts: {last_error}"
                )

    # ==================== MAIN SCRAPING ORCHESTRATION ====================
    
    def _create_worker_driver(self, worker_id=None):
        """Create a new WebDriver for a worker thread and track its PID."""
        firefox_options = self._build_firefox_options()
        service = FirefoxService()
        
        driver = webdriver.Firefox(service=service, options=firefox_options)
        
        # Install uBlock Origin
        ublock_xpi = self._get_ublock_xpi()
        if ublock_xpi:
            try:
                driver.install_addon(ublock_xpi, temporary=True)
                time.sleep(0.3)
            except Exception:
                pass
        
        # Track geckodriver PID and Firefox child process PIDs for cleanup
        if worker_id is not None:
            for attempt in range(5):
                try:
                    if hasattr(service, 'process') and service.process:
                        geckodriver_pid = service.process.pid
                        if geckodriver_pid:
                            self.save_worker_pid(worker_id, geckodriver_pid)
                            
                            # Also try to find and save the Firefox child process PID
                            try:
                                # Use wmic to find Firefox child processes of geckodriver
                                result = subprocess.run(
                                    ['wmic', 'process', 'where', f'ParentProcessId={geckodriver_pid}', 
                                     'get', 'ProcessId', '/value'],
                                    capture_output=True, text=True, timeout=2, check=False
                                )
                                for line in result.stdout.split('\n'):
                                    if 'ProcessId' in line:
                                        firefox_pid = line.split('=')[-1].strip()
                                        if firefox_pid.isdigit():
                                            # Save Firefox PID separately with a suffix to track it
                                            self.save_worker_pid(f"{worker_id}_firefox", int(firefox_pid))
                            except Exception:
                                pass
                            break
                except Exception:
                    pass
                time.sleep(0.1)
        
        return driver
    
    def save_worker_pid(self, worker_id, pid):
        """Save worker geckodriver PID for cleanup."""
        pids_file = os.path.join(DATA_DIR, '.worker_pids.json')
        with self._lock:
            try:
                if os.path.exists(pids_file):
                    with open(pids_file, 'r') as f:
                        pids = json.load(f)
                else:
                    pids = {}
                pids[str(worker_id)] = pid
                os.makedirs(DATA_DIR, exist_ok=True)
                with open(pids_file, 'w') as f:
                    json.dump(pids, f)
            except Exception as e:
                logger.debug(f"Failed to save worker PID {worker_id}: {e}")

    def clear_worker_pids(self):
        """Clear tracked worker PIDs after scraping completes."""
        pids_file = os.path.join(DATA_DIR, '.worker_pids.json')
        with self._lock:
            try:
                if os.path.exists(pids_file):
                    os.remove(pids_file)
            except OSError as e:
                logger.debug(f"Could not remove worker PIDs file: {e}")
    
    def _authenticate_driver(self, driver, label=None, max_attempts=3):
        """Authenticate a worker driver via cookies or full login.
        
        Tries cookie-based auth first, falls back to full login.
        Retries up to max_attempts times.
        
        Args:
            driver: WebDriver instance to authenticate
            label: Label for log messages (e.g. 'Worker #3', 'discovery_0')
            max_attempts: Number of auth attempts before giving up
            
        Returns:
            bool: True if authenticated successfully
        """
        label = label or 'driver'
        for attempt in range(max_attempts):
            try:
                if self._apply_cookies_to_driver(driver) and self.is_logged_in(driver):
                    logger.debug(f"{label}: authenticated via cookies")
                    return True
                else:
                    self.login(driver)
                    if self.is_logged_in(driver):
                        logger.debug(f"{label}: authenticated via full login")
                        return True
                    else:
                        logger.warning(f"{label}: login verification failed (attempt {attempt + 1}/{max_attempts})")
                        print(f"  \u26a0 {label}: Login verification failed (try {attempt + 1}/{max_attempts})")
                        time.sleep(1)
            except Exception as e:
                logger.warning(f"{label}: auth exception (attempt {attempt + 1}/{max_attempts}): {e}")
                print(f"  \u26a0 {label}: Auth failed - {str(e)[:80]}")
                time.sleep(1)
        
        logger.error(f"{label}: failed to authenticate after {max_attempts} attempts")
        return False

    def _apply_cookies_to_driver(self, driver):
        """Apply auth cookies from main driver to a worker driver (thread-safe snapshot)."""
        cookies_snapshot = list(self.auth_cookies)
        if not cookies_snapshot:
            return False
        try:
            driver.get(self.get_site_url())
            self._wait_for_page_ready(driver)
            for cookie in cookies_snapshot:
                try:
                    driver.add_cookie({
                        'name': cookie.get('name'),
                        'value': cookie.get('value'),
                        'domain': cookie.get('domain'),
                        'path': cookie.get('path', '/'),
                        'secure': cookie.get('secure', False),
                        'httpOnly': cookie.get('httpOnly', False)
                    })
                except Exception:
                    continue
            # Refresh to apply cookies (don't navigate again, just reload)
            driver.refresh()
            self._wait_for_page_ready(driver)
            return True
        except Exception:
            return False
    
    def _finish_scrape(self, start_time, failed_count):
        """Save failed series and print timing summary."""
        if self.failed_links:
            self.save_failed_series()
            print(f"\n\u26a0 {len(self.failed_links)} series failed. Saved for retry.")
        total_time = time.time() - start_time
        total_mins = int(total_time / 60)
        total_secs = int(total_time % 60)
        if failed_count:
            print(f"\n\u2713 Completed in {total_mins}m {total_secs}s ({failed_count} failed)")
        else:
            print(f"\n\u2713 Completed in {total_mins}m {total_secs}s")

    def _scrape_all_seasons_verified(self, series_slug, seasons, driver=None, max_retries=None):
        """
        Scrape all seasons of a series with verification and retry for missing ones.
        
        Knows how many seasons to expect upfront and verifies every one was scraped.
        Retries individual missing seasons before giving up.
        
        Args:
            series_slug: Series slug (e.g., 'attack-on-titan')
            seasons: List of season numbers as strings (e.g., ['0', '1', '2'])
            driver: Optional WebDriver instance (defaults to self.driver)
            max_retries: Max retries per missing season (default from config)
            
        Returns:
            dict: {
                'season_results': list of season data dicts,
                'missing_seasons': list of season numbers that failed,
                'title': str or None,
            }
        """
        drv = driver or self.driver
        if max_retries is None:
            val = self.get_timing('max_retries_season', default=None)
            max_retries = int(val) if val is not None else 3
        
        expected = set(seasons)
        season_results = []
        title = None
        consecutive_failures = 0
        
        # First pass: scrape all seasons
        for season in seasons:
            season_url = f"https://s.to/serie/{series_slug}/staffel-{season}"
            try:
                # Check if driver is still alive before each season
                if not self._is_driver_alive(drv):
                    logger.error(f"{series_slug}: Driver died before season {season} — skipping remaining seasons")
                    break
                
                data = self.scrape_series_detail(season_url, drv, max_retries=max_retries)
                if data and data.get('total_episodes', 0) > 0:
                    season_results.append(data)
                    if data.get('title'):
                        title = data['title']
                    consecutive_failures = 0
                elif data and data.get('total_episodes', 0) == 0:
                    # Season exists but has 0 episodes — still valid (empty season)
                    season_results.append(data)
                    if data.get('title'):
                        title = data['title']
                    consecutive_failures = 0
                else:
                    # data is None → failed, will be retried below
                    consecutive_failures += 1
                    logger.warning(f"{series_slug}: Season {season} returned no data (fail #{consecutive_failures})")
                    # Add delay before next season to avoid hammering a struggling server
                    if consecutive_failures > 0:
                        backoff = min(self.get_timing('error_backoff_max') or 8.0,
                                      (self.get_timing('error_backoff_base') or 1.0) * (2 ** min(consecutive_failures, 4)))
                        time.sleep(backoff + random.uniform(0, 0.3))
            except Exception as e:
                consecutive_failures += 1
                logger.warning(f"Error scraping {series_slug}/staffel-{season}: {e}")
                # Add delay before next season after an exception
                if consecutive_failures > 0:
                    backoff = min(self.get_timing('error_backoff_max') or 8.0,
                                  (self.get_timing('error_backoff_base') or 1.0) * (2 ** min(consecutive_failures, 4)))
                    time.sleep(backoff + random.uniform(0, 0.3))
        
        # Verification: which seasons did we actually get?
        scraped = {r['season'] for r in season_results}
        missing = expected - scraped
        
        # Retry pass for any missing seasons
        if missing:
            logger.warning(f"{series_slug}: Missing {len(missing)}/{len(expected)} seasons after first pass: {sorted(missing)}")
            print(f"  ⚠ {series_slug}: Retrying {len(missing)} missing season(s): {','.join(sorted(missing))}")
            
            # Check driver health before starting retry pass
            if not self._is_driver_alive(drv):
                logger.error(f"{series_slug}: Driver dead before retry pass — cannot retry missing seasons")
            else:
                for season in sorted(missing):
                    season_url = f"https://s.to/serie/{series_slug}/staffel-{season}"
                    # Use extra retries for the verification retry pass
                    val = self.get_timing('max_retries_retry', default=None)
                    retry_max = int(val) if val is not None else 5
                    for attempt in range(retry_max):
                        try:
                            data = self.scrape_series_detail(season_url, drv, max_retries=2)
                            if data is not None:
                                season_results.append(data)
                                if data.get('title'):
                                    title = data['title']
                                logger.info(f"{series_slug}: Recovered season {season} on retry attempt {attempt + 1}")
                                print(f"    ✓ {series_slug}: Recovered season {season} (retry {attempt + 1}/{retry_max})")
                                break
                        except Exception as e:
                            logger.warning(f"{series_slug}: Retry {attempt + 1}/{retry_max} for season {season}: {e}")
                        
                        backoff = min(self.get_timing('error_backoff_max') or 8.0,
                                      (self.get_timing('error_backoff_base') or 1.0) * (2 ** attempt))
                        time.sleep(backoff + random.uniform(0, 0.5))
        
        # Final verification
        scraped_final = {r['season'] for r in season_results}
        still_missing = expected - scraped_final
        
        if still_missing:
            logger.error(f"{series_slug}: Still missing {len(still_missing)} season(s) after retries: {sorted(still_missing)}")
        
        # Sort results by season number for consistent output
        season_results.sort(key=lambda r: int(r['season']) if r['season'].isdigit() else -1)
        
        return {
            'season_results': season_results,
            'missing_seasons': sorted(still_missing) if still_missing else [],
            'title': title,
        }

    def _scrape_series_parallel(self, series_urls, max_workers):
        """
        True parallel scraping: each worker gets its own browser, authenticates
        via cookie sharing, and pulls work from a shared queue.
        
        Uses a thread-safe queue so if a worker dies (e.g. auth failure),
        its remaining items are picked up by other workers automatically.
        
        Returns:
            dict: Series data keyed by slug with seasons list
        """
        self.clear_pause_request()
        
        series_data = {}
        total_series = len(series_urls)
        start_time = time.time()
        completed = 0
        failed = 0
        stop_event = threading.Event()
        
        # Shared work queue — all workers pull from the same pool
        worker_count = min(max_workers, total_series) or 1
        work_queue = queue.Queue()
        for item in series_urls:
            work_queue.put(item)
        
        print(f"→ {total_series} series queued for {worker_count} workers (shared work queue)")

        def worker_loop(worker_id):
            nonlocal completed, failed
            
            # Get timing config for backoff and health checks
            success_delay = self.get_timing('success_delay') or 0.3
            backoff_base = self.get_timing('error_backoff_base') or 1.0
            backoff_max = self.get_timing('error_backoff_max') or 8.0
            health_every = int(self.get_timing('health_check_every') or 15)
            restart_threshold = int(self.get_timing('error_restart_threshold') or 8)
            
            driver = None
            try:
                driver = self._create_worker_driver(worker_id)
            except Exception as e:
                logger.error(f"Worker #{worker_id}: failed to create driver: {e}")
                print(f"  ✗ Worker #{worker_id}: Failed to create browser: {str(e)[:80]}")
                return
            self.inject_aggressive_adblock(driver)
            
            # Authenticate worker: try cookies first, fall back to full login
            authenticated = self._authenticate_driver(driver, label=f"Worker #{worker_id}")
            
            if not authenticated:
                logger.error(f"Worker #{worker_id}: failed to authenticate after 3 attempts — remaining items stay in queue for other workers")
                print(f"  ✗ Worker #{worker_id}: Failed to authenticate. Items remain in queue for other workers.")
                try:
                    driver.quit()
                except Exception:
                    pass
                return
            
            error_streak = 0
            tasks_since_check = 0
            
            # Pull work from shared queue until empty or stopped
            while not stop_event.is_set():
                try:
                    item = work_queue.get_nowait()
                except queue.Empty:
                    break  # No more work
                
                if stop_event.is_set():
                    break
                
                if self.is_pause_requested():
                    print(f"\n⏸ Worker #{worker_id} pausing (pause file detected)")
                    break
                
                # Extract URL, slug, and title from item
                series_url, series_slug, display_title = self._extract_item_info(item)
                if series_slug == 'unknown':
                    continue
                
                try:
                    # Inline season detection with this worker's driver
                    # On failure, mark series as failed instead of silently defaulting to season 1
                    try:
                        seasons = self.get_all_seasons_for_series(series_slug, driver)
                    except SeasonDetectionError as e:
                        logger.error(f"W{worker_id}: Season detection failed for {series_slug}: {e}")
                        print(f"  ✗ W{worker_id}: Season detection failed for {series_slug}: {str(e)[:80]}")
                        with self._lock:
                            completed += 1
                            failed += 1
                            self.failed_links.append(self._normalize_failed_item(item, series_url, display_title))
                            print(self._format_progress_line(completed, total_series, start_time, display_title, error=f"season detection: {str(e)[:60]}", worker_id=worker_id, worker_count=worker_count))
                        error_streak += 1
                        continue
                    
                    # Scrape all seasons with verification
                    result = self._scrape_all_seasons_verified(series_slug, seasons, driver)
                    season_results = result['season_results']
                    missing_seasons = result['missing_seasons']
                    series_title = result['title'] or display_title
                    
                    # Aggregate results
                    with self._lock:
                        series_watched, series_total_eps, series_had_error, is_sub, is_wl = self._aggregate_season_results(
                            series_slug, season_results, missing_seasons, series_data)
                    
                    if series_had_error:
                        logger.warning(f"W{worker_id}: {series_slug} missing seasons {missing_seasons} — NOT marking as completed")
                    elif series_total_eps == 0 and not series_had_error:
                        logger.warning(f"W{worker_id}: {series_slug} returned 0 episodes across {len(seasons)} season(s) — marking as failed for retry")
                    
                    with self._lock:
                        completed += 1
                        if series_slug not in series_data:
                            failed += 1
                            print(self._format_progress_line(completed, total_series, start_time, series_title, error='failed', worker_id=worker_id, worker_count=worker_count))
                        elif series_had_error:
                            failed += 1
                            self.failed_links.append(self._normalize_failed_item(item, series_url, display_title))
                            scraped_seasons = [r['season'] for r in season_results]
                            print(self._format_progress_line(completed, total_series, start_time, series_title,
                                          error=f"missing seasons {missing_seasons} (got {scraped_seasons})",
                                          worker_id=worker_id, worker_count=worker_count))
                        elif series_total_eps == 0:
                            failed += 1
                            self.failed_links.append(self._normalize_failed_item(item, series_url, display_title))
                            print(self._format_progress_line(completed, total_series, start_time, series_title, empty=True, worker_id=worker_id, worker_count=worker_count, season_labels=seasons, subscribed=is_sub, watchlist=is_wl))
                        else:
                            print(self._format_progress_line(completed, total_series, start_time, series_title, watched=series_watched, episode_total=series_total_eps, worker_id=worker_id, worker_count=worker_count, season_labels=seasons, subscribed=is_sub, watchlist=is_wl))
                        error_streak = 0
                        tasks_since_check += 1
                        
                        if completed % 10 == 0:
                            self.save_checkpoint()
                    
                except Exception as e:
                    logger.error(f"W{worker_id}: Unhandled error for {series_slug}: {e}")
                    with self._lock:
                        completed += 1
                        failed += 1
                        self.failed_links.append(self._normalize_failed_item(item, series_url, display_title))
                        print(self._format_progress_line(completed, total_series, start_time, display_title, error=str(e)[:80], worker_id=worker_id, worker_count=worker_count))
                    error_streak += 1
                
                # Delay after success or backoff after errors
                if error_streak == 0 and success_delay > 0:
                    time.sleep(float(success_delay) + random.uniform(0.1, 0.5))
                elif error_streak > 0:
                    # Exponential backoff with jitter
                    exp = min(6, error_streak)
                    backoff = min(float(backoff_max), float(backoff_base) * (2 ** exp))
                    jitter = random.uniform(0.2, 1.0)
                    time.sleep(backoff + jitter)
                
                # Periodic health check: every N tasks or after 3+ errors
                do_health_check = (tasks_since_check >= health_every) or (error_streak >= 3)
                if do_health_check:
                    tasks_since_check = 0
                    
                    if error_streak >= 3:
                        # Error streak: do full page-navigation check
                        logger.debug(f"W{worker_id}: Full health check (error streak={error_streak})")
                        try:
                            if not self.is_logged_in(driver):
                                logger.warning(f"W{worker_id}: Session expired, re-authenticating...")
                                if self._authenticate_driver(driver, label=f"W{worker_id}", max_attempts=2):
                                    error_streak = 0
                                    logger.info(f"W{worker_id}: Re-authentication successful")
                                else:
                                    error_streak += 1
                        except Exception as e:
                            logger.error(f"W{worker_id}: Health check failed: {e}")
                            error_streak += 1
                    else:
                        # Routine check: lightweight cookie check (no page loads)
                        logger.debug(f"W{worker_id}: Lightweight health check (tasks_since={tasks_since_check})")
                        if not self._has_auth_cookies(driver):
                            logger.warning(f"W{worker_id}: Cookies missing, verifying with full check...")
                            try:
                                if not self.is_logged_in(driver):
                                    if self._authenticate_driver(driver, label=f"W{worker_id}", max_attempts=2):
                                        error_streak = 0
                                        logger.info(f"W{worker_id}: Re-authentication successful")
                                    else:
                                        error_streak += 1
                            except Exception as e:
                                logger.error(f"W{worker_id}: Health check failed: {e}")
                                error_streak += 1
                
                # Restart driver after too many consecutive errors
                if error_streak >= restart_threshold:
                    logger.warning(f"W{worker_id}: Error streak {error_streak} >= threshold {restart_threshold}, restarting driver")
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    driver = self._create_worker_driver(worker_id)
                    self._authenticate_driver(driver, label=f"W{worker_id}", max_attempts=2)
                    error_streak = 0
                    logger.info(f"W{worker_id}: Driver restarted and re-authenticated")
            
            # Cleanup worker driver
            try:
                driver.quit()
            except Exception:
                pass

        executor = ThreadPoolExecutor(max_workers=worker_count)
        futures = []
        try:
            for worker_id in range(1, worker_count + 1):
                print(f"  🔺 Worker #{worker_id} starting")
                futures.append(executor.submit(worker_loop, worker_id))
                # Stagger worker startup so they don't all hit the site at once
                if worker_id < worker_count:
                    time.sleep(2.0)

            for f in as_completed(futures):
                pass
        except KeyboardInterrupt:
            print(f"\n\n⚠ Scraping interrupted by user (Ctrl+C)")
            print(f"✓ Progress saved: {len(series_data)}/{total_series} series scraped")
            print("→ Use 'Resume from checkpoint' option to continue later\n")
            stop_event.set()
            executor.shutdown(wait=False, cancel_futures=True)
            return series_data
        finally:
            stop_event.set()
            executor.shutdown(wait=True, cancel_futures=False)
            for f in futures:
                try:
                    f.result()
                except Exception:
                    pass
        
        # Drain any remaining items from queue (e.g. all workers died)
        orphaned = 0
        while True:
            try:
                item = work_queue.get_nowait()
                self.failed_links.append(self._normalize_failed_item(item))
                failed += 1
                orphaned += 1
            except queue.Empty:
                break
        if orphaned:
            logger.warning(f"{orphaned} series left unprocessed in queue — marked as failed for retry")
            print(f"  ⚠ {orphaned} series were not picked up by any worker — saved for retry")
        
        # Save checkpoint
        self.save_checkpoint()
        
        # Check if pause was requested
        if self.is_pause_requested():
            print(f"\n⏸ Scraping paused by user")
            print(f"✓ Progress saved: {len(series_data)}/{total_series} series scraped")
            print(f"→ Resume later with checkpoint option\n")
            self.clear_pause_request()
        
        self._finish_scrape(start_time, failed)
        
        return series_data

    def _scrape_series_sequential(self, series_urls):
        """
        Sequential scraping with inline season detection, progress bar, ETA,
        and per-series result output.
        
        Season detection happens per-series (not upfront) so the progress bar
        appears immediately.
        
        Returns:
            dict: Series data keyed by slug with seasons list
        """
        series_data = {}
        total_series = len(series_urls)
        start_time = time.time()
        failed = 0
        consecutive_series_failures = 0
        
        try:
            for idx, item in enumerate(series_urls, 1):
                # Check for pause request
                if self.is_pause_requested():
                    print(f"\n⏸ Scraping paused by user")
                    self.clear_pause_request()
                    break
                
                # Check if main driver is still alive; try to recover if not
                if not self._is_driver_alive():
                    logger.warning("Main driver died in sequential mode — attempting recovery")
                    print("  ⚠ Browser crashed — restarting...")
                    try:
                        self.setup_driver()
                        self.inject_aggressive_adblock()
                        self.login()
                        consecutive_series_failures = 0
                        logger.info("Main driver recovered successfully")
                    except Exception as e:
                        logger.error(f"Failed to recover main driver: {e}")
                        print(f"  ✗ Could not restart browser: {str(e)[:60]}")
                        print(f"  → Saving progress and aborting ({len(series_data)}/{total_series} scraped)")
                        self.save_checkpoint()
                        break
                
                # Extract URL, slug, and title from item
                series_url, series_slug, display_title = self._extract_item_info(item)
                if series_slug == 'unknown':
                    print(f"⚠ Skipping invalid URL: {series_url}")
                    continue
                
                # Progress bar and ETA
                
                # Inline season detection (not upfront)
                try:
                    seasons = self.get_all_seasons_for_series(series_slug)
                except SeasonDetectionError as e:
                    logger.error(f"Season detection failed for {series_slug}: {e}")
                    print(self._format_progress_line(idx, total_series, start_time, display_title, error=f"season detection failed — {str(e)[:60]}"))
                    failed += 1
                    consecutive_series_failures += 1
                    self.failed_links.append(self._normalize_failed_item(item, series_url, display_title))
                    self.save_checkpoint()
                    continue
                
                # Scrape all seasons with verification — wrapped in try/except
                # so a single series crash doesn't kill the entire scraping loop
                try:
                    result = self._scrape_all_seasons_verified(series_slug, seasons)
                    season_results = result['season_results']
                    missing_seasons = result['missing_seasons']
                    series_title = result['title'] or display_title
                except Exception as e:
                    logger.error(f"Unexpected error scraping seasons for {series_slug}: {e}")
                    print(self._format_progress_line(idx, total_series, start_time, display_title, error=f"crash: {str(e)[:60]}"))
                    failed += 1
                    consecutive_series_failures += 1
                    self.failed_links.append(self._normalize_failed_item(item, series_url, display_title))
                    self.save_checkpoint()
                    continue
                
                series_watched, series_total_eps, series_had_error, is_sub, is_wl = self._aggregate_season_results(
                    series_slug, season_results, missing_seasons, series_data)
                
                if series_had_error and not any(
                    (isinstance(f, dict) and f.get('url', '').endswith(series_slug)) or f == series_slug
                    for f in self.failed_links
                ):
                    self.failed_links.append(self._normalize_failed_item(item, series_url, display_title))
                
                if series_total_eps == 0 and not series_had_error:
                    logger.warning(f"{series_slug} returned 0 episodes across {len(seasons)} season(s) — marking as failed for retry")
                
                # Single-line result (progress bar + status + seasons)
                scraped_count = len(season_results)
                expected_count = len(seasons)
                if series_had_error:
                    failed += 1
                    consecutive_series_failures += 1
                    scraped_seasons = [r['season'] for r in season_results]
                    logger.error(f"Series incomplete: {series_slug} — got {scraped_seasons}, missing {missing_seasons}")
                    print(self._format_progress_line(idx, total_series, start_time, series_title, error=f"{scraped_count}/{expected_count} seasons (missing: {','.join(missing_seasons)})"))
                elif series_slug not in series_data:
                    failed += 1
                    consecutive_series_failures += 1
                    logger.error(f"Series failed: {series_slug}")
                    print(self._format_progress_line(idx, total_series, start_time, series_title, error='Failed'))
                elif series_total_eps == 0:
                    failed += 1
                    consecutive_series_failures += 1
                    self.failed_links.append(self._normalize_failed_item(item, series_url, display_title))
                    print(self._format_progress_line(idx, total_series, start_time, series_title, empty=True, season_labels=seasons, subscribed=is_sub, watchlist=is_wl))
                else:
                    consecutive_series_failures = 0
                    print(self._format_progress_line(idx, total_series, start_time, series_title, watched=series_watched, episode_total=series_total_eps, season_labels=seasons, subscribed=is_sub, watchlist=is_wl))
                
                # Checkpoint every 10 series (matches parallel mode)
                if idx % 10 == 0:
                    self.save_checkpoint()
                self.human_delay(0.3, 0.8)
        
        except KeyboardInterrupt:
            print(f"\n\n⚠ Scraping interrupted by user (Ctrl+C)")
            print(f"✓ Progress saved: {len(self.completed_links)}/{total_series} series scraped")
            print("→ Use 'Resume from checkpoint' option to continue later\n")
            self.save_checkpoint()
        
        self._finish_scrape(start_time, failed)
        
        return series_data

    def _finalize_series_data(self, series_data):
        """
        Finalize series data by extracting subscription/title from already-scraped seasons.
        
        No extra page loads needed — subscription status and title are captured
        during scrape_series_detail.
        
        Args:
            series_data: Dict of series data keyed by slug
            
        Returns:
            list: Formatted series list ready for JSON output
        """
        results = []
        
        for series_slug, series_info in series_data.items():
            if not series_info.get('seasons'):
                print(f"⚠ Skipping {series_slug} - no season data")
                continue
            
            # Extract subscription status and title from first season's data
            first_season = series_info['seasons'][0]
            series_info['subscribed'] = first_season.get('subscribed', False)
            series_info['watchlist'] = first_season.get('watchlist', False)
            series_info['title'] = first_season.get('title') or series_slug
            series_info['alt_titles'] = first_season.get('alt_titles', [])
            
            # Clean per-season fields (no need to keep subscription/title on each season)
            for season in series_info['seasons']:
                season.pop('subscribed', None)
                season.pop('watchlist', None)
                season.pop('title', None)
                season.pop('alt_titles', None)
            
            # Calculate series-level episode counts from season data
            total_eps = sum(len(s.get('episodes', [])) for s in series_info['seasons'])
            watched_eps = sum(
                sum(1 for ep in s.get('episodes', []) if ep.get('watched'))
                for s in series_info['seasons']
            )
            
            # Build ordered dict: metadata first, then seasons
            ordered = {
                'url': series_info.get('url', ''),
                'link': series_info.get('link', ''),
                'subscribed': series_info.get('subscribed', False),
                'watchlist': series_info.get('watchlist', False),
                'title': series_info.get('title', series_slug),
                'alt_titles': series_info.get('alt_titles', []),
                'total_episodes': total_eps,
                'watched_episodes': watched_eps,
                'empty': total_eps == 0,
                'seasons': series_info['seasons'],
            }
            
            results.append(ordered)
        
        return results
    
    # ==================== SCRAPING MODES ====================

    def scrape_resume_checkpoint(self):
        """Resume scraping from a previous checkpoint"""
        if not self.load_checkpoint():
            print("⚠ No checkpoint found. Starting fresh...")
            self.scrape_series_list()
            return

        time.sleep(self.get_timing('initial_delay'))

        all_series = self.get_all_series()
        remaining_series = [s for s in all_series
                           if s['url'].rstrip('/').split('/')[-1] not in self.completed_links]

        completed_count = len(all_series) - len(remaining_series)
        print(f"✓ Resuming from checkpoint: {completed_count}/{len(all_series)} already done")
        print(f"→ Remaining to scrape: {len(remaining_series)}")

        if not remaining_series:
            print("✓ All series already scraped")
            return

        if self._use_parallel:
            raw = self._scrape_series_parallel(remaining_series, MAX_WORKERS)
        else:
            raw = self._scrape_series_sequential(remaining_series)
        # Merge newly scraped data with checkpoint-restored data
        self.series_data.extend(self._finalize_series_data(raw))

    def scrape_series_list(self):
        """Scrape all series from the s.to/serien index page"""
        time.sleep(self.get_timing('initial_delay'))
        all_series = self.get_all_series()

        if self._use_parallel:
            print("→ Starting series scraping (parallel mode)...")
            raw = self._scrape_series_parallel(all_series, MAX_WORKERS)
        else:
            print("→ Starting series scraping (sequential mode)...")
            raw = self._scrape_series_sequential(all_series)
        self.series_data = self._finalize_series_data(raw)

        print(f"\n✓ Successfully scraped {len(self.series_data)} series")

    def scrape_new_series_only(self, index_file):
        """Scrape only series not already in the index"""
        time.sleep(self.get_timing('initial_delay'))
        all_series = self.get_all_series()

        existing_slugs = set()
        if os.path.exists(index_file):
            try:
                with open(index_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if isinstance(data, list):
                    for s in data:
                        url = s.get('url', '')
                        if url:
                            existing_slugs.add(self.get_series_slug_from_url(url))
                elif isinstance(data, dict):
                    for v in data.values():
                        url = v.get('url', '')
                        if url:
                            existing_slugs.add(self.get_series_slug_from_url(url))
            except Exception as e:
                logger.warning(f"Could not load existing index: {e}")

        new_series = [s for s in all_series
                      if self.get_series_slug_from_url(s['url']) not in existing_slugs]

        print(f"→ New series to scrape: {len(new_series)} (out of {len(all_series)})")
        if not new_series:
            print("✓ No new series detected — skipping scraper spin-up")
            return

        raw = self._scrape_series_sequential(new_series)
        self.series_data = self._finalize_series_data(raw)

    def scrape_single_series(self, url):
        """Scrape exactly one series by URL (all seasons)"""
        time.sleep(self.get_timing('initial_delay'))
        main_url = self.normalize_to_series_url(url)
        print(f"→ Scraping single series: {main_url}")
        raw = self._scrape_series_sequential([{'url': main_url, 'title': main_url.split('/')[-1]}])
        self.series_data = self._finalize_series_data(raw)

    def scrape_multiple_series(self, urls):
        """Scrape multiple series from a list of URLs"""
        time.sleep(self.get_timing('initial_delay'))
        series_list = []
        for url in urls:
            main_url = self.normalize_to_series_url(url)
            slug = self.get_series_slug_from_url(main_url)
            series_list.append({
                'title': slug,
                'link': f"/serie/{slug}",
                'url': main_url
            })
        if self._use_parallel:
            print(f"→ Scraping {len(urls)} series from URL list (parallel mode)...")
            raw = self._scrape_series_parallel(series_list, MAX_WORKERS)
        else:
            print(f"→ Scraping {len(urls)} series from URL list (sequential mode)...")
            raw = self._scrape_series_sequential(series_list)
        self.series_data = self._finalize_series_data(raw)

    # ==================== MAIN PIPELINE ====================

    def run(self, output_file, single_url=None, url_list=None, new_only=False,
            retry_failed=False, resume_only=False, parallel=None):
        """
        Execute scraping workflow - collects data but does NOT save (caller handles save).
        
        Mirrors Save bs.to's run() signature.
        
        Args:
            output_file: Path to save/load JSON index
            single_url: Scrape exactly one series URL
            url_list: Scrape a list of URLs
            new_only: Only scrape series not already in the index
            retry_failed: Retry previously failed series
            resume_only: Resume from a previous checkpoint
            parallel: Force parallel (True) or sequential (False). None = use default.
        """
        if parallel is not None:
            self._use_parallel = parallel
            mode_str = "parallel" if parallel else "sequential"
            print(f"→ Using {mode_str} mode")

        try:
            self.setup_driver()
            self.inject_aggressive_adblock()
            self.login()

            # Set checkpoint paths from output file location
            data_dir = os.path.dirname(output_file)
            self.set_checkpoint_paths(data_dir)

            if resume_only:
                print("→ Running in 'resume from checkpoint' mode")
                self.scrape_resume_checkpoint()
            elif single_url:
                self._checkpoint_mode = 'single'
                self.scrape_single_series(single_url)
            elif url_list:
                self._checkpoint_mode = 'batch'
                self.scrape_multiple_series(url_list)
            elif retry_failed:
                self._checkpoint_mode = 'retry'
                print("→ Running in 'retry failed series' mode")
                failed_list = self.load_failed_series()
                if not failed_list:
                    print("✓ No failed series found. Nothing to retry.")
                    return
                print(f"  Found {len(failed_list)} failed series")
                raw = self._scrape_series_sequential(failed_list)
                self.series_data = self._finalize_series_data(raw)
                self.clear_failed_series()
            elif new_only:
                self._checkpoint_mode = 'new_only'
                print("→ Running in 'new series only' mode")
                self.scrape_new_series_only(output_file)
            else:
                self._checkpoint_mode = 'all_series'
                self.scrape_series_list()

            self.clear_checkpoint()
            if not self.failed_links:
                self.clear_failed_series()
            else:
                self.save_failed_series()

        except BaseException:
            # Catches Exception, SystemExit (from SIGINT), KeyboardInterrupt
            # Save full checkpoint with series_data so no progress is lost on resume
            self.save_checkpoint(include_data=True)
            if self.failed_links:
                self.save_failed_series()
            raise
        finally:
            self.clear_worker_pids()
            self.close()
