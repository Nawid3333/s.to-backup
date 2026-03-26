"""
S.TO Index Manager
Manages the persistent series index and handles data merging, change detection, and analytics.
Ported from Save bs.to with s.to-specific extensions (subscription/watchlist tracking).
"""

import json
import logging
import os
import re
import shutil
import tempfile
import time
from collections import defaultdict
from datetime import datetime

logger = logging.getLogger(__name__)

# Pre-compiled regex for season number extraction
_SEASON_NUMBER_RE = re.compile(r'(staffel|season|s)\s*(\d+)', re.IGNORECASE)

# Pre-compiled regex for valid s.to series URL/path
_VALID_SERIES_URL_RE = re.compile(r'^https://s\.to/serie/[^/]+/?$')
_VALID_SERIES_PATH_RE = re.compile(r'^/serie/[^/]+/?$')


def _is_valid_series_url(url):
    """Check if a URL is a valid s.to series URL or relative path.
    
    Rejects dangerous schemes (javascript:, data:, file://) and
    only allows https://s.to/serie/... or /serie/... paths.
    """
    if not url or not isinstance(url, str):
        return False
    return bool(_VALID_SERIES_URL_RE.match(url) or _VALID_SERIES_PATH_RE.match(url))


class FileLock:
    """Simple file-based lock for preventing concurrent access to critical files.
    
    Uses a .lock file to indicate exclusive access. Waits for lock with timeout.
    Works cross-platform (Windows, Linux, Mac).
    """
    def __init__(self, filepath, timeout=10, poll_interval=0.1):
        self.filepath = filepath
        self.lock_file = filepath + '.lock'
        self.timeout = timeout
        self.poll_interval = poll_interval
        self.lock_acquired = False
    
    def acquire(self):
        """Acquire lock, waiting up to timeout seconds.
        
        If the timeout expires and a stale lock from a dead process is detected,
        the stale lock is removed and acquisition is retried once.
        """
        start = time.time()
        while time.time() - start < self.timeout:
            try:
                # Try to create lock file exclusively (atomic on most filesystems)
                fd = os.open(self.lock_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, f"{os.getpid()}\n".encode())
                os.close(fd)
                self.lock_acquired = True
                return True
            except (OSError, FileExistsError):
                time.sleep(self.poll_interval)
        
        # Timeout expired — check if the lock is stale (owner process died)
        if self._is_lock_stale():
            logger.warning(f"Removing stale lock on {self.filepath} (owner process dead)")
            try:
                os.remove(self.lock_file)
            except OSError:
                pass
            # Retry once after removing stale lock
            try:
                fd = os.open(self.lock_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, f"{os.getpid()}\n".encode())
                os.close(fd)
                self.lock_acquired = True
                return True
            except (OSError, FileExistsError):
                pass
        
        logger.warning(f"Could not acquire lock on {self.filepath} after {self.timeout}s")
        return False
    
    def _is_lock_stale(self):
        """Check if the lock file was left by a process that is no longer running."""
        try:
            with open(self.lock_file, 'r') as f:
                pid_str = f.read().strip()
            pid = int(pid_str)
            # Check if the process is still alive
            try:
                os.kill(pid, 0)
                return False  # Process is still alive — lock is valid
            except OSError:
                return True  # Process is dead — lock is stale
        except (OSError, ValueError):
            # Can't read or parse pid — treat as stale to unblock
            return True
    
    def release(self):
        """Release lock by removing lock file."""
        if self.lock_acquired:
            try:
                os.remove(self.lock_file)
                self.lock_acquired = False
            except OSError:
                pass
    
    def __enter__(self):
        self.acquire()
        return self
    
    def __exit__(self, *args):
        self.release()


def _create_file_backup(filepath):
    """Create a backup of a file (up to 3 generations kept)."""
    if not os.path.exists(filepath):
        return
    try:
        backup_dir = os.path.dirname(filepath)
        filename = os.path.basename(filepath)
        
        # Remove oldest backup if 3 already exist
        for i in range(3, 10):
            old_backup = os.path.join(backup_dir, f"{filename}.bak{i}")
            if os.path.exists(old_backup):
                try:
                    os.remove(old_backup)
                except OSError:
                    pass
        
        # Shift existing backups: .bak2 -> .bak3, .bak1 -> .bak2, original -> .bak1
        for i in range(2, 0, -1):
            src = os.path.join(backup_dir, f"{filename}.bak{i}")
            dst = os.path.join(backup_dir, f"{filename}.bak{i+1}")
            if os.path.exists(src):
                try:
                    shutil.move(src, dst)
                except OSError:
                    pass
        
        # Create new backup
        backup_path = os.path.join(backup_dir, f"{filename}.bak1")
        shutil.copy2(filepath, backup_path)
        logger.debug(f"Created backup: {backup_path}")
    except Exception as e:
        logger.warning(f"Could not create backup of {filepath}: {e}")


def _atomic_write_json(filepath, data):
    """Write JSON to file atomically via temp file + os.replace.
    
    Creates backup before writing to prevent data loss on corruption.
    Prevents corrupted files if the process is killed mid-write.
    """
    dirpath = os.path.dirname(filepath)
    os.makedirs(dirpath, exist_ok=True)
    
    # Create backup of existing file before overwriting
    if os.path.exists(filepath):
        _create_file_backup(filepath)
    
    fd, tmp_path = tempfile.mkstemp(dir=dirpath, suffix='.tmp')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp_path, filepath)
    except Exception:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        raise


def _validate_series_entry(series, title=''):
    """Validate that a series entry has the required structure. Returns True if valid."""
    if not isinstance(series, dict):
        logger.warning(f"Skipping invalid series entry (not dict): {title}")
        return False
    url = series.get('url', '')
    if not url:
        logger.warning(f"Skipping series '{title}' - missing 'url' field")
        return False
    if not _is_valid_series_url(url):
        logger.warning(f"Skipping series '{title}' - invalid URL scheme/format: {url[:80]}")
        return False
    seasons = series.get('seasons')
    if seasons is not None and not isinstance(seasons, list):
        logger.warning(f"Skipping series '{title}' - 'seasons' must be list, got {type(seasons)}")
        return False
    for season in (seasons or []):
        if not isinstance(season, dict):
            continue
        episodes = season.get('episodes')
        if episodes is not None and not isinstance(episodes, list):
            logger.warning(f"Series '{title}' season '{season.get('season', '?')}' has invalid episodes type")
            season['episodes'] = []
    return True


def _find_series(new_data, title):
    """Look up a series by title in either a dict or list."""
    if isinstance(new_data, dict):
        return new_data.get(title)
    if isinstance(new_data, list):
        return next((s for s in new_data if s.get('title') == title), None)
    return None


def _get_season_stats(series, season_label):
    """Get (total_episodes, watched_episodes) for a specific season."""
    if not series:
        return 0, 0
    for s in series.get('seasons', []):
        if s.get('season') == season_label:
            eps = s.get('episodes', [])
            return len(eps), sum(1 for ep in eps if ep.get('watched', False))
    return 0, 0


def paginate_list(items, formatter, page_size=50):
    """Show items with pagination, Enter = next page, q = skip"""
    if not items:
        return
    total = len(items)
    idx = 0
    while idx < total:
        end = min(idx + page_size, total)
        for item in items[idx:end]:
            print(formatter(item))
        idx = end
        if idx < total:
            choice = input(f"  ({idx}/{total}) Enter = more, q = skip: ").strip().lower()
            if choice == 'q':
                print(f"  ... skipped {total - idx} remaining")
                break


def format_season_ep(season_label, ep_num):
    """Format season/episode for display.
    Regular seasons -> S1E5, Special seasons -> [Specials] Ep 3
    """
    match = _SEASON_NUMBER_RE.search(str(season_label))
    if match:
        return f"S{match.group(2)}E{ep_num}"
    # Numeric-only season
    if str(season_label).strip().isdigit():
        return f"S{season_label}E{ep_num}"
    return f"[{season_label}] Ep {ep_num}"


def group_episodes_by_season(episode_list, new_data, prefix='[+]'):
    """Group episodes by series and season, showing count per season."""
    grouped = defaultdict(list)
    for item in episode_list:
        title, season, ep_num = item[0], item[1], item[2]
        grouped[(title, season)].append(ep_num)
    
    if isinstance(new_data, list):
        new_data_dict = {s.get('title'): s for s in new_data}
    elif isinstance(new_data, dict):
        new_data_dict = new_data
    else:
        new_data_dict = {}
    
    result = []
    for (title, season), ep_nums in sorted(grouped.items()):
        series = new_data_dict.get(title, {})
        total_in_season, watched_in_season = _get_season_stats(series, season)
        sub = '✓' if series.get('subscribed') else '✗'
        wl = '✓' if series.get('watchlist') else '✗'
        sub_wl = f" (Sub:{sub} WL:{wl})"
        if total_in_season > 0:
            result.append(f"  {prefix} {title} [{season}]: {watched_in_season}/{total_in_season} episodes{sub_wl}")
        else:
            for ep_num in sorted(ep_nums):
                result.append(f"  {prefix} {title} {format_season_ep(season, ep_num)}{sub_wl}")
    return result


def detect_changes(old_data, new_data):
    """Detect changes between old and new data.
    Returns dict with change counts and details."""
    changes = {
        "new_series": [],
        "new_episodes": [],
        "newly_watched": [],
        "newly_unwatched": [],
        "newly_subscribed": [],
        "newly_unsubscribed": [],
        "watchlist_added": [],
        "watchlist_removed": [],
        "title_ger_changed": [],
        "title_eng_changed": []
    }
    
    # Filter out None titles to avoid set issues
    old_titles = set(old_data.keys()) if isinstance(old_data, dict) else {s.get('title') for s in old_data if s.get('title')}
    new_titles = set(new_data.keys()) if isinstance(new_data, dict) else {s.get('title') for s in new_data if s.get('title')}
    
    if isinstance(old_data, list):
        old_data = {s.get('title'): s for s in old_data}
    if isinstance(new_data, list):
        new_data = {s.get('title'): s for s in new_data}
    
    # New series
    for title in new_titles - old_titles:
        changes["new_series"].append(title)
    
    # Episode and subscription/watchlist changes for existing series
    for title in old_titles & new_titles:
        old_series = old_data[title]
        new_series = new_data[title]
        
        # Subscription changes
        old_sub = old_series.get('subscribed', False)
        new_sub = new_series.get('subscribed', False)
        if old_sub != new_sub:
            if new_sub:
                changes["newly_subscribed"].append(title)
            else:
                changes["newly_unsubscribed"].append(title)
        
        # Watchlist changes
        old_wl = old_series.get('watchlist', False)
        new_wl = new_series.get('watchlist', False)
        if old_wl != new_wl:
            if new_wl:
                changes["watchlist_added"].append(title)
            else:
                changes["watchlist_removed"].append(title)
        
        # Title German changes
        old_ger = old_series.get('title_ger', '')
        new_ger = new_series.get('title_ger', '')
        if old_ger and new_ger and old_ger != new_ger:
            changes["title_ger_changed"].append((title, old_ger, new_ger))
        
        # Title English changes
        old_eng = old_series.get('title_eng', '')
        new_eng = new_series.get('title_eng', '')
        if old_eng and new_eng and old_eng != new_eng:
            changes["title_eng_changed"].append((title, old_eng, new_eng))
        
        old_eps = {}
        for season in old_series.get('seasons', []):
            s_label = season.get('season', '')
            for ep in season.get('episodes', []):
                old_eps[(s_label, ep.get('number'))] = ep.get('watched', False)
        
        for season in new_series.get('seasons', []):
            s_label = season.get('season', '')
            for ep in season.get('episodes', []):
                ep_num = ep.get('number')
                ep_key = (s_label, ep_num)
                new_watched = ep.get('watched', False)
                
                if ep_key not in old_eps:
                    changes["new_episodes"].append((title, s_label, ep_num))
                elif old_eps[ep_key] != new_watched:
                    if not old_eps[ep_key] and new_watched:
                        changes["newly_watched"].append((title, s_label, ep_num))
                    elif old_eps[ep_key] and not new_watched:
                        changes["newly_unwatched"].append((title, s_label, ep_num))
    
    return changes


def show_changes(changes, include_unwatched=True, include_watched=True,
                 include_subscribe=True, include_unsubscribe=True,
                 include_watchlist_add=True, include_watchlist_remove=True, new_data=None):
    """Display changes with pagination and smart season grouping."""
    total = 0
    for k, v in changes.items():
        if k == 'newly_unwatched' and not include_unwatched:
            continue
        if k == 'newly_watched' and not include_watched:
            continue
        if k == 'newly_subscribed' and not include_subscribe:
            continue
        if k == 'newly_unsubscribed' and not include_unsubscribe:
            continue
        if k == 'watchlist_added' and not include_watchlist_add:
            continue
        if k == 'watchlist_removed' and not include_watchlist_remove:
            continue
        total += len(v)
    if total == 0:
        return 0

    print("\n" + "="*70)
    print("  CHANGES DETECTED")
    print("="*70)

    if changes["new_series"]:
        print(f"\n[NEW SERIES] ({len(changes['new_series'])})")
        def format_new_series(title):
            if not new_data:
                return f"  + {title}"
            series = _find_series(new_data, title)
            if not series:
                return f"  + {title}"
            watched = series.get('watched_episodes', 0)
            total = series.get('total_episodes', 0)
            season_labels = [str(sn.get('season', '?')) for sn in series.get('seasons', [])]
            season_info = f" [{','.join(season_labels)}]" if season_labels else ""
            sub = '✓' if series.get('subscribed') else '✗'
            wl = '✓' if series.get('watchlist') else '✗'
            return f"  + {title}{season_info}: {watched}/{total} watched (Sub:{sub} WL:{wl})"
        paginate_list(changes["new_series"], format_new_series)

    if changes["new_episodes"]:
        if new_data:
            grouped_lines = group_episodes_by_season(changes["new_episodes"], new_data)
            print(f"\n[NEW EPISODES] ({len(changes['new_episodes'])})")
            paginate_list(grouped_lines, lambda line: line)
        else:
            print(f"\n[NEW EPISODES] ({len(changes['new_episodes'])})")
            paginate_list(changes["new_episodes"], lambda x: f"  + {x[0]} [{x[1]}] Ep {x[2]}")

    if changes["newly_watched"] and include_watched:
        print(f"\n[NEWLY WATCHED] ({len(changes['newly_watched'])} episodes)")
        watched_lines = group_episodes_by_season(changes["newly_watched"], new_data)
        paginate_list(watched_lines, lambda line: line)

    if changes.get("newly_unwatched") and include_unwatched:
        print(f"\n[SITE REPORTS UNWATCHED] ({len(changes['newly_unwatched'])} episodes)")
        unwatched_lines = group_episodes_by_season(changes["newly_unwatched"], new_data, prefix='[!]')
        paginate_list(unwatched_lines, lambda line: line)

    if changes.get("newly_subscribed") and include_subscribe:
        print(f"\n[NEWLY SUBSCRIBED] ({len(changes['newly_subscribed'])} series)")
        paginate_list(
            changes["newly_subscribed"],
            lambda x: f"  [+] {x}"
        )

    if changes.get("newly_unsubscribed") and include_unsubscribe:
        print(f"\n[NEWLY UNSUBSCRIBED] ({len(changes['newly_unsubscribed'])} series)")
        paginate_list(
            changes["newly_unsubscribed"],
            lambda x: f"  [-] {x}"
        )

    if changes.get("watchlist_added") and include_watchlist_add:
        print(f"\n[ADDED TO WATCHLIST] ({len(changes['watchlist_added'])} series)")
        paginate_list(
            changes["watchlist_added"],
            lambda x: f"  [+] {x}"
        )

    if changes.get("watchlist_removed") and include_watchlist_remove:
        print(f"\n[REMOVED FROM WATCHLIST] ({len(changes['watchlist_removed'])} series)")
        paginate_list(
            changes["watchlist_removed"],
            lambda x: f"  [-] {x}"
        )

    if changes.get("title_ger_changed"):
        print(f"\n[GERMAN TITLE CHANGED] ({len(changes['title_ger_changed'])} series)")
        paginate_list(
            changes["title_ger_changed"],
            lambda x: f"  [~] {x[0]}: '{x[1]}' → '{x[2]}'"
        )

    if changes.get("title_eng_changed"):
        print(f"\n[ENGLISH TITLE CHANGED] ({len(changes['title_eng_changed'])} series)")
        paginate_list(
            changes["title_eng_changed"],
            lambda x: f"  [~] {x[0]}: '{x[1]}' → '{x[2]}'"
        )

    print("\n" + "="*70)
    return total


class IndexManager:
    """Manages persistent series index for s.to with file locking"""
    
    def __init__(self, index_file):
        """Initialize index manager with path to index file"""
        self.index_file = index_file
        self.series_index = {}
        self.file_lock = FileLock(index_file, timeout=10)  # 10-second lock timeout
        self.load_index()
    
    def load_index(self):
        """Load existing series index from file with corruption detection and file locking"""
        with self.file_lock:
            if not os.path.exists(self.index_file):
                logger.info(f"No existing index found at {self.index_file}")
                self.series_index = {}
                return
            try:
                with open(self.index_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        self.series_index = {s.get('title'): s for s in data if s.get('title') and isinstance(s, dict)}
                    elif isinstance(data, dict):
                        first_item = next(iter(data.values()), None)
                        if first_item and isinstance(first_item, dict) and first_item.get('title'):
                            self.series_index = data
                        else:
                            self.series_index = {item.get('title'): item for item in data.values()
                                                 if isinstance(item, dict) and item.get('title')}
                    else:
                        self.series_index = {}
                    
                    # Validate each loaded series has required fields
                    validated_index = {}
                    for title, series in self.series_index.items():
                        if _validate_series_entry(series, title):
                            validated_index[title] = series
                    self.series_index = validated_index
                    # Check that loaded data is not empty
                    if not self.series_index:
                        logger.warning("Loaded index is empty or contains no valid series")
                    
                print(f"[OK] Loaded {len(self.series_index)} series from index")
                logger.info(f"Loaded index with {len(self.series_index)} series")
            except json.JSONDecodeError as e:
                print(f"[ERROR] Index file corrupted: {e}")
                logger.error(f"Index file corrupted: {e}")
                self.series_index = {}
            except OSError as e:
                print(f"[ERROR] Cannot read index file: {e}")
                logger.error(f"Cannot read index file: {e}")
                self.series_index = {}
            except Exception as e:
                print(f"[WARN] Error loading index: {e}")
                logger.error(f"Error loading index: {e}")
                self.series_index = {}
    
    def save_index(self):
        """Save series index to file atomically with file locking to prevent corruption"""
        with self.file_lock:
            try:
                series_list = list(self.series_index.values())
                _atomic_write_json(self.index_file, series_list)
                logger.info(f"Saved index with {len(self.series_index)} series")
            except Exception as e:
                print(f"[ERROR] Failed to save index: {e}")
                logger.error(f"Error saving index: {e}")
                raise
    
    def get_series_with_progress(self, sort_by='completion', reverse=False):
        """Get series with computed episode progress information"""
        series_list = []
        for s in self.series_index.values():
            total_eps = 0
            watched_eps = 0
            for season in s.get('seasons', []):
                eps = season.get('episodes', [])
                total_eps += len(eps)
                watched_eps += sum(1 for ep in eps if ep.get('watched', False))
            is_incomplete = (total_eps == 0) or (watched_eps < total_eps)
            completion = round((watched_eps / total_eps) * 100, 2) if total_eps > 0 else 0.0
            series_list.append({
                'title': s.get('title', ''),
                'watched_episodes': watched_eps,
                'total_episodes': total_eps,
                'is_incomplete': is_incomplete,
                'completion': completion,
                'subscribed': s.get('subscribed', False),
                'watchlist': s.get('watchlist', False),
                'empty': s.get('empty', False),
                'season_labels': [str(sn.get('season', '?')) for sn in s.get('seasons', [])]
            })
        if sort_by:
            series_list.sort(key=lambda x: x.get(sort_by, 0), reverse=reverse)
        return series_list
    
    def get_statistics(self):
        """Enhanced statistics with detailed analytics"""
        series_with_progress = self.get_series_with_progress()
        total = len(series_with_progress)
        
        if total == 0:
            return {
                'total_series': 0, 'watched': 0, 'unwatched': 0,
                'watched_percentage': 0.0, 'empty_series': 0
            }
        
        watched = sum(1 for s in series_with_progress if not s['is_incomplete'])
        unwatched = total - watched
        empty_count = sum(1 for s in self.series_index.values() if s.get('empty', False))
        
        completion_percentages = [s['completion'] for s in series_with_progress]
        avg_completion = round(sum(completion_percentages) / total, 2)
        
        total_episodes = sum(s['total_episodes'] for s in series_with_progress)
        watched_episodes = sum(s['watched_episodes'] for s in series_with_progress)
        avg_episodes_per_series = round(total_episodes / total, 1) if total > 0 else 0
        
        # Subscription stats
        subscribed_count = sum(1 for s in series_with_progress if s['subscribed'])
        watchlist_count = sum(1 for s in series_with_progress if s['watchlist'])
        both_count = sum(1 for s in series_with_progress if s['subscribed'] and s['watchlist'])
        
        # Completion distribution
        completion_distribution = {
            '0-25%': sum(1 for p in completion_percentages if 0 <= p < 25),
            '25-50%': sum(1 for p in completion_percentages if 25 <= p < 50),
            '50-75%': sum(1 for p in completion_percentages if 50 <= p < 75),
            '75-99%': sum(1 for p in completion_percentages if 75 <= p < 100),
            '100%': sum(1 for p in completion_percentages if p == 100)
        }
        
        # Top/bottom performers
        sorted_by_completion = sorted(series_with_progress, key=lambda x: x['completion'], reverse=True)
        most_completed = sorted_by_completion[:5]
        least_completed = [s for s in sorted_by_completion[-5:] if s['completion'] < 100][:5]
        
        return {
            'total_series': total,
            'watched': watched,
            'unwatched': unwatched,
            'watched_percentage': round((watched / total * 100), 2),
            'empty_series': empty_count,
            'average_completion': avg_completion,
            'total_episodes': total_episodes,
            'watched_episodes': watched_episodes,
            'unwatched_episodes': total_episodes - watched_episodes,
            'average_episodes_per_series': avg_episodes_per_series,
            'subscribed_count': subscribed_count,
            'watchlist_count': watchlist_count,
            'both_subscribed_and_watchlist': both_count,
            'completion_distribution': completion_distribution,
            'most_completed_series': [
                {'title': s['title'], 'completion': s['completion'],
                 'progress': f"{s['watched_episodes']}/{s['total_episodes']}"}
                for s in most_completed
            ],
            'least_completed_series': [
                {'title': s['title'], 'completion': s['completion'],
                 'progress': f"{s['watched_episodes']}/{s['total_episodes']}"}
                for s in least_completed
            ]
        }
    
    def get_full_report(self, filter_subscribed=None, filter_watchlist=None, filter_mode='and'):
        """Generate a comprehensive report with detailed analytics"""
        series_progress = self.get_series_with_progress()
        stats = self.get_statistics()
        
        # Apply subscription/watchlist filters if specified
        if filter_mode == 'or' and filter_subscribed is not None and filter_watchlist is not None:
            series_progress = [
                s for s in series_progress
                if s['subscribed'] == filter_subscribed or s['watchlist'] == filter_watchlist
            ]
        else:
            if filter_subscribed is not None:
                series_progress = [s for s in series_progress if s['subscribed'] == filter_subscribed]
            if filter_watchlist is not None:
                series_progress = [s for s in series_progress if s['watchlist'] == filter_watchlist]
        
        # Categorize series
        # Watchlist/subscribed series with watched > 0 are "ongoing" (expecting future episodes)
        watched_series = [s for s in series_progress if not s['is_incomplete'] and not s.get('watchlist')]
        ongoing_series = [
            s for s in series_progress
            if s['watched_episodes'] > 0
            and (s.get('watchlist') or (s['is_incomplete'] and s['watched_episodes'] > 0))
        ]
        not_started_series = [
            s for s in series_progress
            if s['is_incomplete'] and s['watched_episodes'] == 0 and not s.get('watchlist')
            and not s.get('subscribed')
        ]
        # Not started but subscribed or on watchlist
        not_started_sub_wl = [
            s for s in series_progress
            if s['watched_episodes'] == 0 and s['total_episodes'] > 0
            and (s.get('subscribed') or s.get('watchlist'))
        ]
        not_started_sub_wl_sorted = sorted(not_started_sub_wl, key=lambda x: x['title'])
        # Surprise: subscribed (not watchlist) series that have new unwatched episodes
        surprise_series = [
            s for s in series_progress
            if s.get('subscribed') and not s.get('watchlist')
            and s['is_incomplete'] and s['watched_episodes'] > 0
        ]
        surprise_sorted = sorted(surprise_series, key=lambda x: x['completion'], reverse=True)
        
        ongoing_sorted = sorted(ongoing_series, key=lambda x: x['completion'], reverse=True)
        
        # Episode count ranges
        episode_ranges = {
            'short_series': [s['title'] for s in series_progress if s['total_episodes'] <= 5],
            'medium_series': [s['title'] for s in series_progress if 6 <= s['total_episodes'] <= 25],
            'long_series': [s['title'] for s in series_progress if s['total_episodes'] > 25]
        }
        
        # Completion insights
        near_completion = [s['title'] for s in ongoing_sorted if 80 <= s['completion'] < 100][:10]
        stalled = [s['title'] for s in ongoing_sorted if s['completion'] < 25][:10]
        
        report = {
            'metadata': {
                'generated': datetime.now().isoformat(),
                'total_series_in_index': len(self.series_index),
                'active_series': len(series_progress),
                'filter_subscribed': filter_subscribed,
                'filter_watchlist': filter_watchlist,
                'statistics': stats
            },
            'categories': {
                'watched': {
                    'count': len(watched_series),
                    'titles': sorted([s['title'] for s in watched_series])
                },
                'ongoing': {
                    'count': len(ongoing_series),
                    'titles': [s['title'] for s in ongoing_sorted],
                    'details': [{'title': s['title'], 'completion': s['completion'],
                                 'progress': f"{s['watched_episodes']}/{s['total_episodes']}",
                                 'seasons': s.get('season_labels', [])}
                                for s in ongoing_sorted[:20]]
                },
                'not_started': {
                    'count': len(not_started_series),
                    'titles': sorted([s['title'] for s in not_started_series])
                },
                'surprise_new_episodes': {
                    'count': len(surprise_series),
                    'titles': [s['title'] for s in surprise_sorted],
                    'details': [{'title': s['title'], 'completion': s['completion'],
                                 'progress': f"{s['watched_episodes']}/{s['total_episodes']}",
                                 'seasons': s.get('season_labels', [])}
                                for s in surprise_sorted]
                },
                'not_started_subscribed_watchlist': {
                    'count': len(not_started_sub_wl),
                    'titles': [s['title'] for s in not_started_sub_wl_sorted],
                    'details': [{'title': s['title'],
                                 'total_episodes': s['total_episodes'],
                                 'subscribed': s['subscribed'],
                                 'watchlist': s['watchlist'],
                                 'seasons': s.get('season_labels', [])}
                                for s in not_started_sub_wl_sorted]
                }
            },
            'insights': {
                'completion_distribution': stats.get('completion_distribution', {}),
                'episode_ranges': episode_ranges,
                'near_completion': near_completion,
                'stalled_series': stalled,
                'most_completed': stats.get('most_completed_series', [])[:10],
                'least_completed': stats.get('least_completed_series', [])[:10]
            },
            'raw_data': {
                'all_series': self.series_index,
                'series_progress': series_progress
            }
        }
        return report


def _prompt_change_confirmations(changes, new_dict):
    """
    Prompt the user to confirm each category of detected changes.

    Returns:
        dict mapping change category to bool (allowed or not).
    """
    allowed = {
        'watched': False,
        'unwatched': False,
        'subscribe': False,
        'unsubscribe': False,
        'watchlist_add': False,
        'watchlist_remove': False,
        'title_ger': False,
        'title_eng': False,
    }

    # Confirm watched (unwatched -> watched)
    if changes['newly_watched']:
        print(f"\n[OK] {len(changes['newly_watched'])} episode(s) would change from UNWATCHED to WATCHED")
        print("   (manual confirmation required)")
        print("\n" + "-"*70)
        grouped = defaultdict(list)
        for title, season, ep_num in changes['newly_watched']:
            grouped[(title, season)].append(ep_num)
        for (title, season), ep_nums in grouped.items():
            series = new_dict.get(title)
            total_in_season, watched_in_season = _get_season_stats(series, season)
            sub = '✓' if series and series.get('subscribed') else '✗'
            wl = '✓' if series and series.get('watchlist') else '✗'
            sub_wl = f" (Sub:{sub} WL:{wl})"
            if total_in_season > 0:
                print(f"  [+] {title} [{season}]: {watched_in_season}/{total_in_season} episodes{sub_wl}")
            else:
                print(f"  [+] {title} [{season}]: {len(ep_nums)} episode(s){sub_wl}")
        print("-"*70)
        resp = input("\nAllow these episodes to be marked as WATCHED? (y/n): ").strip().lower()
        if resp == 'y':
            allowed['watched'] = True
            logger.info("User allowed watched changes.")
        else:
            print("  -> Watched changes will be ignored (episodes stay unwatched)")
            logger.info("User denied watched changes.")

    # Confirm unwatched (watched -> unwatched)
    if changes['newly_unwatched']:
        print(f"\n[WARN] {len(changes['newly_unwatched'])} episode(s) would change from WATCHED to UNWATCHED")
        print("   (manual confirmation required)")
        print("\n" + "-"*70)
        grouped = defaultdict(list)
        for title, season, ep_num in changes['newly_unwatched']:
            grouped[(title, season)].append(ep_num)
        for (title, season), ep_nums in grouped.items():
            series = new_dict.get(title)
            total_in_season, watched_in_season = _get_season_stats(series, season)
            sub = '✓' if series and series.get('subscribed') else '✗'
            wl = '✓' if series and series.get('watchlist') else '✗'
            sub_wl = f" (Sub:{sub} WL:{wl})"
            if total_in_season > 0:
                print(f"  [!] {title} [{season}]: {watched_in_season}/{total_in_season} episodes{sub_wl}")
            else:
                print(f"  [!] {title} [{season}]: {len(ep_nums)} episode(s){sub_wl}")
        print("-"*70)
        resp = input("\nAllow these episodes to be marked as UNWATCHED? (y/n): ").strip().lower()
        if resp == 'y':
            allowed['unwatched'] = True
            logger.info("User allowed unwatched changes.")
        else:
            print("  -> Unwatched changes will be ignored (episodes stay watched)")
            logger.info("User denied unwatched changes.")

    # Confirm newly subscribed
    if changes['newly_subscribed']:
        print(f"\n[+] {len(changes['newly_subscribed'])} series newly SUBSCRIBED")
        print("   (manual confirmation required)")
        print("\n" + "-"*70)
        for title in changes['newly_subscribed']:
            print(f"  [+] {title}")
        print("-"*70)
        resp = input("\nAllow these series to be marked as SUBSCRIBED? (y/n): ").strip().lower()
        if resp == 'y':
            allowed['subscribe'] = True
            logger.info("User allowed subscribe changes.")
        else:
            print("  -> Subscribe changes will be ignored")
            logger.info("User denied subscribe changes.")

    # Confirm newly unsubscribed
    if changes['newly_unsubscribed']:
        print(f"\n[-] {len(changes['newly_unsubscribed'])} series UNSUBSCRIBED")
        print("   (manual confirmation required)")
        print("\n" + "-"*70)
        for title in changes['newly_unsubscribed']:
            print(f"  [-] {title}")
        print("-"*70)
        resp = input("\nAllow these series to be marked as UNSUBSCRIBED? (y/n): ").strip().lower()
        if resp == 'y':
            allowed['unsubscribe'] = True
            logger.info("User allowed unsubscribe changes.")
        else:
            print("  -> Unsubscribe changes will be ignored")
            logger.info("User denied unsubscribe changes.")

    # Confirm added to watchlist
    if changes['watchlist_added']:
        print(f"\n[+] {len(changes['watchlist_added'])} series ADDED TO WATCHLIST")
        print("   (manual confirmation required)")
        print("\n" + "-"*70)
        for title in changes['watchlist_added']:
            print(f"  [+] {title}")
        print("-"*70)
        resp = input("\nAllow these series to be added to WATCHLIST? (y/n): ").strip().lower()
        if resp == 'y':
            allowed['watchlist_add'] = True
            logger.info("User allowed watchlist add changes.")
        else:
            print("  -> Watchlist add changes will be ignored")
            logger.info("User denied watchlist add changes.")

    # Confirm removed from watchlist
    if changes['watchlist_removed']:
        print(f"\n[-] {len(changes['watchlist_removed'])} series REMOVED FROM WATCHLIST")
        print("   (manual confirmation required)")
        print("\n" + "-"*70)
        for title in changes['watchlist_removed']:
            print(f"  [-] {title}")
        print("-"*70)
        resp = input("\nAllow these series to be removed from WATCHLIST? (y/n): ").strip().lower()
        if resp == 'y':
            allowed['watchlist_remove'] = True
            logger.info("User allowed watchlist remove changes.")
        else:
            print("  -> Watchlist remove changes will be ignored")
            logger.info("User denied watchlist remove changes.")

    # Confirm title_ger changes
    if changes['title_ger_changed']:
        print(f"\n[~] {len(changes['title_ger_changed'])} German title(s) changed")
        print("   (manual confirmation required)")
        print("\n" + "-"*70)
        for title, old_val, new_val in changes['title_ger_changed']:
            print(f"  [~] {title}")
            print(f"      Old: {old_val}")
            print(f"      New: {new_val}")
        print("-"*70)
        resp = input("\nAllow German title changes? (y/n): ").strip().lower()
        if resp == 'y':
            allowed['title_ger'] = True
            logger.info("User allowed German title changes.")
        else:
            print("  -> German title changes will be ignored")
            logger.info("User denied German title changes.")

    # Confirm title_eng changes
    if changes['title_eng_changed']:
        print(f"\n[~] {len(changes['title_eng_changed'])} English title(s) changed")
        print("   (manual confirmation required)")
        print("\n" + "-"*70)
        for title, old_val, new_val in changes['title_eng_changed']:
            print(f"  [~] {title}")
            print(f"      Old: {old_val}")
            print(f"      New: {new_val}")
        print("-"*70)
        resp = input("\nAllow English title changes? (y/n): ").strip().lower()
        if resp == 'y':
            allowed['title_eng'] = True
            logger.info("User allowed English title changes.")
        else:
            print("  -> English title changes will be ignored")
            logger.info("User denied English title changes.")

    return allowed


def _build_merged_data(old_data, new_dict, allowed):
    """
    Merge new scraped data into old data, respecting user-allowed change categories.

    Returns:
        dict: merged series index.
    """
    merged = dict(old_data)
    for title, new_entry in new_dict.items():
        if title in merged:
            old_entry = merged[title]
            old_seasons = {s.get('season'): s for s in old_entry.get('seasons', [])}
            for new_season in new_entry.get('seasons', []):
                season_label = new_season.get('season')
                if season_label in old_seasons:
                    old_eps = {ep.get('number'): ep for ep in old_seasons[season_label].get('episodes', [])}
                    merged_episodes = []
                    for new_ep in new_season.get('episodes', []):
                        ep_num = new_ep.get('number')
                        if ep_num in old_eps:
                            old_watched = old_eps[ep_num].get('watched', False)
                            new_watched = new_ep.get('watched', False)
                            if allowed['watched'] and (not old_watched and new_watched):
                                new_ep['watched'] = True
                            elif allowed['unwatched'] and (old_watched and not new_watched):
                                new_ep['watched'] = False
                            else:
                                new_ep['watched'] = old_watched
                        merged_episodes.append(new_ep)
                    old_seasons[season_label]['episodes'] = merged_episodes
                else:
                    old_seasons[season_label] = new_season
            old_entry['seasons'] = list(old_seasons.values())
            # Recalculate counts from actual episode data
            old_entry['total_seasons'] = len(old_entry['seasons'])
            old_entry['watched_episodes'] = sum(
                sum(1 for ep in s.get('episodes', []) if ep.get('watched'))
                for s in old_entry['seasons']
            )
            old_entry['total_episodes'] = sum(
                len(s.get('episodes', []))
                for s in old_entry['seasons']
            )
            old_entry['unwatched_episodes'] = old_entry['total_episodes'] - old_entry['watched_episodes']
            # Validate URLs before merging to reject malicious values
            new_url = new_entry.get('url', '')
            new_link = new_entry.get('link', '')
            if new_url and _is_valid_series_url(new_url):
                old_entry['url'] = new_url
            elif new_url:
                logger.warning(f"Rejected invalid URL during merge for '{title}': {new_url[:80]}")
            if new_link and _is_valid_series_url(new_link):
                old_entry['link'] = new_link
            elif new_link:
                logger.warning(f"Rejected invalid link during merge for '{title}': {new_link[:80]}")
            if 'subscribed' in new_entry:
                old_sub = old_entry.get('subscribed', False)
                new_sub = new_entry['subscribed']
                if old_sub != new_sub:
                    if new_sub and allowed['subscribe']:
                        old_entry['subscribed'] = True
                    elif not new_sub and allowed['unsubscribe']:
                        old_entry['subscribed'] = False
                # If unchanged, keep as-is (no action needed)
            if 'watchlist' in new_entry:
                old_wl = old_entry.get('watchlist', False)
                new_wl = new_entry['watchlist']
                if old_wl != new_wl:
                    if new_wl and allowed['watchlist_add']:
                        old_entry['watchlist'] = True
                    elif not new_wl and allowed['watchlist_remove']:
                        old_entry['watchlist'] = False
            if allowed['title_ger'] and 'title_ger' in new_entry:
                old_entry['title_ger'] = new_entry['title_ger']
            if allowed['title_eng'] and 'title_eng' in new_entry:
                old_entry['title_eng'] = new_entry['title_eng']
            # Merge alt_titles: combine old + new, deduplicate, preserve order
            old_alts = old_entry.get('alt_titles', [])
            new_alts = new_entry.get('alt_titles', [])
            combined = list(dict.fromkeys(old_alts + new_alts))
            old_entry['alt_titles'] = combined
            old_entry['empty'] = old_entry['total_episodes'] == 0
            old_entry['last_updated'] = datetime.now().isoformat()
            # Reorder keys: metadata first, then seasons
            merged[title] = {
                'url': old_entry.get('url', ''),
                'link': old_entry.get('link', ''),
                'subscribed': old_entry.get('subscribed', False),
                'watchlist': old_entry.get('watchlist', False),
                'title': old_entry.get('title', title),
                'alt_titles': old_entry.get('alt_titles', []),
                'total_episodes': old_entry.get('total_episodes', 0),
                'watched_episodes': old_entry.get('watched_episodes', 0),
                'empty': old_entry.get('empty', False),
                'added_date': old_entry.get('added_date', ''),
                'last_updated': old_entry.get('last_updated', ''),
                'seasons': old_entry.get('seasons', []),
            }
        else:
            # New entry: build with proper field order
            # Do not default subscribed/watchlist to False — if they are missing
            # (None), the series should have been filtered out by _finalize_series_data.
            # Only set default if the key is truly absent (legacy data).
            if 'subscribed' not in new_entry:
                logger.warning(f"New entry '{title}' missing 'subscribed' field — setting to False")
                new_entry['subscribed'] = False
            if 'watchlist' not in new_entry:
                logger.warning(f"New entry '{title}' missing 'watchlist' field — setting to False")
                new_entry['watchlist'] = False
            new_entry.setdefault('alt_titles', [])
            new_entry['added_date'] = datetime.now().isoformat()
            seasons = new_entry.pop('seasons', [])
            new_entry['seasons'] = seasons
            merged[title] = new_entry

    return merged


def _extract_slug_from_field(value):
    """Extract series slug from a link or URL field containing '/serie/'.

    Returns the slug string, or None if extraction fails.
    """
    if not value or not isinstance(value, str):
        return None
    idx = value.find('/serie/')
    if idx == -1:
        return None
    slug = value[idx + len('/serie/'):].strip('/').split('/')[0]
    return slug if slug else None


def show_vanished_series(old_data, all_discovered_slugs, scrape_scope):
    """Show informational notification about previously indexed series not found in the current scrape.

    This is purely informational — no index changes are made.

    Args:
        old_data: dict of old series index (title -> series dict)
        all_discovered_slugs: set of slugs discovered in the current scrape
        scrape_scope: str indicating scrape mode:
            'all' / 'new_only' — full s.to catalogue was fetched, show all vanished
            'watchlist' — only show vanished with watchlist=True
            'subscribed' — only show vanished with subscribed=True
            'both' — show vanished with either flag; annotate if both were True
            None / other — suppress notification (partial scrape)

    Returns:
        list of (title, reason) tuples for vanished series, or empty list
    """
    if scrape_scope not in ('all', 'new_only', 'watchlist', 'subscribed', 'both'):
        return []

    vanished = []
    corrupt_entries = []

    for title, entry in old_data.items():
        # Extract slug from index entry
        slug = _extract_slug_from_field(entry.get('link', ''))
        if slug is None:
            slug = _extract_slug_from_field(entry.get('url', ''))
            if slug is not None:
                logger.warning(f"Used URL fallback for slug extraction: {title}")
            else:
                corrupt_entries.append(title)
                continue

        if slug in all_discovered_slugs:
            continue  # Still exists, not vanished

        # Filter by scope
        is_sub = entry.get('subscribed', False)
        is_wl = entry.get('watchlist', False)

        if scrape_scope in ('all', 'new_only'):
            vanished.append((title, 'not found on s.to'))
        elif scrape_scope == 'watchlist':
            if is_wl:
                vanished.append((title, 'was on watchlist'))
        elif scrape_scope == 'subscribed':
            if is_sub:
                vanished.append((title, 'was subscribed'))
        elif scrape_scope == 'both':
            if is_sub and is_wl:
                vanished.append((title, 'was subscribed + on watchlist — possibly deleted from s.to'))
            elif is_sub:
                vanished.append((title, 'was subscribed'))
            elif is_wl:
                vanished.append((title, 'was on watchlist'))

    # Print corrupt entries warning
    if corrupt_entries:
        print(f"\n⚠ {len(corrupt_entries)} index entry(s) have corrupt/missing URL data:")
        for t in corrupt_entries[:10]:
            print(f"  • {t}")
        if len(corrupt_entries) > 10:
            print(f"  ... and {len(corrupt_entries) - 10} more")
        print("  These entries were skipped during vanished-series detection.")
        logger.warning(f"Corrupt URL data in {len(corrupt_entries)} index entries: {corrupt_entries[:5]}")

    # Print vanished series notification
    if vanished:
        print(f"\n{'─'*70}")
        print(f"  [INFO] {len(vanished)} previously indexed series NOT found in current scrape:")
        print(f"{'─'*70}")
        for title, reason in vanished[:20]:
            print(f"  • {title}  ({reason})")
        if len(vanished) > 20:
            print(f"  ... and {len(vanished) - 20} more")
        print(f"{'─'*70}")
        print("  These series are preserved unchanged in the index.")
        logger.info(f"Vanished series notification: {len(vanished)} series not found in scrape scope '{scrape_scope}'")

    return vanished


def confirm_and_save_changes(new_data, description, index_manager):
    """
    Show changes, ask for separate watched/unwatched confirmation, merge, and save.
    Preserves old watch status by default unless user explicitly confirms changes.
    
    Args:
        new_data: List or dict of scraped series
        description: What was scraped (for display)
        index_manager: IndexManager instance
    
    Returns:
        True if saved, False if cancelled
    """
    old_data = dict(index_manager.series_index)
    
    # Ensure new_data is a dict
    if isinstance(new_data, list):
        new_dict = {s.get('title'): s for s in new_data if s.get('title')}
    else:
        new_dict = dict(new_data)
    
    changes = detect_changes(old_data, new_dict)
    logger.info(f"Detected changes: { {k: len(v) for k, v in changes.items()} }")
    
    # Step 1: Prompt user for each change category
    allowed = _prompt_change_confirmations(changes, new_dict)

    # Clear disallowed changes
    if not allowed['watched']:
        changes['newly_watched'] = []
    if not allowed['unwatched']:
        changes['newly_unwatched'] = []
    if not allowed['subscribe']:
        changes['newly_subscribed'] = []
    if not allowed['unsubscribe']:
        changes['newly_unsubscribed'] = []
    if not allowed['watchlist_add']:
        changes['watchlist_added'] = []
    if not allowed['watchlist_remove']:
        changes['watchlist_removed'] = []
    if not allowed['title_ger']:
        changes['title_ger_changed'] = []
    if not allowed['title_eng']:
        changes['title_eng_changed'] = []
    
    # Step 2: Build merged data
    merged = _build_merged_data(old_data, new_dict, allowed)
    
    # Step 3: Check if there are remaining changes to save
    main_changes = sum(len(v) for k, v in changes.items()
                       if k not in ('newly_unwatched', 'newly_subscribed', 'newly_unsubscribed',
                                    'watchlist_added', 'watchlist_removed'))
    if allowed['unwatched']:
        main_changes += len(changes['newly_unwatched'])
    if allowed['subscribe']:
        main_changes += len(changes['newly_subscribed'])
    if allowed['unsubscribe']:
        main_changes += len(changes['newly_unsubscribed'])
    if allowed['watchlist_add']:
        main_changes += len(changes['watchlist_added'])
    if allowed['watchlist_remove']:
        main_changes += len(changes['watchlist_removed'])
    if main_changes == 0:
        print(f"\n✓ {description} already up to date.")
        logger.info(f"No changes to save for {description}.")
        return True
    
    # Display remaining changes (watched/unwatched/subscription/watchlist already handled above)
    show_changes(changes, include_unwatched=False, include_watched=False,
                    include_subscribe=False, include_unsubscribe=False,
                    include_watchlist_add=False, include_watchlist_remove=False,
                    new_data=new_dict)
    
    response = input(f"\nSave these changes? (y/n): ").strip().lower()
    if response != 'y':
        print("✗ Changes discarded. Nothing saved.")
        logger.info("User discarded changes. Nothing saved.")
        return False
    
    # Save merged data via index_manager
    index_manager.series_index = merged
    index_manager.save_index()
    print(f"✓ Saved {len(merged)} series to index")
    logger.info(f"Saved {len(merged)} series to index")
    return True
