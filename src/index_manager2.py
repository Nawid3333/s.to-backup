"""
S.TO Index Manager
Manages the persistent series index and handles data merging, change detection, and analytics.
Ported from Save bs.to with s.to-specific extensions (subscription/watchlist tracking).
"""

import hashlib
import json
import logging
import os
import re
import shutil
import tempfile
import threading
import time
from collections import defaultdict
from datetime import datetime

logger = logging.getLogger(__name__)

# Pre-compiled regex for season number extraction
_SEASON_NUMBER_RE = re.compile(r'(staffel|season|s)\s*(\d+)', re.IGNORECASE)


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
        """Acquire lock, waiting up to timeout seconds."""
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
        
        logger.warning(f"Could not acquire lock on {self.filepath} after {self.timeout}s")
        return False
    
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


def _compute_file_checksum(filepath):
    """Compute SHA256 checksum of a file for corruption detection."""
    try:
        sha256_hash = hashlib.sha256()
        with open(filepath, 'rb') as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except Exception as e:
        logger.warning(f"Could not compute checksum for {filepath}: {e}")
        return None


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
        "subscription_changed": [],
        "watchlist_changed": [],
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
            changes["subscription_changed"].append((title, old_sub, new_sub))
        
        # Watchlist changes
        old_wl = old_series.get('watchlist', False)
        new_wl = new_series.get('watchlist', False)
        if old_wl != new_wl:
            changes["watchlist_changed"].append((title, old_wl, new_wl))
        
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
                 include_subscription=True, include_watchlist=True, new_data=None):
    """Display changes with pagination and smart season grouping."""
    total = 0
    for k, v in changes.items():
        if k == 'newly_unwatched' and not include_unwatched:
            continue
        if k == 'newly_watched' and not include_watched:
            continue
        if k == 'subscription_changed' and not include_subscription:
            continue
        if k == 'watchlist_changed' and not include_watchlist:
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

    if changes.get("subscription_changed") and include_subscription:
        print(f"\n[SUBSCRIPTION CHANGED] ({len(changes['subscription_changed'])} series)")
        paginate_list(
            changes["subscription_changed"],
            lambda x: f"  [~] {x[0]}: {'subscribed' if x[2] else 'unsubscribed'}"
        )

    if changes.get("watchlist_changed") and include_watchlist:
        print(f"\n[WATCHLIST CHANGED] ({len(changes['watchlist_changed'])} series)")
        paginate_list(
            changes["watchlist_changed"],
            lambda x: f"  [~] {x[0]}: {'added to watchlist' if x[2] else 'removed from watchlist'}"
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
                        if not isinstance(series, dict):
                            logger.warning(f"Skipping invalid series entry (not dict): {title}")
                            continue
                        # Check for critical fields
                        if not series.get('url'):
                            logger.warning(f"Skipping series '{title}' - missing 'url' field")
                            continue
                        if not isinstance(series.get('seasons'), list):
                            logger.warning(f"Skipping series '{title}' - 'seasons' must be list, got {type(series.get('seasons'))}")  
                            continue
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
        # Watchlist series are always "ongoing" (expecting future episodes), even at 100%
        watched_series = [s for s in series_progress if not s['is_incomplete'] and not s.get('watchlist')]
        ongoing_series = [
            s for s in series_progress
            if s.get('watchlist') or (s['is_incomplete'] and s['watched_episodes'] > 0)
        ]
        not_started_series = [
            s for s in series_progress
            if s['is_incomplete'] and s['watched_episodes'] == 0 and not s.get('watchlist')
        ]
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
    
    allow_watched = False
    allow_unwatched = False
    allow_subscription = False
    allow_watchlist = False
    allow_title_ger = False
    allow_title_eng = False
    
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
            allow_watched = True
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
            allow_unwatched = True
            logger.info("User allowed unwatched changes.")
        else:
            print("  -> Unwatched changes will be ignored (episodes stay watched)")
            logger.info("User denied unwatched changes.")
    
    # Confirm subscription changes
    if changes['subscription_changed']:
        print(f"\n[~] {len(changes['subscription_changed'])} series subscription(s) changed")
        print("   (manual confirmation required)")
        print("\n" + "-"*70)
        for title, old_val, new_val in changes['subscription_changed']:
            arrow = "subscribed" if new_val else "unsubscribed"
            print(f"  [~] {title}: {arrow}")
        print("-"*70)
        resp = input("\nAllow subscription changes? (y/n): ").strip().lower()
        if resp == 'y':
            allow_subscription = True
            logger.info("User allowed subscription changes.")
        else:
            print("  -> Subscription changes will be ignored")
            logger.info("User denied subscription changes.")
    
    # Confirm watchlist changes
    if changes['watchlist_changed']:
        print(f"\n[~] {len(changes['watchlist_changed'])} series watchlist status changed")
        print("   (manual confirmation required)")
        print("\n" + "-"*70)
        for title, old_val, new_val in changes['watchlist_changed']:
            arrow = "added to watchlist" if new_val else "removed from watchlist"
            print(f"  [~] {title}: {arrow}")
        print("-"*70)
        resp = input("\nAllow watchlist changes? (y/n): ").strip().lower()
        if resp == 'y':
            allow_watchlist = True
            logger.info("User allowed watchlist changes.")
        else:
            print("  -> Watchlist changes will be ignored")
            logger.info("User denied watchlist changes.")
    
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
            allow_title_ger = True
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
            allow_title_eng = True
            logger.info("User allowed English title changes.")
        else:
            print("  -> English title changes will be ignored")
            logger.info("User denied English title changes.")
    
    # Clear disallowed changes
    if not allow_watched:
        changes['newly_watched'] = []
    if not allow_unwatched:
        changes['newly_unwatched'] = []
    if not allow_subscription:
        changes['subscription_changed'] = []
    if not allow_watchlist:
        changes['watchlist_changed'] = []
    if not allow_title_ger:
        changes['title_ger_changed'] = []
    if not allow_title_eng:
        changes['title_eng_changed'] = []
    
    # Build merged data preserving old entries
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
                            if allow_watched and (not old_watched and new_watched):
                                new_ep['watched'] = True
                            elif allow_unwatched and (old_watched and not new_watched):
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
            old_entry['url'] = new_entry.get('url', old_entry.get('url'))
            old_entry['link'] = new_entry.get('link', old_entry.get('link'))
            if allow_subscription and 'subscribed' in new_entry:
                old_entry['subscribed'] = new_entry['subscribed']
            if allow_watchlist and 'watchlist' in new_entry:
                old_entry['watchlist'] = new_entry['watchlist']
            if allow_title_ger and 'title_ger' in new_entry:
                old_entry['title_ger'] = new_entry['title_ger']
            if allow_title_eng and 'title_eng' in new_entry:
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
            new_entry.setdefault('subscribed', False)
            new_entry.setdefault('watchlist', False)
            new_entry.setdefault('alt_titles', [])
            new_entry['added_date'] = datetime.now().isoformat()
            seasons = new_entry.pop('seasons', [])
            new_entry['seasons'] = seasons
            merged[title] = new_entry
    
    # Check if there are remaining changes to save
    main_changes = sum(len(v) for k, v in changes.items()
                       if k not in ('newly_unwatched', 'subscription_changed', 'watchlist_changed'))
    if allow_unwatched:
        main_changes += len(changes['newly_unwatched'])
    if allow_subscription:
        main_changes += len(changes['subscription_changed'])
    if allow_watchlist:
        main_changes += len(changes['watchlist_changed'])
    if main_changes == 0:
        print(f"\n✓ {description} already up to date.")
        logger.info(f"No changes to save for {description}.")
        return True
    
    # Display remaining changes (watched/unwatched/subscription/watchlist already handled above)
    show_changes(changes, include_unwatched=False, include_watched=False,
                    include_subscription=False, include_watchlist=False, new_data=new_dict)
    
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
