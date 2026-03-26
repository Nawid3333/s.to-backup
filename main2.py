#!/usr/bin/env python3
"""
S.TO Series Scraper and Index Manager
Automatically scrapes your watched TV series from s.to and maintains a local index
"""

import copy
import json
import logging
import logging.handlers
import os
import re
import shutil
import subprocess
import sys
from urllib.parse import urlparse

# Add the project root directory to Python path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from config.config2 import EMAIL, PASSWORD, DATA_DIR, SERIES_INDEX_FILE, LOG_FILE
from src.index_manager2 import IndexManager, confirm_and_save_changes, show_vanished_series, _extract_slug_from_field
from src.scraper2 import SToBackupScraper, MAX_WORKERS

# Pre-compiled regex for URL validation
_SERIE_URL_RE = re.compile(r'/serie/[^/]+')


def _extract_slug(entry):
    """Extract series slug from an index entry using link (primary) or url (fallback).

    Returns slug string or None if both fail.
    """
    slug = _extract_slug_from_field(entry.get('link', ''))
    if slug:
        return slug
    slug = _extract_slug_from_field(entry.get('url', ''))
    if slug:
        title = entry.get('title', '?')
        print(f"  ⚠ Used URL fallback for slug extraction: {title}")
        logger.warning(f"Used URL fallback for slug extraction: {title}")
        return slug
    return None

# Mode labels for checkpoint tracking
_MODE_LABELS = {
    'all_series': 'Scrape all series (option 1)',
    'new_only': 'Scrape NEW series only (option 2)',
    'batch': 'Batch add (option 3)',
    'subscribed': 'Subscribed series (option 5)',
    'watchlist': 'Watchlist series (option 5)',
    'both': 'Subscribed+Watchlist series (option 5)',
    'retry': 'Retry failed (option 6)',
}


def _check_checkpoint(expected_mode):
    """Check for an existing checkpoint and prompt the user to resume or discard.

    Returns dict with 'ok' (proceed?) and 'resume' (resume from checkpoint?).
    """
    checkpoint_mode = SToBackupScraper.get_checkpoint_mode(DATA_DIR)
    if checkpoint_mode is None:
        return {'ok': True, 'resume': False}

    checkpoint_file = os.path.join(DATA_DIR, '.scrape_checkpoint.json')
    saved_label = _MODE_LABELS.get(checkpoint_mode, checkpoint_mode)
    expected_label = _MODE_LABELS.get(expected_mode, expected_mode)

    if checkpoint_mode == expected_mode:
        print(f"\n⚠ Checkpoint found from a previous \"{saved_label}\" run!\n")
        choice = input("Resume from checkpoint? (y/n): ").strip().lower()
        if choice == 'y':
            return {'ok': True, 'resume': True}
        # User declined resume — ask whether to discard
        discard = input("Discard old checkpoint and start fresh? (y/n): ").strip().lower()
        if discard == 'y':
            try:
                os.remove(checkpoint_file)
            except OSError:
                pass
            return {'ok': True, 'resume': False}
        return {'ok': False, 'resume': False}
    else:
        print(f"\n⚠ A checkpoint exists from a different mode: \"{saved_label}\"")
        print(f"   You are about to run: \"{expected_label}\"\n")
        discard = input("Discard the old checkpoint and continue? (y/n): ").strip().lower()
        if discard == 'y':
            try:
                os.remove(checkpoint_file)
            except OSError:
                pass
            return {'ok': True, 'resume': False}
        return {'ok': False, 'resume': False}

# Configure logging with rotation (prevents log files from growing indefinitely)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.handlers.RotatingFileHandler(
            LOG_FILE,
            maxBytes=10*1024*1024,  # 10 MB max per file
            backupCount=5  # Keep 5 rotated files
        ),
        logging.StreamHandler()
    ]
)
# Suppress urllib3 retry noise — these flood the console when geckodriver is killed externally
logging.getLogger('urllib3').setLevel(logging.ERROR)

logger = logging.getLogger(__name__)


def print_header():
    """Print application header"""
    print("\n" + "="*60)
    print("  S.TO SERIES SCRAPER & INDEX MANAGER")
    print("="*60 + "\n")


def print_scraped_series_status():
    """Reload index and print status for all newly scraped/updated series"""
    try:
        index_manager = IndexManager(SERIES_INDEX_FILE)

        if not index_manager.series_index:
            return

        series_list = list(index_manager.series_index.values())
        if not series_list:
            return

        # Sort by last updated (most recent first)
        sorted_series = sorted(
            series_list,
            key=lambda s: s.get('last_updated', s.get('added_date', '')),
            reverse=True
        )

        # Show first 5 updated series
        display_count = min(5, len(sorted_series))
        if display_count > 0:
            print("\n" + "-"*70)
            print("EPISODE STATUS (from merged index):")
            print("-"*70)
            for s in sorted_series[:display_count]:
                watched = s.get('watched_episodes', 0)
                total = s.get('total_episodes', 0)
                percent = round((watched / total * 100), 1) if total else 0
                season_labels = [str(sn.get('season', '?')) for sn in s.get('seasons', [])]
                season_info = f" [{','.join(season_labels)}]" if season_labels else ""
                sub = "✓" if s.get('subscribed') else "✗"
                wl = "✓" if s.get('watchlist') else "✗"
                print(f"  • {s.get('title')}{season_info}: {watched}/{total} episodes ({percent}%) | Sub: {sub} | WL: {wl}")
    except Exception as e:
        logger.error(f"Error printing series status: {e}")


def print_completed_series_alerts(index_manager=None):
    """Alert user about series that need attention:
    1. Fully watched but not subscribed
    2. Ongoing (started but incomplete) but not on watchlist
    3. Not started (0 watched) but subscribed or on watchlist
    """
    try:
        if index_manager is None:
            index_manager = IndexManager(SERIES_INDEX_FILE)

        if not index_manager.series_index:
            return

        # Rule 1: Fully watched + not subscribed + not on watchlist
        completed_not_sub = []
        # Rule 2: Ongoing + not on watchlist
        ongoing_no_wl = []
        # Rule 3: Not started but subscribed or on watchlist
        not_started_sub_wl = []

        for s in index_manager.series_index.values():
            # Recalculate from actual episode data to avoid stale top-level counts
            total = sum(len(sn.get('episodes', [])) for sn in s.get('seasons', []))
            watched = sum(
                sum(1 for ep in sn.get('episodes', []) if ep.get('watched', False))
                for sn in s.get('seasons', [])
            )
            subscribed = s.get('subscribed', False)
            watchlist = s.get('watchlist', False)

            if total > 0 and watched == total and not subscribed and not watchlist:
                completed_not_sub.append(s)
            elif total > 0 and 0 < watched < total and not watchlist:
                ongoing_no_wl.append(s)
            elif total > 0 and watched == 0 and (subscribed or watchlist):
                not_started_sub_wl.append(s)

        # --- Alert 1: Fully watched + not subscribed + not on watchlist ---
        if completed_not_sub:
            completed_not_sub.sort(key=lambda s: s.get('title', ''))
            print("\n" + "⚠"*35)
            print("⚠ COMPLETED SERIES — NOT SUBSCRIBED:")
            print("─" * 70)
            for s in completed_not_sub:
                total = sum(len(sn.get('episodes', [])) for sn in s.get('seasons', []))
                season_labels = [str(sn.get('season', '?')) for sn in s.get('seasons', [])]
                season_info = f" [{','.join(season_labels)}]" if season_labels else ""
                print(f"  • {s.get('title')}{season_info}: {total}/{total} episodes (100%) | Sub: ✗ | WL: ✗")
            print("─" * 70)
            print(f"→ {len(completed_not_sub)} series fully watched but not subscribed.")
            print("  Consider subscribing or leaving as-is.")
            print("⚠" * 35)

            # Offer to rescrape these series
            rescrape = input("\nRescrape these series to update Sub/WL status? (y/n): ").strip().lower()
            if rescrape == 'y':
                urls = [s.get('url') for s in completed_not_sub if s.get('url')]
                if not urls:
                    print("✗ No URLs found for these series")
                else:
                    print(f"\n→ Rescraping {len(urls)} completed series...")
                    print("  (Browser will open - do not close it manually)\n")
                    _run_scrape_and_save(
                        run_kwargs=dict(url_list=urls, parallel=False),
                        description=f"Rescrape completed series ({len(urls)})",
                        success_msg=f"Rescrape completed! {len(urls)} series updated.",
                        no_data_msg="No data scraped",
                    )

        # --- Alert 2: Ongoing + not on watchlist ---
        if ongoing_no_wl:
            ongoing_no_wl.sort(key=lambda s: s.get('title', ''))
            print("\n" + "⚠"*35)
            print("⚠ ONGOING SERIES — NOT ON WATCHLIST:")
            print("─" * 70)
            for s in ongoing_no_wl:
                total = sum(len(sn.get('episodes', [])) for sn in s.get('seasons', []))
                watched = sum(
                    sum(1 for ep in sn.get('episodes', []) if ep.get('watched', False))
                    for sn in s.get('seasons', [])
                )
                percent = round((watched / total * 100), 1) if total else 0
                season_labels = [str(sn.get('season', '?')) for sn in s.get('seasons', [])]
                season_info = f" [{','.join(season_labels)}]" if season_labels else ""
                sub = "✓" if s.get('subscribed') else "✗"
                print(f"  • {s.get('title')}{season_info}: {watched}/{total} episodes ({percent}%) | Sub: {sub} | WL: ✗")
            print("─" * 70)
            print(f"→ {len(ongoing_no_wl)} ongoing series not on your watchlist.")
            print("  Consider adding them to your watchlist.")
            print("⚠" * 35)

        # --- Alert 3: Not started but subscribed or on watchlist ---
        if not_started_sub_wl:
            not_started_sub_wl.sort(key=lambda s: s.get('title', ''))
            print("\n" + "⚠"*35)
            print("⚠ NOT STARTED — BUT SUBSCRIBED/WATCHLIST:")
            print("─" * 70)
            for s in not_started_sub_wl:
                total = sum(len(sn.get('episodes', [])) for sn in s.get('seasons', []))
                season_labels = [str(sn.get('season', '?')) for sn in s.get('seasons', [])]
                season_info = f" [{','.join(season_labels)}]" if season_labels else ""
                sub = "✓" if s.get('subscribed') else "✗"
                wl = "✓" if s.get('watchlist') else "✗"
                print(f"  • {s.get('title')}{season_info}: 0/{total} episodes (0%) | Sub: {sub} | WL: {wl}")
            print("─" * 70)
            print(f"→ {len(not_started_sub_wl)} series not started but subscribed/on watchlist.")
            print("  You may want to start watching or remove them.")
            print("⚠" * 35)

    except Exception as e:
        logger.error(f"Error printing series alerts: {e}")


def check_disk_space(min_mb=100):
    """Check if enough disk space is available. Returns True if OK, False if low."""
    try:
        stat = shutil.disk_usage(DATA_DIR)
        available_mb = stat.free / (1024 * 1024)
        if available_mb < min_mb:
            print(f"\n✗ WARNING: Low disk space!")
            print(f"  Available: {available_mb:.1f} MB (minimum needed: {min_mb} MB)")
            print(f"  Please free up disk space before scraping.\n")
            return False
        return True
    except Exception as e:
        logger.warning(f"Could not check disk space: {e}")
        return True  # Assume OK if we can't check

def validate_credentials():
    """Check that credentials are configured (does not test login)."""
    if not EMAIL or not PASSWORD:
        print("\n✗ ERROR: Credentials not configured!")
        print("\nPlease follow these steps:")
        print("1. Copy '.env.example' to '.env'")
        print("2. Add your s.to email and password to the .env file")
        print("3. Save the file and try again\n")
        return False
    return True


def show_menu():
    """Display interactive main menu"""
    print("\nOptions:")
    print("  1. Scrape all series")
    print("  2. Scrape only NEW series")
    print("  3. Single link / batch add")
    print("  4. Generate report")
    print("  5. Scrape subscribed/watchlist series")
    print("  6. Retry failed scrapes")
    print("  7. Pause scraping")
    print("  8. Show active workers")
    print("  9. Exit\n")


def _run_scrape_and_save(run_kwargs, description, success_msg, no_data_msg):
    """Common pattern: create scraper, run, confirm & save, handle errors.
    
    Returns the scraper instance (or None on error) so callers can inspect
    failed_links or series_data if needed.
    """
    try:
        scraper = SToBackupScraper()
        scraper.run(output_file=SERIES_INDEX_FILE, **run_kwargs)

        if scraper.series_data:
            index_manager = IndexManager(SERIES_INDEX_FILE)
            
            # Show vanished-series notification if full catalogue was fetched
            if scraper.all_discovered_series is not None:
                all_slugs = set()
                for s in scraper.all_discovered_series:
                    slug = s.get('slug') or _extract_slug(s)
                    if slug:
                        all_slugs.add(slug)
                scope = 'new_only' if run_kwargs.get('new_only') else 'all'
                show_vanished_series(index_manager.series_index, all_slugs, scope)
            
            if confirm_and_save_changes(scraper.series_data, description, index_manager):
                print(f"\n✓ {success_msg}")
                print_scraped_series_status()
                print_completed_series_alerts()
                logger.info(success_msg)
        else:
            print(f"\n⚠ {no_data_msg}")
            logger.warning(no_data_msg)

        # Scraping completed normally — safe to clear checkpoint now that user has confirmed/declined
        scraper.clear_checkpoint()

        if scraper.failed_links:
            print(f"\n⚠ {len(scraper.failed_links)} series failed during scraping.")
            print("→ Use option 6 (Retry failed series) to rescrape these later.")

        return scraper
    except (KeyboardInterrupt, SystemExit):
        print(f"\n⚠ Scraping interrupted by Ctrl+C")
        if 'scraper' in locals() and scraper.series_data:
            index_manager = IndexManager(SERIES_INDEX_FILE)
            if confirm_and_save_changes(scraper.series_data, description, index_manager):
                print(f"\n✓ Partial data saved ({len(scraper.series_data)} series)")
                logger.info(f"{description} interrupted — partial data saved")
        if 'scraper' in locals() and scraper.failed_links:
            print(f"\n⚠ {len(scraper.failed_links)} series failed.")
            print("→ Use option 6 (Retry failed series) to rescrape these later.")
        return scraper if 'scraper' in locals() else None
    except OSError as e:
        print(f"\n✗ Network error occurred: {str(e)}")
        logger.error(f"Network error in {description}: {e}")
    except Exception as e:
        print(f"\n✗ Unexpected error: {str(e)}")
        logger.error(f"Unexpected error in {description}: {e}")
    return None


def scrape_all_series():
    """Scrape all series with sequential or parallel mode selection"""
    print("\n→ Starting S.TO complete scraper...")
    print("  (Browser will open - do not close it manually)\n")
    
    # Check for existing checkpoint from this mode
    chk = _check_checkpoint('all_series')
    if not chk['ok']:
        print("✗ Cancelled")
        return
    resume = chk['resume']
    
    print("\nScraping mode:")
    print("  1. Sequential (slower, but most reliable)")
    print("  2. Parallel (faster, uses multiple workers)")
    print("  0. Back\n")
    mode_choice = input("Choose mode (0-2) [default: 2]: ").strip() or '2'
    
    if mode_choice == '0':
        return
    if mode_choice not in ['1', '2']:
        print("⚠ Invalid choice, using default (parallel)")
        use_parallel = True
    else:
        use_parallel = mode_choice == '2'
    
    _run_scrape_and_save(
        run_kwargs=dict(resume_only=resume, parallel=use_parallel),
        description="All series scrape",
        success_msg="Scraping completed and saved!",
        no_data_msg="No series data scraped",
    )


def scrape_new_series():
    """Scrape only new series not yet in the index"""
    print("\n→ Starting S.TO scraper (NEW series only)...")
    print("  (Browser will open - do not close it manually)\n")
    
    chk = _check_checkpoint('new_only')
    if not chk['ok']:
        print("\u2717 Cancelled")
        return
    resume = chk['resume']
    
    _run_scrape_and_save(
        run_kwargs=dict(new_only=True, resume_only=resume),
        description="New series data",
        success_msg="New series scraping completed successfully!",
        no_data_msg="No new series found",
    )


def single_or_batch_add():
    """Add single series by URL or batch from file with auto-detect"""
    default_file = os.path.join(os.path.dirname(__file__), 'series_urls.txt')
    print("\n→ Add single link / batch from file")
    print("  • Paste URL → scrapes single series")
    print("  • Enter filename → uses that file for batch")
    print(f"  • Press Enter → uses default (series_urls.txt)")
    print("  • Type 0   → back to main menu\n")
    
    user_input = input(f"Enter [default: series_urls.txt]: ").strip()
    
    if user_input == '0':
        return
    if not user_input:
        user_input = default_file
    
    if user_input.startswith(('http://', 'https://')):
        # Single URL
        add_single_series(user_input)
    else:
        # Treat as filename
        if not os.path.exists(user_input):
            print(f"✗ File not found: {user_input}")
            return
        batch_add_from_file(user_input)


def add_single_series(url):
    """Add a single series to the index by URL"""
    print(f"\n→ Scraping single series: {url}\n")
    
    # Validate URL format
    if not _SERIE_URL_RE.search(urlparse(url).path):
        print("✗ Invalid s.to series URL format")
        return
    
    _run_scrape_and_save(
        run_kwargs=dict(single_url=url, parallel=False),
        description="Single series",
        success_msg="Series added/updated successfully!",
        no_data_msg="No data scraped for this series",
    )


def batch_add_from_file(file_path):
    """Add multiple series from a text file containing URLs"""
    try:
        urls = []
        skipped = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                url = line.strip()
                if not url or url.startswith('#'):  # Skip empty lines and comments
                    continue
                # Validate URL format
                parsed = urlparse(url)
                if parsed.scheme and parsed.scheme not in ('http', 'https'):
                    skipped.append((line_num, url))
                    continue
                if not _SERIE_URL_RE.search(parsed.path):
                    skipped.append((line_num, url))
                    continue
                urls.append(url)
        if skipped:
            print(f"⚠ Skipped {len(skipped)} invalid URL(s):")
            for line_num, bad_url in skipped[:5]:
                print(f"  Line {line_num}: {bad_url[:80]}")
            if len(skipped) > 5:
                print(f"  ... and {len(skipped) - 5} more")
    except Exception as e:
        print(f"✗ Failed to read file: {str(e)}")
        logger.error(f"Failed to read file {file_path}: {e}")
        return
    
    if not urls:
        print("✗ No valid URLs found in file")
        return
    
    print(f"✓ Found {len(urls)} valid URL(s) in file\n")
    print("URLs to process:")
    for url in urls[:5]:  # Show first 5
        print(f"  • {url}")
    if len(urls) > 5:
        print(f"  ... and {len(urls) - 5} more")
    
    confirm = input("\nProceed with batch add? (y/n): ").strip().lower()
    if confirm != 'y':
        print("✗ Cancelled")
        return
    
    # Check for existing batch checkpoint
    chk = _check_checkpoint('batch')
    if not chk['ok']:
        print("\u2717 Cancelled")
        return
    resume = chk['resume']
    
    if not resume:
        # Ask user for scraping mode
        print("\nScraping mode:")
        print("  1. Sequential (slower, but most reliable)")
        print("  2. Parallel (faster, uses multiple workers)")
        print("  0. Back\n")
        mode_choice = input("Choose mode (0-2) [default: 1]: ").strip() or '1'
        if mode_choice == '0':
            return
        use_parallel = mode_choice == '2'
    else:
        use_parallel = None  # Let checkpoint decide
    
    print(f"\n→ Starting batch scraper for {len(urls)} series...")
    print("  (Browser will open - do not close it manually)\n")
    
    run_kwargs = dict(url_list=urls, resume_only=resume)
    if not resume:
        run_kwargs['parallel'] = use_parallel
    
    _run_scrape_and_save(
        run_kwargs=run_kwargs,
        description=f"Batch add ({len(urls)} series)",
        success_msg=f"Batch add completed! {len(urls)} series processed.",
        no_data_msg="No data scraped",
    )


def _show_ongoing_and_export(report, index_manager):
    """Show ongoing series and offer to export their URLs to series_urls.txt"""
    ongoing_count = report['categories']['ongoing']['count']
    if ongoing_count == 0:
        return
    
    print(f"ONGOING SERIES ({ongoing_count}):")
    ongoing_titles = report['categories']['ongoing']['titles']
    for title in ongoing_titles[:10]:
        print(f"  - {title}")
    if ongoing_count > 10:
        print(f"  ... and {ongoing_count - 10} more\n")
    
    export = input(f"\nExport {ongoing_count} ongoing series URLs to series_urls.txt? (y/n): ").strip().lower()
    if export == 'y':
        try:
            urls = []
            for title in ongoing_titles:
                series_data = index_manager.series_index.get(title, {})
                url = series_data.get('url') or series_data.get('link')
                if url:
                    if not url.startswith('http'):
                        url = f"https://s.to{url}"
                    urls.append(url)
            
            if urls:
                urls_file = os.path.join(os.path.dirname(__file__), 'series_urls.txt')
                with open(urls_file, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(urls) + '\n')
                print(f"\n✓ Exported {len(urls)} URLs to series_urls.txt")
                print(f"  → Use option 3 (Batch add from file) to rescrape these series")
                logger.info(f"Exported {len(urls)} URLs to series_urls.txt")
            else:
                print("\n⚠ Could not extract URLs from ongoing series")
        except Exception as e:
            print(f"\n✗ Failed to export URLs: {str(e)}")
            logger.error(f"Failed to export URLs: {e}")


def _print_report_summary(report, report_file, filter_name=None):
    """Print enhanced report summary to console."""
    stats = report['metadata']['statistics']
    ongoing_count = report['categories']['ongoing']['count']
    not_started_count = report['categories']['not_started']['count']
    not_started_sub_wl_count = report['categories']['not_started_subscribed_watchlist']['count']

    header = f"REPORT SUMMARY ({filter_name.upper().replace('_', ' ')}):" if filter_name else "REPORT SUMMARY:"
    print(f"\n" + "-"*70)
    print(header)
    print("-"*70)
    print(f"  Total series:        {stats['total_series']}")
    print(f"  Watched (100%):      {stats['watched']} ({stats.get('watched_percentage', 0):.1f}%)")
    print(f"  Unwatched:           {stats.get('unwatched', 0)}")
    print(f"  Ongoing (started):   {ongoing_count}")
    print(f"  Not started:         {not_started_count}")
    if not_started_sub_wl_count > 0:
        print(f"  Not started (Sub/WL):{not_started_sub_wl_count}")
    print(f"  Total episodes:      {stats['total_episodes']}")
    print(f"  Watched episodes:    {stats['watched_episodes']}")
    print(f"  Unwatched episodes:  {stats.get('unwatched_episodes', 0)}")
    print(f"  Avg episodes/series: {stats.get('average_episodes_per_series', 0)}")
    print(f"  Average completion:  {stats['average_completion']:.1f}%")
    print(f"  Subscribed:          {stats.get('subscribed_count', 0)}")
    print(f"  Watchlist:           {stats.get('watchlist_count', 0)}")
    print(f"  Both (Sub+WL):       {stats.get('both_subscribed_and_watchlist', 0)}")

    # Completion distribution
    dist = stats.get('completion_distribution', {})
    if dist:
        parts = [f"{k}: {v}" for k, v in dist.items()]
        print(f"\n  Completion Distribution:")
        print(f"    {'  |  '.join(parts)}")

    # Most completed series
    most = stats.get('most_completed_series', [])
    if most:
        print(f"\n  Most Completed (top {len(most)}):")
        for i, s in enumerate(most, 1):
            print(f"    {i}. {s['title']} \u2014 {s['completion']:.1f}% ({s['progress']})")

    # Least completed series
    least = stats.get('least_completed_series', [])
    if least:
        print(f"  Least Completed (bottom {len(least)}):")
        for i, s in enumerate(least, 1):
            print(f"    {i}. {s['title']} \u2014 {s['completion']:.1f}% ({s['progress']})")

    print(f"\n  Saved to:            {report_file}")
    print("-"*70 + "\n")


def generate_report():
    """Generate series report with optional filtering by subscription status"""
    print("\n→ Generate report")
    print("  1. Full report (all series)")
    print("  2. Subscription/watchlist filtered report")
    print("  0. Back\n")
    
    choice = input("Choose report type (0-2): ").strip()
    
    if choice == '0':
        return
    
    try:
        index_manager = IndexManager(SERIES_INDEX_FILE)
        
        if choice == '1':
            print("\n→ Generating full report...")
            report = index_manager.get_full_report()
            
            # Save report
            report_file = os.path.join(DATA_DIR, 'series_report.json')
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            
            _print_report_summary(report, report_file)
            logger.info("Full report generated")
            
            print_completed_series_alerts(index_manager)
            _show_ongoing_and_export(report, index_manager)
        
        elif choice == '2':
            print("\n→ Subscription/watchlist report")
            print("  1. Only subscribed")
            print("  2. Only watchlist")
            print("  3. Both")
            print("  0. Back\n")
            
            sub_choice = input("Choose filter (0-3): ").strip()
            
            if sub_choice == '0':
                return
            
            if sub_choice == '1':
                print("\n→ Generating report for subscribed series...")
                report = index_manager.get_full_report(filter_subscribed=True, filter_watchlist=False)
                filter_name = "subscribed_only"
            
            elif sub_choice == '2':
                print("\n→ Generating report for watchlist series...")
                report = index_manager.get_full_report(filter_subscribed=False, filter_watchlist=True)
                filter_name = "watchlist_only"
            
            elif sub_choice == '3':
                print("\n→ Generating report for subscribed AND watchlist...")
                report = index_manager.get_full_report(filter_subscribed=True, filter_watchlist=True)
                filter_name = "both_subscribed_watchlist"
            
            else:
                print("⚠ Invalid choice")
                return
            
            # Save to separate file per filter (does not overwrite the full report)
            report_file = os.path.join(DATA_DIR, f'series_report_{filter_name}.json')
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            
            _print_report_summary(report, report_file, filter_name)
            logger.info(f"Filtered report generated: {filter_name}")
            
            print_completed_series_alerts(index_manager)
            _show_ongoing_and_export(report, index_manager)
        
        else:
            print("⚠ Invalid choice")
    
    except Exception as e:
        print(f"\n✗ Error generating report: {str(e)}")
        logger.error(f"Error generating report: {e}")


def scrape_subscribed_watchlist():
    """Scrape subscribed and watchlist series from account pages"""
    print("\n→ Scrape subscribed/watchlist series")
    print("  1. Only subscribed")
    print("  2. Only watchlist")
    print("  3. Both")
    print("  0. Back\n")
    
    sub_choice = input("Choose source (0-3) [default: 3]: ").strip() or '3'
    
    if sub_choice == '0':
        return
    
    if sub_choice == '1':
        source = 'subscribed'
    elif sub_choice == '2':
        source = 'watchlist'
    elif sub_choice == '3':
        source = 'both'
    else:
        print("⚠ Invalid choice, using default (both)")
        source = 'both'
    
    print(f"\n  (Browser will open - do not close it manually)\n")
    
    output_file = SERIES_INDEX_FILE
    chk = _check_checkpoint(source)
    if not chk['ok']:
        print("\u2717 Cancelled")
        return
    resume = chk['resume']
    
    scraper = None
    try:
        scraper = SToBackupScraper()
        scraper.setup_driver()
        scraper.inject_aggressive_adblock()
        scraper.login()
        
        account_series = scraper.get_account_series(source=source)
        if not account_series:
            print("\n⚠ No series found on your account pages.")
            return
        
        # Detect series that disappeared from account pages (for flag flipping later)
        discovered_slugs = {s.get('slug', '') for s in account_series if s.get('slug')}
        pre_index = IndexManager(SERIES_INDEX_FILE)
        disappeared_watchlist = []
        disappeared_subscribed = []
        for title, entry in pre_index.series_index.items():
            slug = _extract_slug(entry)
            if slug is None:
                continue
            if slug in discovered_slugs:
                continue
            if source in ('watchlist', 'both') and entry.get('watchlist', False):
                disappeared_watchlist.append((title, entry))
            if source in ('subscribed', 'both') and entry.get('subscribed', False):
                disappeared_subscribed.append((title, entry))
        
        scraper.set_checkpoint_paths(os.path.dirname(output_file))
        scraper._checkpoint_mode = source
        
        if resume and scraper.load_checkpoint():
            # Filter out already-completed series
            remaining = [s for s in account_series
                         if s['url'].rstrip('/').split('/')[-1] not in scraper.completed_links]
            done = len(account_series) - len(remaining)
            print(f"✓ Resuming from checkpoint: {done}/{len(account_series)} already done")
            print(f"→ Remaining to scrape: {len(remaining)}")
            if not remaining:
                print("✓ All series already scraped")
                scraper.clear_checkpoint()
                return
            account_series = remaining
        
        print("→ Using parallel mode for account series...")
        raw = scraper._scrape_series_parallel(account_series, MAX_WORKERS)
        scraper.series_data = scraper._finalize_series_data(raw)
        scraper.clear_checkpoint()
        
        # Inject stubs for series that disappeared from account pages (flag flipping)
        failed_slugs = set()
        for fl in scraper.failed_links:
            fs = _extract_slug(fl) if isinstance(fl, dict) else None
            if fs:
                failed_slugs.add(fs)
        
        # Build set of already-scraped titles to avoid duplicates
        scraped_titles = set()
        if isinstance(scraper.series_data, dict):
            scraped_titles = set(scraper.series_data.keys())
        elif isinstance(scraper.series_data, list):
            scraped_titles = {s.get('title') for s in scraper.series_data if s.get('title')}
        
        injected_count_wl = 0
        injected_count_sub = 0
        
        for title, entry in disappeared_watchlist:
            slug = _extract_slug(entry)
            if slug and slug in failed_slugs:
                continue
            if title in scraped_titles:
                continue
            stub = copy.deepcopy(entry)
            stub['watchlist'] = False
            if isinstance(scraper.series_data, dict):
                scraper.series_data[title] = stub
            elif isinstance(scraper.series_data, list):
                scraper.series_data.append(stub)
            scraped_titles.add(title)
            injected_count_wl += 1
        
        for title, entry in disappeared_subscribed:
            slug = _extract_slug(entry)
            if slug and slug in failed_slugs:
                continue
            if title in scraped_titles:
                # Already injected (e.g. from watchlist pass) — just flip the sub flag
                if isinstance(scraper.series_data, dict) and title in scraper.series_data:
                    scraper.series_data[title]['subscribed'] = False
                elif isinstance(scraper.series_data, list):
                    for item in scraper.series_data:
                        if item.get('title') == title:
                            item['subscribed'] = False
                            break
                continue
            stub = copy.deepcopy(entry)
            stub['subscribed'] = False
            if isinstance(scraper.series_data, dict):
                scraper.series_data[title] = stub
            elif isinstance(scraper.series_data, list):
                scraper.series_data.append(stub)
            scraped_titles.add(title)
            injected_count_sub += 1
        
        if injected_count_wl > 0:
            print(f"  ⚠ {injected_count_wl} series no longer on watchlist (will prompt for confirmation)")
        if injected_count_sub > 0:
            print(f"  ⚠ {injected_count_sub} series no longer subscribed (will prompt for confirmation)")
        
        # Show vanished-series informational notification
        # Exclude series already handled by flag flipping above (they'll appear in change prompts)
        handled_titles = {t for t, _ in disappeared_watchlist} | {t for t, _ in disappeared_subscribed}
        vanished_index = {t: e for t, e in pre_index.series_index.items() if t not in handled_titles}
        show_vanished_series(vanished_index, discovered_slugs, source)
        
        if scraper.series_data:
            index_manager = IndexManager(SERIES_INDEX_FILE)
            if confirm_and_save_changes(scraper.series_data, "Account series", index_manager):
                print("\n✓ Account series scraping completed!")
                print_scraped_series_status()
                print_completed_series_alerts()
                logger.info("Account series scraping completed")
        else:
            print("\n⚠ No data scraped")
        
        if scraper.failed_links:
            print(f"\n⚠ {len(scraper.failed_links)} series failed during scraping.")
            print("→ Use option 6 (Retry failed series) to rescrape these later.")
    except (KeyboardInterrupt, SystemExit):
        print(f"\n⚠ Scraping interrupted by Ctrl+C")
        if scraper and scraper.series_data:
            index_manager = IndexManager(SERIES_INDEX_FILE)
            if confirm_and_save_changes(scraper.series_data, "Account series", index_manager):
                print(f"\n✓ Partial data saved ({len(scraper.series_data)} series)")
                logger.info("Account series interrupted — partial data saved")
        if scraper and scraper.failed_links:
            print(f"\n⚠ {len(scraper.failed_links)} series failed.")
            print("→ Use option 6 (Retry failed series) to rescrape these later.")
    except OSError as e:
        print(f"\n✗ Network error occurred: {str(e)}")
        logger.error(f"Network error in scrape_subscribed_watchlist: {e}")
    except Exception as e:
        print(f"\n✗ Failed: {str(e)}")
        logger.error(f"Error in scrape_subscribed_watchlist: {e}")
    finally:
        if scraper:
            try:
                if scraper.failed_links:
                    scraper.save_failed_series()
                scraper.clear_worker_pids()
                scraper.close()
            except Exception:
                pass


def retry_failed_series():
    """Retry previously failed series in sequential mode"""
    print("\n→ Retry failed series from last run")
    print("  (Browser will open - do not close it manually)\n")
    
    chk = _check_checkpoint('retry')
    if not chk['ok']:
        print("\u2717 Cancelled")
        return
    resume = chk['resume']
    
    # Pre-check for failed series before launching browser
    temp_scraper = SToBackupScraper()
    temp_scraper.set_checkpoint_paths(DATA_DIR)
    failed_list = temp_scraper.load_failed_series()
    if not failed_list:
        print("✓ No failed series found. Nothing to retry.")
        return
    print(f"✓ Found {len(failed_list)} failed series from last run")
    print("\n→ Starting retry in sequential mode (for reliability)...")
    
    _run_scrape_and_save(
        run_kwargs=dict(retry_failed=True, parallel=False, resume_only=resume),
        description="Retry data",
        success_msg="Retry completed successfully!",
        no_data_msg="No data from retry",
    )


def pause_scraping():
    """Create a pause file to signal workers to pause scraping"""
    pause_file = os.path.join(DATA_DIR, '.pause_scraping')
    try:
        with open(pause_file, 'w', encoding='utf-8') as f:
            f.write('PAUSE')
        print(f"\n✓ Pause file created: {pause_file}")
        print("Workers will pause at next checkpoint.\n")
        logger.info(f"Pause file created: {pause_file}")
    except Exception as e:
        print(f"\n✗ Failed to create pause file: {str(e)}")
        logger.error(f"Failed to create pause file: {e}")


def show_active_workers():
    """Display active worker processes"""
    try:
        pid_files = [
            os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR)
            if f.startswith('.worker_pids_') and f.endswith('.json')
        ]
    except OSError:
        pid_files = []

    if not pid_files:
        print("\n✓ No active workers found\n")
        return

    # Collect all workers across all instances
    all_workers = {}  # { (owner_pid, worker_id): (pid, filepath) }
    live_files = []
    for fpath in pid_files:
        try:
            with open(fpath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if not isinstance(data, dict):
                continue
            owner_pid = data.get('_owner_pid', '?')
            workers = {k: v for k, v in data.items() if k != '_owner_pid'}
            if not workers:
                continue
            live_files.append(fpath)
            for worker_id, pid in workers.items():
                all_workers[(str(owner_pid), str(worker_id))] = (pid, fpath)
        except Exception as e:
            logger.error(f"Error reading {fpath}: {e}")

    if not all_workers:
        print("\n✓ No active workers found\n")
        return

    print(f"\n📊 ACTIVE WORKERS ({len(all_workers)} across {len(live_files)} instance(s)):")
    print("Instance PID | Worker ID       | Worker PID | Type")
    print("-------------|-----------------|------------|-----")
    for (owner_pid, worker_id), (pid, _) in sorted(
        all_workers.items(),
        key=lambda x: (x[0][0], int(x[0][1].split('_')[0]) if x[0][1].split('_')[0].isdigit() else 999)
    ):
        worker_type = "Main" if worker_id == "0" else ("Firefox" if "_firefox" in worker_id else "Geckodriver")
        print(f"{owner_pid:>12} | {worker_id:>15} | {pid:>10} | {worker_type}")

    print()
    kill_choice = input("Kill all workers? (y/n): ").strip().lower()
    if kill_choice == 'y':
        print("\n🔴 Killing all workers...")
        killed_count = 0
        for (owner_pid, worker_id), (pid, _) in all_workers.items():
            try:
                if sys.platform == 'win32':
                    subprocess.run(['taskkill', '/F', '/PID', str(pid), '/T'],
                                   capture_output=True, check=False, timeout=5)
                else:
                    subprocess.run(['kill', '-9', str(pid)],
                                   capture_output=True, check=False)
                killed_count += 1
            except Exception as e:
                logger.error(f"Failed to kill worker {worker_id} (PID {pid}): {e}")
        removed_files = 0
        for fpath in set(fp for _, (_, fp) in all_workers.items()):
            try:
                os.remove(fpath)
                removed_files += 1
            except Exception as e:
                logger.error(f"Failed to clean up {fpath}: {e}")
        print(f"✓ Killed {killed_count} tracked process(es) across {removed_files} instance(s)\n")
        logger.info(f"Killed {killed_count} workers and cleaned up {removed_files} tracking files")
    else:
        print("✓ Workers left running\n")


def main():
    """Main application loop"""
    print_header()
    
    if not validate_credentials():
        sys.exit(1)
    
    # Check disk space at startup
    if not check_disk_space():
        response = input("Continue anyway? (y/n): ").strip().lower()
        if response != 'y':
            sys.exit(1)
    
    while True:
        show_menu()
        choice = input("Enter your choice (1-9): ").strip()
        
        if not choice.isdigit() or not (1 <= int(choice) <= 9):
            print("✗ Invalid choice. Please enter a number between 1 and 9.")
            continue
        
        # Check disk space before each scraping operation
        if choice in ['1', '2', '3', '5', '6']:
            if not check_disk_space():
                print("⚠ Aborting due to low disk space.")
                continue
        
        if choice == '1':
            scrape_all_series()
        elif choice == '2':
            scrape_new_series()
        elif choice == '3':
            single_or_batch_add()
        elif choice == '4':
            generate_report()
        elif choice == '5':
            scrape_subscribed_watchlist()
        elif choice == '6':
            retry_failed_series()
        elif choice == '7':
            pause_scraping()
        elif choice == '8':
            show_active_workers()
        elif choice == '9':
            print("\n✓ Goodbye!\n")
            break


if __name__ == "__main__":
    main()

