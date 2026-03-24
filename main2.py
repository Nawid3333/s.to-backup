#!/usr/bin/env python3
"""
S.TO Series Scraper and Index Manager
Automatically scrapes your watched TV series from s.to and maintains a local index
"""

import json
import logging
import os
import re
import subprocess
import sys
import time
from urllib.parse import urlparse

# Add the project root directory to Python path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from config.config2 import EMAIL, PASSWORD, DATA_DIR, SERIES_INDEX_FILE, SERIES_INDEX_S_TO_FILE, LOG_FILE
from src.index_manager2 import IndexManager, confirm_and_save_changes
from src.scraper2 import SToBackupScraper, MAX_WORKERS

# Pre-compiled regex for URL validation
_SERIE_URL_RE = re.compile(r'/serie/[^/]+')

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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

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
    """Alert user about series that are 100% watched but not subscribed and not on watchlist.
    These need manual attention — the user should decide whether to subscribe, add to watchlist, or ignore."""
    try:
        if index_manager is None:
            index_manager = IndexManager(SERIES_INDEX_FILE)

        if not index_manager.series_index:
            return

        alert_series = []
        for s in index_manager.series_index.values():
            total = s.get('total_episodes', 0)
            watched = s.get('watched_episodes', 0)
            if total > 0 and watched == total and not s.get('subscribed') and not s.get('watchlist'):
                alert_series.append(s)

        if not alert_series:
            return

        alert_series.sort(key=lambda s: s.get('title', ''))

        print("\n" + "⚠"*35)
        print("⚠ COMPLETED SERIES — ACTION NEEDED:")
        print("─" * 70)
        for s in alert_series:
            watched = s.get('watched_episodes', 0)
            total = s.get('total_episodes', 0)
            season_labels = [str(sn.get('season', '?')) for sn in s.get('seasons', [])]
            season_info = f" [{','.join(season_labels)}]" if season_labels else ""
            print(f"  • {s.get('title')}{season_info}: {watched}/{total} episodes (100%) | Sub: ✗ | WL: ✗")
        print("─" * 70)
        print(f"→ {len(alert_series)} series fully watched but not subscribed or on your watchlist.")
        print("  Consider subscribing, adding to watchlist, or leaving as-is.")
        print("⚠" * 35)

        # Offer to rescrape these series
        rescrape = input("\nRescrape these series to update Sub/WL status? (y/n): ").strip().lower()
        if rescrape == 'y':
            urls = [s.get('url') for s in alert_series if s.get('url')]
            if not urls:
                print("✗ No URLs found for these series")
                return
            print(f"\n→ Rescraping {len(urls)} completed series...")
            print("  (Browser will open - do not close it manually)\n")
            _run_scrape_and_save(
                run_kwargs=dict(url_list=urls, parallel=False),
                description=f"Rescrape completed series ({len(urls)})",
                success_msg=f"Rescrape completed! {len(urls)} series updated.",
                no_data_msg="No data scraped",
            )
    except Exception as e:
        logger.error(f"Error printing completed series alerts: {e}")


def validate_credentials():
    """Validate that credentials are configured"""
    if not EMAIL or not PASSWORD:
        print("✗ ERROR: Credentials not configured!")
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
        scraper.run(output_file=SERIES_INDEX_S_TO_FILE, **run_kwargs)

        if scraper.series_data:
            index_manager = IndexManager(SERIES_INDEX_FILE)
            if confirm_and_save_changes(scraper.series_data, description, index_manager):
                print(f"\n✓ {success_msg}")
                print_scraped_series_status()
                print_completed_series_alerts()
                logger.info(success_msg)
        else:
            print(f"\n⚠ {no_data_msg}")
            logger.warning(no_data_msg)

        if scraper.failed_links:
            print(f"\n⚠ {len(scraper.failed_links)} series failed during scraping.")
            print("→ Use option 6 (Retry failed series) to rescrape these later.")

        return scraper
    except OSError as e:
        print(f"\n✗ Network error occurred: {str(e)}")
        logger.error(f"Network error in {description}: {e}")
    except KeyboardInterrupt:
        print("\n⚠ Scraping interrupted by user")
        logger.info(f"{description} interrupted by user")
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
    print("  2. Parallel (faster, uses multiple workers)\n")
    mode_choice = input("Choose mode (1-2) [default: 2]: ").strip() or '2'
    
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
    print("\n→ Add single link / batch from file")
    print("  • Paste URL → scrapes single series")
    print("  • Enter filename → uses that file for batch\n")
    
    user_input = input("Enter: ").strip()
    
    if not user_input:
        print("✗ No input provided")
        return
    
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
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                url = line.strip()
                if url and not url.startswith('#'):  # Skip empty lines and comments
                    urls.append(url)
    except Exception as e:
        print(f"✗ Failed to read file: {str(e)}")
        logger.error(f"Failed to read file {file_path}: {e}")
        return
    
    if not urls:
        print("✗ No valid URLs found in file")
        return
    
    print(f"✓ Found {len(urls)} URL(s) in file\n")
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
        print("  2. Parallel (faster, uses multiple workers)\n")
        mode_choice = input("Choose mode (1-2) [default: 1]: ").strip() or '1'
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


def generate_report():
    """Generate series report with optional filtering by subscription status"""
    print("\n→ Generate report")
    print("  1. Full report (all series)")
    print("  2. Subscription/watchlist filtered report\n")
    
    choice = input("Choose report type (1-2): ").strip()
    
    try:
        index_manager = IndexManager(SERIES_INDEX_FILE)
        
        if choice == '1':
            print("\n→ Generating full report...")
            report = index_manager.get_full_report()
            
            # Save report
            report_file = os.path.join(DATA_DIR, f'series_report_{time.strftime("%Y%m%d_%H%M%S")}.json')
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            
            # Display summary
            stats = report['metadata']['statistics']
            print(f"\n" + "-"*70)
            print("REPORT SUMMARY:")
            print("-"*70)
            print(f"  Total series:        {stats['total_series']}")
            print(f"  Watched (100%):      {stats['watched']}")
            ongoing_count = report['categories']['ongoing']['count']
            not_started_count = report['categories']['not_started']['count']
            print(f"  Ongoing (started):   {ongoing_count}")
            print(f"  Not started:         {not_started_count}")
            print(f"  Total episodes:      {stats['total_episodes']}")
            print(f"  Watched episodes:    {stats['watched_episodes']}")
            print(f"  Average completion:  {stats['average_completion']:.1f}%")
            print(f"  Subscribed:          {stats.get('subscribed_count', 0)}")
            print(f"  Watchlist:           {stats.get('watchlist_count', 0)}")
            print(f"  Saved to:            {report_file}")
            print("-"*70 + "\n")
            logger.info("Full report generated")
            
            print_completed_series_alerts(index_manager)
            _show_ongoing_and_export(report, index_manager)
        
        elif choice == '2':
            print("\n→ Subscription/watchlist report")
            print("  1. Only subscribed")
            print("  2. Only watchlist")
            print("  3. Both\n")
            
            sub_choice = input("Choose filter (1-3): ").strip()
            
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
            
            # Save report
            report_file = os.path.join(DATA_DIR, f'series_report_{filter_name}_{time.strftime("%Y%m%d_%H%M%S")}.json')
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            
            # Display summary
            stats = report['metadata']['statistics']
            print(f"\n" + "-"*70)
            print(f"REPORT SUMMARY ({filter_name.upper().replace('_', ' ')}):")
            print("-"*70)
            print(f"  Total series:        {stats['total_series']}")
            print(f"  Watched (100%):      {stats['watched']}")
            ongoing_count = report['categories']['ongoing']['count']
            not_started_count = report['categories']['not_started']['count']
            print(f"  Ongoing (started):   {ongoing_count}")
            print(f"  Not started:         {not_started_count}")
            print(f"  Total episodes:      {stats['total_episodes']}")
            print(f"  Watched episodes:    {stats['watched_episodes']}")
            print(f"  Average completion:  {stats['average_completion']:.1f}%")
            print(f"  Saved to:            {report_file}")
            print("-"*70 + "\n")
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
    print("  3. Both\n")
    
    sub_choice = input("Choose source (1-3) [default: 3]: ").strip() or '3'
    
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
    
    output_file = SERIES_INDEX_S_TO_FILE
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
    worker_pids_file = os.path.join(DATA_DIR, '.worker_pids.json')
    
    if not os.path.exists(worker_pids_file):
        print("\n✓ No active workers found\n")
        return
    
    try:
        with open(worker_pids_file, 'r', encoding='utf-8') as f:
            workers = json.load(f)
        
        if not isinstance(workers, dict) or not workers:
            print("\n✓ No active workers\n")
            return
        
        print(f"\n📊 ACTIVE WORKERS ({len(workers)}):")
        print("ID | PID | Type")
        print("---|-----|------")
        
        for worker_id, pid in sorted(workers.items(), key=lambda x: int(x[0].split('_')[0]) if x[0].split('_')[0].isdigit() else 999):
            worker_type = "Main" if worker_id == "0" else ("Firefox" if "_firefox" in str(worker_id) else "Geckodriver")
            print(f"{worker_id:>15} | {pid} | {worker_type}")
        
        print()
        kill_choice = input("Kill all workers? (y/n): ").strip().lower()
        if kill_choice == 'y':
            print("\n🔴 Killing all workers...")
            killed_count = 0
            
            # First, kill all tracked PIDs
            for worker_id, pid in workers.items():
                try:
                    if sys.platform == 'win32':
                        subprocess.run(['taskkill', '/F', '/PID', str(pid), '/T'], 
                                     capture_output=True, check=False, timeout=5)
                    else:
                        subprocess.run(['kill', '-9', str(pid)], 
                                     capture_output=True, check=False)
                    killed_count += 1
                except Exception as e:
                    logger.error(f"Failed to kill worker {worker_id}: {e}")
            
            try:
                os.remove(worker_pids_file)
                print(f"✓ Killed {killed_count} tracked process(es) and their child processes")
                logger.info(f"Killed {killed_count} tracked workers")
            except Exception as e:
                print(f"✓ Killed {killed_count} tracked process(es)")
                print(f"⚠ Could not remove PID file: {e}\n")
                logger.error(f"Failed to clean up PID file: {e}")
        else:
            print("✓ Workers left running\n")
    
    except Exception as e:
        print(f"\n✗ Error reading workers: {str(e)}")
        logger.error(f"Error reading workers: {e}")


def main():
    """Main application loop"""
    print_header()
    
    # Validate credentials
    if not validate_credentials():
        sys.exit(1)
    
    print(f"✓ Credentials found for user: {EMAIL}\n")
    
    while True:
        show_menu()
        choice = input("Enter your choice (1-9): ").strip()
        
        if not choice.isdigit() or not (1 <= int(choice) <= 9):
            print("✗ Invalid choice. Please enter a number between 1 and 9.")
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

