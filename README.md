# s.to Backup Tool

![Python 3.9+](https://img.shields.io/badge/python-3.9%2B-blue)
![Selenium](https://img.shields.io/badge/selenium-4.x-green)
![Firefox](https://img.shields.io/badge/browser-Firefox-orange)
![License: MIT](https://img.shields.io/badge/license-MIT-lightgrey)

> Read-only backup of watched-episode progress, subscriptions, and watchlist data from [s.to](https://s.to). Uses browser automation (Selenium + Firefox) with built-in ad-blocking and adaptive rate limiting.

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Menu Options](#menu-options)
- [Architecture](#architecture)
- [Data Output](#data-output)
- [Parallel Scraping](#parallel-scraping)
- [Troubleshooting](#troubleshooting)
- [License](#license)

---

## Features

- Tracks **watched episode status** per series, season, and episode
- Records **subscription** and **watchlist** status per series
- Scrapes series directly from your **s.to account pages** (subscribed / watchlist)
- **Parallel scraping** with up to 16 Firefox workers for fast full-index updates
- **Adaptive global rate limiter** — backs off automatically on 502/503 server errors
- **Checkpoint / resume** — picks up where it left off after an interruption
- **Retry system** — saves failed series for a dedicated retry pass
- **Change reports** — new episodes, subscription changes, newly watched/unwatched
- **Completed series alerts** — highlights series you just finished watching
- **Atomic JSON writes** with automatic 3-generation backups to prevent data loss
- **uBlock Origin** installed in each browser session to block ads during scraping

---

## Requirements

- Python 3.9 or later
- Mozilla Firefox (installed)
- geckodriver — managed automatically by Selenium 4
- An active s.to account

---

## Installation

1. **Clone or download** this repository.

2. **Create a virtual environment** (recommended):

   ```bat
   python -m venv .venv
   .venv\Scripts\activate
   ```

   Linux / macOS:

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   ```

3. **Install dependencies**:

   ```bat
   pip install -r requirements.txt
   ```

4. **Configure credentials** — see [Configuration](#configuration) below.

---

## Configuration

Credentials are stored in `config/.env`. Copy the example and fill in your details:

```bat
copy config\.env.example config\.env
```

Linux / macOS:

```bash
cp config/.env.example config/.env
```

Edit `config/.env`:

```dotenv
STO_EMAIL=your_email@example.com
STO_PASSWORD=your_password_here
```

> **Important:** the credentials file must be `config/.env`, not `.env` in the project root.

### Settings in `config/config2.py`

| Setting                | Default | Description                                                      |
| ---------------------- | ------- | ---------------------------------------------------------------- |
| `HEADLESS`             | `True`  | Run browser invisibly. Set `False` to watch the automation live. |
| `HTTP_REQUEST_TIMEOUT` | `15.0`  | Per-request HTTP timeout (seconds)                               |
| `PAGE_LOAD_TIMEOUT`    | `20.0`  | Full page load timeout (seconds)                                 |
| `MAX_TOTAL_RETRIES`    | `5`     | Maximum retries per series before marking it failed              |
| `MAX_LOGIN_RETRIES`    | `3`     | Login attempts before aborting                                   |

CSS selectors, timing values, and page paths are defined in `config/selectors_config2.json`. Edit only if s.to changes its site structure.

---

## Usage

```bat
python main2.py
```

On first launch, the tool validates your credentials with a real login attempt before showing the menu. It also checks available disk space.

---

## Menu Options

| #   | Option                            | Description                                                      |
| --- | --------------------------------- | ---------------------------------------------------------------- |
| 1   | **Scrape all series**             | Scrape every series listed on the s.to series index              |
| 2   | **Scrape new series only**        | Scrape only series not yet present in your local index           |
| 3   | **Single / batch add**            | Add one series URL or a list from `series_urls.txt`              |
| 4   | **Generate report**               | View statistics filtered by subscription and/or watchlist status |
| 5   | **Scrape subscribed / watchlist** | Scrape series from your s.to account pages                       |
| 6   | **Retry failed series**           | Re-scrape series that failed in the last run                     |
| 7   | **Pause scraping**                | Signal parallel workers to stop at their next checkpoint         |
| 8   | **Show active workers**           | View running worker PIDs; optionally kill them all               |
| 9   | **Exit**                          | Quit the application                                             |

Options 1, 2, and 3 prompt you to choose between **sequential** (one browser, reliable) and **parallel** (multiple browsers, faster) mode before scraping starts.

---

## Architecture

```
main2.py                        Entry point: menu, credential check, disk-space guard
│
├── src/scraper2.py             Selenium + BeautifulSoup scraper
│   ├── __init__()              Loads selectors config, validates critical selectors
│   ├── setup_driver()          Firefox with uBlock Origin and anti-detection patches
│   ├── login()                 Email/password login with retry and cookie sharing
│   ├── get_account_series()    Crawls /account/subscribed and /account/watchlist
│   ├── run()                   Sequential scrape entry point
│   ├── _scrape_series_parallel()   ThreadPoolExecutor with shared work queue
│   └── _throttle_request()    Adaptive rate limiter (backs off on 502/503 errors)
│
├── src/index_manager2.py       Change detection, merging, analytics
│   ├── detect_changes()        Diffs old vs new data (series, episodes, subs)
│   ├── show_changes()          Paginated change display with season grouping
│   ├── FileLock                Cross-platform file-based lock (OS atomic create)
│   └── _atomic_write_json()    temp file + os.replace + 3-generation backups
│
└── config/
    ├── config2.py              Credentials, paths, timeout/retry constants
    └── selectors_config2.json  CSS selectors, delays, page paths for s.to
```

---

## Data Output

| File                           | Description                                                                     |
| ------------------------------ | ------------------------------------------------------------------------------- |
| `data/series_index.json`       | Primary persistent index of all scraped series                                  |
| `data/series_index_s_to.json`  | s.to-specific output (same schema)                                              |
| `data/.scrape_checkpoint.json` | Resume state (deleted automatically on success)                                 |
| `data/.failed_series.json`     | Failed series list (cleared after successful retry)                             |
| `data/.worker_pids_<pid>.json` | PIDs of running workers for this process (auto-cleaned on exit or next startup) |
| `logs/s_to_backup.log`         | Rotating log file — 10 MB max, 5 backups kept                                   |

### Series entry schema

```json
{
  "title": "Breaking Bad",
  "url": "https://s.to/serie/Breaking-Bad",
  "subscribed": true,
  "watchlist": false,
  "watched_episodes": 62,
  "total_episodes": 62,
  "seasons": [
    {
      "season": "Staffel 1",
      "total_episodes": 7,
      "watched_episodes": 7,
      "episodes": [
        {
          "number": 1,
          "title_ger": "Pilot",
          "title_eng": "Pilot",
          "watched": true
        }
      ]
    }
  ]
}
```

---

## Parallel Scraping

Options 1, 2, and 5 offer a **parallel mode** that spawns multiple Firefox workers simultaneously:

- Workers share a single authenticated session via **cookie injection** — only one real login needed
- A **global adaptive rate limiter** (`_throttle_request`) enforces a minimum interval between all worker requests; it increases the delay automatically when the server returns 502/503 errors and decays back to normal as errors subside
- Up to **16 workers** run in parallel (set by `MAX_WORKERS` in `src/scraper2.py`)
- Workers stagger their start times to avoid a burst of browser launches
- **Option 7** (Pause) creates a flag file that tells all workers to stop at their next checkpoint — data scraped so far is saved
- **Option 8** (Show workers) lists running PIDs and offers a force-kill if needed

> ⚠️ **One scraping instance at a time.** Only one terminal should run a scrape (options 1–3, 5, 6) at any given time. Running two simultaneous scrapes against the same `data/` folder will cause them to overwrite each other’s checkpoint and series index. Use a second terminal **only** for option 7 (pause) or option 8 (show workers) — both are read-only or signal-only operations that are safe to run alongside a live scrape.

For small lists (< 5 series), sequential mode is equally fast and more reliable.

---

## Troubleshooting

**Credentials not found / login fails**

- Ensure the file is `config/.env`, not `.env` in the project root
- Double-check `STO_EMAIL` and `STO_PASSWORD` match your s.to login exactly

**`geckodriver` error / WebDriver exception**

- Selenium 4 downloads geckodriver automatically — run `pip install --upgrade selenium`
- Make sure Firefox is installed and up to date

**Selectors stop matching (site updated)**

- Run with `HEADLESS = False` in `config/config2.py` to watch what Firefox sees
- Update the relevant selectors in `config/selectors_config2.json`

**Checkpoint left over from last run**

- If the tool was interrupted mid-scrape, `.scrape_checkpoint.json` remains in `data/`
- On the next run of the same option, you will be prompted: resume or start fresh

**Parallel workers not stopping after Ctrl+C**

- Use Option 7 to create a pause file, then Option 8 to view and kill worker PIDs
- Each process tracks its own PIDs in `data/.worker_pids_<pid>.json`; stale files from a hard-killed terminal are cleaned up automatically on the next startup
- If you need to clean up immediately, use Option 8 → kill all workers

**Low disk space warning at startup**

- The tool checks free space before every scraping operation
- Free up space or confirm you want to proceed when prompted

---

## License

MIT License
