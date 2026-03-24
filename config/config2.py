"""
S.TO Backup Tool Configuration
Load credentials from .env file and site-specific selectors
"""

import os
import json
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

# ==================== CREDENTIALS ====================
# These are loaded from .env file for security
EMAIL = os.getenv("STO_EMAIL", "")      # s.to login email
PASSWORD = os.getenv("STO_PASSWORD", "") # s.to login password

# ==================== DIRECTORIES ====================
# Data storage location for backups
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

# Logs directory for storing log files
LOGS_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")

# Create directories if they don't exist
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)

# Series index file (maintains persistent index of all series)
SERIES_INDEX_FILE = os.path.join(DATA_DIR, "series_index.json")

# Output file for s.to backup data
SERIES_INDEX_S_TO_FILE = os.path.join(DATA_DIR, "series_index_s_to.json")

# ==================== SELECTORS CONFIGURATION ====================
# Load site-specific CSS/XPath selectors from JSON
CONFIG_DIR = os.path.dirname(__file__)
SELECTORS_CONFIG_FILE = os.path.join(CONFIG_DIR, "selectors_config2.json")

def load_selectors_config():
    """Load site-specific selectors configuration from JSON file"""
    try:
        with open(SELECTORS_CONFIG_FILE, 'r', encoding='utf-8') as f:
            config = json.load(f)
            print(f"✓ Loaded selectors from {SELECTORS_CONFIG_FILE}")
            return config
    except FileNotFoundError:
        print(f"⚠ Selectors config not found: {SELECTORS_CONFIG_FILE}")
        return {}
    except json.JSONDecodeError as e:
        print(f"⚠ Error parsing selectors config: {e}")
        return {}
    except Exception as e:
        print(f"⚠ Warning: Could not load selectors config: {str(e)}")
        return {}

SELECTORS_CONFIG = load_selectors_config()

# ==================== SCRAPING SETTINGS ====================
# Timeout for WebDriver wait operations (in seconds)
TIMEOUT = int(os.getenv("STO_TIMEOUT", SELECTORS_CONFIG.get("timing", {}).get("timeout", 10)))

# Run in headless mode (no visible browser window)
# Set to False to see browser automation for debugging
HEADLESS = os.getenv("STO_HEADLESS", "true").lower() in ('true', '1', 'yes')

# ==================== LOGGING ====================
# Log file location (stored in logs/ directory)
LOG_FILE = os.path.join(LOGS_DIR, "s_to_backup.log")
LOG_LEVEL = "INFO"  # DEBUG, INFO, WARNING, ERROR, CRITICAL

# ==================== ANTI-DETECTION SETTINGS ====================
# These are configured in scraper2.py but can be overridden here if needed
# Images disabled: Yes (faster, blocks visual ads)
# Autoplay disabled: Yes (no audio/video ads)
# Geolocation disabled: Yes (prevents ad-based location tracking)
# WebGL disabled: Yes (prevents ad tracking via WebGL)
# Firefox strict tracking protection: Yes

print(f"✓ Config loaded (DATA_DIR: {DATA_DIR}, TIMEOUT: {TIMEOUT}s)")
