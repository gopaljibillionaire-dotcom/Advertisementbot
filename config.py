import logging
import re
import sys
from pathlib import Path
from typing import List, Union

def _clean_token(raw_token: str) -> str:
    """Removes mobile smart quotes, whitespace, and 'bot' prefixes."""
    if not raw_token:
        return ""
    # Strip whitespace, regular quotes, and mobile smart quotes
    cleaned = str(raw_token).strip().strip("'\"“”` ")
    # Remove 'bot' prefix if accidentally included (e.g. bot12345:ABC -> 12345:ABC)
    if cleaned.lower().startswith("bot"):
        cleaned = cleaned[3:]
    return cleaned.strip()

class Config:
    # ⬇️ PASTE YOUR TOKEN HERE ⬇️
    BOT_TOKEN: str = _clean_token("8225930756:AAHUAdi7YHq4gn5UMB_cICjWNAn57i_5uwM")
    
    SUPER_OWNER_IDS: List[int] = [6428946789, 7581752110]

    # First-Time Welcome Message Channel Configuration
    WELCOME_CHANNEL_ID: Union[int, str] = -1004354672186  # Target Channel ID or @username
    WELCOME_MESSAGE_ID: int = 4                           # Message ID from channel

    # Oxapay Crypto Payment Config
    OXAPAY_MERCHANT_KEY: str = "YOUR_OXAPAY_MERCHANT_KEY"  # Replace with your Oxapay Merchant Key

    # Database Path
    DATABASE_PATH: str = "bot.db"

    @classmethod
    def validate(cls):
        if not cls.BOT_TOKEN:
            logger.critical("FATAL: BOT_TOKEN is missing!")
            sys.exit(1)

        # aiogram token format regex check: DIGITS + COLON + ALPHANUMERIC
        token_pattern = r"^\d+:[A-Za-z0-9_-]{35,}$"
        if not re.match(token_pattern, cls.BOT_TOKEN):
            logger.critical(f"FATAL: BOT_TOKEN '{cls.BOT_TOKEN}' has an invalid format!")
            logger.critical("Ensure there are no extra letters, words, or smart quotes inside config.py.")
            sys.exit(1)

        db_path = Path(cls.DATABASE_PATH)
        if db_path.parent != Path("."):
            db_path.parent.mkdir(parents=True, exist_ok=True)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("AdMarketplaceBot")

Config.validate()
