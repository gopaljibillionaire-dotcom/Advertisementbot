import logging
import re
import sys
from pathlib import Path
from typing import List, Union

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("AdMarketplaceBot")


def _clean_token(raw_token: str) -> str:
    """
    Strips Android smart quotes, whitespace, invisible Unicode characters
    (zero-width spaces, non-breaking spaces), and accidental 'bot' prefixes.
    """
    if not raw_token:
        return ""

    cleaned = str(raw_token)

    # Remove hidden Unicode control characters often inserted by Termux / Gboard
    cleaned = re.sub(r'[\u200b-\u200d\ufeff\u00a0\r\n\t]', '', cleaned)

    # Strip surrounding standard spaces, newlines, tabs, standard quotes, and smart quotes
    quotes_and_spaces = " \t\n\r'\"“”‘’`«»"
    cleaned = cleaned.strip(quotes_and_spaces)

    # Remove 'bot' prefix if accidentally included (e.g., bot12345:ABC -> 12345:ABC)
    if cleaned.lower().startswith("bot"):
        cleaned = cleaned[3:].strip(quotes_and_spaces)

    return cleaned


class Config:
    # ⬇️ PASTE YOUR BOT TOKEN HERE ⬇️
    BOT_TOKEN: str = _clean_token("8225930756:AAHUAdi7YHq4gn5UMB_cICjWNAn57i_5uwM")

    SUPER_OWNER_IDS: List[int] = [6428946789, 7581752110]

    # First-Time Welcome Message Channel Configuration
    WELCOME_CHANNEL_ID: Union[int, str] = -1004354672186  # Target Channel ID or @username
    WELCOME_MESSAGE_ID: int = 4                           # Message ID from channel

    # Oxapay Crypto Payment Config
    OXAPAY_MERCHANT_KEY: str = "CNSQNK-D58COJ-UTTMFV-GOTYR5"  # Replace with your Merchant Key

    # Database Path
    DATABASE_PATH: str = "bot.db"

    # Financial & System Defaults
    DEFAULT_CURRENCY: str = "USD"
    STARS_TO_USD_RATE: float = 0.02  # 1 Star ~ $0.02 USD
    MIN_WITHDRAWAL_TON: float = 1.0
    SUPPORT_LINK: str = "https://t.me/CoreCreations"
    MAIN_CHANNEL_LINK: str = "https://t.me/PostsMarket"

    @classmethod
    def validate(cls):
        """Validates that BOT_TOKEN is present and matches Telegram format."""
        if not cls.BOT_TOKEN:
            logger.critical("FATAL: BOT_TOKEN is missing from config.py!")
            sys.exit(1)

        # Strict Telegram bot token regex: DIGITS + COLON + ALPHANUMERIC/UNDERSCORE/HYPHEN (min 35 chars)
        token_pattern = r"^\d+:[A-Za-z0-9_-]{35,}$"
        if not re.match(token_pattern, cls.BOT_TOKEN):
            logger.critical(f"FATAL: BOT_TOKEN '{cls.BOT_TOKEN}' has an invalid format!")
            logger.critical("Ensure there are no extra letters, words, or hidden characters inside config.py.")
            sys.exit(1)

        db_path = Path(cls.DATABASE_PATH)
        if db_path.parent != Path("."):
            db_path.parent.mkdir(parents=True, exist_ok=True)
