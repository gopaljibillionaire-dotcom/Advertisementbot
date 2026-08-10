import logging
import os
import sys
from pathlib import Path
from typing import List, Union
from dotenv import load_dotenv

# Initialize logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("AdMarketplaceBot")

# Load environment variables from .env file
load_dotenv()


class Config:
    # Telegram Bot Token (automatically trimmed of hidden whitespace/newlines)
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "").strip()

    # Super Owner IDs parsed from comma-separated string
    _raw_owners = os.getenv("SUPER_OWNER_IDS", "6428946789,7581752110")
    SUPER_OWNER_IDS: List[int] = [
        int(owner_id.strip())
        for owner_id in _raw_owners.split(",")
        if owner_id.strip().lstrip("-").isdigit()
    ]

    # First-Time Welcome Message Channel Configuration
    _raw_channel = os.getenv("WELCOME_CHANNEL_ID", "-1004354672186").strip()
    WELCOME_CHANNEL_ID: Union[int, str] = (
        int(_raw_channel) if _raw_channel.lstrip("-").isdigit() else _raw_channel
    )
    WELCOME_MESSAGE_ID: int = int(os.getenv("WELCOME_MESSAGE_ID", "4"))

    # Oxapay Crypto Payment Config
    OXAPAY_MERCHANT_KEY: str = os.getenv("OXAPAY_MERCHANT_KEY", "").strip()

    # Database Path
    DATABASE_PATH: str = os.getenv("DATABASE_PATH", "bot.db").strip()

    @classmethod
    def validate(cls):
        if not cls.BOT_TOKEN:
            logger.critical("FATAL: BOT_TOKEN is missing or empty in environment / .env file!")
            sys.exit(1)

        if ":" not in cls.BOT_TOKEN:
            logger.critical("FATAL: BOT_TOKEN format is invalid! Ensure it matches '123456:ABC...' format.")
            sys.exit(1)

        db_path = Path(cls.DATABASE_PATH)
        if db_path.parent != Path("."):
            db_path.parent.mkdir(parents=True, exist_ok=True)


# Run validation immediately on import
Config.validate()
