import os
import re
import sys
from dotenv import load_dotenv

# Load variables from .env file
load_dotenv()


class Config:
    # Bot & Admin Settings
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "").strip()
    SUPER_OWNER_IDS: list[int] = [
        int(x.strip()) for x in os.getenv("SUPER_OWNER_IDS", "").split(",") if x.strip()
    ]

    # First-Time Welcome Message Settings
    WELCOME_CHANNEL_ID: str = os.getenv("WELCOME_CHANNEL_ID", "").strip()
    WELCOME_MESSAGE_ID: int = int(os.getenv("WELCOME_MESSAGE_ID", "4"))

    # Payment & Database Config
    OXAPAY_MERCHANT_KEY: str = os.getenv("OXAPAY_MERCHANT_KEY", "")
    MONGO_URI: str = os.getenv("MONGO_URI", "mongodb://localhost:27017")

    # System Defaults
    DEFAULT_CURRENCY: str = os.getenv("DEFAULT_CURRENCY", "USD")
    STARS_TO_USD_RATE: float = float(os.getenv("STARS_TO_USD_RATE", "0.02"))
    MIN_WITHDRAWAL_TON: float = float(os.getenv("MIN_WITHDRAWAL_TON", "1.0"))
    SUPPORT_LINK: str = os.getenv("SUPPORT_LINK", "https://t.me/CoreCreations")
    MAIN_CHANNEL_LINK: str = os.getenv("MAIN_CHANNEL_LINK", "https://t.me/PostsMarket")

    @classmethod
    def validate(cls):
        """Ensures required configurations are present."""
        if not cls.BOT_TOKEN:
            sys.exit("FATAL: BOT_TOKEN is missing!")

        if not cls.MONGO_URI.startswith(("mongodb://", "mongodb+srv://")):
            sys.exit("FATAL: MONGO_URI must start with mongodb:// or mongodb+srv://")
