import logging
import os
import sys
from dotenv import load_dotenv

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("BotConfig")


class Config:
    # Bot & Admin Settings
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "").strip()
    SUPER_OWNER_IDS: list[int] = [
        int(x.strip()) for x in os.getenv("SUPER_OWNER_IDS", "").split(",") if x.strip()
    ]

    # Project Identifier for MongoDB Multi-Project Isolation
    # This prevents database/collection collisions across multiple projects sharing the same MongoDB URI.
    PROJECT_PREFIX: str = os.getenv("PROJECT_PREFIX", "project1").strip().lower()

    # Database Configuration
    MONGO_URI: str = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    MONGO_DB_NAME: str = os.getenv("MONGO_DB_NAME", "shared_bot_database").strip()

    # Payment Config
    OXAPAY_MERCHANT_KEY: str = os.getenv("OXAPAY_MERCHANT_KEY", "")

    # System Defaults
    DEFAULT_CURRENCY: str = os.getenv("DEFAULT_CURRENCY", "USD")
    STARS_TO_USD_RATE: float = float(os.getenv("STARS_TO_USD_RATE", "0.02"))
    MIN_WITHDRAWAL_TON: float = float(os.getenv("MIN_WITHDRAWAL_TON", "1.0"))
    SUPPORT_LINK: str = os.getenv("SUPPORT_LINK", "https://t.me/CoreCreations")
    MAIN_CHANNEL_LINK: str = os.getenv("MAIN_CHANNEL_LINK", "https://t.me/PostsMarket")

    @classmethod
    def validate(cls):
        """Ensures required configurations are present and valid."""
        if not cls.BOT_TOKEN:
            sys.exit("FATAL: BOT_TOKEN is missing!")

        if not cls.MONGO_URI.startswith(("mongodb://", "mongodb+srv://")):
            sys.exit("FATAL: MONGO_URI must start with mongodb:// or mongodb+srv://")

        if not cls.PROJECT_PREFIX:
            sys.exit("FATAL: PROJECT_PREFIX is required to isolate MongoDB collections!")


Config.validate()
