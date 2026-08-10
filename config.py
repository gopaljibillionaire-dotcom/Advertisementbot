import logging
import sys
from pathlib import Path
from typing import List, Union

class Config:
    # Adding .strip() removes any hidden spaces or newlines from copy-pasting
    BOT_TOKEN: str = "8327208590:AAHkt3kPKr2ZMd1c4dBTr-KeVMh7x-pUlBA" .strip()
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
