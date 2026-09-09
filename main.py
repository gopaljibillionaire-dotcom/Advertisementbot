import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
import html
import os
import secrets
import string
from typing import List, Optional, Union

import aiohttp
import aiosqlite
from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    LabeledPrice,
    Message,
    PreCheckoutQuery,
)

from config import Config, logger

# ==========================================
# 1. DATABASE SYSTEM
# ==========================================

class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path

    @asynccontextmanager
    async def get_connection(self):
        async with aiosqlite.connect(self.db_path) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = OFF;")
            yield conn

    async def init_db(self):
        async with self.get_connection() as db:
            await db.executescript("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER UNIQUE NOT NULL,
                    username TEXT,
                    first_name TEXT,
                    stars_balance INTEGER DEFAULT 0,
                    usd_balance REAL DEFAULT 0.0,
                    total_deposits REAL DEFAULT 0.0,
                    total_spent REAL DEFAULT 0.0,
                    is_blocked INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS markets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    channel_id TEXT NOT NULL,
                    channel_username TEXT,
                    subscribers TEXT DEFAULT 'N/A',
                    stars_price INTEGER NOT NULL,
                    usd_price REAL NOT NULL,
                    enabled INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS advertisements (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id TEXT UNIQUE NOT NULL,
                    user_id INTEGER NOT NULL,
                    market_id INTEGER NOT NULL,
                    source_chat_id INTEGER NOT NULL,
                    source_message_id INTEGER NOT NULL,
                    media_group_id TEXT,
                    content_type TEXT NOT NULL,
                    caption TEXT,
                    status TEXT DEFAULT 'PENDING_AD',
                    rejection_reason TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (market_id) REFERENCES markets (id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS advertisement_media (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    advertisement_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    media_type TEXT NOT NULL,
                    file_id TEXT,
                    caption TEXT,
                    media_order INTEGER DEFAULT 0,
                    FOREIGN KEY (advertisement_id) REFERENCES advertisements (id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS payments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    method TEXT NOT NULL,
                    currency TEXT NOT NULL,
                    amount REAL NOT NULL,
                    telegram_payment_id TEXT,
                    oxapay_track_id TEXT,
                    status TEXT DEFAULT 'WAITING_PAYMENT',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    verified_at TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS withdrawals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    request_id TEXT UNIQUE NOT NULL,
                    user_id INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    wallet_address TEXT NOT NULL,
                    status TEXT DEFAULT 'PENDING',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS published_ads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    advertisement_id INTEGER NOT NULL,
                    channel_id TEXT NOT NULL,
                    message_id INTEGER NOT NULL,
                    post_url TEXT,
                    published_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (advertisement_id) REFERENCES advertisements (id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS admins (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER UNIQUE NOT NULL,
                    role TEXT DEFAULT 'ADMIN',
                    permissions TEXT DEFAULT 'ALL',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id);
                CREATE INDEX IF NOT EXISTS idx_ads_order_id ON advertisements(order_id);
                CREATE INDEX IF NOT EXISTS idx_payments_track ON payments(oxapay_track_id);
            """)

            cursor = await db.execute("SELECT COUNT(*) FROM markets;")
            row = await cursor.fetchone()
            if row and row[0] == 0:
                await db.execute("""
                    INSERT INTO markets (name, channel_id, channel_username, subscribers, stars_price, usd_price, enabled)
                    VALUES 
                    ('PostsMarket', '@PostsMarket', 'PostsMarket', '50K+', 29, 2.50, 1),
                    ('PublicVerified', '@PublicVerified', 'PublicVerified', '10K+', 12, 1.00, 1);
                """)

            await db.commit()
            logger.info("Database schema initialized.")

    async def upsert_user(self, telegram_id: int, username: Optional[str], first_name: Optional[str]) -> bool:
        async with self.get_connection() as db:
            cursor = await db.execute("SELECT id FROM users WHERE telegram_id = ?;", (telegram_id,))
            user_exists = await cursor.fetchone()

            await db.execute("""
                INSERT INTO users (telegram_id, username, first_name, last_seen, is_blocked)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, 0)
                ON CONFLICT(telegram_id) DO UPDATE SET
                    username = excluded.username,
                    first_name = excluded.first_name,
                    last_seen = CURRENT_TIMESTAMP,
                    is_blocked = 0;
            """, (telegram_id, username, first_name))
            await db.commit()
            return user_exists is None

    async def get_user(self, telegram_id: int) -> Optional[aiosqlite.Row]:
        async with self.get_connection() as db:
            cursor = await db.execute("SELECT * FROM users WHERE telegram_id = ?;", (telegram_id,))
            return await cursor.fetchone()

    async def update_balances(self, telegram_id: int, stars_delta: int = 0, usd_delta: float = 0.0, deposit_delta: float = 0.0, spent_delta: float = 0.0):
        async with self.get_connection() as db:
            await db.execute("""
                UPDATE users
                SET stars_balance = stars_balance + ?,
                    usd_balance = usd_balance + ?,
                    total_deposits = total_deposits + ?,
                    total_spent = total_spent + ?
                WHERE telegram_id = ?;
            """, (stars_delta, usd_delta, deposit_delta, spent_delta, telegram_id))
            await db.commit()

db = Database(Config.DATABASE_PATH)

# ==========================================
# 2. FSM STATES & UTILITIES
# ==========================================

class UserStates(StatesGroup):
    waiting_for_ad = State()
    waiting_for_deposit_stars = State()
    waiting_for_deposit_usd = State()
    waiting_for_withdraw_amount = State()
    waiting_for_gram_address = State()


class AdminStates(StatesGroup):
    add_market_name = State()
    add_market_channel = State()
    add_market_subs = State()
    add_market_stars = State()
    add_market_usd = State()
    broadcast_message = State()
    add_admin_id = State()
    waiting_for_reject_reason = State()
    waiting_for_db_import = State()


def clean_html(text: Optional[str]) -> str:
    if not text:
        return ""
    return html.escape(str(text))


def generate_req_id(prefix: str = "REQ") -> str:
    chars = string.ascii_uppercase + string.digits
    return f"{prefix}-{''.join(secrets.choice(chars) for _ in range(6))}"


async def check_is_admin(telegram_id: int) -> bool:
    if telegram_id in Config.SUPER_OWNER_IDS:
        return True
    async with db.get_connection() as conn:
        cursor = await conn.execute("SELECT 1 FROM admins WHERE telegram_id = ?;", (telegram_id,))
        return (await cursor.fetchone()) is not None


async def get_all_admin_ids() -> List[int]:
    admin_ids = set(Config.SUPER_OWNER_IDS)
    try:
        async with db.get_connection() as conn:
            cursor = await conn.execute("SELECT telegram_id FROM admins;")
            rows = await cursor.fetchall()
            for r in rows:
                admin_ids.add(r["telegram_id"])
    except Exception as e:
        logger.error(f"Error fetching admin IDs: {e}")
    return list(admin_ids)


async def send_or_edit_photo(
    event: Union[CallbackQuery, Message],
    photo_path: str,
    caption: str,
    reply_markup: InlineKeyboardMarkup,
    bot: Optional[Bot] = None
):
    file_exists = os.path.exists(photo_path)

    if isinstance(event, CallbackQuery):
        if file_exists:
            try:
                media = InputMediaPhoto(media=FSInputFile(photo_path), caption=caption, parse_mode=ParseMode.HTML)
                await event.message.edit_media(media=media, reply_markup=reply_markup)
                return
            except TelegramBadRequest:
                pass
            except Exception as e:
                logger.warning(f"Failed to edit media to {photo_path}: {e}")

        if event.message.photo:
            try:
                await event.message.edit_caption(caption=caption, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
                return
            except TelegramBadRequest:
                pass

        try:
            await event.message.edit_text(text=caption, reply_markup=reply_markup, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        except TelegramBadRequest:
            try:
                await event.message.delete()
            except Exception:
                pass
            await event.message.answer(text=caption, reply_markup=reply_markup, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

    elif isinstance(event, Message):
        if file_exists:
            try:
                await event.answer_photo(photo=FSInputFile(photo_path), caption=caption, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
                return
            except Exception as e:
                logger.warning(f"Failed to send photo {photo_path}: {e}")

        await event.answer(text=caption, reply_markup=reply_markup, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

# ==========================================
# 3. OXAPAY PAYMENT API CLIENT & PROCESSOR
# ==========================================

class OxapayClient:
    BASE_URL = "https://api.oxapay.com/merchants"

    @staticmethod
    async def create_invoice(
        merchant_key: str,
        amount: float,
        order_id: str,
        description: str,
        pay_currency: Optional[str] = None,
        callback_url: Optional[str] = None
    ) -> dict:
        url = f"{OxapayClient.BASE_URL}/request"
        payload = {
            "merchant": merchant_key,
            "amount": float(amount),
            "currency": "USD",
            "orderId": order_id,
            "description": description,
            "lifeTime": 60,
            "feePaidByPayer": 1
        }
        if pay_currency and pay_currency != "ALL":
            payload["payCurrency"] = pay_currency
        if callback_url:
            payload["callbackUrl"] = callback_url

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=15) as resp:
                    return await resp.json()
        except Exception as err:
            logger.error(f"Oxapay API Invoice Error: {err}")
            return {"result": 500, "message": str(err)}

    @staticmethod
    async def check_payment(merchant_key: str, track_id: str) -> dict:
        url = f"{OxapayClient.BASE_URL}/inquiry"
        payload = {
            "merchant": merchant_key,
            "trackId": str(track_id)
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=15) as resp:
                    return await resp.json()
        except Exception as err:
            logger.error(f"Oxapay API Inquiry Error: {err}")
            return {"result": 500, "message": str(err)}


async def process_successful_crypto_payment(pay_row: aiosqlite.Row, bot: Bot):
    payment_id = pay_row["id"]
    order_id = pay_row["order_id"]
    user_id = pay_row["user_id"]
    amount = float(pay_row["amount"])

    async with db.get_connection() as conn:
        cursor = await conn.execute("SELECT status FROM payments WHERE id = ?;", (payment_id,))
        p = await cursor.fetchone()
        if not p or p["status"] == "PAID":
            return

        await conn.execute("UPDATE payments SET status = 'PAID', verified_at = CURRENT_TIMESTAMP WHERE id = ?;", (payment_id,))
        await conn.commit()

    if order_id.startswith("AD-"):
        async with db.get_connection() as conn:
            await conn.execute("UPDATE advertisements SET status = 'PAID' WHERE order_id = ?;", (order_id,))
            await conn.commit()
        await db.update_balances(user_id, spent_delta=amount)

        msg = (
            f"⚡ <b>CRYPTO PAYMENT VERIFIED</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📂 <b>Order ID:</b> <code>{order_id}</code>\n"
            f"💵 <b>Amount Paid:</b> <code>${amount:.2f} USD</code>\n\n"
            f"✅ <i>Your advertisement booking status has been updated to <b>PAID</b>!</i>"
        )
    else:
        await db.update_balances(user_id, usd_delta=amount, deposit_delta=amount)

        msg = (
            f"✨ <b>CRYPTO DEPOSIT CREDITED</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔖 <b>Reference:</b> <code>{order_id}</code>\n"
            f"💳 <b>Amount Credited:</b> <code>${amount:.2f} USD</code>\n\n"
            f"✅ <i>The funds have been added to your balance.</i>"
        )

    try:
        await bot.send_message(user_id, msg, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Failed to notify user {user_id}: {e}")

    admin_ids = await get_all_admin_ids()
    alert_text = (
        f"💰 <b>SUCCESSFUL CRYPTO PAYMENT</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔖 <b>Reference:</b> <code>{order_id}</code>\n"
        f"👤 <b>User ID:</b> <code>{user_id}</code>\n"
        f"💵 <b>Amount:</b> <code>${amount:.2f} USD</code>\n"
        f"🔍 <b>Track ID:</b> <code>{pay_row['oxapay_track_id']}</code>"
    )
    for aid in admin_ids:
        try:
            await bot.send_message(aid, alert_text, parse_mode=ParseMode.HTML)
        except Exception:
            pass


async def oxapay_polling_task(bot: Bot):
    while True:
        try:
            await asyncio.sleep(30)
            async with db.get_connection() as conn:
                cursor = await conn.execute("""
                    SELECT * FROM payments 
                    WHERE method = 'OXAPAY' AND status = 'WAITING_PAYMENT' AND oxapay_track_id IS NOT NULL;
                """)
                pending_payments = await cursor.fetchall()

            for pay in pending_payments:
                track_id = pay["oxapay_track_id"]
                res = await OxapayClient.check_payment(Config.OXAPAY_MERCHANT_KEY, track_id)
                if res.get("result") == 100:
                    status = str(res.get("status", "")).lower()
                    if status in ["paid", "complete"]:
                        await process_successful_crypto_payment(pay, bot)
                    elif status == "expired":
                        async with db.get_connection() as conn:
                            await conn.execute("UPDATE payments SET status = 'EXPIRED' WHERE id = ?;", (pay["id"],))
                            await conn.commit()
        except Exception as e:
            logger.error(f"Error in OxaPay Background Poller: {e}")

# ==========================================
# 4. KEYBOARD BUILDERS (EXACT MATCHING UI)
# ==========================================

class Keyboards:

    @staticmethod
    def main_menu(is_admin: bool = False) -> InlineKeyboardMarkup:
        builder = [
            [
                InlineKeyboardButton(text="Forward ↗", callback_data="btn:forward"),
                InlineKeyboardButton(text="Pin ↗", callback_data="btn:pin")
            ],
            [
                InlineKeyboardButton(text="Profile ↗", callback_data="btn:profile"),
                InlineKeyboardButton(text="Wallet ↗", callback_data="btn:wallet")
            ],
            [InlineKeyboardButton(text="Contact Support ↗", url="https://t.me/CoreCreations")],
            [InlineKeyboardButton(text="Change Language ↗", callback_data="btn:change_lang")],
            [InlineKeyboardButton(text="Host Giveaway - Pre-Paid ↗", callback_data="btn:host_giveaway")]
        ]
        if is_admin:
            builder.append([InlineKeyboardButton(text="Admin Dashboard ↗", callback_data="admin:main")])
        return InlineKeyboardMarkup(inline_keyboard=builder)

    @staticmethod
    def main_menu_only() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Main menu ↗", callback_data="user:main_menu")]
        ])

    @staticmethod
    def forward_menu() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Continue ↗", callback_data="btn:forward_continue")],
            [
                InlineKeyboardButton(text="Back ↗", callback_data="user:main_menu"),
                InlineKeyboardButton(text="Recharge wallet ↗", callback_data="btn:recharge_wallet")
            ],
            [InlineKeyboardButton(text="Main menu ↗", callback_data="user:main_menu")]
        ])

    @staticmethod
    def wallet_menu() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="Deposit ↗", callback_data="btn:deposit"),
                InlineKeyboardButton(text="Withdraw ↗", callback_data="btn:withdraw")
            ],
            [InlineKeyboardButton(text="Back to Main menu ↗", callback_data="user:main_menu")],
            [InlineKeyboardButton(text="Contact Support ↗", url="https://t.me/CoreCreations")]
        ])

    @staticmethod
    def payment_options_menu(order_id: Optional[str] = None) -> InlineKeyboardMarkup:
        suffix = f":{order_id}" if order_id else ""
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="Stars ↗", callback_data=f"pay_opt:stars{suffix}"),
                InlineKeyboardButton(text="Crypto ↗", callback_data=f"pay_opt:crypto{suffix}")
            ]
        ])

    @staticmethod
    def crypto_coin_menu(target_id: str, is_deposit: bool = False, amount: float = 0.0) -> InlineKeyboardMarkup:
        prefix = f"coin_dep:{amount:.2f}:" if is_deposit else f"coin_ad:{target_id}:"
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="USDT ₮ ↗", callback_data=f"{prefix}USDT"),
                InlineKeyboardButton(text="BTC ₿ ↗", callback_data=f"{prefix}BTC"),
                InlineKeyboardButton(text="ETH Ξ ↗", callback_data=f"{prefix}ETH")
            ],
            [
                InlineKeyboardButton(text="LTC Ł ↗", callback_data=f"{prefix}LTC"),
                InlineKeyboardButton(text="TON 💎 ↗", callback_data=f"{prefix}TON"),
                InlineKeyboardButton(text="TRX ⚡ ↗", callback_data=f"{prefix}TRX")
            ],
            [InlineKeyboardButton(text="All Cryptos (OxaPay Page) ↗", callback_data=f"{prefix}ALL")],
            [InlineKeyboardButton(text="Main menu ↗", callback_data="user:main_menu")]
        ])

    @staticmethod
    def crypto_invoice_menu(pay_url: str, track_id: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Pay via OxaPay ↗", url=pay_url)],
            [InlineKeyboardButton(text="Check Payment Status ↗", callback_data=f"verify_crypto:{track_id}")],
            [InlineKeyboardButton(text="Main menu ↗", callback_data="user:main_menu")]
        ])

    @staticmethod
    def market_selection_menu(markets: List[aiosqlite.Row], selected_ids: List[int]) -> InlineKeyboardMarkup:
        keyboard = []
        for m in markets:
            checked = "[x] " if m["id"] in selected_ids else ""
            keyboard.append([
                InlineKeyboardButton(
                    text=f"{checked}{m['name']} ({m['subscribers']}) - Stars: {m['stars_price']} / ${m['usd_price']} ↗",
                    callback_data=f"mkt_toggle:{m['id']}"
                )
            ])
        keyboard.append([InlineKeyboardButton(text="Continue ↗", callback_data="mkt_confirm_selection")])
        keyboard.append([
            InlineKeyboardButton(text="Back ↗", callback_data="btn:forward"),
            InlineKeyboardButton(text="Recharge wallet ↗", callback_data="btn:recharge_wallet")
        ])
        keyboard.append([InlineKeyboardButton(text="Main menu ↗", callback_data="user:main_menu")])
        return InlineKeyboardMarkup(inline_keyboard=keyboard)

    @staticmethod
    def withdraw_prompt_menu() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Back ↗", callback_data="btn:wallet")],
            [InlineKeyboardButton(text="Contact Support ↗", url="https://t.me/CoreCreations")]
        ])

    @staticmethod
    def withdraw_recheck_menu() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="Confirm ↗", callback_data="withdraw:confirm"),
                InlineKeyboardButton(text="Edit ↗", callback_data="withdraw:edit")
            ],
            [InlineKeyboardButton(text="Contact Support ↗", url="https://t.me/CoreCreations")]
        ])

    @staticmethod
    def withdraw_created_menu() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="Okay ↗", callback_data="withdraw:okay"),
                InlineKeyboardButton(text="Donate ↗", callback_data="btn:donate")
            ],
            [InlineKeyboardButton(text="Contact Support ↗", url="https://t.me/CoreCreations")]
        ])

    @staticmethod
    def donate_menu() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="Stars ↗", callback_data="donate:stars"),
                InlineKeyboardButton(text="Crypto ↗", callback_data="donate:crypto")
            ],
            [InlineKeyboardButton(text="No thanks ↗", callback_data="donate:nothanks")]
        ])

    @staticmethod
    def admin_main_menu() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Manage Channels ↗", callback_data="admin:markets")],
            [
                InlineKeyboardButton(text="All Bookings ↗", callback_data="admin:ads:all"),
                InlineKeyboardButton(text="Users List ↗", callback_data="admin:users")
            ],
            [InlineKeyboardButton(text="Mass Broadcast ↗", callback_data="admin:broadcast")],
            [
                InlineKeyboardButton(text="Export DB ↗", callback_data="admin:backup"),
                InlineKeyboardButton(text="Import DB ↗", callback_data="admin:import_db")
            ],
            [InlineKeyboardButton(text="Admin Privileges ↗", callback_data="admin:manage_admins")],
            [InlineKeyboardButton(text="Exit Admin View ↗", callback_data="user:main_menu")]
        ])

    @staticmethod
    def admin_markets_menu(markets: List[aiosqlite.Row]) -> InlineKeyboardMarkup:
        keyboard = []
        for m in markets:
            status = "ON" if m["enabled"] else "OFF"
            keyboard.append([
                InlineKeyboardButton(text=f"[{status}] {m['name']} ({m['subscribers']}) ↗", callback_data=f"admin:mkt_view:{m['id']}"),
                InlineKeyboardButton(text="Toggle ↗", callback_data=f"admin:mkt_toggle:{m['id']}"),
                InlineKeyboardButton(text="Delete ↗", callback_data=f"admin:mkt_del:{m['id']}")
            ])
        keyboard.append([InlineKeyboardButton(text="Add New Channel ↗", callback_data="admin:mkt_add")])
        keyboard.append([InlineKeyboardButton(text="Back to Dashboard ↗", callback_data="admin:main")])
        return InlineKeyboardMarkup(inline_keyboard=keyboard)

# ==========================================
# 5. USER ROUTER & INTERFACE HANDLERS
# ==========================================

user_router = Router()

MAIN_TEXT = (
    "Welcome to @Paytoforwardbot\n\n"
    "Want to forward or purchase Pins in markets provided by Core Creations ?\n"
    "<blockquote>You are on the correct bot 📑 ❞</blockquote>\n\n"
    "<blockquote>Load the wallet, Send your advertisement to the bot and get it forwarded through supported markets — quickly and conveniently. ❞</blockquote>\n\n"
    "Payment : Telegram Stars / Crypto\n\n"
    "Powered by @CoreCreations"
)

@user_router.message(CommandStart())
async def cmd_start_handler(message: Message, state: FSMContext, bot: Bot):
    await state.clear()
    user = message.from_user
    await db.upsert_user(user.id, user.username, user.first_name)
    is_admin = await check_is_admin(user.id)

    await send_or_edit_photo(
        event=message,
        photo_path="1000284193.jpg",
        caption=MAIN_TEXT,
        reply_markup=Keyboards.main_menu(is_admin),
        bot=bot
    )


@user_router.callback_query(F.data == "user:main_menu")
async def cb_user_main_menu(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await state.clear()
    is_admin = await check_is_admin(callback.from_user.id)
    await send_or_edit_photo(
        event=callback,
        photo_path="1000284193.jpg",
        caption=MAIN_TEXT,
        reply_markup=Keyboards.main_menu(is_admin),
        bot=bot
    )
    await callback.answer()


@user_router.callback_query(F.data == "btn:profile")
async def cb_profile_handler(callback: CallbackQuery, bot: Bot):
    u = await db.get_user(callback.from_user.id)
    raw_username = callback.from_user.username
    display_user = f"@{raw_username}" if raw_username else clean_html(callback.from_user.first_name)

    deposits = f"${u['total_deposits']:.2f}" if u else "$0.00"
    spent = f"${u['total_spent']:.2f}" if u else "$0.00"

    caption = (
        f"User Profile\n\n"
        f"User: {display_user}\n"
        f"Total Deposits: {deposits}\n"
        f"Total Spent: {spent}"
    )

    await send_or_edit_photo(
        event=callback,
        photo_path="1000284193.jpg",
        caption=caption,
        reply_markup=Keyboards.main_menu_only(),
        bot=bot
    )
    await callback.answer()


@user_router.callback_query(F.data.in_({"btn:forward", "btn:pin"}))
async def cb_forward_handler(callback: CallbackQuery, bot: Bot):
    caption = (
        "Forward your advertisement -\n\n"
        "After payment I will forward it in\n"
        "@PostsMarket"
    )
    await send_or_edit_photo(
        event=callback,
        photo_path="1000284199.jpg",
        caption=caption,
        reply_markup=Keyboards.forward_menu(),
        bot=bot
    )
    await callback.answer()


@user_router.callback_query(F.data.in_({"btn:forward_continue", "btn:forward_ad"}))
async def cb_forward_continue(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await state.set_state(UserStates.waiting_for_ad)
    await state.update_data(selected_markets=[])

    caption = (
        "Forward your advertisement -\n\n"
        "After payment I will forward it in\n"
        "@PostsMarket"
    )
    await send_or_edit_photo(
        event=callback,
        photo_path="1000284199.jpg",
        caption=caption,
        reply_markup=Keyboards.forward_menu(),
        bot=bot
    )
    await callback.answer()


@user_router.message(~F.text.startswith("/"))
async def process_ad_content(message: Message, state: FSMContext, bot: Bot):
    current_state = await state.get_state()
    
    if current_state not in [UserStates.waiting_for_ad.state, None]:
        return

    user_id = message.from_user.id
    content_type = message.content_type
    caption_text = message.caption or message.text or ""

    file_id = None
    if message.photo:
        file_id = message.photo[-1].file_id
    elif message.video:
        file_id = message.video.file_id
    elif message.document:
        file_id = message.document.file_id

    order_id = generate_req_id("AD")

    await state.set_state(UserStates.waiting_for_ad)
    await state.update_data(
        ad_order_id=order_id,
        source_chat_id=message.chat.id,
        source_message_id=message.message_id,
        content_type=content_type,
        caption_text=caption_text,
        file_id=file_id,
        selected_markets=[]
    )

    async with db.get_connection() as conn:
        cursor = await conn.execute("SELECT * FROM markets WHERE enabled = 1;")
        markets = await cursor.fetchall()

    if not markets:
        await message.answer("No channels are available for booking currently.", parse_mode=ParseMode.HTML)
        return

    postmarket_caption = (
        "Wallet balance :\n\n"
        "Cross verify the market via link given\n"
        "https://t.me/PostsMarket"
    )

    await send_or_edit_photo(
        event=message,
        photo_path="1000284197.jpg",
        caption=postmarket_caption,
        reply_markup=Keyboards.forward_menu(),
        bot=bot
    )


@user_router.callback_query(F.data.startswith("mkt_toggle:"))
async def cb_market_toggle(callback: CallbackQuery, state: FSMContext, bot: Bot):
    m_id = int(callback.data.split(":")[1])
    data = await state.get_data()
    selected = data.get("selected_markets", [])

    if m_id in selected:
        selected.remove(m_id)
    else:
        selected.append(m_id)

    await state.update_data(selected_markets=selected)

    async with db.get_connection() as conn:
        cursor = await conn.execute("SELECT * FROM markets WHERE enabled = 1;")
        markets = await cursor.fetchall()

    postmarket_caption = (
        "Wallet balance :\n\n"
        "Cross verify the market via link given\n"
        "https://t.me/PostsMarket"
    )

    await send_or_edit_photo(
        event=callback,
        photo_path="1000284197.jpg",
        caption=postmarket_caption,
        reply_markup=Keyboards.market_selection_menu(markets, selected),
        bot=bot
    )
    await callback.answer()


@user_router.callback_query(F.data == "mkt_confirm_selection")
async def cb_market_confirm(callback: CallbackQuery, state: FSMContext, bot: Bot):
    data = await state.get_data()
    selected = data.get("selected_markets", [])

    if not selected:
        await callback.answer("Please select at least one channel to proceed!", show_alert=True)
        return

    order_id = data.get("ad_order_id")
    user_id = callback.from_user.id

    async with db.get_connection() as conn:
        for mkt_id in selected:
            await conn.execute("""
                INSERT INTO advertisements (order_id, user_id, market_id, source_chat_id, source_message_id, content_type, caption, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING_PAYMENT');
            """, (order_id, user_id, mkt_id, data.get("source_chat_id"), data.get("source_message_id"), data.get("content_type"), data.get("caption_text")))
        await conn.commit()

    caption = (
        "Donate some money to founder & developer\n\n"
        "Distributed equally (50-50)"
    )
    await send_or_edit_photo(
        event=callback,
        photo_path="1000284217.jpg",
        caption=caption,
        reply_markup=Keyboards.donate_menu(),
        bot=bot
    )
    await callback.answer()


@user_router.callback_query(F.data == "btn:wallet")
@user_router.callback_query(F.data == "btn:recharge_wallet")
async def cb_wallet_handler(callback: CallbackQuery, bot: Bot):
    caption = (
        "Your current balance in -\n"
        "Telegram stars :\n"
        "Crypto - Example Russian ruble :"
    )

    await send_or_edit_photo(
        event=callback,
        photo_path="1000284207.jpg",
        caption=caption,
        reply_markup=Keyboards.wallet_menu(),
        bot=bot
    )
    await callback.answer()


@user_router.callback_query(F.data == "btn:deposit")
async def cb_deposit_handler(callback: CallbackQuery, bot: Bot):
    caption = (
        "Your current balance in -\n"
        "Telegram stars :\n"
        "Crypto - Example Russian ruble :"
    )
    await send_or_edit_photo(
        event=callback,
        photo_path="1000284207.jpg",
        caption=caption,
        reply_markup=Keyboards.payment_options_menu(),
        bot=bot
    )
    await callback.answer()


@user_router.callback_query(F.data.startswith("pay_opt:stars"))
async def cb_pay_opt_stars(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    order_id = parts[2] if len(parts) > 2 else None

    if order_id:
        data = await state.get_data()
        selected = data.get("selected_markets", [])
        async with db.get_connection() as conn:
            cursor = await conn.execute("SELECT * FROM markets WHERE id IN ({})".format(','.join('?' for _ in selected)), selected)
            mkts = await cursor.fetchall()

        tot_stars = sum(m["stars_price"] for m in mkts)
        u = await db.get_user(callback.from_user.id)

        if u and u["stars_balance"] >= tot_stars:
            await db.update_balances(callback.from_user.id, stars_delta=-tot_stars, spent_delta=float(tot_stars))
            async with db.get_connection() as conn:
                await conn.execute("UPDATE advertisements SET status = 'PAID' WHERE order_id = ?;", (order_id,))
                await conn.commit()
            await callback.message.answer(f"Successfully deducted {tot_stars} Stars from your balance!", parse_mode=ParseMode.HTML)
            await callback.answer("Paid from wallet balance!")
            return

        prices = [LabeledPrice(label="Ad Placement", amount=tot_stars)]
        await callback.message.bot.send_invoice(
            chat_id=callback.from_user.id,
            title="Ad Placement Payment",
            description=f"Payment for {len(selected)} channel posts",
            payload=f"stars_ad_{order_id}",
            provider_token="",
            currency="XTR",
            prices=prices,
            start_parameter=f"ad-{order_id}"
        )
        await callback.answer("Stars Invoice Generated!")

    else:
        await state.set_state(UserStates.waiting_for_deposit_stars)
        await callback.message.answer("Enter the amount of Stars you want to deposit (e.g. 100):", parse_mode=ParseMode.HTML)
        await callback.answer()


@user_router.message(UserStates.waiting_for_deposit_stars)
async def process_stars_deposit_amount(message: Message, state: FSMContext):
    try:
        amount = int(message.text.strip())
        if amount <= 0:
            raise ValueError()
    except ValueError:
        await message.answer("Invalid number. Please enter a valid positive integer.")
        return

    await state.clear()
    prices = [LabeledPrice(label=f"Deposit {amount} Stars", amount=amount)]

    await message.bot.send_invoice(
        chat_id=message.from_user.id,
        title="Wallet Deposit",
        description=f"Add {amount} Stars to your bot balance",
        payload=f"stars_deposit_{amount}",
        provider_token="",
        currency="XTR",
        prices=prices,
        start_parameter="deposit-stars"
    )

# --- OXAPAY CRYPTO PAYMENT WORKFLOW ---

@user_router.callback_query(F.data.startswith("pay_opt:crypto"))
async def cb_pay_opt_crypto(callback: CallbackQuery, state: FSMContext, bot: Bot):
    parts = callback.data.split(":")
    order_id = parts[2] if len(parts) > 2 else None

    if order_id:
        data = await state.get_data()
        selected = data.get("selected_markets", [])
        async with db.get_connection() as conn:
            cursor = await conn.execute("SELECT * FROM markets WHERE id IN ({})".format(','.join('?' for _ in selected)), selected)
            mkts = await cursor.fetchall()

        tot_usd = sum(m["usd_price"] for m in mkts)

        caption = (
            f"Crypto Payment\n\n"
            f"Order ID: {order_id}\n"
            f"Total Due: ${tot_usd:.2f} USD\n\n"
            f"Select your preferred Cryptocurrency for payment:"
        )
        await send_or_edit_photo(
            event=callback,
            photo_path="1000284207.jpg",
            caption=caption,
            reply_markup=Keyboards.crypto_coin_menu(target_id=order_id, is_deposit=False, amount=tot_usd),
            bot=bot
        )
    else:
        await state.set_state(UserStates.waiting_for_deposit_usd)
        await callback.message.answer("Enter the USD amount you want to deposit (e.g. 10.00):", parse_mode=ParseMode.HTML)

    await callback.answer()


@user_router.message(UserStates.waiting_for_deposit_usd)
async def process_usd_deposit_amount(message: Message, state: FSMContext, bot: Bot):
    try:
        amount = float(message.text.strip())
        if amount <= 0:
            raise ValueError()
    except ValueError:
        await message.answer("Invalid amount. Please enter a valid positive number.")
        return

    await state.clear()
    caption = f"Deposit Value: ${amount:.2f} USD\n\nSelect your preferred Cryptocurrency:"
    await send_or_edit_photo(
        event=message,
        photo_path="1000284207.jpg",
        caption=caption,
        reply_markup=Keyboards.crypto_coin_menu(target_id="", is_deposit=True, amount=amount),
        bot=bot
    )


@user_router.callback_query(F.data.startswith("coin_ad:"))
async def cb_coin_ad(callback: CallbackQuery, state: FSMContext, bot: Bot):
    parts = callback.data.split(":")
    order_id = parts[1]
    coin = parts[2]

    data = await state.get_data()
    selected = data.get("selected_markets", [])
    async with db.get_connection() as conn:
        cursor = await conn.execute("SELECT * FROM markets WHERE id IN ({})".format(','.join('?' for _ in selected)), selected)
        mkts = await cursor.fetchall()

    tot_usd = sum(m["usd_price"] for m in mkts)

    res = await OxapayClient.create_invoice(
        merchant_key=Config.OXAPAY_MERCHANT_KEY,
        amount=tot_usd,
        order_id=order_id,
        description=f"Ad Slot Placement ({order_id})",
        pay_currency=coin
    )

    if res.get("result") == 100 and "trackId" in res:
        track_id = str(res["trackId"])
        pay_url = res.get("payLink", "")
        address = res.get("address")
        pay_amount = res.get("payAmount")
        pay_currency = res.get("payCurrency", coin)

        async with db.get_connection() as conn:
            await conn.execute("""
                INSERT INTO payments (order_id, user_id, method, currency, amount, oxapay_track_id, status)
                VALUES (?, ?, 'OXAPAY', ?, ?, ?, 'WAITING_PAYMENT');
            """, (order_id, callback.from_user.id, pay_currency, tot_usd, track_id))
            await conn.commit()

        if address and pay_amount:
            caption = (
                f"Crypto Payment Invoice\n\n"
                f"Order ID: {order_id}\n"
                f"Total Due: ${tot_usd:.2f} USD\n"
                f"Currency: {pay_currency}\n\n"
                f"Amount to Send:\n{pay_amount} {pay_currency}\n\n"
                f"Deposit Address:\n{address}"
            )
        else:
            caption = (
                f"Crypto Payment Invoice\n\n"
                f"Order ID: {order_id}\n"
                f"Total Due: ${tot_usd:.2f} USD\n"
                f"Track ID: {track_id}"
            )

        await send_or_edit_photo(
            event=callback,
            photo_path="1000284207.jpg",
            caption=caption,
            reply_markup=Keyboards.crypto_invoice_menu(pay_url, track_id),
            bot=bot
        )
    else:
        await callback.message.answer(f"Oxapay API Error: {res.get('message', 'Failed to generate invoice.')}")

    await callback.answer()


@user_router.callback_query(F.data.startswith("coin_dep:"))
async def cb_coin_dep(callback: CallbackQuery, bot: Bot):
    parts = callback.data.split(":")
    tot_usd = float(parts[1])
    coin = parts[2]
    dep_id = generate_req_id("DEP")

    res = await OxapayClient.create_invoice(
        merchant_key=Config.OXAPAY_MERCHANT_KEY,
        amount=tot_usd,
        order_id=dep_id,
        description=f"Wallet Deposit ({dep_id})",
        pay_currency=coin
    )

    if res.get("result") == 100 and "trackId" in res:
        track_id = str(res["trackId"])
        pay_url = res.get("payLink", "")
        address = res.get("address")
        pay_amount = res.get("payAmount")
        pay_currency = res.get("payCurrency", coin)

        async with db.get_connection() as conn:
            await conn.execute("""
                INSERT INTO payments (order_id, user_id, method, currency, amount, oxapay_track_id, status)
                VALUES (?, ?, 'OXAPAY', ?, ?, ?, 'WAITING_PAYMENT');
            """, (dep_id, callback.from_user.id, pay_currency, tot_usd, track_id))
            await conn.commit()

        if address and pay_amount:
            caption = (
                f"Crypto Deposit Invoice\n\n"
                f"Ref ID: {dep_id}\n"
                f"Deposit Value: ${tot_usd:.2f} USD\n"
                f"Currency: {pay_currency}\n\n"
                f"Amount to Send:\n{pay_amount} {pay_currency}\n\n"
                f"Deposit Address:\n{address}"
            )
        else:
            caption = (
                f"Crypto Deposit Invoice\n\n"
                f"Ref ID: {dep_id}\n"
                f"Deposit Value: ${tot_usd:.2f} USD\n"
                f"Track ID: {track_id}"
            )

        await send_or_edit_photo(
            event=callback,
            photo_path="1000284207.jpg",
            caption=caption,
            reply_markup=Keyboards.crypto_invoice_menu(pay_url, track_id),
            bot=bot
        )
    else:
        await callback.message.answer(f"Oxapay API Error: {res.get('message', 'Failed to generate invoice.')}")

    await callback.answer()


@user_router.callback_query(F.data.startswith("verify_crypto:"))
async def cb_verify_crypto(callback: CallbackQuery, bot: Bot):
    track_id = callback.data.split(":")[1]
    async with db.get_connection() as conn:
        cursor = await conn.execute("SELECT * FROM payments WHERE oxapay_track_id = ?;", (track_id,))
        pay = await cursor.fetchone()

    if not pay:
        await callback.answer("Payment record not found.", show_alert=True)
        return

    if pay["status"] == "PAID":
        await callback.answer("Payment has already been verified!", show_alert=True)
        return

    res = await OxapayClient.check_payment(Config.OXAPAY_MERCHANT_KEY, track_id)
    if res.get("result") == 100:
        status = str(res.get("status", "")).lower()
        if status in ["paid", "complete"]:
            await process_successful_crypto_payment(pay, bot)
            await callback.answer("Payment verified successfully!", show_alert=True)
        elif status in ["waiting", "paying"]:
            await callback.answer("Payment not detected yet.", show_alert=True)
        elif status == "expired":
            async with db.get_connection() as conn:
                await conn.execute("UPDATE payments SET status = 'EXPIRED' WHERE id = ?;", (pay["id"],))
                await conn.commit()
            await callback.answer("Payment invoice expired.", show_alert=True)
        else:
            await callback.answer(f"Status: {status}", show_alert=True)
    else:
        await callback.answer("Unable to fetch status from OxaPay.", show_alert=True)

# --- WITHDRAWAL WORKFLOWS ---

@user_router.callback_query(F.data == "btn:withdraw")
async def cb_withdraw_start(callback: CallbackQuery, state: FSMContext, bot: Bot):
    caption = (
        "Wallet Balance :\n\n"
        "All withdrawals are in TON when your request is created your balance will be deducted pending approval.\n\n"
        "Enter the amount you would like to withdraw"
    )

    await state.set_state(UserStates.waiting_for_withdraw_amount)
    await send_or_edit_photo(
        event=callback,
        photo_path="1000284209.jpg",
        caption=caption,
        reply_markup=Keyboards.withdraw_prompt_menu(),
        bot=bot
    )
    await callback.answer()


@user_router.message(UserStates.waiting_for_withdraw_amount)
async def process_withdraw_amount(message: Message, state: FSMContext, bot: Bot):
    try:
        requested = float(message.text.strip())
        if requested <= 0:
            raise ValueError()
    except ValueError:
        await message.answer("Invalid number. Please enter a valid numerical amount.")
        return

    u = await db.get_user(message.from_user.id)
    usd_bal = u["usd_balance"] if u else 0.0

    if requested > usd_bal:
        await message.answer(f"Max withdrawable balance is ${usd_bal:.2f} TON", parse_mode=ParseMode.HTML)
        return

    await state.update_data(withdraw_amount=requested)
    await state.set_state(UserStates.waiting_for_gram_address)

    caption = "Please send your gram address"
    await send_or_edit_photo(
        event=message,
        photo_path="1000284211.jpg",
        caption=caption,
        reply_markup=Keyboards.withdraw_prompt_menu(),
        bot=bot
    )


@user_router.message(UserStates.waiting_for_gram_address)
async def process_gram_address(message: Message, state: FSMContext, bot: Bot):
    address = message.text.strip()
    await state.update_data(gram_address=address)

    caption = "Please re-check the address and click on confirm else click on edit button."
    await send_or_edit_photo(
        event=message,
        photo_path="1000284213.jpg",
        caption=caption,
        reply_markup=Keyboards.withdraw_recheck_menu(),
        bot=bot
    )


@user_router.callback_query(F.data == "withdraw:edit")
async def cb_withdraw_edit(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await state.set_state(UserStates.waiting_for_gram_address)
    caption = "Please send your gram address"
    await send_or_edit_photo(
        event=callback,
        photo_path="1000284211.jpg",
        caption=caption,
        reply_markup=Keyboards.withdraw_prompt_menu(),
        bot=bot
    )
    await callback.answer()


@user_router.callback_query(F.data == "withdraw:confirm")
async def cb_withdraw_confirm(callback: CallbackQuery, state: FSMContext, bot: Bot):
    data = await state.get_data()
    amount = data.get("withdraw_amount", 0.0)
    address = data.get("gram_address", "N/A")
    user_id = callback.from_user.id

    u = await db.get_user(user_id)
    if not u or u["usd_balance"] < amount:
        await callback.answer("Insufficient balance for withdrawal.", show_alert=True)
        return

    req_id = generate_req_id("WTH")

    await db.update_balances(user_id, usd_delta=-amount)

    async with db.get_connection() as conn:
        await conn.execute("""
            INSERT INTO withdrawals (request_id, user_id, amount, wallet_address, status)
            VALUES (?, ?, ?, ?, 'PENDING');
        """, (req_id, user_id, amount, address))
        await conn.commit()

    await state.update_data(last_req_id=req_id, last_req_amount=amount, last_req_address=address)

    caption = (
        "A withdrawal request has been successfully created\n\n"
        f"Request number : {req_id}\n"
        "<blockquote>Don't share this to anybody except a member from support team. ❞</blockquote>\n\n"
        "Thanks for trusting our bot\n"
        "Powered by @CoreCreations"
    )

    await send_or_edit_photo(
        event=callback,
        photo_path="1000284215.jpg",
        caption=caption,
        reply_markup=Keyboards.withdraw_created_menu(),
        bot=bot
    )
    await callback.answer()


@user_router.callback_query(F.data == "withdraw:okay")
async def cb_withdraw_okay(callback: CallbackQuery, state: FSMContext, bot: Bot):
    data = await state.get_data()
    req_id = data.get("last_req_id", "N/A")
    amount = data.get("last_req_amount", 0.0)
    address = data.get("last_req_address", "N/A")

    admin_ids = await get_all_admin_ids()
    alert_text = (
        f"NEW WITHDRAWAL REQUEST\n"
        f"Request ID: {req_id}\n"
        f"User: @{callback.from_user.username or 'None'} ({callback.from_user.id})\n"
        f"Amount: {amount:.2f} TON\n"
        f"Address: {address}"
    )

    for aid in admin_ids:
        try:
            await bot.send_message(aid, alert_text, parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.error(f"Failed notifying admin {aid}: {e}")

    await callback.answer("Request details sent to support team!")
    await cb_user_main_menu(callback, state, bot)


@user_router.callback_query(F.data == "btn:donate")
async def cb_donate_handler(callback: CallbackQuery, bot: Bot):
    caption = (
        "Donate some money to founder & developer\n\n"
        "Distributed equally (50-50)"
    )
    await send_or_edit_photo(
        event=callback,
        photo_path="1000284217.jpg",
        caption=caption,
        reply_markup=Keyboards.donate_menu(),
        bot=bot
    )
    await callback.answer()


@user_router.callback_query(F.data == "donate:nothanks")
async def cb_donate_nothanks(callback: CallbackQuery, bot: Bot):
    caption = "Thank you for using our bot!"
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Main menu ↗", callback_data="user:main_menu")]
    ])
    await send_or_edit_photo(
        event=callback,
        photo_path="1000284193.jpg",
        caption=caption,
        reply_markup=markup,
        bot=bot
    )
    await callback.answer()


@user_router.callback_query(F.data.in_({"btn:change_lang", "btn:host_giveaway", "donate:stars", "donate:crypto"}))
async def cb_feature_placeholder(callback: CallbackQuery):
    await callback.answer("This feature is operating automatically or currently in maintenance mode.", show_alert=True)

# ==========================================
# 6. INVOICE AND PAYMENT SUCCESS HANDLERS
# ==========================================

payment_router = Router()

@payment_router.pre_checkout_query()
async def process_pre_checkout_query(pre_checkout: PreCheckoutQuery):
    await pre_checkout.answer(ok=True)

@payment_router.message(F.successful_payment)
async def process_successful_payment_handler(message: Message):
    payment_info = message.successful_payment
    payload = payment_info.invoice_payload

    if payload.startswith("stars_deposit_"):
        amount = int(payload.replace("stars_deposit_", ""))
        await db.update_balances(message.from_user.id, stars_delta=amount, deposit_delta=float(amount))
        await message.answer(f"Successfully deposited {amount} Stars to your wallet!", parse_mode=ParseMode.HTML)

    elif payload.startswith("stars_ad_"):
        order_id = payload.replace("stars_ad_", "")
        await db.update_balances(message.from_user.id, spent_delta=float(payment_info.total_amount))
        async with db.get_connection() as conn:
            await conn.execute("UPDATE advertisements SET status = 'PAID' WHERE order_id = ?;", (order_id,))
            await conn.commit()
        await message.answer(f"Payment of {payment_info.total_amount} Stars verified for Order {order_id}!", parse_mode=ParseMode.HTML)

# ==========================================
# 7. ADMIN ROUTER & HANDLERS
# ==========================================

admin_router = Router()

@admin_router.callback_query(F.data == "admin:main")
async def cb_admin_main(callback: CallbackQuery, bot: Bot):
    if not await check_is_admin(callback.from_user.id):
        await callback.answer("Unauthorized.", show_alert=True)
        return

    admin_text = "Admin Dashboard"
    await send_or_edit_photo(
        event=callback,
        photo_path="1000284193.jpg",
        caption=admin_text,
        reply_markup=Keyboards.admin_main_menu(),
        bot=bot
    )
    await callback.answer()


@admin_router.callback_query(F.data == "admin:markets")
async def cb_admin_markets_list(callback: CallbackQuery, bot: Bot):
    if not await check_is_admin(callback.from_user.id):
        return

    async with db.get_connection() as conn:
        cursor = await conn.execute("SELECT * FROM markets ORDER BY id ASC;")
        markets = await cursor.fetchall()

    text = "Channel Network Management"
    await send_or_edit_photo(
        event=callback,
        photo_path="1000284193.jpg",
        caption=text,
        reply_markup=Keyboards.admin_markets_menu(markets),
        bot=bot
    )
    await callback.answer()


@admin_router.callback_query(F.data.startswith("admin:mkt_toggle:"))
async def cb_admin_market_toggle(callback: CallbackQuery, bot: Bot):
    if not await check_is_admin(callback.from_user.id):
        return

    mkt_id = int(callback.data.split(":")[2])
    async with db.get_connection() as conn:
        await conn.execute("UPDATE markets SET enabled = CASE WHEN enabled = 1 THEN 0 ELSE 1 END WHERE id = ?;", (mkt_id,))
        await conn.commit()

    await cb_admin_markets_list(callback, bot)


@admin_router.callback_query(F.data.startswith("admin:mkt_del:"))
async def cb_admin_market_delete(callback: CallbackQuery, bot: Bot):
    if not await check_is_admin(callback.from_user.id):
        return

    mkt_id = int(callback.data.split(":")[2])
    async with db.get_connection() as conn:
        await conn.execute("DELETE FROM markets WHERE id = ?;", (mkt_id,))
        await conn.commit()

    await callback.answer("Channel deleted successfully!")
    await cb_admin_markets_list(callback, bot)


@admin_router.callback_query(F.data == "admin:mkt_add")
async def cb_admin_market_add_start(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if not await check_is_admin(callback.from_user.id):
        return

    await state.set_state(AdminStates.add_market_name)
    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Back ↗", callback_data="admin:markets")]])
    await send_or_edit_photo(
        event=callback,
        photo_path="1000284193.jpg",
        caption="Enter Channel Display Name (e.g. PostsMarket):",
        reply_markup=markup,
        bot=bot
    )
    await callback.answer()


@admin_router.message(AdminStates.add_market_name)
async def process_add_mkt_name(message: Message, state: FSMContext):
    await state.update_data(mkt_name=message.text.strip())
    await state.set_state(AdminStates.add_market_channel)
    await message.answer("Enter Channel ID / Username (e.g. @PostsMarket):", parse_mode=ParseMode.HTML)


@admin_router.message(AdminStates.add_market_channel)
async def process_add_mkt_channel(message: Message, state: FSMContext):
    await state.update_data(mkt_channel=message.text.strip())
    await state.set_state(AdminStates.add_market_subs)
    await message.answer("Enter Subscriber Count Label (e.g. 50K+):", parse_mode=ParseMode.HTML)


@admin_router.message(AdminStates.add_market_subs)
async def process_add_mkt_subs(message: Message, state: FSMContext):
    await state.update_data(mkt_subs=message.text.strip())
    await state.set_state(AdminStates.add_market_stars)
    await message.answer("Enter Price in Telegram Stars (e.g. 29):", parse_mode=ParseMode.HTML)


@admin_router.message(AdminStates.add_market_stars)
async def process_add_mkt_stars(message: Message, state: FSMContext):
    try:
        stars_price = int(message.text.strip())
    except ValueError:
        await message.answer("Please enter a valid integer for Stars price.")
        return

    await state.update_data(mkt_stars=stars_price)
    await state.set_state(AdminStates.add_market_usd)
    await message.answer("Enter Price in USD (e.g. 2.50):", parse_mode=ParseMode.HTML)


@admin_router.message(AdminStates.add_market_usd)
async def process_add_mkt_usd(message: Message, state: FSMContext, bot: Bot):
    try:
        usd_price = float(message.text.strip())
    except ValueError:
        await message.answer("Please enter a valid float number for USD price.")
        return

    data = await state.get_data()
    await state.clear()

    async with db.get_connection() as conn:
        await conn.execute("""
            INSERT INTO markets (name, channel_id, channel_username, subscribers, stars_price, usd_price, enabled)
            VALUES (?, ?, ?, ?, ?, ?, 1);
        """, (data["mkt_name"], data["mkt_channel"], data["mkt_channel"].replace("@", ""), data["mkt_subs"], data["mkt_stars"], usd_price))
        await conn.commit()

    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Back to Channels ↗", callback_data="admin:markets")]])
    await message.answer(f"Channel '{data['mkt_name']}' successfully added to network!", reply_markup=markup, parse_mode=ParseMode.HTML)


@admin_router.callback_query(F.data == "admin:ads:all")
async def cb_admin_ads_all(callback: CallbackQuery, bot: Bot):
    if not await check_is_admin(callback.from_user.id):
        return

    async with db.get_connection() as conn:
        cursor = await conn.execute("""
            SELECT a.order_id, a.user_id, a.status, a.created_at, m.name as market_name 
            FROM advertisements a
            LEFT JOIN markets m ON a.market_id = m.id
            ORDER BY a.id DESC LIMIT 15;
        """)
        ads = await cursor.fetchall()

    if not ads:
        text = "No ad bookings found in database."
    else:
        text = "Recent Advertisement Bookings:\n\n"
        for ad in ads:
            text += f"• Order: {ad['order_id']} | User: {ad['user_id']} | Market: {ad['market_name']} | Status: {ad['status']}\n"

    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Back to Dashboard ↗", callback_data="admin:main")]])
    await send_or_edit_photo(
        event=callback,
        photo_path="1000284193.jpg",
        caption=text,
        reply_markup=markup,
        bot=bot
    )
    await callback.answer()


@admin_router.callback_query(F.data == "admin:users")
async def cb_admin_users_list(callback: CallbackQuery, bot: Bot):
    if not await check_is_admin(callback.from_user.id):
        return

    async with db.get_connection() as conn:
        cursor = await conn.execute("SELECT telegram_id, username, stars_balance, usd_balance FROM users ORDER BY id DESC LIMIT 20;")
        users = await cursor.fetchall()

    text = "Registered Users (Recent 20):\n\n"
    for u in users:
        un = f"@{u['username']}" if u['username'] else "No Username"
        text += f"• {u['telegram_id']} | {un} | Stars: {u['stars_balance']} | USD: ${u['usd_balance']:.2f}\n"

    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Back to Dashboard ↗", callback_data="admin:main")]])
    await send_or_edit_photo(
        event=callback,
        photo_path="1000284193.jpg",
        caption=text,
        reply_markup=markup,
        bot=bot
    )
    await callback.answer()


@admin_router.callback_query(F.data == "admin:broadcast")
async def cb_admin_broadcast_start(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if not await check_is_admin(callback.from_user.id):
        return

    await state.set_state(AdminStates.broadcast_message)
    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Back to Dashboard ↗", callback_data="admin:main")]])
    await send_or_edit_photo(
        event=callback,
        photo_path="1000284193.jpg",
        caption="Send or forward the message/media you want to broadcast to all users:",
        reply_markup=markup,
        bot=bot
    )
    await callback.answer()


@admin_router.message(AdminStates.broadcast_message)
async def process_broadcast_message(message: Message, state: FSMContext, bot: Bot):
    if not await check_is_admin(message.from_user.id):
        return
    await state.clear()

    async with db.get_connection() as conn:
        cursor = await conn.execute("SELECT telegram_id FROM users WHERE is_blocked = 0;")
        users = await cursor.fetchall()

    success, failed = 0, 0
    for u in users:
        try:
            await bot.copy_message(chat_id=u["telegram_id"], from_chat_id=message.chat.id, message_id=message.message_id)
            success += 1
        except Exception:
            failed += 1

    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Back to Dashboard ↗", callback_data="admin:main")]])
    await message.answer(f"Mass Broadcast Completed!\n\nDelivered: {success}\nFailed: {failed}", reply_markup=markup, parse_mode=ParseMode.HTML)


@admin_router.callback_query(F.data == "admin:backup")
async def cb_admin_backup_db(callback: CallbackQuery):
    if not await check_is_admin(callback.from_user.id):
        return

    if os.path.exists(Config.DATABASE_PATH):
        db_file = FSInputFile(Config.DATABASE_PATH)
        await callback.message.answer_document(document=db_file, caption=f"Database Backup exported on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        await callback.answer("Database exported successfully!")
    else:
        await callback.answer("Database file not found!", show_alert=True)


@admin_router.callback_query(F.data == "admin:import_db")
async def cb_admin_import_db_start(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if callback.from_user.id not in Config.SUPER_OWNER_IDS:
        await callback.answer("Only Super Owners can import database file.", show_alert=True)
        return

    await state.set_state(AdminStates.waiting_for_db_import)
    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Back to Dashboard ↗", callback_data="admin:main")]])
    await send_or_edit_photo(
        event=callback,
        photo_path="1000284193.jpg",
        caption="Please upload the new SQLite .db file:",
        reply_markup=markup,
        bot=bot
    )
    await callback.answer()


@admin_router.message(AdminStates.waiting_for_db_import, F.document)
async def process_db_import(message: Message, state: FSMContext, bot: Bot):
    if message.from_user.id not in Config.SUPER_OWNER_IDS:
        return

    document = message.document
    if not document.file_name.endswith((".db", ".sqlite")):
        await message.answer("Invalid file type. Please send a valid SQLite .db file.")
        return

    await state.clear()
    file_info = await bot.get_file(document.file_id)
    await bot.download_file(file_info.file_path, Config.DATABASE_PATH)

    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Back to Dashboard ↗", callback_data="admin:main")]])
    await message.answer("Database successfully imported and replaced live file!", reply_markup=markup, parse_mode=ParseMode.HTML)


@admin_router.callback_query(F.data == "admin:manage_admins")
async def cb_admin_manage_admins(callback: CallbackQuery, bot: Bot):
    if callback.from_user.id not in Config.SUPER_OWNER_IDS:
        await callback.answer("Super Owner privileges required.", show_alert=True)
        return

    async with db.get_connection() as conn:
        cursor = await conn.execute("SELECT telegram_id FROM admins;")
        admins = await cursor.fetchall()

    text = f"Super Owner: {Config.SUPER_OWNER_IDS}\n"
    for a in admins:
        text += f"• Admin: {a['telegram_id']}\n"

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Add Admin ↗", callback_data="admin:add_admin_start")],
        [InlineKeyboardButton(text="Back to Dashboard ↗", callback_data="admin:main")]
    ])
    await send_or_edit_photo(
        event=callback,
        photo_path="1000284193.jpg",
        caption=text,
        reply_markup=markup,
        bot=bot
    )
    await callback.answer()


@admin_router.callback_query(F.data == "admin:add_admin_start")
async def cb_admin_add_start(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if callback.from_user.id not in Config.SUPER_OWNER_IDS:
        return

    await state.set_state(AdminStates.add_admin_id)
    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Back ↗", callback_data="admin:manage_admins")]])
    await send_or_edit_photo(
        event=callback,
        photo_path="1000284193.jpg",
        caption="Enter Telegram ID of the user to grant Admin privileges:",
        reply_markup=markup,
        bot=bot
    )
    await callback.answer()


@admin_router.message(AdminStates.add_admin_id)
async def process_add_admin_id(message: Message, state: FSMContext):
    if message.from_user.id not in Config.SUPER_OWNER_IDS:
        return

    try:
        new_admin_id = int(message.text.strip())
    except ValueError:
        await message.answer("Invalid Telegram ID format. Please enter a valid integer.")
        return

    await state.clear()
    async with db.get_connection() as conn:
        await conn.execute("INSERT OR IGNORE INTO admins (telegram_id, role) VALUES (?, 'ADMIN');", (new_admin_id,))
        await conn.commit()

    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Back to Dashboard ↗", callback_data="admin:main")]])
    await message.answer(f"Granted Admin privileges to Telegram ID {new_admin_id}!", reply_markup=markup, parse_mode=ParseMode.HTML)

# ==========================================
# 8. MAIN ENTRYPOINT
# ==========================================

async def main():
    logger.info("Initializing database...")
    await db.init_db()

    bot = Bot(token=Config.BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())

    dp.include_router(user_router)
    dp.include_router(payment_router)
    dp.include_router(admin_router)

    asyncio.create_task(oxapay_polling_task(bot))

    logger.info("Bot is running...")
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot execution terminated.")
