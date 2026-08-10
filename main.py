import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
import html
import logging
import os
import secrets
import string
import sys
from typing import List, Optional, Tuple, Union

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
from aiogram.utils.token import TokenValidationError

# Import central Config and Logger from config.py
from config import Config, logger

# ==============================================================================
# 1. DATABASE SYSTEM & CRUD OPERATIONS
# ==============================================================================

class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path

    @asynccontextmanager
    async def get_connection(self):
        async with aiosqlite.connect(self.db_path) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            yield conn

    async def init_db(self):
        async with self.get_connection() as db:
            await db.executescript("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER UNIQUE NOT NULL,
                    username TEXT,
                    first_name TEXT,
                    language TEXT DEFAULT 'en',
                    stars_balance INTEGER DEFAULT 0,
                    usd_balance REAL DEFAULT 0.0,
                    total_deposits REAL DEFAULT 0.0,
                    total_spent REAL DEFAULT 0.0,
                    referrer_id INTEGER DEFAULT NULL,
                    total_referrals INTEGER DEFAULT 0,
                    is_blocked INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS markets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    channel_id TEXT NOT NULL UNIQUE,
                    channel_username TEXT,
                    subscribers TEXT DEFAULT 'N/A',
                    stars_price INTEGER NOT NULL,
                    usd_price REAL NOT NULL,
                    category TEXT DEFAULT 'General',
                    description TEXT DEFAULT '',
                    enabled INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS advertisements (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id TEXT UNIQUE NOT NULL,
                    user_id INTEGER NOT NULL,
                    market_ids TEXT NOT NULL,
                    source_chat_id INTEGER NOT NULL,
                    source_message_id INTEGER NOT NULL,
                    media_group_id TEXT,
                    content_type TEXT NOT NULL,
                    caption TEXT,
                    total_stars INTEGER DEFAULT 0,
                    total_usd REAL DEFAULT 0.0,
                    status TEXT DEFAULT 'PENDING_PAYMENT',
                    rejection_reason TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (telegram_id) ON DELETE CASCADE
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
                    order_id TEXT UNIQUE NOT NULL,
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
                    admin_note TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed_at TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS published_ads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    advertisement_id INTEGER NOT NULL,
                    market_id INTEGER NOT NULL,
                    channel_id TEXT NOT NULL,
                    message_id INTEGER NOT NULL,
                    post_url TEXT,
                    published_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (advertisement_id) REFERENCES advertisements (id) ON DELETE CASCADE,
                    FOREIGN KEY (market_id) REFERENCES markets (id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS admins (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER UNIQUE NOT NULL,
                    role TEXT DEFAULT 'ADMIN',
                    permissions TEXT DEFAULT 'ALL',
                    added_by INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS giveaways (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT UNIQUE NOT NULL,
                    creator_id INTEGER NOT NULL,
                    amount_stars INTEGER DEFAULT 0,
                    amount_usd REAL DEFAULT 0.0,
                    max_claims INTEGER DEFAULT 1,
                    claimed_count INTEGER DEFAULT 0,
                    is_active INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS giveaway_claims (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    giveaway_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    claimed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (giveaway_id) REFERENCES giveaways (id) ON DELETE CASCADE,
                    UNIQUE(giveaway_id, user_id)
                );

                CREATE TABLE IF NOT EXISTS audit_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    admin_id INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    details TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS system_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id);
                CREATE INDEX IF NOT EXISTS idx_ads_order_id ON advertisements(order_id);
                CREATE INDEX IF NOT EXISTS idx_withdrawals_req_id ON withdrawals(request_id);
            """)

            # Seed Default Super Owners into Admin Table
            for owner_id in Config.SUPER_OWNER_IDS:
                await db.execute("""
                    INSERT INTO admins (telegram_id, role, permissions)
                    VALUES (?, 'SUPER_ADMIN', 'ALL')
                    ON CONFLICT(telegram_id) DO NOTHING;
                """, (owner_id,))

            # Seed Initial Markets if empty
            cursor = await db.execute("SELECT COUNT(*) FROM markets;")
            row = await cursor.fetchone()
            if row and row[0] == 0:
                await db.execute("""
                    INSERT INTO markets (name, channel_id, channel_username, subscribers, stars_price, usd_price, category, enabled)
                    VALUES 
                    ('PostsMarket Premium', '@PostsMarket', 'PostsMarket', '50K+', 29, 2.50, 'General', 1),
                    ('Public Verified Hub', '@PublicVerified', 'PublicVerified', '10K+', 12, 1.00, 'Crypto & Tech', 1),
                    ('Core Creations Promo', '@CoreCreations', 'CoreCreations', '25K+', 20, 1.80, 'Official', 1);
                """)

            await db.commit()
            logger.info("⚡ Enhanced Database Schema Initialized Successfully.")

    # ------------------ User Queries ------------------
    async def upsert_user(self, telegram_id: int, username: Optional[str], first_name: Optional[str], referrer_id: Optional[int] = None) -> bool:
        async with self.get_connection() as db:
            cursor = await db.execute("SELECT id FROM users WHERE telegram_id = ?;", (telegram_id,))
            exists = await cursor.fetchone()

            if not exists:
                valid_ref = None
                if referrer_id and referrer_id != telegram_id:
                    ref_cur = await db.execute("SELECT telegram_id FROM users WHERE telegram_id = ?;", (referrer_id,))
                    if await ref_cur.fetchone():
                        valid_ref = referrer_id
                        await db.execute("UPDATE users SET total_referrals = total_referrals + 1 WHERE telegram_id = ?;", (referrer_id,))

                await db.execute("""
                    INSERT INTO users (telegram_id, username, first_name, referrer_id, last_seen, is_blocked)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, 0);
                """, (telegram_id, username, first_name, valid_ref))
                await db.commit()
                return True
            else:
                await db.execute("""
                    UPDATE users
                    SET username = ?, first_name = ?, last_seen = CURRENT_TIMESTAMP, is_blocked = 0
                    WHERE telegram_id = ?;
                """, (username, first_name, telegram_id))
                await db.commit()
                return False

    async def get_user(self, telegram_id: int) -> Optional[aiosqlite.Row]:
        async with self.get_connection() as db:
            cursor = await db.execute("SELECT * FROM users WHERE telegram_id = ?;", (telegram_id,))
            return await cursor.fetchone()

    async def get_all_users(self, limit: int = 100, offset: int = 0) -> List[aiosqlite.Row]:
        async with self.get_connection() as db:
            cursor = await db.execute("SELECT * FROM users ORDER BY id DESC LIMIT ? OFFSET ?;", (limit, offset))
            return await cursor.fetchall()

    async def get_user_count(self) -> int:
        async with self.get_connection() as db:
            cursor = await db.execute("SELECT COUNT(*) FROM users;")
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def update_balances(
        self,
        telegram_id: int,
        stars_delta: int = 0,
        usd_delta: float = 0.0,
        deposit_delta: float = 0.0,
        spent_delta: float = 0.0
    ):
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

    # ------------------ Market Queries ------------------
    async def get_all_markets(self, enabled_only: bool = False) -> List[aiosqlite.Row]:
        async with self.get_connection() as db:
            query = "SELECT * FROM markets WHERE enabled = 1 ORDER BY id ASC;" if enabled_only else "SELECT * FROM markets ORDER BY id ASC;"
            cursor = await db.execute(query)
            return await cursor.fetchall()

    async def get_market_by_id(self, market_id: int) -> Optional[aiosqlite.Row]:
        async with self.get_connection() as db:
            cursor = await db.execute("SELECT * FROM markets WHERE id = ?;", (market_id,))
            return await cursor.fetchone()

    async def add_market(self, name: str, channel_id: str, channel_username: str, subscribers: str, stars_price: int, usd_price: float, category: str = 'General') -> int:
        async with self.get_connection() as db:
            cursor = await db.execute("""
                INSERT INTO markets (name, channel_id, channel_username, subscribers, stars_price, usd_price, category, enabled)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1);
            """, (name, channel_id, channel_username, subscribers, stars_price, usd_price, category))
            await db.commit()
            return cursor.lastrowid

    async def delete_market(self, market_id: int):
        async with self.get_connection() as db:
            await db.execute("DELETE FROM markets WHERE id = ?;", (market_id,))
            await db.commit()

    async def toggle_market(self, market_id: int) -> int:
        async with self.get_connection() as db:
            cursor = await db.execute("SELECT enabled FROM markets WHERE id = ?;", (market_id,))
            row = await cursor.fetchone()
            if not row:
                return 0
            new_status = 0 if row["enabled"] else 1
            await db.execute("UPDATE markets SET enabled = ? WHERE id = ?;", (new_status, market_id))
            await db.commit()
            return new_status

    # ------------------ Advertisement Queries ------------------
    async def create_advertisement(
        self,
        order_id: str,
        user_id: int,
        market_ids: List[int],
        source_chat_id: int,
        source_message_id: int,
        content_type: str,
        caption: str,
        total_stars: int,
        total_usd: float
    ) -> int:
        market_ids_str = ",".join(map(str, market_ids))
        async with self.get_connection() as db:
            cursor = await db.execute("""
                INSERT INTO advertisements (
                    order_id, user_id, market_ids, source_chat_id, source_message_id,
                    content_type, caption, total_stars, total_usd, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING_PAYMENT');
            """, (order_id, user_id, market_ids_str, source_chat_id, source_message_id, content_type, caption, total_stars, total_usd))
            await db.commit()
            return cursor.lastrowid

    async def update_ad_status(self, order_id: str, status: str, rejection_reason: Optional[str] = None):
        async with self.get_connection() as db:
            await db.execute("""
                UPDATE advertisements
                SET status = ?, rejection_reason = ?, updated_at = CURRENT_TIMESTAMP
                WHERE order_id = ?;
            """, (status, rejection_reason, order_id))
            await db.commit()

    async def get_ad_by_order_id(self, order_id: str) -> Optional[aiosqlite.Row]:
        async with self.get_connection() as db:
            cursor = await db.execute("SELECT * FROM advertisements WHERE order_id = ?;", (order_id,))
            return await cursor.fetchone()

    async def get_pending_ads(self) -> List[aiosqlite.Row]:
        async with self.get_connection() as db:
            cursor = await db.execute("SELECT * FROM advertisements WHERE status = 'PENDING_APPROVAL' ORDER BY id ASC;")
            return await cursor.fetchall()

    async def record_published_ad(self, ad_id: int, market_id: int, channel_id: str, message_id: int, post_url: str):
        async with self.get_connection() as db:
            await db.execute("""
                INSERT INTO published_ads (advertisement_id, market_id, channel_id, message_id, post_url)
                VALUES (?, ?, ?, ?, ?);
            """, (ad_id, market_id, channel_id, message_id, post_url))
            await db.commit()

    # ------------------ Withdrawal Queries ------------------
    async def create_withdrawal_request(self, request_id: str, user_id: int, amount: float, wallet_address: str) -> bool:
        async with self.get_connection() as db:
            await db.execute("""
                INSERT INTO withdrawals (request_id, user_id, amount, wallet_address, status)
                VALUES (?, ?, ?, ?, 'PENDING');
            """, (request_id, user_id, amount, wallet_address))
            await db.commit()
            return True

    async def get_pending_withdrawals(self) -> List[aiosqlite.Row]:
        async with self.get_connection() as db:
            cursor = await db.execute("SELECT * FROM withdrawals WHERE status = 'PENDING' ORDER BY id ASC;")
            return await cursor.fetchall()

    async def update_withdrawal_status(self, request_id: str, status: str, admin_note: Optional[str] = None):
        async with self.get_connection() as db:
            await db.execute("""
                UPDATE withdrawals
                SET status = ?, admin_note = ?, processed_at = CURRENT_TIMESTAMP
                WHERE request_id = ?;
            """, (status, admin_note, request_id))
            await db.commit()

    async def get_withdrawal_by_req_id(self, request_id: str) -> Optional[aiosqlite.Row]:
        async with self.get_connection() as db:
            cursor = await db.execute("SELECT * FROM withdrawals WHERE request_id = ?;", (request_id,))
            return await cursor.fetchone()

    # ------------------ Admin Queries ------------------
    async def get_admins(self) -> List[aiosqlite.Row]:
        async with self.get_connection() as db:
            cursor = await db.execute("SELECT * FROM admins ORDER BY id ASC;")
            return await cursor.fetchall()

    # ------------------ Giveaway Queries ------------------
    async def create_giveaway(self, code: str, creator_id: int, amount_stars: int, amount_usd: float, max_claims: int) -> bool:
        async with self.get_connection() as db:
            try:
                await db.execute("""
                    INSERT INTO giveaways (code, creator_id, amount_stars, amount_usd, max_claims, claimed_count, is_active)
                    VALUES (?, ?, ?, ?, ?, 0, 1);
                """, (code, creator_id, amount_stars, amount_usd, max_claims))
                await db.commit()
                return True
            except aiosqlite.IntegrityError:
                return False

    async def claim_giveaway(self, code: str, user_id: int) -> Tuple[bool, str, int, float]:
        async with self.get_connection() as db:
            cursor = await db.execute("SELECT * FROM giveaways WHERE code = ? AND is_active = 1;", (code,))
            giveaway = await cursor.fetchone()
            if not giveaway:
                return False, "❌ Invalid or expired giveaway code.", 0, 0.0

            if giveaway["claimed_count"] >= giveaway["max_claims"]:
                await db.execute("UPDATE giveaways SET is_active = 0 WHERE id = ?;", (giveaway["id"],))
                await db.commit()
                return False, "⚠️ This giveaway has already reached its maximum claims limit.", 0, 0.0

            c_cur = await db.execute("SELECT 1 FROM giveaway_claims WHERE giveaway_id = ? AND user_id = ?;", (giveaway["id"], user_id))
            if await c_cur.fetchone():
                return False, "⛔ You have already claimed this giveaway voucher!", 0, 0.0

            await db.execute("INSERT INTO giveaway_claims (giveaway_id, user_id) VALUES (?, ?);", (giveaway["id"], user_id))
            new_count = giveaway["claimed_count"] + 1
            is_active = 1 if new_count < giveaway["max_claims"] else 0
            await db.execute("UPDATE giveaways SET claimed_count = ?, is_active = ? WHERE id = ?;", (new_count, is_active, giveaway["id"]))

            stars = giveaway["amount_stars"]
            usd = giveaway["amount_usd"]
            await db.execute("UPDATE users SET stars_balance = stars_balance + ?, usd_balance = usd_balance + ? WHERE telegram_id = ?;", (stars, usd, user_id))

            await db.commit()
            return True, "🎉 Giveaway claimed successfully!", stars, usd

db = Database(Config.DATABASE_PATH)

# ==============================================================================
# 2. FSM STATES
# ==============================================================================

class UserStates(StatesGroup):
    waiting_for_ad = State()
    waiting_for_deposit_stars = State()
    waiting_for_withdraw_amount = State()
    waiting_for_gram_address = State()
    waiting_for_giveaway_code = State()

class AdminStates(StatesGroup):
    add_market_name = State()
    add_market_channel = State()
    add_market_subs = State()
    add_market_stars = State()
    add_market_usd = State()
    waiting_for_reject_reason = State()
    create_giveaway_stars = State()
    create_giveaway_claims = State()
    broadcast_message = State()

# ==============================================================================
# 3. UTILITY & HELPER FUNCTIONS
# ==============================================================================

def clean_html(text: Optional[str]) -> str:
    return html.escape(str(text or ""))

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
        admins = await db.get_admins()
        for a in admins:
            admin_ids.add(a["telegram_id"])
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
                logger.warning(f"Failed to edit media: {e}")

        try:
            await event.message.edit_caption(caption=caption, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
        except TelegramBadRequest:
            await event.message.edit_text(text=caption, reply_markup=reply_markup, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

    elif isinstance(event, Message):
        if file_exists:
            try:
                await event.answer_photo(photo=FSInputFile(photo_path), caption=caption, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
                return
            except Exception as e:
                logger.warning(f"Failed to send photo {photo_path}: {e}")

        await event.answer(text=caption, reply_markup=reply_markup, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

# ==============================================================================
# 4. OXAPAY CRYPTO CLIENT
# ==============================================================================

class OxapayClient:
    BASE_URL = "https://api.oxapay.com/merchants"

    @staticmethod
    async def create_invoice(merchant_key: str, amount: float, order_id: str, description: str) -> dict:
        if not merchant_key or merchant_key == "YOUR_OXAPAY_MERCHANT_KEY":
            logger.warning("Oxapay merchant key is unconfigured.")
            return {"result": 400, "message": "Merchant key not configured"}

        url = f"{OxapayClient.BASE_URL}/request"
        payload = {
            "merchant": merchant_key,
            "amount": amount,
            "currency": "USD",
            "orderId": order_id,
            "description": description,
            "lifeTime": 60,
            "feePaidByPayer": 1
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=15) as resp:
                    return await resp.json()
        except Exception as err:
            logger.error(f"Oxapay API Exception: {err}")
            return {"result": 500, "message": str(err)}

# ==============================================================================
# 5. KEYBOARD BUILDERS
# ==============================================================================

class Keyboards:
    @staticmethod
    def main_menu(is_admin: bool = False) -> InlineKeyboardMarkup:
        builder = [
            [
                InlineKeyboardButton(text="⚡ Forward Ad ↗", callback_data="btn:forward"),
                InlineKeyboardButton(text="📌 Pin Post ↗", callback_data="btn:pin")
            ],
            [
                InlineKeyboardButton(text="👤 My Profile ↗", callback_data="btn:profile"),
                InlineKeyboardButton(text="💳 Wallet & Balances ↗", callback_data="btn:wallet")
            ],
            [
                InlineKeyboardButton(text="🎁 Redeem Voucher ↗", callback_data="btn:redeem_voucher"),
                InlineKeyboardButton(text="👥 Referrals ↗", callback_data="btn:referrals")
            ],
            [
                InlineKeyboardButton(text="💬 Contact Support ↗", url=Config.SUPPORT_LINK),
                InlineKeyboardButton(text="🌐 Official Channel ↗", url=Config.MAIN_CHANNEL_LINK)
            ],
        ]
        if is_admin:
            builder.append([InlineKeyboardButton(text="👑 Admin Dashboard ↗", callback_data="admin:main")])
        return InlineKeyboardMarkup(inline_keyboard=builder)

    @staticmethod
    def main_menu_only() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🏠 Back to Main Menu ↗", callback_data="user:main_menu")]
        ])

    @staticmethod
    def forward_menu() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚀 Continue Forwarding ↗", callback_data="btn:forward_continue")],
            [
                InlineKeyboardButton(text="⬅️ Back", callback_data="user:main_menu"),
                InlineKeyboardButton(text="💰 Recharge Wallet ↗", callback_data="btn:recharge_wallet")
            ]
        ])

    @staticmethod
    def wallet_menu(stars_bal: int, usd_bal: float) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="📥 Deposit Stars / USD ↗", callback_data="btn:deposit"),
                InlineKeyboardButton(text="📤 Withdraw TON ↗", callback_data="btn:withdraw")
            ],
            [InlineKeyboardButton(text="🏠 Main Menu ↗", callback_data="user:main_menu")]
        ])

    @staticmethod
    def payment_options_menu(order_id: Optional[str] = None) -> InlineKeyboardMarkup:
        suffix = f":{order_id}" if order_id else ""
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="⭐ Pay via Telegram Stars ↗", callback_data=f"pay_opt:stars{suffix}"),
                InlineKeyboardButton(text="💎 Pay via Crypto (Oxapay) ↗", callback_data=f"pay_opt:crypto{suffix}")
            ],
            [InlineKeyboardButton(text="⬅️ Back to Markets", callback_data="btn:forward_continue")]
        ])

    @staticmethod
    def market_selection_menu(markets: List[aiosqlite.Row], selected_ids: List[int]) -> InlineKeyboardMarkup:
        keyboard = []
        for m in markets:
            checked = "✅ " if m["id"] in selected_ids else "🔲 "
            keyboard.append([
                InlineKeyboardButton(
                    text=f"{checked}{m['name']} ({m['subscribers']}) — ⭐{m['stars_price']} / ${m['usd_price']:.2f}",
                    callback_data=f"mkt_toggle:{m['id']}"
                )
            ])
        keyboard.append([InlineKeyboardButton(text="🚀 Proceed with Selected Markets ↗", callback_data="mkt_confirm_selection")])
        keyboard.append([InlineKeyboardButton(text="🏠 Main Menu ↗", callback_data="user:main_menu")])
        return InlineKeyboardMarkup(inline_keyboard=keyboard)

    @staticmethod
    def withdraw_prompt_menu() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Back to Wallet", callback_data="btn:wallet")]
        ])

    @staticmethod
    def withdraw_recheck_menu() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Confirm & Submit ↗", callback_data="withdraw:confirm"),
                InlineKeyboardButton(text="✏️ Edit Address ↗", callback_data="withdraw:edit")
            ]
        ])

    @staticmethod
    def admin_main_menu() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="📢 Network Channels ↗", callback_data="admin:markets"),
                InlineKeyboardButton(text="📝 Ad Queue ↗", callback_data="admin:ads_queue")
            ],
            [
                InlineKeyboardButton(text="💸 Pending Withdrawals ↗", callback_data="admin:withdrawals"),
                InlineKeyboardButton(text="📊 Platform Analytics ↗", callback_data="admin:stats")
            ],
            [
                InlineKeyboardButton(text="📣 Mass Broadcast ↗", callback_data="admin:broadcast"),
                InlineKeyboardButton(text="🎁 Voucher Generator ↗", callback_data="admin:create_giveaway")
            ],
            [InlineKeyboardButton(text="🚪 Exit Admin Panel ↗", callback_data="user:main_menu")]
        ])

    @staticmethod
    def admin_markets_menu(markets: List[aiosqlite.Row]) -> InlineKeyboardMarkup:
        keyboard = []
        for m in markets:
            status = "🟢 ACTIVE" if m["enabled"] else "🔴 OFF"
            keyboard.append([
                InlineKeyboardButton(text=f"{m['name']} [{status}]", callback_data=f"admin:mkt_view:{m['id']}"),
                InlineKeyboardButton(text="⚙️ Toggle", callback_data=f"admin:mkt_toggle:{m['id']}"),
                InlineKeyboardButton(text="🗑️ Delete", callback_data=f"admin:mkt_del:{m['id']}")
            ])
        keyboard.append([InlineKeyboardButton(text="➕ Add Channel Market ↗", callback_data="admin:mkt_add")])
        keyboard.append([InlineKeyboardButton(text="⬅️ Return to Dashboard ↗", callback_data="admin:main")])
        return InlineKeyboardMarkup(inline_keyboard=keyboard)

    @staticmethod
    def admin_ad_review_menu(order_id: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Approve & Publish ↗", callback_data=f"admin:ad_approve:{order_id}"),
                InlineKeyboardButton(text="❌ Reject Order ↗", callback_data=f"admin:ad_reject:{order_id}")
            ],
            [InlineKeyboardButton(text="⬅️ Back to Queue", callback_data="admin:ads_queue")]
        ])

    @staticmethod
    def admin_withdrawal_review_menu(req_id: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Mark Paid ↗", callback_data=f"admin:wth_approve:{req_id}"),
                InlineKeyboardButton(text="❌ Reject & Refund ↗", callback_data=f"admin:wth_reject:{req_id}")
            ],
            [InlineKeyboardButton(text="⬅️ Back to List", callback_data="admin:withdrawals")]
        ])

# ==============================================================================
# 6. USER ROUTER & HANDLERS
# ==============================================================================

user_router = Router()

MAIN_TEXT = (
    "<b>🌟 Welcome to Core Creations Pay-To-Forward System</b>\n\n"
    "Looking to forward advertisements, promote posts, or secure channel pin placements across premier markets?\n"
    "<b>You're in the right place! 🎯</b>\n\n"
    "✨ <b>How it works:</b>\n"
    "1️⃣ Load your wallet using <b>Telegram Stars</b> or <b>Crypto</b>.\n"
    "2️⃣ Forward or send your advertisement payload directly to the bot.\n"
    "3️⃣ Pick target markets and get published upon approval!\n\n"
    "💳 <b>Accepted Payments:</b> Telegram Stars (XTR) / Crypto (USDT, TON, BTC)\n\n"
    "<b>⚡ Powered by @CoreCreations Network</b>"
)

@user_router.message(CommandStart())
async def cmd_start_handler(message: Message, state: FSMContext, bot: Bot):
    await state.clear()
    user = message.from_user

    referrer_id = None
    args = message.text.split()
    if len(args) > 1 and args[1].isdigit():
        referrer_id = int(args[1])

    is_new = await db.upsert_user(user.id, user.username, user.first_name, referrer_id)
    if is_new and referrer_id:
        try:
            await bot.send_message(
                referrer_id,
                f"🎉 <b>New Referral Joined!</b>\nUser {clean_html(user.first_name)} (@{user.username or 'N/A'}) joined via your link.",
                parse_mode=ParseMode.HTML
            )
        except Exception:
            pass

    is_admin = await check_is_admin(user.id)

    await send_or_edit_photo(
        event=message,
        photo_path="core.jpg",
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
        photo_path="core.jpg",
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
    stars = u['stars_balance'] if u else 0
    usd = u['usd_balance'] if u else 0.0
    referrals = u['total_referrals'] if u else 0

    caption = (
        f"👤 <b>Account Profile — {display_user}</b>\n"
        f"──────────────────────────\n"
        f"🆔 <b>Telegram ID:</b> <code>{callback.from_user.id}</code>\n"
        f"⭐ <b>Stars Balance:</b> <code>{stars} Stars</code>\n"
        f"💵 <b>USD Balance:</b> <code>${usd:.2f} USD</code>\n\n"
        f"📥 <b>Total Deposits:</b> <code>{deposits}</code>\n"
        f"💸 <b>Total Spent:</b> <code>{spent}</code>\n"
        f"👥 <b>Total Referrals:</b> <code>{referrals} Users</code>"
    )

    await send_or_edit_photo(
        event=callback,
        photo_path="profile.jpg",
        caption=caption,
        reply_markup=Keyboards.main_menu_only(),
        bot=bot
    )
    await callback.answer()

@user_router.callback_query(F.data == "btn:referrals")
async def cb_referrals_handler(callback: CallbackQuery, bot: Bot):
    user_id = callback.from_user.id
    u = await db.get_user(user_id)
    bot_info = await bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start={user_id}"

    caption = (
        f"👥 <b>Affiliate & Referral System</b>\n"
        f"──────────────────────────\n"
        f"Invite users to promote posts using your custom link!\n\n"
        f"🔗 <b>Your Referral Link:</b>\n"
        f"<code>{ref_link}</code>\n\n"
        f"📊 <b>Total Users Invited:</b> <code>{u['total_referrals'] if u else 0}</code>"
    )

    await send_or_edit_photo(
        event=callback,
        photo_path="profile.jpg",
        caption=caption,
        reply_markup=Keyboards.main_menu_only(),
        bot=bot
    )
    await callback.answer()

@user_router.callback_query(F.data.in_({"btn:forward", "btn:pin"}))
async def cb_forward_handler(callback: CallbackQuery, bot: Bot):
    caption = (
        "📢 <b>Forward or Pin Your Advertisement</b>\n"
        "──────────────────────────\n"
        "After completing payment, your media post will be forwarded across our official channels:\n"
        "<b>👉 @PostsMarket</b>\n\n"
        "Click <b>Continue</b> below to send your post payload!"
    )
    await send_or_edit_photo(
        event=callback,
        photo_path="forward.jpg",
        caption=caption,
        reply_markup=Keyboards.forward_menu(),
        bot=bot
    )
    await callback.answer()

@user_router.callback_query(F.data == "btn:forward_continue")
async def cb_forward_continue(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await state.set_state(UserStates.waiting_for_ad)
    await state.update_data(selected_markets=[])

    caption = "📩 <b>Send or Forward your Advertisement Payload</b> (Photo, Video, Document, or Text) to this chat now:"
    await send_or_edit_photo(
        event=callback,
        photo_path="forward.jpg",
        caption=caption,
        reply_markup=Keyboards.main_menu_only(),
        bot=bot
    )
    await callback.answer()

@user_router.message(UserStates.waiting_for_ad)
async def process_ad_content(message: Message, state: FSMContext, bot: Bot):
    user_id = message.from_user.id
    content_type = message.content_type
    caption_text = message.caption or message.text or ""

    order_id = generate_req_id("AD")

    await state.update_data(
        ad_order_id=order_id,
        source_chat_id=message.chat.id,
        source_message_id=message.message_id,
        content_type=str(content_type),
        caption_text=caption_text,
        selected_markets=[]
    )

    markets = await db.get_all_markets(enabled_only=True)
    u = await db.get_user(user_id)
    stars_bal = u["stars_balance"] if u else 0
    usd_bal = u["usd_balance"] if u else 0.0

    postmarket_caption = (
        f"💳 <b>Wallet Balance:</b> <code>⭐{stars_bal} Stars</code> | <code>${usd_bal:.2f} USD</code>\n"
        f"──────────────────────────\n"
        f"Select target channel markets for placement below:\n"
        f"🔗 https://t.me/PostsMarket"
    )

    await send_or_edit_photo(
        event=message,
        photo_path="postsmarket.jpg",
        caption=postmarket_caption,
        reply_markup=Keyboards.market_selection_menu(markets, []),
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
    markets = await db.get_all_markets(enabled_only=True)
    u = await db.get_user(callback.from_user.id)
    stars_bal = u["stars_balance"] if u else 0
    usd_bal = u["usd_balance"] if u else 0.0

    postmarket_caption = (
        f"💳 <b>Wallet Balance:</b> <code>⭐{stars_bal} Stars</code> | <code>${usd_bal:.2f} USD</code>\n"
        f"──────────────────────────\n"
        f"Select target channel markets for placement below:\n"
        f"🔗 https://t.me/PostsMarket"
    )

    await send_or_edit_photo(
        event=callback,
        photo_path="postsmarket.jpg",
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
        await callback.answer("⚠️ Select at least one market channel!", show_alert=True)
        return

    order_id = data.get("ad_order_id")

    total_stars, total_usd = 0, 0.0
    for m_id in selected:
        mkt = await db.get_market_by_id(m_id)
        if mkt:
            total_stars += mkt["stars_price"]
            total_usd += mkt["usd_price"]

    await state.update_data(total_stars=total_stars, total_usd=total_usd)

    await db.create_advertisement(
        order_id=order_id,
        user_id=callback.from_user.id,
        market_ids=selected,
        source_chat_id=data.get("source_chat_id"),
        source_message_id=data.get("source_message_id"),
        content_type=data.get("content_type", "text"),
        caption=data.get("caption_text", ""),
        total_stars=total_stars,
        total_usd=total_usd
    )

    caption = (
        f"🎯 <b>Campaign Confirmation</b>\n"
        f"──────────────────────────\n"
        f"📋 <b>Order ID:</b> <code>{order_id}</code>\n"
        f"📢 <b>Channels Selected:</b> <code>{len(selected)}</code>\n"
        f"⭐ <b>Total Price (Stars):</b> <code>⭐{total_stars} XTR</code>\n"
        f"💵 <b>Total Price (Crypto):</b> <code>${total_usd:.2f} USD</code>\n\n"
        f"Choose payment method below:"
    )

    await send_or_edit_photo(
        event=callback,
        photo_path="paymentoptions.jpg",
        caption=caption,
        reply_markup=Keyboards.payment_options_menu(order_id),
        bot=bot
    )
    await callback.answer()

@user_router.callback_query(F.data.in_({"btn:wallet", "btn:recharge_wallet"}))
async def cb_wallet_handler(callback: CallbackQuery, bot: Bot):
    u = await db.get_user(callback.from_user.id)
    stars_bal = u["stars_balance"] if u else 0
    usd_bal = u["usd_balance"] if u else 0.0

    caption = (
        f"💳 <b>Your Wallet Overview</b>\n"
        f"──────────────────────────\n"
        f"⭐ <b>Telegram Stars Balance:</b> <code>{stars_bal} Stars</code>\n"
        f"💵 <b>Crypto / USD Balance:</b> <code>${usd_bal:.2f} USD</code>\n\n"
        f"Deposit funds via Stars/Crypto, or request payouts in TON."
    )

    await send_or_edit_photo(
        event=callback,
        photo_path="wallet.jpg",
        caption=caption,
        reply_markup=Keyboards.wallet_menu(stars_bal, usd_bal),
        bot=bot
    )
    await callback.answer()

@user_router.callback_query(F.data == "btn:deposit")
async def cb_deposit_handler(callback: CallbackQuery, bot: Bot):
    caption = "💳 <b>Select deposit gateway below:</b>"
    await send_or_edit_photo(
        event=callback,
        photo_path="paymentoptions.jpg",
        caption=caption,
        reply_markup=Keyboards.payment_options_menu(),
        bot=bot
    )
    await callback.answer()

@user_router.callback_query(F.data.startswith("pay_opt:stars"))
async def cb_pay_opt_stars(callback: CallbackQuery, state: FSMContext, bot: Bot):
    parts = callback.data.split(":")
    order_id = parts[2] if len(parts) > 2 else None

    if order_id:
        ad = await db.get_ad_by_order_id(order_id)
        if not ad:
            await callback.answer("Order not found.", show_alert=True)
            return

        tot_stars = ad["total_stars"]
        u = await db.get_user(callback.from_user.id)

        if u and u["stars_balance"] >= tot_stars:
            await db.update_balances(callback.from_user.id, stars_delta=-tot_stars, spent_delta=float(tot_stars * Config.STARS_TO_USD_RATE))
            await db.update_ad_status(order_id, "PENDING_APPROVAL")
            await callback.message.answer(f"✅ <b>Paid ⭐{tot_stars} Stars from wallet!</b>\nOrder <code>{order_id}</code> is pending moderation.", parse_mode=ParseMode.HTML)
            await callback.answer("Payment Successful!")

            admin_ids = await get_all_admin_ids()
            for aid in admin_ids:
                try:
                    await bot.send_message(aid, f"🔔 <b>NEW AD ORDER PENDING APPROVAL</b>\nOrder ID: <code>{order_id}</code>", parse_mode=ParseMode.HTML)
                except Exception:
                    pass
            return

        prices = [LabeledPrice(label=f"Order {order_id}", amount=tot_stars)]
        await bot.send_invoice(
            chat_id=callback.from_user.id,
            title="Ad Placement Payment",
            description=f"Pay ⭐{tot_stars} Stars for order {order_id}",
            payload=f"stars_ad_{order_id}",
            provider_token="",
            currency="XTR",
            prices=prices,
            start_parameter=f"ad-{order_id}"
        )
        await callback.answer("Stars Invoice Generated!")
    else:
        await state.set_state(UserStates.waiting_for_deposit_stars)
        await callback.message.answer("⭐ <b>Enter Stars amount to deposit (e.g., 100):</b>", parse_mode=ParseMode.HTML)
        await callback.answer()

@user_router.message(UserStates.waiting_for_deposit_stars)
async def process_stars_deposit_amount(message: Message, state: FSMContext, bot: Bot):
    try:
        amount = int(message.text.strip())
        if amount <= 0:
            raise ValueError()
    except ValueError:
        await message.answer("❌ Invalid amount. Enter a positive integer.")
        return

    await state.clear()
    prices = [LabeledPrice(label=f"Deposit {amount} Stars", amount=amount)]

    await bot.send_invoice(
        chat_id=message.from_user.id,
        title="Wallet Deposit - Telegram Stars",
        description=f"Add {amount} Stars to your bot balance",
        payload=f"stars_deposit_{amount}",
        provider_token="",
        currency="XTR",
        prices=prices,
        start_parameter="deposit-stars"
    )

@user_router.callback_query(F.data.startswith("pay_opt:crypto"))
async def cb_pay_opt_crypto(callback: CallbackQuery, state: FSMContext, bot: Bot):
    parts = callback.data.split(":")
    order_id = parts[2] if len(parts) > 2 else None

    if order_id:
        ad = await db.get_ad_by_order_id(order_id)
        if not ad:
            await callback.answer("Order not found.", show_alert=True)
            return

        tot_usd = ad["total_usd"]
        res = await OxapayClient.create_invoice(
            merchant_key=Config.OXAPAY_MERCHANT_KEY,
            amount=tot_usd,
            order_id=order_id,
            description=f"Order {order_id} Ad Placement"
        )

        if res.get("result") == 100 and "payLink" in res:
            pay_url = res["payLink"]
            markup = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💎 Pay Crypto via Oxapay ↗", url=pay_url)],
                [InlineKeyboardButton(text="🏠 Main Menu ↗", callback_data="user:main_menu")]
            ])
            await callback.message.answer(f"💎 <b>Total Due: ${tot_usd:.2f} USD</b>\n\nClick below to complete payment:", reply_markup=markup, parse_mode=ParseMode.HTML)
        else:
            await callback.message.answer("⚠️ Oxapay gateway offline or unconfigured. Contact support @CoreCreations.")
    else:
        await callback.message.answer("💡 For direct Crypto deposits, contact support: @CoreCreations")

    await callback.answer()

@user_router.callback_query(F.data == "btn:withdraw")
async def cb_withdraw_start(callback: CallbackQuery, state: FSMContext, bot: Bot):
    u = await db.get_user(callback.from_user.id)
    usd_bal = u["usd_balance"] if u else 0.0

    caption = (
        f"💸 <b>Withdraw Funds (TON Network)</b>\n"
        f"──────────────────────────\n"
        f"💵 <b>Available Balance:</b> <code>${usd_bal:.2f} USD</code>\n\n"
        f"<b>Enter the dollar amount you wish to withdraw:</b>"
    )

    await state.set_state(UserStates.waiting_for_withdraw_amount)
    await send_or_edit_photo(
        event=callback,
        photo_path="withdraw.jpg",
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
        await message.answer("❌ Invalid input. Enter a valid numerical amount.")
        return

    u = await db.get_user(message.from_user.id)
    usd_bal = u["usd_balance"] if u else 0.0

    if requested > usd_bal:
        await message.answer(f"⚠️ <b>Insufficient Balance!</b> Max withdrawable: <code>${usd_bal:.2f} USD</code>", parse_mode=ParseMode.HTML)
        return

    await state.update_data(withdraw_amount=requested)
    await state.set_state(UserStates.waiting_for_gram_address)

    caption = "💎 <b>Please enter your TON / Gram Wallet Address:</b>"
    await send_or_edit_photo(
        event=message,
        photo_path="gramaddress.jpg",
        caption=caption,
        reply_markup=Keyboards.withdraw_prompt_menu(),
        bot=bot
    )

@user_router.message(UserStates.waiting_for_gram_address)
async def process_gram_address(message: Message, state: FSMContext, bot: Bot):
    address = message.text.strip()
    await state.update_data(gram_address=address)

    caption = (
        f"🔍 <b>Re-verify Withdrawal Details:</b>\n"
        f"──────────────────────────\n"
        f"💼 <b>Address:</b> <code>{clean_html(address)}</code>\n\n"
        f"Click <b>Confirm</b> to submit request."
    )

    await send_or_edit_photo(
        event=message,
        photo_path="recheck.jpg",
        caption=caption,
        reply_markup=Keyboards.withdraw_recheck_menu(),
        bot=bot
    )

@user_router.callback_query(F.data == "withdraw:confirm")
async def cb_withdraw_confirm(callback: CallbackQuery, state: FSMContext, bot: Bot):
    data = await state.get_data()
    amount = data.get("withdraw_amount", 0.0)
    address = data.get("gram_address", "N/A")
    user_id = callback.from_user.id

    u = await db.get_user(user_id)
    if not u or u["usd_balance"] < amount:
        await callback.answer("Insufficient balance.", show_alert=True)
        return

    req_id = generate_req_id("WTH")
    await db.update_balances(user_id, usd_delta=-amount)
    await db.create_withdrawal_request(req_id, user_id, amount, address)

    admin_ids = await get_all_admin_ids()
    alert_text = (
        f"🚨 <b>NEW WITHDRAWAL REQUEST</b>\n"
        f"🆔 <b>Request ID:</b> <code>{req_id}</code>\n"
        f"👤 <b>User:</b> @{callback.from_user.username or 'N/A'} (<code>{user_id}</code>)\n"
        f"💵 <b>Amount:</b> <code>${amount:.2f} USD</code>\n"
        f"💼 <b>Address:</b> <code>{address}</code>"
    )

    for aid in admin_ids:
        try:
            await bot.send_message(aid, alert_text, parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.error(f"Failed to alert admin {aid}: {e}")

    await state.clear()
    await callback.message.answer(f"✅ <b>Withdrawal Request Created!</b>\nID: <code>{req_id}</code>", parse_mode=ParseMode.HTML)
    await callback.answer()

@user_router.callback_query(F.data == "btn:redeem_voucher")
async def cb_redeem_voucher_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(UserStates.waiting_for_giveaway_code)
    await callback.message.answer("🎁 <b>Enter your Voucher Code:</b>", parse_mode=ParseMode.HTML)
    await callback.answer()

@user_router.message(UserStates.waiting_for_giveaway_code)
async def process_giveaway_claim(message: Message, state: FSMContext):
    code = message.text.strip().upper()
    await state.clear()

    success, msg, stars, usd = await db.claim_giveaway(code, message.from_user.id)
    if success:
        await message.answer(f"🎉 <b>CONGRATULATIONS!</b>\n{msg}\n\n<b>Rewarded:</b> ⭐ <code>+{stars} Stars</code> | 💵 <code>+${usd:.2f} USD</code>", parse_mode=ParseMode.HTML)
    else:
        await message.answer(f"{msg}", parse_mode=ParseMode.HTML)

# ==============================================================================
# 7. PAYMENT PROCESSOR ROUTER
# ==============================================================================

payment_router = Router()

@payment_router.pre_checkout_query()
async def process_pre_checkout_query(pre_checkout: PreCheckoutQuery):
    await pre_checkout.answer(ok=True)

@payment_router.message(F.successful_payment)
async def process_successful_payment_handler(message: Message, bot: Bot):
    payment_info = message.successful_payment
    payload = payment_info.invoice_payload

    if payload.startswith("stars_deposit_"):
        amount = int(payload.replace("stars_deposit_", ""))
        usd_val = amount * Config.STARS_TO_USD_RATE
        await db.update_balances(message.from_user.id, stars_delta=amount, deposit_delta=usd_val)
        await message.answer(f"✅ <b>Deposited ⭐{amount} Stars to balance!</b>", parse_mode=ParseMode.HTML)

    elif payload.startswith("stars_ad_"):
        order_id = payload.replace("stars_ad_", "")
        await db.update_balances(message.from_user.id, spent_delta=float(payment_info.total_amount * Config.STARS_TO_USD_RATE))
        await db.update_ad_status(order_id, "PENDING_APPROVAL")
        await message.answer(f"✅ <b>Payment verified for Order {order_id}!</b>\nSent to moderation queue.", parse_mode=ParseMode.HTML)

        admin_ids = await get_all_admin_ids()
        for aid in admin_ids:
            try:
                await bot.send_message(aid, f"🔔 <b>NEW AD ORDER PAID ({order_id})</b>", parse_mode=ParseMode.HTML)
            except Exception:
                pass

# ==============================================================================
# 8. ADMIN DASHBOARD ROUTER
# ==============================================================================

admin_router = Router()

@admin_router.callback_query(F.data == "admin:main")
async def cb_admin_main(callback: CallbackQuery, bot: Bot):
    if not await check_is_admin(callback.from_user.id):
        await callback.answer("⛔ Unauthorized access.", show_alert=True)
        return

    admin_text = "👑 <b>Core Creations — Control Panel</b>\n──────────────────────────"
    await send_or_edit_photo(
        event=callback,
        photo_path="core.jpg",
        caption=admin_text,
        reply_markup=Keyboards.admin_main_menu(),
        bot=bot
    )
    await callback.answer()

@admin_router.callback_query(F.data == "admin:markets")
async def cb_admin_markets_list(callback: CallbackQuery):
    if not await check_is_admin(callback.from_user.id):
        return
    markets = await db.get_all_markets()
    text = "📢 <b>Network Channels Configuration</b>\n──────────────────────────"
    await callback.message.edit_text(text=text, reply_markup=Keyboards.admin_markets_menu(markets), parse_mode=ParseMode.HTML)
    await callback.answer()

@admin_router.callback_query(F.data == "admin:mkt_add")
async def cb_admin_mkt_add_start(callback: CallbackQuery, state: FSMContext):
    if not await check_is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.add_market_name)
    await callback.message.edit_text("➕ <b>Enter New Market Display Name:</b>", parse_mode=ParseMode.HTML)
    await callback.answer()

@admin_router.message(AdminStates.add_market_name)
async def process_add_mkt_name(message: Message, state: FSMContext):
    await state.update_data(mkt_name=message.text.strip())
    await state.set_state(AdminStates.add_market_channel)
    await message.answer("🆔 <b>Enter Target Channel ID (e.g. '@PostsMarket'):</b>", parse_mode=ParseMode.HTML)

@admin_router.message(AdminStates.add_market_channel)
async def process_add_mkt_channel(message: Message, state: FSMContext):
    await state.update_data(mkt_channel=message.text.strip())
    await state.set_state(AdminStates.add_market_subs)
    await message.answer("👥 <b>Enter Subscriber Count Label (e.g. '50K+'):</b>", parse_mode=ParseMode.HTML)

@admin_router.message(AdminStates.add_market_subs)
async def process_add_mkt_subs(message: Message, state: FSMContext):
    await state.update_data(mkt_subs=message.text.strip())
    await state.set_state(AdminStates.add_market_stars)
    await message.answer("⭐ <b>Enter Price in Stars (XTR):</b>", parse_mode=ParseMode.HTML)

@admin_router.message(AdminStates.add_market_stars)
async def process_add_mkt_stars(message: Message, state: FSMContext):
    try:
        stars = int(message.text.strip())
    except ValueError:
        await message.answer("Invalid number.")
        return
    await state.update_data(mkt_stars=stars)
    await state.set_state(AdminStates.add_market_usd)
    await message.answer("💵 <b>Enter Price in USD (e.g. 2.50):</b>", parse_mode=ParseMode.HTML)

@admin_router.message(AdminStates.add_market_usd)
async def process_add_mkt_usd(message: Message, state: FSMContext):
    try:
        usd = float(message.text.strip())
    except ValueError:
        await message.answer("Invalid float.")
        return

    data = await state.get_data()
    await state.clear()

    m_id = await db.add_market(
        name=data["mkt_name"],
        channel_id=data["mkt_channel"],
        channel_username=data["mkt_channel"].replace("@", ""),
        subscribers=data["mkt_subs"],
        stars_price=data["mkt_stars"],
        usd_price=usd
    )
    await message.answer(f"✅ <b>Channel Market '{data['mkt_name']}' Added (ID: {m_id})!</b>", parse_mode=ParseMode.HTML)

@admin_router.callback_query(F.data.startswith("admin:mkt_toggle:"))
async def cb_admin_mkt_toggle(callback: CallbackQuery):
    if not await check_is_admin(callback.from_user.id):
        return
    m_id = int(callback.data.split(":")[2])
    new_st = await db.toggle_market(m_id)
    await callback.answer(f"Status toggled to {'Active' if new_st else 'Inactive'}.")
    await cb_admin_markets_list(callback)

@admin_router.callback_query(F.data.startswith("admin:mkt_del:"))
async def cb_admin_mkt_del(callback: CallbackQuery):
    if not await check_is_admin(callback.from_user.id):
        return
    m_id = int(callback.data.split(":")[2])
    await db.delete_market(m_id)
    await callback.answer("Market deleted.")
    await cb_admin_markets_list(callback)

@admin_router.callback_query(F.data == "admin:ads_queue")
async def cb_admin_ads_queue(callback: CallbackQuery):
    if not await check_is_admin(callback.from_user.id):
        return

    pending = await db.get_pending_ads()
    if not pending:
        markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Dashboard", callback_data="admin:main")]])
        await callback.message.edit_text("✅ <b>No pending ads in queue.</b>", reply_markup=markup, parse_mode=ParseMode.HTML)
        await callback.answer()
        return

    ad = pending[0]
    text = (
        f"📝 <b>AD APPROVAL QUEUE ({len(pending)} Pending)</b>\n"
        f"──────────────────────────\n"
        f"📋 <b>Order ID:</b> <code>{ad['order_id']}</code>\n"
        f"👤 <b>User ID:</b> <code>{ad['user_id']}</code>\n"
        f"⭐ <b>Stars:</b> <code>⭐{ad['total_stars']}</code> | 💵 <code>${ad['total_usd']:.2f}</code>\n"
        f"💬 <b>Caption:</b>\n{clean_html(ad['caption'])[:200]}"
    )
    await callback.message.edit_text(text=text, reply_markup=Keyboards.admin_ad_review_menu(ad['order_id']), parse_mode=ParseMode.HTML)
    await callback.answer()

@admin_router.callback_query(F.data.startswith("admin:ad_approve:"))
async def cb_admin_ad_approve(callback: CallbackQuery, bot: Bot):
    if not await check_is_admin(callback.from_user.id):
        return
    order_id = callback.data.split(":")[2]
    ad = await db.get_ad_by_order_id(order_id)

    if not ad:
        await callback.answer("Ad missing.", show_alert=True)
        return

    m_ids = [int(x) for x in ad["market_ids"].split(",") if x]
    published_count = 0

    for m_id in m_ids:
        mkt = await db.get_market_by_id(m_id)
        if mkt:
            try:
                sent = await bot.copy_message(
                    chat_id=mkt["channel_id"],
                    from_chat_id=ad["source_chat_id"],
                    message_id=ad["source_message_id"]
                )
                post_url = f"https://t.me/{mkt['channel_username']}/{sent.message_id}" if mkt['channel_username'] else "N/A"
                await db.record_published_ad(ad["id"], mkt["id"], mkt["channel_id"], sent.message_id, post_url)
                published_count += 1
            except Exception as e:
                logger.error(f"Failed to post to {mkt['channel_id']}: {e}")

    await db.update_ad_status(order_id, "PUBLISHED")

    try:
        await bot.send_message(ad["user_id"], f"🎉 <b>AD PUBLISHED!</b> Order <code>{order_id}</code> posted to {published_count} channel(s).", parse_mode=ParseMode.HTML)
    except Exception:
        pass

    await callback.answer("Ad approved!")
    await cb_admin_ads_queue(callback)

@admin_router.callback_query(F.data.startswith("admin:ad_reject:"))
async def cb_admin_ad_reject(callback: CallbackQuery, state: FSMContext):
    if not await check_is_admin(callback.from_user.id):
        return
    order_id = callback.data.split(":")[2]
    await state.update_data(reject_order_id=order_id)
    await state.set_state(AdminStates.waiting_for_reject_reason)
    await callback.message.edit_text("❌ <b>Enter rejection reason:</b>", parse_mode=ParseMode.HTML)
    await callback.answer()

@admin_router.message(AdminStates.waiting_for_reject_reason)
async def process_ad_rejection_reason(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    order_id = data.get("reject_order_id")
    reason = message.text.strip()
    await state.clear()

    ad = await db.get_ad_by_order_id(order_id)
    if ad:
        await db.update_ad_status(order_id, "REJECTED", reason)
        await db.update_balances(ad["user_id"], stars_delta=ad["total_stars"], usd_delta=ad["total_usd"])
        try:
            await bot.send_message(ad["user_id"], f"❌ <b>AD REJECTED & REFUNDED</b>\nOrder ID: <code>{order_id}</code>\nReason: {clean_html(reason)}", parse_mode=ParseMode.HTML)
        except Exception:
            pass

    await message.answer("Order rejected and user refunded.")

@admin_router.callback_query(F.data == "admin:withdrawals")
async def cb_admin_withdrawals(callback: CallbackQuery):
    if not await check_is_admin(callback.from_user.id):
        return

    pending = await db.get_pending_withdrawals()
    if not pending:
        markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Dashboard", callback_data="admin:main")]])
        await callback.message.edit_text("✅ <b>No pending withdrawal requests.</b>", reply_markup=markup, parse_mode=ParseMode.HTML)
        await callback.answer()
        return

    w = pending[0]
    text = (
        f"💸 <b>PENDING WITHDRAWALS ({len(pending)} Queue)</b>\n"
        f"──────────────────────────\n"
        f"🆔 <b>Request ID:</b> <code>{w['request_id']}</code>\n"
        f"👤 <b>User ID:</b> <code>{w['user_id']}</code>\n"
        f"💵 <b>Amount:</b> <code>${w['amount']:.2f} USD</code>\n"
        f"💼 <b>Address:</b> <code>{w['wallet_address']}</code>"
    )

    await callback.message.edit_text(text=text, reply_markup=Keyboards.admin_withdrawal_review_menu(w['request_id']), parse_mode=ParseMode.HTML)
    await callback.answer()

@admin_router.callback_query(F.data.startswith("admin:wth_approve:"))
async def cb_admin_wth_approve(callback: CallbackQuery, bot: Bot):
    if not await check_is_admin(callback.from_user.id):
        return
    req_id = callback.data.split(":")[2]
    w = await db.get_withdrawal_by_req_id(req_id)

    if w:
        await db.update_withdrawal_status(req_id, "APPROVED", "Paid by admin")
        try:
            await bot.send_message(w["user_id"], f"✅ <b>WITHDRAWAL PROCESSED!</b> Request <code>{req_id}</code> completed.", parse_mode=ParseMode.HTML)
        except Exception:
            pass

    await callback.answer("Withdrawal approved!")
    await cb_admin_withdrawals(callback)

@admin_router.callback_query(F.data.startswith("admin:wth_reject:"))
async def cb_admin_wth_reject(callback: CallbackQuery, bot: Bot):
    if not await check_is_admin(callback.from_user.id):
        return
    req_id = callback.data.split(":")[2]
    w = await db.get_withdrawal_by_req_id(req_id)

    if w:
        await db.update_withdrawal_status(req_id, "REJECTED", "Refunded by admin")
        await db.update_balances(w["user_id"], usd_delta=w["amount"])
        try:
            await bot.send_message(w["user_id"], f"❌ <b>WITHDRAWAL REJECTED</b> Request <code>{req_id}</code> declined and refunded.", parse_mode=ParseMode.HTML)
        except Exception:
            pass

    await callback.answer("Withdrawal rejected!")
    await cb_admin_withdrawals(callback)

@admin_router.callback_query(F.data == "admin:stats")
async def cb_admin_stats(callback: CallbackQuery):
    if not await check_is_admin(callback.from_user.id):
        return

    total_users = await db.get_user_count()
    async with db.get_connection() as conn:
        total_ads = (await (await conn.execute("SELECT COUNT(*) FROM advertisements;")).fetchone())[0]
        total_wth = (await (await conn.execute("SELECT COUNT(*) FROM withdrawals;")).fetchone())[0]

    stats_text = (
        f"📊 <b>Core Creations Analytics</b>\n"
        f"──────────────────────────\n"
        f"👥 <b>Registered Users:</b> <code>{total_users}</code>\n"
        f"📢 <b>Ad Campaigns:</b> <code>{total_ads}</code>\n"
        f"💸 <b>Withdrawal Requests:</b> <code>{total_wth}</code>\n"
        f"🟢 <b>Bot Status:</b> <code>ONLINE</code>"
    )
    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Dashboard", callback_data="admin:main")]])
    await callback.message.edit_text(text=stats_text, reply_markup=markup, parse_mode=ParseMode.HTML)
    await callback.answer()

@admin_router.callback_query(F.data == "admin:create_giveaway")
async def cb_admin_create_giveaway_start(callback: CallbackQuery, state: FSMContext):
    if not await check_is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.create_giveaway_stars)
    await callback.message.edit_text("🎁 <b>Enter Stars per claim (e.g. 50):</b>", parse_mode=ParseMode.HTML)
    await callback.answer()

@admin_router.message(AdminStates.create_giveaway_stars)
async def process_giveaway_stars(message: Message, state: FSMContext):
    try:
        stars = int(message.text.strip())
    except ValueError:
        await message.answer("Invalid input.")
        return
    await state.update_data(gw_stars=stars)
    await state.set_state(AdminStates.create_giveaway_claims)
    await message.answer("👥 <b>Enter Max Claims allowed (e.g. 10):</b>", parse_mode=ParseMode.HTML)

@admin_router.message(AdminStates.create_giveaway_claims)
async def process_giveaway_claims(message: Message, state: FSMContext):
    try:
        claims = int(message.text.strip())
    except ValueError:
        await message.answer("Invalid input.")
        return

    data = await state.get_data()
    await state.clear()

    code = f"CORE-{''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(6))}"
    await db.create_giveaway(code, message.from_user.id, data["gw_stars"], 0.0, claims)

    await message.answer(
        f"🎉 <b>VOUCHER CODE CREATED!</b>\n\n"
        f"🔑 <b>Code:</b> <code>{code}</code>\n"
        f"⭐ <b>Reward:</b> <code>{data['gw_stars']} Stars</code>\n"
        f"👥 <b>Max Claims:</b> <code>{claims} Users</code>",
        parse_mode=ParseMode.HTML
    )

@admin_router.callback_query(F.data == "admin:broadcast")
async def cb_admin_broadcast_start(callback: CallbackQuery, state: FSMContext):
    if not await check_is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.broadcast_message)
    await callback.message.edit_text("📣 <b>Send the message to broadcast to all users:</b>", parse_mode=ParseMode.HTML)
    await callback.answer()

@admin_router.message(AdminStates.broadcast_message)
async def process_broadcast_message(message: Message, state: FSMContext, bot: Bot):
    await state.clear()
    users = await db.get_all_users(limit=10000)
    sent_count, failed_count = 0, 0

    status_msg = await message.answer("🚀 <b>Broadcasting message...</b>", parse_mode=ParseMode.HTML)

    for user in users:
        try:
            await bot.copy_message(
                chat_id=user["telegram_id"],
                from_chat_id=message.chat.id,
                message_id=message.message_id
            )
            sent_count += 1
            await asyncio.sleep(0.05)
        except Exception:
            failed_count += 1

    await status_msg.edit_text(
        f"✅ <b>Broadcast Complete!</b>\n\n"
        f"📤 <b>Delivered:</b> {sent_count}\n"
        f"❌ <b>Failed / Blocked:</b> {failed_count}",
        parse_mode=ParseMode.HTML
    )

# ==============================================================================
# 9. APPLICATION ENTRY POINT
# ==============================================================================

async def main():
    # 1. Run Config regex validation
    Config.validate()

    # 2. Safe Bot Initialization catching TokenValidationError gracefully
    try:
        bot = Bot(token=Config.BOT_TOKEN)
    except TokenValidationError as e:
        logger.critical(f"FATAL: Bot initialization failed due to invalid token: {e}")
        logger.critical("Please double-check BOT_TOKEN in config.py and ensure no hidden characters exist.")
        sys.exit(1)

    dp = Dispatcher(storage=MemoryStorage())

    # 3. Include Routers
    dp.include_router(user_router)
    dp.include_router(payment_router)
    dp.include_router(admin_router)

    # 4. Initialize Database
    await db.init_db()

    logger.info("⚡ Bot initialized and starting polling...")
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    except Exception as e:
        logger.error(f"Unexpected error during bot execution: {e}")
    finally:
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot execution terminated gracefully.")
