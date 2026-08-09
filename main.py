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
                    order_id TEXT UNIQUE NOT NULL,
                    user_id INTEGER NOT NULL,
                    method TEXT NOT NULL,
                    currency TEXT NOT NULL,
                    amount REAL NOT NULL,
                    telegram_payment_id TEXT,
                    oxapay_track_id TEXT,
                    status TEXT DEFAULT 'WAITING_PAYMENT',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    verified_at TIMESTAMP,
                    FOREIGN KEY (order_id) REFERENCES advertisements (order_id) ON DELETE CASCADE
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
                CREATE INDEX IF NOT EXISTS idx_ads_user_id ON advertisements(user_id);
                CREATE INDEX IF NOT EXISTS idx_ads_status ON advertisements(status);
            """)

            cursor = await db.execute("SELECT COUNT(*) FROM markets;")
            row = await cursor.fetchone()
            if row and row[0] == 0:
                await db.execute("""
                    INSERT INTO markets (name, channel_id, channel_username, subscribers, stars_price, usd_price, enabled)
                    VALUES 
                    ('📢 @PaidMP', '@PaidMP', 'PaidMP', '50K+', 29, 2.50, 1),
                    ('📢 @PublicVerified', '@PublicVerified', 'PublicVerified', '10K+', 12, 1.00, 1);
                """)

            await db.commit()
            logger.info("Database initialized successfully.")

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


db = Database(Config.DATABASE_PATH)

# ==========================================
# 2. FSM STATES & UTILITIES
# ==========================================

class UserStates(StatesGroup):
    waiting_for_ad = State()


class AdminStates(StatesGroup):
    add_market_name = State()
    add_market_channel = State()
    add_market_subs = State()
    add_market_stars = State()
    add_market_usd = State()
    edit_market_subs = State()
    broadcast_message = State()
    add_admin_id = State()
    waiting_for_reject_reason = State()
    waiting_for_db_import = State()


def clean_html(text: Optional[str]) -> str:
    if not text:
        return ""
    return html.escape(str(text))


async def show_live_loader(event: Union[CallbackQuery, Message], text: str = "<i>⏳ Processing, please wait...</i>"):
    try:
        if isinstance(event, CallbackQuery):
            await event.answer("⚡ Processing request...")
            await event.message.edit_text(text, parse_mode=ParseMode.HTML)
        else:
            msg = await event.answer(text, parse_mode=ParseMode.HTML)
            return msg
    except Exception:
        pass
    return None


def generate_order_id() -> str:
    chars = string.ascii_uppercase + string.digits
    return f"AD-{''.join(secrets.choice(chars) for _ in range(6))}"


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

# ==========================================
# 3. OXAPAY PAYMENT API CLIENT
# ==========================================

class OxapayClient:
    BASE_URL = "https://api.oxapay.com/merchants"

    @staticmethod
    async def create_invoice(merchant_key: str, amount: float, order_id: str, description: str) -> dict:
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
            logger.error(f"Oxapay Invoice API Error: {err}")
            return {"result": 500, "message": str(err)}

    @staticmethod
    async def check_invoice_status(merchant_key: str, track_id: str) -> dict:
        url = f"{OxapayClient.BASE_URL}/inquiry"
        payload = {
            "merchant": merchant_key,
            "trackId": track_id
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=15) as resp:
                    return await resp.json()
        except Exception as err:
            logger.error(f"Oxapay Inquiry API Error: {err}")
            return {"result": 500, "message": str(err)}

# ==========================================
# 4. KEYBOARDS BUILDER
# ==========================================

class Keyboards:

    @staticmethod
    def user_main_menu(markets: List[aiosqlite.Row], is_admin: bool = False) -> InlineKeyboardMarkup:
        builder = []
        for m in markets:
            builder.append([
                InlineKeyboardButton(
                    text=f"📢 {m['name']} ({m['subscribers']}) • ⭐{m['stars_price']} / ${m['usd_price']}",
                    callback_data=f"user:select_market:{m['id']}"
                )
            ])
        
        builder.append([
            InlineKeyboardButton(text="📋 My Bookings", callback_data="user:my_orders"),
            InlineKeyboardButton(text="❓ Support & Info", callback_data="user:help")
        ])

        if is_admin:
            builder.append([InlineKeyboardButton(text="👑 Admin Dashboard", callback_data="admin:main")])

        return InlineKeyboardMarkup(inline_keyboard=builder)

    @staticmethod
    def cancel_button(callback_data: str = "user:main_menu") -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Cancel Operation", callback_data=callback_data)]
        ])

    @staticmethod
    def payment_method_selector(order_id: str, stars_price: int, usd_price: float) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"⭐ Instant Post via {stars_price} Stars", callback_data=f"pay:stars:{order_id}")],
            [InlineKeyboardButton(text=f"🪙 Crypto Auto-Pay (${usd_price:.2f} USD)", callback_data=f"pay:oxapay:{order_id}")],
            [InlineKeyboardButton(text="🗑 Cancel Booking", callback_data=f"user:cancel_order:{order_id}")]
        ])

    @staticmethod
    def oxapay_checkout_menu(order_id: str, pay_url: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔗 Pay with Crypto (Oxapay)", url=pay_url)],
            [InlineKeyboardButton(text="🔄 Verify Payment & Auto-Publish", callback_data=f"pay:check_oxapay:{order_id}")],
            [InlineKeyboardButton(text="❌ Cancel Booking", callback_data=f"user:cancel_order:{order_id}")]
        ])

    @staticmethod
    def admin_main_menu() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📢 Manage Channels", callback_data="admin:markets")],
            [InlineKeyboardButton(text="📋 All Bookings", callback_data="admin:ads:all"), InlineKeyboardButton(text="👥 Users List", callback_data="admin:users")],
            [InlineKeyboardButton(text="📊 Analytics", callback_data="admin:stats"), InlineKeyboardButton(text="📢 Mass Broadcast", callback_data="admin:broadcast")],
            [InlineKeyboardButton(text="📥 Export DB", callback_data="admin:backup"), InlineKeyboardButton(text="📤 Import DB", callback_data="admin:import_db")],
            [InlineKeyboardButton(text="👑 Admin Privileges", callback_data="admin:manage_admins")],
            [InlineKeyboardButton(text="🔙 Exit Admin View", callback_data="user:main_menu")]
        ])

    @staticmethod
    def admin_markets_menu(markets: List[aiosqlite.Row]) -> InlineKeyboardMarkup:
        keyboard = []
        for m in markets:
            status = "🟢" if m["enabled"] else "🔴"
            keyboard.append([
                InlineKeyboardButton(text=f"{status} {m['name']} ({m['subscribers']})", callback_data=f"admin:mkt_view:{m['id']}"),
                InlineKeyboardButton(text="👥 Subs", callback_data=f"admin:mkt_editsubs:{m['id']}"),
                InlineKeyboardButton(text="⚡ Toggle", callback_data=f"admin:mkt_toggle:{m['id']}"),
                InlineKeyboardButton(text="🗑 Delete", callback_data=f"admin:mkt_del:{m['id']}")
            ])
        keyboard.append([InlineKeyboardButton(text="➕ Add New Channel", callback_data="admin:mkt_add")])
        keyboard.append([InlineKeyboardButton(text="🔙 Back to Dashboard", callback_data="admin:main")])
        return InlineKeyboardMarkup(inline_keyboard=keyboard)

# ==========================================
# 5. USER ROUTER & HANDLERS
# ==========================================

user_router = Router()

@user_router.message(CommandStart())
async def cmd_start_handler(message: Message, state: FSMContext, bot: Bot):
    await state.clear()
    user_id = message.from_user.id
    user_name = clean_html(message.from_user.first_name)
    
    is_first_time = await db.upsert_user(user_id, message.from_user.username, message.from_user.first_name)

    if is_first_time:
        try:
            channel_msg = await bot.forward_message(
                chat_id=user_id,
                from_chat_id=Config.WELCOME_CHANNEL_ID,
                message_id=Config.WELCOME_MESSAGE_ID
            )
            channel_buttons = channel_msg.reply_markup
            await bot.delete_message(chat_id=user_id, message_id=channel_msg.message_id)

            await bot.copy_message(
                chat_id=user_id,
                from_chat_id=Config.WELCOME_CHANNEL_ID,
                message_id=Config.WELCOME_MESSAGE_ID,
                reply_markup=channel_buttons
            )
        except Exception as err:
            logger.error(f"Failed to copy welcome message with buttons to user {user_id}: {err}")
            try:
                await bot.copy_message(
                    chat_id=user_id,
                    from_chat_id=Config.WELCOME_CHANNEL_ID,
                    message_id=Config.WELCOME_MESSAGE_ID
                )
            except Exception as e:
                logger.error(f"Fallback copy failed: {e}")

    is_admin = await check_is_admin(user_id)

    async with db.get_connection() as conn:
        cursor = await conn.execute("SELECT * FROM markets WHERE enabled = 1 ORDER BY id ASC;")
        markets = await cursor.fetchall()

    start_text = (
        f"<b>🚀 Welcome, {user_name}!</b>\n"
        f"──────────────────────────\n"
        f"<b>Telegram Ad Marketplace Network</b>\n\n"
        f"<blockquote>Promote your Telegram channel, group, product, or bot across our network with instant automated posting and verified reach!</blockquote>\n\n"
        f"<b>✨ Features:</b>\n"
        f"• ⚡ <b>Instant Auto-Posting:</b> Released instantly via Stars & Crypto.\n"
        f"• 🪙 <b>Oxapay Automated Payments:</b> BTC, USDT, ETH, TRX & 30+ Cryptos supported.\n\n"
        f"👇 <b>Select a Target Channel Below to Begin:</b>"
    )

    await message.answer(
        text=start_text,
        reply_markup=Keyboards.user_main_menu(markets, is_admin),
        parse_mode=ParseMode.HTML
    )


@user_router.callback_query(F.data == "user:main_menu")
async def cb_user_main_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    is_admin = await check_is_admin(callback.from_user.id)

    async with db.get_connection() as conn:
        cursor = await conn.execute("SELECT * FROM markets WHERE enabled = 1 ORDER BY id ASC;")
        markets = await cursor.fetchall()

    start_text = (
        f"<b>📢 Ad Marketplace Channels</b>\n"
        f"──────────────────────────\n"
        f"Choose your target destination from the list below to book a promo slot:"
    )

    try:
        await callback.message.edit_text(
            text=start_text,
            reply_markup=Keyboards.user_main_menu(markets, is_admin),
            parse_mode=ParseMode.HTML
        )
    except TelegramBadRequest:
        await callback.message.answer(
            text=start_text,
            reply_markup=Keyboards.user_main_menu(markets, is_admin),
            parse_mode=ParseMode.HTML
        )
    await callback.answer()

@user_router.callback_query(F.data.startswith("user:select_market:"))
async def cb_select_market_handler(callback: CallbackQuery, state: FSMContext):
    market_id = int(callback.data.split(":")[2])

    async with db.get_connection() as conn:
        cursor = await conn.execute("SELECT * FROM markets WHERE id = ? AND enabled = 1;", (market_id,))
        market = await cursor.fetchone()

    if not market:
        await callback.answer("⚠️ Selected channel is currently offline.", show_alert=True)
        return

    await state.update_data(market_id=market_id)
    await state.set_state(UserStates.waiting_for_ad)

    prompt_text = (
        f"<b>📌 Target Selected:</b> <code>{clean_html(market['name'])}</code>\n"
        f"──────────────────────────\n"
        f"👥 <b>Audience Size:</b> <code>{clean_html(market['subscribers'])} Subscribers</code>\n"
        f"💰 <b>Pricing Rate:</b> <code>⭐ {market['stars_price']} Stars</code> or <code>${market['usd_price']:.2f} USD (Crypto)</code>\n\n"
        f"<blockquote>📩 <b>Action Required:</b> Please <b>send or forward</b> your promotion content (Photo, Video, Document, or Text) to this chat now.</blockquote>"
    )

    await callback.message.edit_text(
        text=prompt_text,
        reply_markup=Keyboards.cancel_button("user:main_menu"),
        parse_mode=ParseMode.HTML
    )
    await callback.answer()

@user_router.message(UserStates.waiting_for_ad)
async def ad_submission_handler(message: Message, state: FSMContext, bot: Bot):
    loader_msg = await show_live_loader(message, "<i>⏳ Receiving & registering your ad payload...</i>")
    
    data = await state.get_data()
    market_id = data.get("market_id")

    if not market_id:
        await message.answer("⚠️ Session expired. Please tap /start to restart.")
        await state.clear()
        return

    async with db.get_connection() as conn:
        cursor = await conn.execute("SELECT * FROM markets WHERE id = ?;", (market_id,))
        market = await cursor.fetchone()

    if not market:
        await message.answer("⚠️ Selected market is unavailable.")
        await state.clear()
        return

    user_id = message.from_user.id
    raw_username = message.from_user.username
    username_str = f"@{raw_username}" if raw_username else "No Username"
    media_group_id = message.media_group_id
    content_type = message.content_type
    caption = message.caption or message.text or ""

    file_id = None
    if message.photo:
        file_id = message.photo[-1].file_id
    elif message.video:
        file_id = message.video.file_id
    elif message.document:
        file_id = message.document.file_id

    async with db.get_connection() as conn:
        existing_order_id = None
        if media_group_id:
            cursor = await conn.execute(
                "SELECT order_id FROM advertisements WHERE user_id = ? AND media_group_id = ? LIMIT 1;",
                (user_id, media_group_id)
            )
            row = await cursor.fetchone()
            if row:
                existing_order_id = row["order_id"]

        if existing_order_id:
            order_id = existing_order_id
            cursor = await conn.execute("SELECT id FROM advertisements WHERE order_id = ?;", (order_id,))
            ad_row = await cursor.fetchone()
            ad_id = ad_row["id"]

            await conn.execute("""
                INSERT INTO advertisement_media (advertisement_id, message_id, media_type, file_id, caption)
                VALUES (?, ?, ?, ?, ?);
            """, (ad_id, message.message_id, content_type, file_id, caption))
            await conn.commit()
            if loader_msg:
                try:
                    await loader_msg.delete()
                except Exception:
                    pass
            return

        else:
            order_id = generate_order_id()
            cursor = await conn.execute("""
                INSERT INTO advertisements (
                    order_id, user_id, market_id, source_chat_id, source_message_id,
                    media_group_id, content_type, caption, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'WAITING_PAYMENT');
            """, (
                order_id, user_id, market_id, message.chat.id,
                message.message_id, media_group_id, content_type, caption
            ))
            ad_id = cursor.lastrowid

            await conn.execute("""
                INSERT INTO advertisement_media (advertisement_id, message_id, media_type, file_id, caption)
                VALUES (?, ?, ?, ?, ?);
            """, (ad_id, message.message_id, content_type, file_id, caption))

            await conn.execute("""
                INSERT INTO payments (order_id, user_id, method, currency, amount, status)
                VALUES (?, ?, 'UNSELECTED', 'USD', ?, 'WAITING_PAYMENT');
            """, (order_id, user_id, market['usd_price']))

            await conn.commit()

    await state.clear()
    if loader_msg:
        try:
            await loader_msg.delete()
        except Exception:
            pass

    admin_ids = await get_all_admin_ids()

    slot_notice = (
        f"<b>🆕 NEW AD BOOKING REGISTERED</b>\n"
        f"──────────────────────────\n"
        f"📋 <b>Order ID:</b> <code>{order_id}</code>\n"
        f"👤 <b>User:</b> {clean_html(username_str)} (<code>{user_id}</code>)\n"
        f"📢 <b>Target Channel:</b> <code>{clean_html(market['name'])}</code>\n"
        f"📝 <b>Caption Snippet:</b> <i>{clean_html(caption[:150]) if caption else 'None'}</i>"
    )

    admin_slot_buttons = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Approve Slot", callback_data=f"admin:slot_app:{order_id}"),
            InlineKeyboardButton(text="❌ Reject Slot", callback_data=f"admin:slot_rej:{order_id}")
        ]
    ])

    for aid in admin_ids:
        try:
            if file_id and content_type == "photo":
                await bot.send_photo(aid, photo=file_id, caption=slot_notice, reply_markup=admin_slot_buttons, parse_mode=ParseMode.HTML)
            elif file_id and content_type == "video":
                await bot.send_video(aid, video=file_id, caption=slot_notice, reply_markup=admin_slot_buttons, parse_mode=ParseMode.HTML)
            elif file_id and content_type == "document":
                await bot.send_document(aid, document=file_id, caption=slot_notice, reply_markup=admin_slot_buttons, parse_mode=ParseMode.HTML)
            else:
                await bot.send_message(aid, text=slot_notice, reply_markup=admin_slot_buttons, parse_mode=ParseMode.HTML)
        except Exception as err:
            logger.error(f"Failed sending slot alert to admin {aid}: {err}")

    summary_text = (
        f"<b>✅ Ad Content Registered Successfully!</b>\n"
        f"──────────────────────────\n"
        f"📋 <b>Order Reference:</b> <code>{order_id}</code>\n"
        f"📢 <b>Destination:</b> <code>{clean_html(market['name'])}</code>\n\n"
        f"<b>💰 Choose Automated Payment Gateway:</b>\n"
        f"• <b>⭐ Telegram Stars:</b> <code>{market['stars_price']} Stars</code>\n"
        f"• <b>🪙 Oxapay Crypto:</b> <code>${market['usd_price']:.2f} USD</code> (Auto-Published)"
    )

    await message.answer(
        text=summary_text,
        reply_markup=Keyboards.payment_method_selector(order_id, market['stars_price'], market['usd_price']),
        parse_mode=ParseMode.HTML
    )

@user_router.callback_query(F.data.startswith("user:cancel_order:"))
async def cb_cancel_order_handler(callback: CallbackQuery):
    order_id = callback.data.split(":")[2]

    async with db.get_connection() as conn:
        await conn.execute("UPDATE advertisements SET status = 'CANCELLED' WHERE order_id = ?;", (order_id,))
        await conn.execute("UPDATE payments SET status = 'CANCELLED' WHERE order_id = ?;", (order_id,))
        await conn.commit()

    await callback.message.edit_text(text=f"<b>❌ Booking <code>{order_id}</code> has been cancelled.</b>", parse_mode=ParseMode.HTML)
    await callback.answer()

@user_router.callback_query(F.data == "user:my_orders")
async def cb_my_orders_handler(callback: CallbackQuery):
    user_id = callback.from_user.id

    async with db.get_connection() as conn:
        cursor = await conn.execute("""
            SELECT a.order_id, a.status, a.rejection_reason, m.name as market_name, a.created_at, p.post_url
            FROM advertisements a
            JOIN markets m ON a.market_id = m.id
            LEFT JOIN published_ads p ON a.id = p.advertisement_id
            WHERE a.user_id = ?
            ORDER BY a.id DESC LIMIT 10;
        """, (user_id,))
        orders = await cursor.fetchall()

    if not orders:
        text = "<b>📋 My Bookings</b>\n──────────────────────────\nYou have not placed any advertisement bookings yet."
    else:
        text = "<b>📋 Your Recent Bookings:</b>\n──────────────────────────\n\n"
        for o in orders:
            status_badge = "🟢 PUBLISHED" if o["status"] == "PUBLISHED" else "⏳ PENDING" if "WAITING" in o["status"] or "PROCESSING" in o["status"] else "🔴 REJECTED"
            link_str = f' • <a href="{o["post_url"]}">View Post</a>' if o["post_url"] else ""
            reason_str = f"\n   ↳ <i>Reason:</i> <code>{clean_html(o['rejection_reason'])}</code>" if o["rejection_reason"] else ""
            text += f"• <code>{o['order_id']}</code> | <b>{clean_html(o['market_name'])}</b> | <b>{status_badge}</b>{link_str}{reason_str}\n"

    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Main Menu", callback_data="user:main_menu")]])
    await callback.message.edit_text(text=text, reply_markup=markup, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    await callback.answer()

@user_router.callback_query(F.data == "user:help")
async def cb_help_handler(callback: CallbackQuery):
    help_text = (
        "<b>❓ How to Book & Publish Advertisements:</b>\n"
        "──────────────────────────\n"
        "1️⃣ <b>Select Target Channel:</b> Pick a channel from our active market list.\n"
        "2️⃣ <b>Submit Content:</b> Forward or upload your ad photo, video, or text.\n"
        "3️⃣ <b>Choose Oxapay Crypto Payment:</b> Select your preferred cryptocurrency (BTC, USDT, TRX, ETH, etc.).\n"
        "4️⃣ <b>Auto-Verification:</b> As soon as Oxapay confirms transaction, your ad is auto-forwarded & published!"
    )
    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Main Menu", callback_data="user:main_menu")]])
    await callback.message.edit_text(text=help_text, reply_markup=markup, parse_mode=ParseMode.HTML)
    await callback.answer()

# ==========================================
# 6. PAYMENT ROUTER & PUBLISHER ENGINE
# ==========================================

payment_router = Router()

async def publish_advertisement_order(order_id: str, bot: Bot):
    """Automatically forwards and publishes the ad to target channel when paid."""
    async with db.get_connection() as conn:
        cursor = await conn.execute("""
            SELECT a.*, m.channel_id, m.name as market_name
            FROM advertisements a
            JOIN markets m ON a.market_id = m.id
            WHERE a.order_id = ?;
        """, (order_id,))
        ad = await cursor.fetchone()

        if not ad:
            return

        cursor = await conn.execute("SELECT * FROM advertisement_media WHERE advertisement_id = ? ORDER BY id ASC;", (ad["id"],))
        media_items = await cursor.fetchall()

    channel_id = ad["channel_id"]

    try:
        if len(media_items) > 1 and ad["media_group_id"]:
            msg_ids = [m["message_id"] for m in media_items]
            copied_list = await bot.copy_messages(chat_id=channel_id, from_chat_id=ad["source_chat_id"], message_ids=msg_ids)
            published_message_id = copied_list[0].message_id
        else:
            copied_msg = await bot.copy_message(chat_id=channel_id, from_chat_id=ad["source_chat_id"], message_id=ad["source_message_id"])
            published_message_id = copied_msg.message_id

        if str(channel_id).startswith("@"):
            post_url = f"https://t.me/{str(channel_id).replace('@', '')}/{published_message_id}"
        elif str(channel_id).startswith("-100"):
            post_url = f"https://t.me/c/{str(channel_id).replace('-100', '')}/{published_message_id}"
        else:
            post_url = f"https://t.me/{channel_id}/{published_message_id}"

        async with db.get_connection() as conn:
            await conn.execute("UPDATE advertisements SET status = 'PUBLISHED' WHERE order_id = ?;", (order_id,))
            await conn.execute("INSERT INTO published_ads (advertisement_id, channel_id, message_id, post_url) VALUES (?, ?, ?, ?);",
                               (ad["id"], str(channel_id), published_message_id, post_url))
            await conn.commit()

        success_text = (
            f"<b>🎉 Your Slot Has Been Published Live!</b>\n"
            f"──────────────────────────\n"
            f"📢 <b>Destination Channel:</b> {clean_html(ad['market_name'])}\n"
            f"📋 <b>Order ID:</b> <code>{order_id}</code>\n\n"
            f"🔗 <a href='{post_url}'>Click Here to View Live Published Post</a>"
        )
        markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📢 View Live Post", url=post_url)]])
        try:
            await bot.send_message(ad["user_id"], text=success_text, reply_markup=markup, parse_mode=ParseMode.HTML)
        except Exception:
            pass

    except Exception as exc:
        logger.error(f"Publish failure on order {order_id}: {exc}")
        async with db.get_connection() as conn:
            await conn.execute("UPDATE advertisements SET status = 'PUBLISH_FAILED' WHERE order_id = ?;", (order_id,))
            await conn.commit()


@payment_router.callback_query(F.data.startswith("pay:stars:"))
async def cb_pay_stars_handler(callback: CallbackQuery, bot: Bot):
    order_id = callback.data.split(":")[2]

    async with db.get_connection() as conn:
        cursor = await conn.execute("""
            SELECT a.*, m.name as market_name, m.stars_price
            FROM advertisements a
            JOIN markets m ON a.market_id = m.id
            WHERE a.order_id = ?;
        """, (order_id,))
        ad = await cursor.fetchone()

    if not ad:
        await callback.answer("Order reference not found.", show_alert=True)
        return

    stars_price = int(ad["stars_price"])
    prices = [LabeledPrice(label="Ad Placement", amount=stars_price)]

    try:
        await bot.send_invoice(
            chat_id=callback.from_user.id,
            title="Ad Placement Booking",
            description=f"Slot in {ad['market_name']} (#{order_id})",
            payload=f"stars_payload_{order_id}",
            provider_token="",
            currency="XTR",
            prices=prices,
            start_parameter=f"ad-{order_id}"
        )
        await callback.answer("⭐ Invoice generated below!")
    except Exception as e:
        logger.error(f"Failed to issue Stars invoice: {e}")
        await callback.answer("❌ Error generating Stars invoice.", show_alert=True)

@payment_router.pre_checkout_query()
async def process_pre_checkout_query(pre_checkout: PreCheckoutQuery):
    await pre_checkout.answer(ok=True)

@payment_router.message(F.successful_payment)
async def process_successful_payment_handler(message: Message, bot: Bot):
    payment_info = message.successful_payment
    payload = payment_info.invoice_payload

    if not payload.startswith("stars_payload_"):
        return

    order_id = payload.replace("stars_payload_", "")

    async with db.get_connection() as conn:
        await conn.execute("""
            UPDATE payments
            SET method = 'STARS',
                amount = ?,
                telegram_payment_id = ?,
                status = 'SUCCESSFUL',
                verified_at = CURRENT_TIMESTAMP
            WHERE order_id = ?;
        """, (payment_info.total_amount, payment_info.telegram_payment_charge_id, order_id))

        await conn.execute("UPDATE advertisements SET status = 'PAID' WHERE order_id = ?;", (order_id,))
        await conn.commit()

    await message.answer("⚡ <b>Payment Verified via Telegram Stars!</b> Publishing your post now...", parse_mode=ParseMode.HTML)
    await publish_advertisement_order(order_id, bot)


@payment_router.callback_query(F.data.startswith("pay:oxapay:"))
async def cb_pay_oxapay_handler(callback: CallbackQuery):
    order_id = callback.data.split(":")[2]
    await show_live_loader(callback, "<i>🪙 Generating secure Oxapay crypto payment link...</i>")

    async with db.get_connection() as conn:
        cursor = await conn.execute("""
            SELECT a.*, m.name as market_name, m.usd_price
            FROM advertisements a
            JOIN markets m ON a.market_id = m.id
            WHERE a.order_id = ?;
        """, (order_id,))
        ad = await cursor.fetchone()

    if not ad:
        await callback.answer("Order reference not found.", show_alert=True)
        return

    usd_amount = float(ad["usd_price"])

    res = await OxapayClient.create_invoice(
        merchant_key=Config.OXAPAY_MERCHANT_KEY,
        amount=usd_amount,
        order_id=order_id,
        description=f"Ad Slot in {ad['market_name']}"
    )

    if res.get("result") != 100 or "payLink" not in res:
        err_msg = res.get("message", "Unable to reach Oxapay Gateway.")
        logger.error(f"Oxapay Error: {res}")
        await callback.message.edit_text(
            f"❌ <b>Payment Gateway Error:</b> <code>{clean_html(err_msg)}</code>\nPlease contact support or try again later.",
            reply_markup=Keyboards.cancel_button("user:main_menu"),
            parse_mode=ParseMode.HTML
        )
        return

    pay_url = res["payLink"]
    track_id = str(res.get("trackId", ""))

    async with db.get_connection() as conn:
        await conn.execute("""
            UPDATE payments
            SET method = 'OXAPAY',
                currency = 'USD',
                amount = ?,
                oxapay_track_id = ?,
                status = 'WAITING_PAYMENT'
            WHERE order_id = ?;
        """, (usd_amount, track_id, order_id))
        await conn.commit()

    invoice_text = (
        f"<b>🪙 Oxapay Automated Crypto Payment Gateway</b>\n"
        f"──────────────────────────\n"
        f"📋 <b>Order Ref:</b> <code>{order_id}</code>\n"
        f"💰 <b>Total Due:</b> <code>${usd_amount:.2f} USD</code>\n"
        f"📢 <b>Target Channel:</b> <code>{clean_html(ad['market_name'])}</code>\n\n"
        f"<blockquote>👇 Click <b>'Pay with Crypto'</b> to select your coin (USDT, BTC, TRX, ETH, LTC, SOL, etc.). Once payment is sent, tap <b>'Verify Payment'</b> below for instant auto-publishing!</blockquote>"
    )

    await callback.message.edit_text(
        text=invoice_text,
        reply_markup=Keyboards.oxapay_checkout_menu(order_id, pay_url),
        parse_mode=ParseMode.HTML
    )
    await callback.answer()


@payment_router.callback_query(F.data.startswith("pay:check_oxapay:"))
async def cb_check_oxapay_handler(callback: CallbackQuery, bot: Bot):
    order_id = callback.data.split(":")[2]

    async with db.get_connection() as conn:
        cursor = await conn.execute("""
            SELECT p.*, a.user_id, a.status as ad_status
            FROM payments p
            JOIN advertisements a ON p.order_id = a.order_id
            WHERE p.order_id = ?;
        """, (order_id,))
        pay = await cursor.fetchone()

    if not pay or not pay["oxapay_track_id"]:
        await callback.answer("⚠️ No active Oxapay transaction record found.", show_alert=True)
        return

    if pay["status"] == "SUCCESSFUL" or pay["ad_status"] == "PUBLISHED":
        await callback.answer("✅ This booking has already been verified and published live!", show_alert=True)
        return

    await callback.answer("🔍 Checking blockchain status with Oxapay...")

    inquiry = await OxapayClient.check_invoice_status(
        merchant_key=Config.OXAPAY_MERCHANT_KEY,
        track_id=pay["oxapay_track_id"]
    )

    status_code = inquiry.get("result")
    pay_status = str(inquiry.get("status", "")).lower()

    if status_code == 100 and pay_status in ["paid", "complete"]:
        async with db.get_connection() as conn:
            await conn.execute("""
                UPDATE payments
                SET status = 'SUCCESSFUL', verified_at = CURRENT_TIMESTAMP
                WHERE order_id = ?;
            """, (order_id,))
            await conn.execute("UPDATE advertisements SET status = 'PAID' WHERE order_id = ?;", (order_id,))
            await conn.commit()

        await callback.message.edit_text(
            f"⚡ <b>Payment Verified via Oxapay Blockchain!</b>\nPublishing your post to channel instantly...",
            parse_mode=ParseMode.HTML
        )
        await publish_advertisement_order(order_id, bot)

    elif pay_status == "waiting":
        await callback.answer("⏳ Payment not detected yet. Please ensure you sent full amount and wait for network confirmations.", show_alert=True)
    elif pay_status in ["expired", "rejected"]:
        await callback.answer("❌ This Oxapay invoice has expired or was rejected.", show_alert=True)
    else:
        await callback.answer(f"ℹ️ Transaction Status: {pay_status.capitalize() if pay_status else 'Pending'}. Please try again shortly.", show_alert=True)

# ==========================================
# 7. ADMIN ROUTER & HANDLERS
# ==========================================

admin_router = Router()

@admin_router.callback_query(F.data == "admin:main")
async def cb_admin_main(callback: CallbackQuery):
    if not await check_is_admin(callback.from_user.id):
        await callback.answer("Unauthorized.", show_alert=True)
        return

    admin_text = (
        "<b>👑 Admin Control Panel Dashboard</b>\n"
        "──────────────────────────\n"
        "Manage channel network, verify slot bookings, import/export databases, and handle broadcasts."
    )
    await callback.message.edit_text(text=admin_text, reply_markup=Keyboards.admin_main_menu(), parse_mode=ParseMode.HTML)
    await callback.answer()

@admin_router.callback_query(F.data.startswith("admin:slot_app:"))
async def cb_admin_approve_slot(callback: CallbackQuery, bot: Bot):
    if not await check_is_admin(callback.from_user.id):
        return
    order_id = callback.data.split(":")[2]

    async with db.get_connection() as conn:
        await conn.execute("UPDATE advertisements SET status = 'APPROVED' WHERE order_id = ?;", (order_id,))
        cursor = await conn.execute("SELECT user_id FROM advertisements WHERE order_id = ?;", (order_id,))
        ad = await cursor.fetchone()
        await conn.commit()

    if ad:
        try:
            await bot.send_message(ad["user_id"], f"✅ <b>Slot <code>{order_id}</code> Approved!</b> You may now proceed to payment.", parse_mode=ParseMode.HTML)
        except Exception:
            pass

    await callback.answer("Slot Approved!")
    try:
        await callback.message.delete()
    except Exception:
        pass

@admin_router.callback_query(F.data.startswith("admin:slot_rej:"))
async def cb_admin_reject_slot_prompt(callback: CallbackQuery, state: FSMContext):
    if not await check_is_admin(callback.from_user.id):
        return
    order_id = callback.data.split(":")[2]

    await state.update_data(reject_order_id=order_id, reject_type="slot", prompt_msg_id=callback.message.message_id)
    await state.set_state(AdminStates.waiting_for_reject_reason)

    await callback.message.reply(f"✍️ <b>Enter Rejection Reason for Slot <code>{order_id}</code>:</b>", parse_mode=ParseMode.HTML)
    await callback.answer()

@admin_router.message(AdminStates.waiting_for_reject_reason)
async def process_rejection_reason(message: Message, state: FSMContext, bot: Bot):
    if not await check_is_admin(message.from_user.id):
        return

    reason = message.text.strip()
    data = await state.get_data()
    order_id = data.get("reject_order_id")
    prompt_msg_id = data.get("prompt_msg_id")

    await state.clear()

    async with db.get_connection() as conn:
        await conn.execute("UPDATE advertisements SET status = 'REJECTED', rejection_reason = ? WHERE order_id = ?;", (reason, order_id))
        cursor = await conn.execute("SELECT user_id FROM advertisements WHERE order_id = ?;", (order_id,))
        ad = await cursor.fetchone()
        await conn.commit()

    if ad:
        try:
            user_msg = f"❌ <b>Your Booking <code>{order_id}</code> was rejected by Admin.</b>\n\n💬 <b>Reason:</b> <code>{clean_html(reason)}</code>"
            await bot.send_message(ad["user_id"], user_msg, parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.error(f"Failed sending rejection notice to user: {e}")

    await message.answer(f"❌ Order <code>{order_id}</code> marked as REJECTED and notified to user.", parse_mode=ParseMode.HTML)

    if prompt_msg_id:
        try:
            await bot.delete_message(chat_id=message.chat.id, message_id=prompt_msg_id)
        except Exception:
            pass

@admin_router.callback_query(F.data == "admin:markets")
async def cb_admin_markets_list(callback: CallbackQuery):
    if not await check_is_admin(callback.from_user.id):
        return

    async with db.get_connection() as conn:
        cursor = await conn.execute("SELECT * FROM markets ORDER BY id ASC;")
        markets = await cursor.fetchall()

    text = "📢 <b>Channel Network Management</b>\n──────────────────────────\nClick <b>👥 Subs</b> to modify subscriber badge:"
    await callback.message.edit_text(text=text, reply_markup=Keyboards.admin_markets_menu(markets), parse_mode=ParseMode.HTML)
    await callback.answer()

@admin_router.callback_query(F.data.startswith("admin:mkt_editsubs:"))
async def cb_admin_mkt_editsubs(callback: CallbackQuery, state: FSMContext):
    if not await check_is_admin(callback.from_user.id):
        return

    market_id = int(callback.data.split(":")[2])
    await state.update_data(edit_market_id=market_id)
    await state.set_state(AdminStates.edit_market_subs)

    await callback.message.edit_text("👥 <b>Enter New Subscriber Metric</b> (e.g., <code>25K+</code>, <code>100K+</code>):", parse_mode=ParseMode.HTML)
    await callback.answer()

@admin_router.message(AdminStates.edit_market_subs)
async def process_edit_market_subs(message: Message, state: FSMContext):
    if not await check_is_admin(message.from_user.id):
        return

    new_subs = message.text.strip()
    data = await state.get_data()
    market_id = data.get("edit_market_id")
    await state.clear()

    async with db.get_connection() as conn:
        await conn.execute("UPDATE markets SET subscribers = ? WHERE id = ?;", (new_subs, market_id))
        await conn.commit()

    await message.answer(f"✅ Subscriber count metric updated to <b>{clean_html(new_subs)}</b>!", parse_mode=ParseMode.HTML)

@admin_router.callback_query(F.data.startswith("admin:mkt_toggle:"))
async def cb_admin_mkt_toggle(callback: CallbackQuery):
    if not await check_is_admin(callback.from_user.id):
        return
    m_id = int(callback.data.split(":")[2])
    async with db.get_connection() as conn:
        await conn.execute("UPDATE markets SET enabled = CASE WHEN enabled = 1 THEN 0 ELSE 1 END WHERE id = ?;", (m_id,))
        await conn.commit()
    await cb_admin_markets_list(callback)

@admin_router.callback_query(F.data.startswith("admin:mkt_del:"))
async def cb_admin_mkt_delete(callback: CallbackQuery):
    if not await check_is_admin(callback.from_user.id):
        return
    m_id = int(callback.data.split(":")[2])
    async with db.get_connection() as conn:
        await conn.execute("DELETE FROM markets WHERE id = ?;", (m_id,))
        await conn.commit()
    await cb_admin_markets_list(callback)

@admin_router.callback_query(F.data == "admin:mkt_add")
async def cb_admin_mkt_add_start(callback: CallbackQuery, state: FSMContext):
    if not await check_is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.add_market_name)
    await callback.message.edit_text("➕ <b>Step 1/5:</b> Enter Channel Display Name (e.g. <code>📢 @PaidMP</code>):", parse_mode=ParseMode.HTML)
    await callback.answer()

@admin_router.message(AdminStates.add_market_name)
async def process_add_market_name(message: Message, state: FSMContext):
    await state.update_data(m_name=message.text.strip())
    await state.set_state(AdminStates.add_market_channel)
    await message.answer("➕ <b>Step 2/5:</b> Enter Channel Username/ID (e.g. <code>@MyChannel</code> or <code>-100...</code>):", parse_mode=ParseMode.HTML)

@admin_router.message(AdminStates.add_market_channel)
async def process_add_market_channel(message: Message, state: FSMContext, bot: Bot):
    channel_input = message.text.strip()
    try:
        chat_info = await bot.get_chat(channel_input)
        bot_member = await bot.get_chat_member(chat_info.id, bot.id)
        if bot_member.status not in ("administrator", "creator"):
            await message.answer("❌ Bot must be added as an Administrator in that channel first.")
            return
    except Exception as err:
        await message.answer(f"❌ Unable to access channel <code>{clean_html(channel_input)}</code>: <code>{clean_html(str(err))}</code>", parse_mode=ParseMode.HTML)
        return

    await state.update_data(m_channel=channel_input)
    await state.set_state(AdminStates.add_market_subs)
    await message.answer("➕ <b>Step 3/5:</b> Enter Channel Subscriber Metric (e.g. <code>15.5K</code>):", parse_mode=ParseMode.HTML)

@admin_router.message(AdminStates.add_market_subs)
async def process_add_market_subs(message: Message, state: FSMContext):
    await state.update_data(m_subs=message.text.strip())
    await state.set_state(AdminStates.add_market_stars)
    await message.answer("➕ <b>Step 4/5:</b> Enter Stars Price (e.g. <code>29</code>):", parse_mode=ParseMode.HTML)

@admin_router.message(AdminStates.add_market_stars)
async def process_add_market_stars(message: Message, state: FSMContext):
    try:
        stars = int(message.text.strip())
    except ValueError:
        await message.answer("❌ Invalid number.")
        return

    await state.update_data(m_stars=stars)
    await state.set_state(AdminStates.add_market_usd)
    await message.answer("➕ <b>Step 5/5:</b> Enter Crypto USD Price (e.g. <code>2.50</code>):", parse_mode=ParseMode.HTML)

@admin_router.message(AdminStates.add_market_usd)
async def process_add_market_usd(message: Message, state: FSMContext):
    try:
        usd_val = float(message.text.strip())
    except ValueError:
        await message.answer("❌ Invalid number.")
        return

    data = await state.get_data()
    await state.clear()

    async with db.get_connection() as conn:
        await conn.execute("""
            INSERT INTO markets (name, channel_id, subscribers, stars_price, usd_price, enabled)
            VALUES (?, ?, ?, ?, ?, 1);
        """, (data["m_name"], data["m_channel"], data["m_subs"], data["m_stars"], usd_val))
        await conn.commit()

    await message.answer(f"✅ Channel <b>{clean_html(data['m_name'])}</b> added successfully!", parse_mode=ParseMode.HTML)

@admin_router.callback_query(F.data == "admin:backup")
async def cb_admin_backup(callback: CallbackQuery):
    if callback.from_user.id not in Config.SUPER_OWNER_IDS:
        await callback.answer("Super Owner privilege required.", show_alert=True)
        return

    if os.path.exists(Config.DATABASE_PATH):
        doc = FSInputFile(Config.DATABASE_PATH, filename=f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db")
        await callback.message.answer_document(document=doc, caption="💾 <b>Database File Backup</b>", parse_mode=ParseMode.HTML)
        await callback.answer("Database file exported!")

@admin_router.callback_query(F.data == "admin:import_db")
async def cb_admin_import_db_prompt(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in Config.SUPER_OWNER_IDS:
        await callback.answer("Super Owner privilege required.", show_alert=True)
        return

    await state.set_state(AdminStates.waiting_for_db_import)
    await callback.message.edit_text(
        "📤 <b>Import Database Replacement</b>\n──────────────────────────\nPlease upload a valid <code>.db</code> file to restore.",
        reply_markup=Keyboards.cancel_button("admin:main"),
        parse_mode=ParseMode.HTML
    )
    await callback.answer()

@admin_router.message(AdminStates.waiting_for_db_import, F.document)
async def process_import_db_file(message: Message, state: FSMContext, bot: Bot):
    if message.from_user.id not in Config.SUPER_OWNER_IDS:
        return

    doc = message.document
    if not doc.file_name.endswith(".db"):
        await message.answer("❌ Invalid format. File must end in <code>.db</code>")
        return

    temp_path = "temp_restore.db"
    await bot.download(doc, destination=temp_path)

    try:
        async with aiosqlite.connect(temp_path) as conn:
            cursor = await conn.execute("SELECT count(*) FROM sqlite_master;")
            await cursor.fetchone()
    except Exception as e:
        await message.answer(f"❌ Corrupted Database File: <code>{clean_html(str(e))}</code>", parse_mode=ParseMode.HTML)
        if os.path.exists(temp_path):
            os.remove(temp_path)
        return

    await state.clear()

    if os.path.exists(Config.DATABASE_PATH):
        os.remove(Config.DATABASE_PATH)
    os.rename(temp_path, Config.DATABASE_PATH)

    await db.init_db()

    await message.answer("✅ <b>Database successfully restored!</b>", parse_mode=ParseMode.HTML)

@admin_router.callback_query(F.data.startswith("admin:ads:"))
async def cb_admin_ads_log(callback: CallbackQuery):
    if not await check_is_admin(callback.from_user.id):
        return
    async with db.get_connection() as conn:
        cursor = await conn.execute("SELECT * FROM advertisements ORDER BY id DESC LIMIT 15;")
        ads = await cursor.fetchall()

    text = "📋 <b>Recent Submissions:</b>\n──────────────────────────\n\n"
    for a in ads:
        text += f"• <code>{a['order_id']}</code> | User: <code>{a['user_id']}</code> | Status: <b>{a['status']}</b>\n"

    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Back", callback_data="admin:main")]])
    await callback.message.edit_text(text=text, reply_markup=markup, parse_mode=ParseMode.HTML)
    await callback.answer()

@admin_router.callback_query(F.data == "admin:stats")
async def cb_admin_stats(callback: CallbackQuery):
    if not await check_is_admin(callback.from_user.id):
        return

    async with db.get_connection() as conn:
        total_users = (await (await conn.execute("SELECT COUNT(*) FROM users;")).fetchone())[0]
        total_ads = (await (await conn.execute("SELECT COUNT(*) FROM advertisements;")).fetchone())[0]
        published_ads = (await (await conn.execute("SELECT COUNT(*) FROM advertisements WHERE status = 'PUBLISHED';")).fetchone())[0]
        stars_total = (await (await conn.execute("SELECT SUM(amount) FROM payments WHERE method = 'STARS' AND status = 'SUCCESSFUL';")).fetchone())[0] or 0
        crypto_total = (await (await conn.execute("SELECT SUM(amount) FROM payments WHERE method = 'OXAPAY' AND status = 'SUCCESSFUL';")).fetchone())[0] or 0.0

    stats_text = (
        f"<b>📊 Platform Analytics</b>\n"
        f"──────────────────────────\n"
        f"👥 <b>Total Registered Users:</b> <code>{total_users}</code>\n"
        f"📋 <b>Total Bookings:</b> <code>{total_ads}</code>\n"
        f"✅ <b>Successfully Published:</b> <code>{published_ads}</code>\n\n"
        f"⭐ <b>Stars Revenue:</b> <code>{int(stars_total)} Stars</code>\n"
        f"🪙 <b>Oxapay Crypto Revenue:</b> <code>${crypto_total:.2f} USD</code>"
    )

    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Back", callback_data="admin:main")]])
    await callback.message.edit_text(text=stats_text, reply_markup=markup, parse_mode=ParseMode.HTML)
    await callback.answer()

@admin_router.callback_query(F.data == "admin:manage_admins")
async def cb_admin_manage_admins(callback: CallbackQuery):
    if callback.from_user.id not in Config.SUPER_OWNER_IDS:
        return

    async with db.get_connection() as conn:
        cursor = await conn.execute("SELECT * FROM admins;")
        admin_list = await cursor.fetchall()

    text = "<b>👑 Admin Directory:</b>\n\n" + "\n".join([f"• <code>{a['telegram_id']}</code>" for a in admin_list])
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Grant Admin Access", callback_data="admin:add_admin_prompt")],
        [InlineKeyboardButton(text="🔙 Back", callback_data="admin:main")]
    ])
    await callback.message.edit_text(text=text, reply_markup=markup, parse_mode=ParseMode.HTML)
    await callback.answer()

@admin_router.callback_query(F.data == "admin:add_admin_prompt")
async def cb_add_admin_prompt(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in Config.SUPER_OWNER_IDS:
        return
    await state.set_state(AdminStates.add_admin_id)
    await callback.message.edit_text("Send the Numeric Telegram User ID of the new Admin:", parse_mode=ParseMode.HTML)
    await callback.answer()

@admin_router.message(AdminStates.add_admin_id)
async def process_add_admin_id(message: Message, state: FSMContext):
    if message.from_user.id not in Config.SUPER_OWNER_IDS:
        return
    try:
        new_id = int(message.text.strip())
    except ValueError:
        await message.answer("Invalid ID format.")
        return

    await state.clear()
    async with db.get_connection() as conn:
        await conn.execute("INSERT OR IGNORE INTO admins (telegram_id, role) VALUES (?, 'ADMIN');", (new_id,))
        await conn.commit()

    await message.answer(f"✅ Granted Admin privileges to User ID <code>{new_id}</code>.", parse_mode=ParseMode.HTML)

@admin_router.callback_query(F.data == "admin:broadcast")
async def cb_admin_broadcast_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in Config.SUPER_OWNER_IDS:
        return
    await state.set_state(AdminStates.broadcast_message)
    await callback.message.edit_text("📢 Send the message/media payload to broadcast to all users:")
    await callback.answer()

@admin_router.message(AdminStates.broadcast_message)
async def process_broadcast_message(message: Message, state: FSMContext, bot: Bot):
    if message.from_user.id not in Config.SUPER_OWNER_IDS:
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

    await message.answer(f"<b>📢 Mass Broadcast Completed!</b>\n\n✅ Delivered: <code>{success}</code>\n❌ Failed: <code>{failed}</code>", parse_mode=ParseMode.HTML)

# ==========================================
# 8. MAIN ENTRYPOINT
# ==========================================

async def main():
    logger.info("Initializing database schema...")
    await db.init_db()

    bot = Bot(token=Config.BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())

    dp.include_router(user_router)
    dp.include_router(payment_router)
    dp.include_router(admin_router)

    logger.info("Bot service online with Oxapay & Premium Emoji Welcome handler. Starting polling...")
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
