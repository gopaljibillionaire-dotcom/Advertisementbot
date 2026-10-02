import asyncio
from datetime import datetime
import html
import os
import secrets
import string
from typing import List, Optional, Union

import aiohttp
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
from motor.motor_asyncio import AsyncIOMotorClient

from config import Config, logger

# ==========================================
# 1. MONGODB DATABASE SYSTEM (ISOLATED)
# ==========================================

class MongoDatabase:
    def __init__(self, uri: str, db_name: str, prefix: str):
        self.client = AsyncIOMotorClient(uri)
        self.db = self.client[db_name]
        self.prefix = prefix

        # Collection handles with project prefix isolation
        self.users = self.db[f"{prefix}_users"]
        self.markets = self.db[f"{prefix}_markets"]
        self.advertisements = self.db[f"{prefix}_advertisements"]
        self.payments = self.db[f"{prefix}_payments"]
        self.withdrawals = self.db[f"{prefix}_withdrawals"]
        self.admins = self.db[f"{prefix}_admins"]
        self.settings = self.db[f"{prefix}_settings"]

    async def init_db(self):
        # Create indexes
        await self.users.create_index("telegram_id", unique=True)
        await self.advertisements.create_index("order_id", unique=True)
        await self.payments.create_index("oxapay_track_id")
        await self.admins.create_index("telegram_id", unique=True)

        # Initialize Default Settings
        maintenance = await self.settings.find_one({"key": "maintenance_mode"})
        if not maintenance:
            await self.settings.insert_one({"key": "maintenance_mode", "value": False})

        # Seed initial markets if empty
        if await self.markets.count_documents({}) == 0:
            await self.markets.insert_many([
                {
                    "name": "PostsMarket",
                    "channel_id": "@PostsMarket",
                    "channel_username": "PostsMarket",
                    "subscribers": "50K+",
                    "stars_price": 29,
                    "usd_price": 2.50,
                    "enabled": True,
                    "created_at": datetime.utcnow()
                },
                {
                    "name": "PublicVerified",
                    "channel_id": "@PublicVerified",
                    "channel_username": "PublicVerified",
                    "subscribers": "10K+",
                    "stars_price": 12,
                    "usd_price": 1.00,
                    "enabled": True,
                    "created_at": datetime.utcnow()
                }
            ])
        logger.info(f"MongoDB initialized with Prefix: '{self.prefix}_'")

    async def is_maintenance(self) -> bool:
        doc = await self.settings.find_one({"key": "maintenance_mode"})
        return doc.get("value", False) if doc else False

    async def toggle_maintenance() -> bool:
        curr = await self.is_maintenance()
        new_val = not curr
        await self.settings.update_one({"key": "maintenance_mode"}, {"$set": {"value": new_val}}, upsert=True)
        return new_val

    async def upsert_user(self, telegram_id: int, username: Optional[str], first_name: Optional[str]):
        await self.users.update_one(
            {"telegram_id": telegram_id},
            {
                "$set": {
                    "username": username,
                    "first_name": first_name,
                    "last_seen": datetime.utcnow()
                },
                "$setOnInsert": {
                    "stars_balance": 0,
                    "usd_balance": 0.0,
                    "total_deposits": 0.0,
                    "total_spent": 0.0,
                    "is_blocked": False,
                    "created_at": datetime.utcnow()
                }
            },
            upsert=True
        )

    async def get_user(self, telegram_id: int) -> Optional[dict]:
        return await self.users.find_one({"telegram_id": telegram_id})

    async def update_balances(
        self, telegram_id: int, stars_delta: int = 0, usd_delta: float = 0.0, deposit_delta: float = 0.0, spent_delta: float = 0.0
    ):
        await self.users.update_one(
            {"telegram_id": telegram_id},
            {
                "$inc": {
                    "stars_balance": stars_delta,
                    "usd_balance": usd_delta,
                    "total_deposits": deposit_delta,
                    "total_spent": spent_delta
                }
            }
        )


db = MongoDatabase(Config.MONGO_URI, Config.MONGO_DB_NAME, Config.PROJECT_PREFIX)

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
    edit_market_stars = State()
    edit_market_usd = State()
    broadcast_message = State()
    add_admin_id = State()


def clean_html(text: Optional[str]) -> str:
    return html.escape(str(text)) if text else ""


def generate_req_id(prefix: str = "REQ") -> str:
    chars = string.ascii_uppercase + string.digits
    return f"{prefix}-{''.join(secrets.choice(chars) for _ in range(6))}"


async def check_is_admin(telegram_id: int) -> bool:
    if telegram_id in Config.SUPER_OWNER_IDS:
        return True
    admin = await db.admins.find_one({"telegram_id": telegram_id})
    return admin is not None


async def get_all_admin_ids() -> List[int]:
    admin_ids = set(Config.SUPER_OWNER_IDS)
    async for admin in db.admins.find({}, {"telegram_id": 1}):
        admin_ids.add(admin["telegram_id"])
    return list(admin_ids)


async def publish_ad_to_channel(bot: Bot, channel_id: str, ad_data: dict) -> bool:
    try:
        content_type = ad_data.get("content_type")
        raw_caption = ad_data.get("caption_text", "")
        file_id = ad_data.get("file_id")
        quoted_caption = f"<blockquote>{clean_html(raw_caption)}</blockquote>" if raw_caption else ""

        if file_id:
            if content_type == "photo":
                await bot.send_photo(chat_id=channel_id, photo=file_id, caption=quoted_caption, parse_mode=ParseMode.HTML)
            elif content_type == "video":
                await bot.send_video(chat_id=channel_id, video=file_id, caption=quoted_caption, parse_mode=ParseMode.HTML)
            elif content_type == "document":
                await bot.send_document(chat_id=channel_id, document=file_id, caption=quoted_caption, parse_mode=ParseMode.HTML)
            else:
                await bot.copy_message(chat_id=channel_id, from_chat_id=ad_data.get("source_chat_id"), message_id=ad_data.get("source_message_id"))
        else:
            await bot.send_message(chat_id=channel_id, text=quoted_caption or "<blockquote>New Advertisement Post</blockquote>", parse_mode=ParseMode.HTML)
        return True
    except Exception as e:
        logger.error(f"Failed to publish ad to channel {channel_id}: {e}")
        return False


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
            except Exception:
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
            except Exception:
                pass

        await event.answer(text=caption, reply_markup=reply_markup, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

# ==========================================
# 3. OXAPAY PAYMENT API CLIENT & PROCESSOR
# ==========================================

class OxapayClient:
    BASE_URL = "https://api.oxapay.com/merchants"

    @staticmethod
    async def create_invoice(
        merchant_key: str, amount: float, order_id: str, description: str, pay_currency: Optional[str] = None
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
        payload = {"merchant": merchant_key, "trackId": str(track_id)}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=15) as resp:
                    return await resp.json()
        except Exception as err:
            logger.error(f"Oxapay API Inquiry Error: {err}")
            return {"result": 500, "message": str(err)}


async def process_successful_crypto_payment(pay_doc: dict, bot: Bot):
    payment_id = pay_doc["_id"]
    order_id = pay_doc["order_id"]
    user_id = pay_doc["user_id"]
    amount = float(pay_doc["amount"])

    p = await db.payments.find_one({"_id": payment_id})
    if not p or p.get("status") == "PAID":
        return

    await db.payments.update_one({"_id": payment_id}, {"$set": {"status": "PAID", "verified_at": datetime.utcnow()}})

    if order_id.startswith("AD-"):
        await db.advertisements.update_many({"order_id": order_id}, {"$set": {"status": "PAID"}})
        await db.update_balances(user_id, spent_delta=amount)
        msg = f"<b>✨ CRYPTO PAYMENT VERIFIED!</b>\nOrder ID: <code>{order_id}</code>\nAmount Paid: <code>${amount:.2f} USD</code>"
    else:
        await db.update_balances(user_id, usd_delta=amount, deposit_delta=amount)
        msg = f"<b>✨ CRYPTO DEPOSIT CREDITED!</b>\nRef: <code>{order_id}</code>\nAmount: <code>${amount:.2f} USD</code>"

    try:
        await bot.send_message(user_id, msg, parse_mode=ParseMode.HTML)
    except Exception:
        pass


async def oxapay_polling_task(bot: Bot):
    while True:
        try:
            await asyncio.sleep(30)
            async for pay in db.payments.find({"method": "OXAPAY", "status": "WAITING_PAYMENT", "oxapay_track_id": {"$exists": True}}):
                track_id = pay["oxapay_track_id"]
                res = await OxapayClient.check_payment(Config.OXAPAY_MERCHANT_KEY, track_id)
                if res.get("result") == 100:
                    status = str(res.get("status", "")).lower()
                    if status in ["paid", "complete"]:
                        await process_successful_crypto_payment(pay, bot)
                    elif status == "expired":
                        await db.payments.update_one({"_id": pay["_id"]}, {"$set": {"status": "EXPIRED"}})
        except Exception as e:
            logger.error(f"Error in OxaPay Background Poller: {e}")

# ==========================================
# 4. KEYBOARD BUILDERS
# ==========================================

class Keyboards:

    @staticmethod
    def main_menu(is_admin: bool = False) -> InlineKeyboardMarkup:
        builder = [
            [InlineKeyboardButton(text="Forward", callback_data="btn:forward"), InlineKeyboardButton(text="Pin", callback_data="btn:pin")],
            [InlineKeyboardButton(text="Profile", callback_data="btn:profile"), InlineKeyboardButton(text="Wallet", callback_data="btn:wallet", style="success")],
            [InlineKeyboardButton(text="Contact Support", url=Config.SUPPORT_LINK)],
            [InlineKeyboardButton(text="Change Language", callback_data="btn:change_lang")],
            [InlineKeyboardButton(text="Host Giveaway (Pre-paid)", callback_data="btn:host_giveaway")]
        ]
        if is_admin:
            builder.append([InlineKeyboardButton(text="Admin Dashboard 🛠", callback_data="admin:main", style="danger")])
        return InlineKeyboardMarkup(inline_keyboard=builder)

    @staticmethod
    def main_menu_only() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Main menu", callback_data="user:main_menu")]])

    @staticmethod
    def forward_menu() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Continue", callback_data="btn:forward_continue")],
            [InlineKeyboardButton(text="Back", callback_data="user:main_menu", style="danger"), InlineKeyboardButton(text="Recharge wallet", callback_data="btn:recharge_wallet", style="success")],
            [InlineKeyboardButton(text="Main menu", callback_data="user:main_menu")]
        ])

    @staticmethod
    def wallet_menu() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Deposit", callback_data="btn:deposit", style="success"), InlineKeyboardButton(text="Withdraw", callback_data="btn:withdraw", style="danger")],
            [InlineKeyboardButton(text="Back to Main menu", callback_data="user:main_menu")],
            [InlineKeyboardButton(text="Contact Support", url=Config.SUPPORT_LINK)]
        ])

    @staticmethod
    def payment_options_menu(order_id: Optional[str] = None) -> InlineKeyboardMarkup:
        suffix = f":{order_id}" if order_id else ""
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Pay via Stars", callback_data=f"pay_opt:stars{suffix}"), InlineKeyboardButton(text="Pay via USD Balance", callback_data=f"pay_opt:usd_bal{suffix}", style="success")],
            [InlineKeyboardButton(text="Pay via Crypto", callback_data=f"pay_opt:crypto{suffix}", style="success")]
        ])

    @staticmethod
    def crypto_coin_menu(target_id: str, is_deposit: bool = False, amount: float = 0.0) -> InlineKeyboardMarkup:
        prefix = f"coin_dep:{amount:.2f}:" if is_deposit else f"coin_ad:{target_id}:"
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="USDT ₮", callback_data=f"{prefix}USDT", style="success"), InlineKeyboardButton(text="BTC ₿", callback_data=f"{prefix}BTC"), InlineKeyboardButton(text="ETH Ξ", callback_data=f"{prefix}ETH")],
            [InlineKeyboardButton(text="LTC Ł", callback_data=f"{prefix}LTC"), InlineKeyboardButton(text="TON 💎", callback_data=f"{prefix}TON", style="success"), InlineKeyboardButton(text="TRX ⚡", callback_data=f"{prefix}TRX")],
            [InlineKeyboardButton(text="🌐 All Cryptos (OxaPay)", callback_data=f"{prefix}ALL")],
            [InlineKeyboardButton(text="Main menu", callback_data="user:main_menu", style="danger")]
        ])

    @staticmethod
    def crypto_invoice_menu(pay_url: str, track_id: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Pay via OxaPay", url=pay_url, style="success")],
            [InlineKeyboardButton(text="Check Payment Status 🔄", callback_data=f"verify_crypto:{track_id}")],
            [InlineKeyboardButton(text="Main menu", callback_data="user:main_menu", style="danger")]
        ])

    @staticmethod
    def market_selection_menu(markets: list, selected_ids: List[str]) -> InlineKeyboardMarkup:
        keyboard = []
        for m in markets:
            m_id = str(m["_id"])
            checked = "[X] " if m_id in selected_ids else ""
            keyboard.append([InlineKeyboardButton(
                text=f"{checked}{m['name']} ({m['subscribers']}) - Stars: {m['stars_price']} / ${m['usd_price']}",
                callback_data=f"mkt_toggle:{m_id}"
            )])
        keyboard.append([InlineKeyboardButton(text="Continue", callback_data="mkt_confirm_selection")])
        keyboard.append([InlineKeyboardButton(text="Back", callback_data="btn:forward", style="danger"), InlineKeyboardButton(text="Recharge wallet", callback_data="btn:recharge_wallet", style="success")])
        keyboard.append([InlineKeyboardButton(text="Main menu", callback_data="user:main_menu")])
        return InlineKeyboardMarkup(inline_keyboard=keyboard)

    @staticmethod
    def admin_main_menu(is_maint: bool = False) -> InlineKeyboardMarkup:
        maint_text = "Maintenance Mode: 🔴 ON" if is_maint else "Maintenance Mode: 🟢 OFF"
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Manage Channels / Markets 📢", callback_data="admin:markets")],
            [InlineKeyboardButton(text="Database & Financial Stats 📊", callback_data="admin:db_stats")],
            [InlineKeyboardButton(text="All Bookings 📑", callback_data="admin:ads:all"), InlineKeyboardButton(text="Users List 👥", callback_data="admin:users")],
            [InlineKeyboardButton(text="Mass Broadcast 📢", callback_data="admin:broadcast", style="success")],
            [InlineKeyboardButton(text=maint_text, callback_data="admin:toggle_maint")],
            [InlineKeyboardButton(text="Admin Privileges 🔑", callback_data="admin:manage_admins")],
            [InlineKeyboardButton(text="Exit Admin View 🚪", callback_data="user:main_menu", style="danger")]
        ])

    @staticmethod
    def admin_markets_menu(markets: list) -> InlineKeyboardMarkup:
        keyboard = []
        for m in markets:
            status = "ON" if m.get("enabled", True) else "OFF"
            m_id = str(m["_id"])
            keyboard.append([
                InlineKeyboardButton(text=f"[{status}] {m['name']}", callback_data=f"admin:mkt_view:{m_id}"),
                InlineKeyboardButton(text="Toggle 🔄", callback_data=f"admin:mkt_toggle:{m_id}"),
                InlineKeyboardButton(text="Edit Prices ✏️", callback_data=f"admin:mkt_edit:{m_id}"),
                InlineKeyboardButton(text="Delete 🗑", callback_data=f"admin:mkt_del:{m_id}", style="danger")
            ])
        keyboard.append([InlineKeyboardButton(text="Add New Channel / Market ➕", callback_data="admin:mkt_add", style="success")])
        keyboard.append([InlineKeyboardButton(text="Back to Dashboard", callback_data="admin:main", style="danger")])
        return InlineKeyboardMarkup(inline_keyboard=keyboard)

# ==========================================
# 5. USER ROUTER & INTERFACE HANDLERS
# ==========================================

user_router = Router()

MAIN_TEXT = (
    "<b>Welcome to @Paytoforwardbot</b>\n\n"
    "Want to forward or purchase Pins in markets provided by Core Creations ?\n"
    "Load the wallet, Send your advertisement to the bot and get it forwarded through supported markets.\n"
    "<b>Payment :</b> Telegram Stars / Crypto / USD Balance\n\n"
    "<b>Powered by @CoreCreations</b>"
)

@user_router.message(CommandStart())
async def cmd_start_handler(message: Message, state: FSMContext, bot: Bot):
    if await db.is_maintenance() and not await check_is_admin(message.from_user.id):
        await message.answer("⚠️ Bot is currently under maintenance. Please try again later.")
        return

    await state.clear()
    user = message.from_user
    await db.upsert_user(user.id, user.username, user.first_name)
    is_admin = await check_is_admin(user.id)

    await send_or_edit_photo(
        event=message, photo_path="core.jpg", caption=MAIN_TEXT, reply_markup=Keyboards.main_menu(is_admin), bot=bot
    )


@user_router.callback_query(F.data == "user:main_menu")
async def cb_user_main_menu(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await state.clear()
    is_admin = await check_is_admin(callback.from_user.id)
    await send_or_edit_photo(
        event=callback, photo_path="core.jpg", caption=MAIN_TEXT, reply_markup=Keyboards.main_menu(is_admin), bot=bot
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
        f"<b>Profile of {display_user}</b>\n\n"
        f"<b>Total deposits :</b> {deposits}\n"
        f"<b>Total amount spent :</b> {spent}"
    )
    await send_or_edit_photo(event=callback, photo_path="profile.jpg", caption=caption, reply_markup=Keyboards.main_menu_only(), bot=bot)
    await callback.answer()


@user_router.callback_query(F.data.in_({"btn:forward", "btn:pin"}))
async def cb_forward_handler(callback: CallbackQuery, bot: Bot):
    caption = "<b>Forward your advertisement -</b>\n\n<b>After payment I will forward it in selected market channel(s)</b>"
    await send_or_edit_photo(event=callback, photo_path="forward.jpg", caption=caption, reply_markup=Keyboards.forward_menu(), bot=bot)
    await callback.answer()


@user_router.callback_query(F.data == "btn:forward_continue")
async def cb_forward_continue(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await state.set_state(UserStates.waiting_for_ad)
    await state.update_data(selected_markets=[])
    caption = "Send or forward your advertisement payload (Photo, Video, Document, or Text) to this chat now:"
    await send_or_edit_photo(event=callback, photo_path="forward.jpg", caption=caption, reply_markup=Keyboards.main_menu_only(), bot=bot)
    await callback.answer()


@user_router.message(UserStates.waiting_for_ad)
async def process_ad_content(message: Message, state: FSMContext, bot: Bot):
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

    await state.update_data(
        ad_order_id=order_id,
        source_chat_id=message.chat.id,
        source_message_id=message.message_id,
        content_type=content_type,
        caption_text=caption_text,
        file_id=file_id,
        selected_markets=[]
    )

    markets = await db.markets.find({"enabled": True}).to_list(length=100)
    if not markets:
        await message.answer("No channels are available for booking currently. Please check back later.")
        return

    u = await db.get_user(user_id)
    postmarket_caption = (
        f"<b>Wallet balance: Stars {u.get('stars_balance', 0) if u else 0} | USD ${u.get('usd_balance', 0.0):.2f}</b>\n\n"
        f"<b>Select channels below to post your ad:</b>"
    )

    await send_or_edit_photo(event=message, photo_path="postsmarket.jpg", caption=postmarket_caption, reply_markup=Keyboards.market_selection_menu(markets, []), bot=bot)


@user_router.callback_query(F.data.startswith("mkt_toggle:"))
async def cb_market_toggle(callback: CallbackQuery, state: FSMContext, bot: Bot):
    m_id = callback.data.split(":")[1]
    data = await state.get_data()
    selected = data.get("selected_markets", [])

    if m_id in selected:
        selected.remove(m_id)
    else:
        selected.append(m_id)

    await state.update_data(selected_markets=selected)
    markets = await db.markets.find({"enabled": True}).to_list(length=100)
    u = await db.get_user(callback.from_user.id)

    postmarket_caption = (
        f"<b>Wallet balance: Stars {u.get('stars_balance', 0) if u else 0} | USD ${u.get('usd_balance', 0.0):.2f}</b>\n\n"
        f"<b>Select channels below to post your ad:</b>"
    )

    await send_or_edit_photo(event=callback, photo_path="postsmarket.jpg", caption=postmarket_caption, reply_markup=Keyboards.market_selection_menu(markets, selected), bot=bot)
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

    from bson import ObjectId
    for mkt_id in selected:
        await db.advertisements.insert_one({
            "order_id": order_id,
            "user_id": user_id,
            "market_id": ObjectId(mkt_id),
            "source_chat_id": data.get("source_chat_id"),
            "source_message_id": data.get("source_message_id"),
            "content_type": data.get("content_type"),
            "caption": data.get("caption_text"),
            "status": "PENDING_PAYMENT",
            "created_at": datetime.utcnow()
        })

    caption = "<b>Select payment option below:</b>"
    await send_or_edit_photo(event=callback, photo_path="paymentoptions.jpg", caption=caption, reply_markup=Keyboards.payment_options_menu(order_id), bot=bot)
    await callback.answer()


@user_router.callback_query(F.data == "btn:wallet")
@user_router.callback_query(F.data == "btn:recharge_wallet")
async def cb_wallet_handler(callback: CallbackQuery, bot: Bot):
    u = await db.get_user(callback.from_user.id)
    stars_bal = u.get("stars_balance", 0) if u else 0
    usd_bal = u.get("usd_balance", 0.0) if u else 0.0

    caption = (
        f"<b>Your current balance -</b>\n"
        f"<b>Telegram Stars:</b> <code>{stars_bal}</code>\n"
        f"<b>USD Balance:</b> <code>${usd_bal:.2f}</code>"
    )
    await send_or_edit_photo(event=callback, photo_path="wallet.jpg", caption=caption, reply_markup=Keyboards.wallet_menu(), bot=bot)
    await callback.answer()


@user_router.callback_query(F.data == "btn:deposit")
async def cb_deposit_handler(callback: CallbackQuery, bot: Bot):
    caption = "<b>Select payment method to deposit:</b>"
    await send_or_edit_photo(event=callback, photo_path="paymentoptions.jpg", caption=caption, reply_markup=Keyboards.payment_options_menu(), bot=bot)
    await callback.answer()

# --- USD BALANCE PAYMENT & DEDUCTION HANDLER ---

@user_router.callback_query(F.data.startswith("pay_opt:usd_bal"))
async def cb_pay_opt_usd_bal(callback: CallbackQuery, state: FSMContext, bot: Bot):
    parts = callback.data.split(":")
    order_id = parts[2] if len(parts) > 2 else None

    if not order_id:
        await callback.answer("This option is only for paying market bookings directly.", show_alert=True)
        return

    data = await state.get_data()
    selected = data.get("selected_markets", [])

    from bson import ObjectId
    obj_ids = [ObjectId(m) for m in selected]
    mkts = await db.markets.find({"_id": {"$in": obj_ids}}).to_list(length=100)

    tot_usd = sum(m["usd_price"] for m in mkts)
    user_id = callback.from_user.id
    u = await db.get_user(user_id)

    if not u or u.get("usd_balance", 0.0) < tot_usd:
        await callback.answer(f"Insufficient USD balance. Required: ${tot_usd:.2f}", show_alert=True)
        return

    # Deduct USD balance
    await db.update_balances(user_id, usd_delta=-tot_usd, spent_delta=tot_usd)

    # Post to selected markets
    posted_count = 0
    for mkt in mkts:
        channel_id = mkt["channel_id"]
        try:
            member = await bot.get_chat_member(chat_id=channel_id, user_id=bot.id)
            if member.status in ["administrator", "creator"]:
                success = await publish_ad_to_channel(bot, channel_id, data)
                if success:
                    posted_count += 1
        except Exception as err:
            logger.error(f"Bot admin check error on {channel_id}: {err}")

    await db.advertisements.update_many({"order_id": order_id}, {"$set": {"status": "PAID"}})

    success_msg = (
        f"<b>✨ PAYMENT SUCCESSFUL</b>\n\n"
        f"Deducted <code>${tot_usd:.2f}</code> from your balance.\n"
        f"Post published directly in <code>{posted_count}</code> channel(s)."
    )
    await send_or_edit_photo(event=callback, photo_path="postsmarket.jpg", caption=success_msg, reply_markup=Keyboards.main_menu_only(), bot=bot)
    await callback.answer()

# ==========================================
# 6. ADMIN ROUTER & HANDLERS
# ==========================================

admin_router = Router()

@admin_router.callback_query(F.data == "admin:main")
async def cb_admin_main(callback: CallbackQuery, bot: Bot):
    if not await check_is_admin(callback.from_user.id):
        await callback.answer("Unauthorized.", show_alert=True)
        return

    is_maint = await db.is_maintenance()
    admin_text = (
        "<b>Admin Control Panel Dashboard</b>\n"
        "──────────────────────────\n"
        f"<b>Project Prefix:</b> <code>{Config.PROJECT_PREFIX}</code>\n"
        f"<b>Database Name:</b> <code>{Config.MONGO_DB_NAME}</code>\n"
        "Manage channel network, user balances, channel prices, and system settings."
    )
    await send_or_edit_photo(event=callback, photo_path="core.jpg", caption=admin_text, reply_markup=Keyboards.admin_main_menu(is_maint), bot=bot)
    await callback.answer()


@admin_router.callback_query(F.data == "admin:toggle_maint")
async def cb_admin_toggle_maint(callback: CallbackQuery, bot: Bot):
    if not await check_is_admin(callback.from_user.id):
        return

    new_val = await db.toggle_maintenance()
    status_str = "ENABLED" if new_val else "DISABLED"
    await callback.answer(f"Maintenance Mode is now {status_str}!", show_alert=True)
    await cb_admin_main(callback, bot)


@admin_router.callback_query(F.data == "admin:db_stats")
async def cb_admin_db_stats(callback: CallbackQuery, bot: Bot):
    if not await check_is_admin(callback.from_user.id):
        return

    total_users = await db.users.count_documents({})
    total_markets = await db.markets.count_documents({})
    total_ads = await db.advertisements.count_documents({})

    # Aggregate total systemic balances
    pipeline = [{"$group": {"_id": None, "total_usd": {"$sum": "$usd_balance"}, "total_stars": {"$sum": "$stars_balance"}}}]
    res = await db.users.aggregate(pipeline).to_list(length=1)

    tot_usd = res[0]["total_usd"] if res else 0.0
    tot_stars = res[0]["total_stars"] if res else 0

    stats_text = (
        "<b>📊 DATABASE & FINANCIAL STATS</b>\n"
        "──────────────────────────\n"
        f"<b>Project Prefix:</b> <code>{Config.PROJECT_PREFIX}</code>\n"
        f"<b>Total Registered Users:</b> <code>{total_users}</code>\n"
        f"<b>Total Channels/Markets:</b> <code>{total_markets}</code>\n"
        f"<b>Total Ad Bookings:</b> <code>{total_ads}</code>\n\n"
        f"<b>💰 System Wallet Balances:</b>\n"
        f"• <b>Total User USD Balance:</b> <code>${tot_usd:.2f} USD</code>\n"
        f"• <b>Total User Stars Balance:</b> <code>{tot_stars} Stars</code>"
    )

    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Back to Dashboard", callback_data="admin:main", style="danger")]])
    await send_or_edit_photo(event=callback, photo_path="core.jpg", caption=stats_text, reply_markup=markup, bot=bot)
    await callback.answer()

# --- MANAGE MARKETS & PRICES ---

@admin_router.callback_query(F.data == "admin:markets")
async def cb_admin_markets_list(callback: CallbackQuery, bot: Bot):
    if not await check_is_admin(callback.from_user.id):
        return

    markets = await db.markets.find({}).to_list(length=100)
    text = "<b>Channel Network Management</b>\n──────────────────────────\nAdd, toggle status, or modify prices of markets."
    await send_or_edit_photo(event=callback, photo_path="core.jpg", caption=text, reply_markup=Keyboards.admin_markets_menu(markets), bot=bot)
    await callback.answer()


@admin_router.callback_query(F.data.startswith("admin:mkt_toggle:"))
async def cb_admin_market_toggle(callback: CallbackQuery, bot: Bot):
    if not await check_is_admin(callback.from_user.id):
        return

    from bson import ObjectId
    mkt_id = callback.data.split(":")[2]
    mkt = await db.markets.find_one({"_id": ObjectId(mkt_id)})
    if mkt:
        new_status = not mkt.get("enabled", True)
        await db.markets.update_one({"_id": ObjectId(mkt_id)}, {"$set": {"enabled": new_status}})

    await cb_admin_markets_list(callback, bot)


@admin_router.callback_query(F.data.startswith("admin:mkt_del:"))
async def cb_admin_market_delete(callback: CallbackQuery, bot: Bot):
    if not await check_is_admin(callback.from_user.id):
        return

    from bson import ObjectId
    mkt_id = callback.data.split(":")[2]
    await db.markets.delete_one({"_id": ObjectId(mkt_id)})
    await callback.answer("Channel deleted successfully!")
    await cb_admin_markets_list(callback, bot)


@admin_router.callback_query(F.data.startswith("admin:mkt_edit:"))
async def cb_admin_market_edit_start(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if not await check_is_admin(callback.from_user.id):
        return

    mkt_id = callback.data.split(":")[2]
    await state.update_data(editing_mkt_id=mkt_id)
    await state.set_state(AdminStates.edit_market_stars)

    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Back", callback_data="admin:markets", style="danger")]])
    await send_or_edit_photo(event=callback, photo_path="core.jpg", caption="<b>Enter NEW Price in Telegram Stars:</b>", reply_markup=markup, bot=bot)
    await callback.answer()


@admin_router.message(AdminStates.edit_market_stars)
async def process_edit_mkt_stars(message: Message, state: FSMContext):
    try:
        new_stars = int(message.text.strip())
    except ValueError:
        await message.answer("Please enter a valid integer for Stars price.")
        return

    await state.update_data(new_stars_price=new_stars)
    await state.set_state(AdminStates.edit_market_usd)
    await message.answer("<b>Enter NEW Price in USD (e.g. 3.50):</b>", parse_mode=ParseMode.HTML)


@admin_router.message(AdminStates.edit_market_usd)
async def process_edit_mkt_usd(message: Message, state: FSMContext, bot: Bot):
    try:
        new_usd = float(message.text.strip())
    except ValueError:
        await message.answer("Please enter a valid float number for USD price.")
        return

    data = await state.get_data()
    mkt_id = data.get("editing_mkt_id")
    await state.clear()

    from bson import ObjectId
    await db.markets.update_one(
        {"_id": ObjectId(mkt_id)},
        {"$set": {"stars_price": data["new_stars_price"], "usd_price": new_usd, "updated_at": datetime.utcnow()}}
    )

    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Back to Channels", callback_data="admin:markets")]])
    await message.answer("<b>Channel prices updated successfully!</b>", reply_markup=markup, parse_mode=ParseMode.HTML)


@admin_router.callback_query(F.data == "admin:mkt_add")
async def cb_admin_market_add_start(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if not await check_is_admin(callback.from_user.id):
        return

    await state.set_state(AdminStates.add_market_name)
    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Back", callback_data="admin:markets", style="danger")]])
    await send_or_edit_photo(event=callback, photo_path="core.jpg", caption="<b>Enter Channel Display Name (e.g. PostsMarket):</b>", reply_markup=markup, bot=bot)
    await callback.answer()


@admin_router.message(AdminStates.add_market_name)
async def process_add_mkt_name(message: Message, state: FSMContext):
    await state.update_data(mkt_name=message.text.strip())
    await state.set_state(AdminStates.add_market_channel)
    await message.answer("<b>Enter Channel Username / ID (e.g. @PostsMarket):</b>", parse_mode=ParseMode.HTML)


@admin_router.message(AdminStates.add_market_channel)
async def process_add_mkt_channel(message: Message, state: FSMContext):
    await state.update_data(mkt_channel=message.text.strip())
    await state.set_state(AdminStates.add_market_subs)
    await message.answer("<b>Enter Subscriber Count Label (e.g. 50K+):</b>", parse_mode=ParseMode.HTML)


@admin_router.message(AdminStates.add_market_subs)
async def process_add_mkt_subs(message: Message, state: FSMContext):
    await state.update_data(mkt_subs=message.text.strip())
    await state.set_state(AdminStates.add_market_stars)
    await message.answer("<b>Enter Price in Telegram Stars (e.g. 29):</b>", parse_mode=ParseMode.HTML)


@admin_router.message(AdminStates.add_market_stars)
async def process_add_mkt_stars(message: Message, state: FSMContext):
    try:
        stars_price = int(message.text.strip())
    except ValueError:
        await message.answer("Please enter a valid integer for Stars price.")
        return

    await state.update_data(mkt_stars=stars_price)
    await state.set_state(AdminStates.add_market_usd)
    await message.answer("<b>Enter Price in USD (e.g. 2.50):</b>", parse_mode=ParseMode.HTML)


@admin_router.message(AdminStates.add_market_usd)
async def process_add_mkt_usd(message: Message, state: FSMContext, bot: Bot):
    try:
        usd_price = float(message.text.strip())
    except ValueError:
        await message.answer("Please enter a valid float number for USD price.")
        return

    data = await state.get_data()
    await state.clear()

    await db.markets.insert_one({
        "name": data["mkt_name"],
        "channel_id": data["mkt_channel"],
        "channel_username": data["mkt_channel"].replace("@", ""),
        "subscribers": data["mkt_subs"],
        "stars_price": data["mkt_stars"],
        "usd_price": usd_price,
        "enabled": True,
        "created_at": datetime.utcnow()
    })

    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Back to Channels", callback_data="admin:markets")]])
    await message.answer(f"<b>Channel '{data['mkt_name']}' successfully added to network!</b>", reply_markup=markup, parse_mode=ParseMode.HTML)

# ==========================================
# 7. MAIN ENTRYPOINT
# ==========================================

async def main():
    logger.info("Initializing MongoDB connection...")
    await db.init_db()

    bot = Bot(token=Config.BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())

    dp.include_router(user_router)
    dp.include_router(admin_router)

    # Start Oxapay background poller
    asyncio.create_task(oxapay_polling_task(bot))

    logger.info("Core Creations Pay-To-Forward Bot running on MongoDB...")
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
