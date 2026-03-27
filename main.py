import asyncio
import base64
import io
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

import aiohttp
import aiosqlite
import qrcode
from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ChatMemberStatus, ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger("aerovpn")


@dataclass
class Config:
    bot_token: str
    channel_username: str
    channel_url: str
    bot_username: str
    subscription_url: str
    policy_url: str
    agreement_url: str
    platega_base_url: str
    platega_merchant_id: str
    platega_secret: str
    admin_ids: set[int]
    price_month_rub: int
    db_path: str


def load_config() -> Config:
    admin_ids_raw = os.getenv("ADMIN_IDS", "")
    admin_ids = {int(x.strip()) for x in admin_ids_raw.split(",") if x.strip().isdigit()}
    return Config(
        bot_token=os.getenv("BOT_TOKEN", ""),
        channel_username=os.getenv("CHANNEL_USERNAME", "@AeroVPNpro"),
        channel_url=os.getenv("CHANNEL_URL", "https://t.me/AeroVPNpro"),
        bot_username=os.getenv("BOT_USERNAME", "AeroVpnPlus_bot"),
        subscription_url=os.getenv("SUBSCRIPTION_URL", "http://62.60.235.194/sub.txt"),
        policy_url=os.getenv("POLICY_URL", "https://telegra.ph/Politika-konfidencialnosti-08-15-17"),
        agreement_url=os.getenv("AGREEMENT_URL", "https://telegra.ph/Polzovatelskoe-soglashenie-08-15-10"),
        platega_base_url=os.getenv("PLATEGA_BASE_URL", "https://app.platega.io"),
        platega_merchant_id=os.getenv("PLATEGA_MERCHANT_ID", ""),
        platega_secret=os.getenv("PLATEGA_SECRET", ""),
        admin_ids=admin_ids,
        price_month_rub=int(os.getenv("PRICE_MONTH_RUB", "100")),
        db_path=os.getenv("DB_PATH", "aerovpn.db"),
    )


config = load_config()

if not config.bot_token:
    raise RuntimeError("BOT_TOKEN не задан")


# ---------- Database ----------
class Database:
    def __init__(self, path: str):
        self.path = path

    async def init(self):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("PRAGMA foreign_keys = ON")
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    full_name TEXT,
                    balance REAL NOT NULL DEFAULT 0,
                    subscription_until TEXT,
                    created_at TEXT NOT NULL,
                    referrer_id INTEGER,
                    total_ref_income REAL NOT NULL DEFAULT 0,
                    invited_count INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY (referrer_id) REFERENCES users(user_id)
                )
                """
            )
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS payments (
                    transaction_id TEXT PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    payment_method INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    purpose TEXT NOT NULL,
                    status TEXT NOT NULL,
                    payload TEXT,
                    redirect_url TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
                """
            )
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS referral_rewards (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_user_id INTEGER NOT NULL,
                    beneficiary_user_id INTEGER NOT NULL,
                    level INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    payment_transaction_id TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )
            await db.commit()

    async def execute(self, query: str, params: tuple = ()):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(query, params)
            await db.commit()

    async def fetchone(self, query: str, params: tuple = ()):
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(query, params) as cursor:
                return await cursor.fetchone()

    async def fetchall(self, query: str, params: tuple = ()):
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(query, params) as cursor:
                return await cursor.fetchall()

    async def upsert_user(self, user_id: int, username: Optional[str], full_name: str, referrer_id: Optional[int] = None):
        existing = await self.fetchone("SELECT user_id, referrer_id FROM users WHERE user_id = ?", (user_id,))
        now = utcnow_iso()
        if existing:
            await self.execute(
                "UPDATE users SET username = ?, full_name = ? WHERE user_id = ?",
                (username, full_name, user_id),
            )
            return

        await self.execute(
            """
            INSERT INTO users (user_id, username, full_name, balance, subscription_until, created_at, referrer_id, total_ref_income, invited_count)
            VALUES (?, ?, ?, 0, NULL, ?, ?, 0, 0)
            """,
            (user_id, username, full_name, now, referrer_id),
        )
        if referrer_id and referrer_id != user_id:
            await self.execute(
                "UPDATE users SET invited_count = invited_count + 1 WHERE user_id = ?",
                (referrer_id,),
            )

    async def get_user(self, user_id: int):
        return await self.fetchone("SELECT * FROM users WHERE user_id = ?", (user_id,))

    async def add_balance(self, user_id: int, amount: float):
        await self.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, user_id))

    async def subtract_balance(self, user_id: int, amount: float):
        await self.execute("UPDATE users SET balance = MAX(balance - ?, 0) WHERE user_id = ?", (amount, user_id))

    async def extend_subscription(self, user_id: int, days: int = 30):
        user = await self.get_user(user_id)
        current_until = parse_iso_dt(user["subscription_until"]) if user and user["subscription_until"] else None
        now = datetime.now(timezone.utc)
        base = current_until if current_until and current_until > now else now
        new_until = base + timedelta(days=days)
        await self.execute(
            "UPDATE users SET subscription_until = ? WHERE user_id = ?",
            (new_until.isoformat(), user_id),
        )
        return new_until

    async def save_payment(self, transaction_id: str, user_id: int, payment_method: int, amount: float, purpose: str, status: str,
                           payload: Optional[str], redirect_url: Optional[str]):
        now = utcnow_iso()
        await self.execute(
            """
            INSERT OR REPLACE INTO payments (transaction_id, user_id, payment_method, amount, purpose, status, payload, redirect_url, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM payments WHERE transaction_id = ?), ?), ?)
            """,
            (transaction_id, user_id, payment_method, amount, purpose, status, payload, redirect_url, transaction_id, now, now),
        )

    async def update_payment_status(self, transaction_id: str, status: str):
        await self.execute(
            "UPDATE payments SET status = ?, updated_at = ? WHERE transaction_id = ?",
            (status, utcnow_iso(), transaction_id),
        )

    async def get_payment(self, transaction_id: str):
        return await self.fetchone("SELECT * FROM payments WHERE transaction_id = ?", (transaction_id,))

    async def get_pending_payments(self):
        return await self.fetchall("SELECT * FROM payments WHERE status = 'PENDING'")

    async def stats(self):
        users = await self.fetchone("SELECT COUNT(*) AS c FROM users")
        active = await self.fetchone(
            "SELECT COUNT(*) AS c FROM users WHERE subscription_until IS NOT NULL AND subscription_until > ?",
            (utcnow_iso(),),
        )
        paid = await self.fetchone(
            "SELECT COALESCE(SUM(amount), 0) AS s FROM payments WHERE status = 'CONFIRMED'",
        )
        return {
            "users": users["c"] if users else 0,
            "active": active["c"] if active else 0,
            "paid": paid["s"] if paid else 0,
        }

    async def distribute_referral_rewards(self, source_user_id: int, amount: float, transaction_id: str):
        percents = [25, 10, 6, 5, 4]
        current = await self.get_user(source_user_id)
        level = 1
        while current and current["referrer_id"] and level <= len(percents):
            beneficiary_id = current["referrer_id"]
            reward = round(amount * (percents[level - 1] / 100), 2)
            if reward > 0:
                await self.add_balance(beneficiary_id, reward)
                await self.execute(
                    "UPDATE users SET total_ref_income = total_ref_income + ? WHERE user_id = ?",
                    (reward, beneficiary_id),
                )
                await self.execute(
                    """
                    INSERT INTO referral_rewards (source_user_id, beneficiary_user_id, level, amount, payment_transaction_id, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (source_user_id, beneficiary_id, level, reward, transaction_id, utcnow_iso()),
                )
            current = await self.get_user(beneficiary_id)
            level += 1


# ---------- Payments ----------
class PlategaClient:
    def __init__(self, base_url: str, merchant_id: str, secret: str):
        self.base_url = base_url.rstrip("/")
        self.merchant_id = merchant_id
        self.secret = secret

    @property
    def enabled(self) -> bool:
        return bool(self.merchant_id and self.secret)

    def _headers(self):
        return {
            "Content-Type": "application/json",
            "X-MerchantId": self.merchant_id,
            "X-Secret": self.secret,
        }

    async def create_payment(self, amount: float, currency: str, description: str, payment_method: int, payload: str):
        url = f"{self.base_url}/transaction/process"
        body = {
            "paymentMethod": payment_method,
            "paymentDetails": {"amount": amount, "currency": currency},
            "description": description,
            "payload": payload,
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=self._headers(), json=body, timeout=30) as resp:
                text = await resp.text()
                if resp.status >= 400:
                    raise RuntimeError(f"Ошибка создания платежа: {resp.status} {text}")
                return await resp.json()

    async def get_payment(self, transaction_id: str):
        url = f"{self.base_url}/transaction/{transaction_id}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=self._headers(), timeout=30) as resp:
                text = await resp.text()
                if resp.status >= 400:
                    raise RuntimeError(f"Ошибка проверки платежа: {resp.status} {text}")
                return await resp.json()


# ---------- Helpers ----------
def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def format_date(value: Optional[str]) -> str:
    dt = parse_iso_dt(value)
    if not dt:
        return "Не активна"
    return dt.astimezone().strftime("%d.%m.%Y %H:%M")


def is_subscription_active(value: Optional[str]) -> bool:
    dt = parse_iso_dt(value)
    return bool(dt and dt > datetime.now(timezone.utc))


def rub(amount: float | int) -> str:
    return f"{amount:.2f} ₽".replace(".00", "")


def referral_link(user_id: int) -> str:
    return f"https://t.me/{config.bot_username}?start=referral_{user_id}"


def main_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔐 Моя подписка"), KeyboardButton(text="💳 Баланс")],
            [KeyboardButton(text="👥 Пригласить"), KeyboardButton(text="ℹ️ О сервисе")],
        ],
        resize_keyboard=True,
    )


def back_to_menu_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="👤 Личный кабинет", callback_data="menu")]])


def start_subscribe_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📢 Подписаться", url=config.channel_url)],
            [InlineKeyboardButton(text="✅ Я подписался", callback_data="check_sub")],
        ]
    )


def subscription_inline_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📲 Подключить устройство", callback_data="connect_device")],
            [InlineKeyboardButton(text="⏳ Продлить подписку", callback_data="renew_menu")],
            [InlineKeyboardButton(text="📷 Показать QR-код", callback_data="show_qr")],
            [InlineKeyboardButton(text="👤 Личный кабинет", callback_data="menu")],
        ]
    )


def balance_inline_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💰 Пополнить баланс", callback_data="topup_menu")],
            [InlineKeyboardButton(text="📊 История пополнений", callback_data="payment_history")],
            [InlineKeyboardButton(text="🎟 Активировать купон", callback_data="coupon_stub")],
            [InlineKeyboardButton(text="👤 Личный кабинет", callback_data="menu")],
        ]
    )


def invite_inline_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👥 Пригласить", switch_inline_query="")],
            [InlineKeyboardButton(text="📷 Показать QR-код", callback_data="show_ref_qr")],
            [InlineKeyboardButton(text="👤 Личный кабинет", callback_data="menu")],
        ]
    )


def about_inline_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📄 Пользовательское соглашение", url=config.agreement_url)],
            [InlineKeyboardButton(text="🔒 Политика конфиденциальности", url=config.policy_url)],
            [InlineKeyboardButton(text="📢 Канал", url=config.channel_url)],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="menu")],
        ]
    )


def payment_methods_kb(prefix: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="СБП", callback_data=f"{prefix}:2")],
            [InlineKeyboardButton(text="Криптовалюта", callback_data=f"{prefix}:13")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="menu")],
        ]
    )


def payment_action_kb(transaction_id: str, redirect_url: Optional[str]) -> InlineKeyboardMarkup:
    buttons = []
    if redirect_url:
        buttons.append([InlineKeyboardButton(text="💳 Оплатить", url=redirect_url)])
    buttons.append([InlineKeyboardButton(text="🔄 Проверить оплату", callback_data=f"check_payment:{transaction_id}")])
    buttons.append([InlineKeyboardButton(text="👤 Личный кабинет", callback_data="menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def admin_panel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💸 Выдать баланс", callback_data="admin_add_balance")],
            [InlineKeyboardButton(text="➖ Списать баланс", callback_data="admin_sub_balance")],
            [InlineKeyboardButton(text="🔐 Выдать подписку", callback_data="admin_grant_sub")],
            [InlineKeyboardButton(text="📣 Рассылка", callback_data="admin_broadcast")],
            [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
        ]
    )


async def user_is_subscribed(bot: Bot, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(config.channel_username, user_id)
        return member.status in {
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.CREATOR,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.RESTRICTED,
        }
    except TelegramBadRequest:
        return False


def build_qr_image_bytes(data: str) -> bytes:
    img = qrcode.make(data)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


# ---------- States ----------
class AdminStates(StatesGroup):
    wait_add_balance = State()
    wait_sub_balance = State()
    wait_grant_sub = State()
    wait_broadcast = State()


# ---------- Bot setup ----------
db = Database(config.db_path)
payments = PlategaClient(config.platega_base_url, config.platega_merchant_id, config.platega_secret)
router = Router()


async def ensure_user(message: Message, start_arg: Optional[str] = None):
    referrer_id = None
    if start_arg and start_arg.startswith("referral_"):
        try:
            referrer_id = int(start_arg.split("_", 1)[1])
        except ValueError:
            referrer_id = None
    await db.upsert_user(message.from_user.id, message.from_user.username, message.from_user.full_name, referrer_id)


async def send_profile(target: Message | CallbackQuery, edit: bool = False):
    user = await db.get_user(target.from_user.id)
    active = is_subscription_active(user["subscription_until"])
    count_subs = 1 if active else 0
    text = (
        f"👤 <b>Профиль AeroVPN</b>\n\n"
        f"🆔 ID: <code>{user['user_id']}</code>\n"
        f"💰 Баланс: <b>{rub(user['balance'])}</b>\n"
        f"📦 Кол-во подписок: <b>{count_subs}</b>\n\n"
        f"🔥 AeroVPN — обход блокировок, обход глушилок, быстрый интернет."
    )
    if isinstance(target, CallbackQuery):
        await target.message.edit_text(text, reply_markup=None)
        await target.message.answer("Выберите раздел ниже 👇", reply_markup=main_menu_kb())
        await target.answer()
    else:
        await target.answer(text, reply_markup=main_menu_kb())


@router.message(CommandStart())
async def start_handler(message: Message, command: CommandObject = None):
    start_arg = command.args if command else None
    await ensure_user(message, start_arg)
    if not await user_is_subscribed(message.bot, message.from_user.id):
        await message.answer(
            "Для использования бота, пожалуйста, подпишитесь на наш канал:",
            reply_markup=start_subscribe_kb(),
        )
        return
    await send_profile(message)


# aiogram command object type fallback
class CommandObject:
    def __init__(self, args: Optional[str] = None):
        self.args = args


@router.callback_query(F.data == "check_sub")
async def check_sub_handler(call: CallbackQuery):
    if await user_is_subscribed(call.bot, call.from_user.id):
        await call.message.delete()
        fake_message = call.message
        fake_message.from_user = call.from_user
        await send_profile(fake_message)
        await call.answer("Подписка подтверждена ✅")
    else:
        await call.answer("Подписка не найдена. Подпишитесь и попробуйте снова.", show_alert=True)


@router.message(F.text == "🔐 Моя подписка")
async def my_subscription_handler(message: Message):
    if not await user_is_subscribed(message.bot, message.from_user.id):
        await message.answer("Сначала подпишитесь на канал.", reply_markup=start_subscribe_kb())
        return
    user = await db.get_user(message.from_user.id)
    active = is_subscription_active(user["subscription_until"])
    status = "🟢 Активна" if active else "🔴 Истекла"
    text = (
        f"🔐 <b>Ваша подписка</b>\n\n"
        f"<code>{config.subscription_url}</code>\n\n"
        f"🕒 Статус подписки: <b>{status}</b>\n"
        f"📅 Истекает: <b>{format_date(user['subscription_until'])}</b>\n\n"
        f"📦 Тариф: <b>VPN с обходом блокировок</b>\n"
        f"💳 Цена: <b>{config.price_month_rub} ₽ / месяц</b>\n"
        f"🌍 Доступ: <b>много серверов, высокая скорость</b>\n\n"
        f"Подключите своё устройство по кнопкам ниже 👇"
    )
    await message.answer(text, reply_markup=subscription_inline_kb())


@router.message(F.text == "💳 Баланс")
async def balance_handler(message: Message):
    user = await db.get_user(message.from_user.id)
    text = f"💳 <b>Управление балансом</b>\n\nВаш баланс: <b>{rub(user['balance'])}</b>"
    await message.answer(text, reply_markup=balance_inline_kb())


@router.message(F.text == "👥 Пригласить")
async def invite_handler(message: Message):
    user = await db.get_user(message.from_user.id)
    text = (
        f"👥 <b>Ваша реферальная ссылка:</b>\n\n"
        f"<code>{referral_link(message.from_user.id)}</code>\n\n"
        f"🤝 Приглашайте друзей и получайте бонусы на каждом уровне.\n\n"
        f"🏆 <b>Бонусы:</b>\n"
        f"1 уровень — 25%\n"
        f"2 уровень — 10%\n"
        f"3 уровень — 6%\n"
        f"4 уровень — 5%\n"
        f"5 уровень — 4%\n\n"
        f"📊 Приглашено: <b>{user['invited_count']}</b>\n"
        f"💰 Общий доход от рефералов: <b>{rub(user['total_ref_income'])}</b>"
    )
    await message.answer(text, reply_markup=invite_inline_kb())


@router.message(F.text == "ℹ️ О сервисе")
async def about_handler(message: Message):
    text = (
        "🌐 <b>О VPN</b>\n\n"
        "🚀 Высокоскоростные серверы в разных странах для стабильного соединения.\n"
        "🔒 Защита данных и приватности.\n"
        "🌍 Удобный обход блокировок и глушилок.\n\n"
        "⚠️ Не передавайте свою ссылку подписки другим людям."
    )
    await message.answer(text, reply_markup=about_inline_kb())


@router.callback_query(F.data == "menu")
async def menu_callback(call: CallbackQuery):
    fake_message = call.message
    fake_message.from_user = call.from_user
    await send_profile(fake_message)


@router.callback_query(F.data == "connect_device")
async def connect_device(call: CallbackQuery):
    text = (
        "📲 <b>Инструкция по подключению</b>\n\n"
        "1. Скачайте приложение <b>HAPP</b>.\n"
        "2. Откройте приложение.\n"
        "3. Найдите раздел <b>Подписка</b> или <b>Import subscription</b>.\n"
        f"4. Скопируйте и вставьте ссылку:\n<code>{config.subscription_url}</code>\n"
        "5. Сохраните и подключитесь."
    )
    await call.message.answer(text, reply_markup=back_to_menu_inline())
    await call.answer()


@router.callback_query(F.data == "show_qr")
async def show_qr(call: CallbackQuery):
    data = build_qr_image_bytes(config.subscription_url)
    file = BufferedInputFile(data, filename="aerovpn_subscription_qr.png")
    await call.message.answer_photo(file, caption="QR-код для вашей подписки")
    await call.answer()


@router.callback_query(F.data == "show_ref_qr")
async def show_ref_qr(call: CallbackQuery):
    data = build_qr_image_bytes(referral_link(call.from_user.id))
    file = BufferedInputFile(data, filename="aerovpn_ref_qr.png")
    await call.message.answer_photo(file, caption="QR-код вашей реферальной ссылки")
    await call.answer()


@router.callback_query(F.data == "renew_menu")
async def renew_menu(call: CallbackQuery):
    await call.message.answer(
        f"Продление подписки на 30 дней — <b>{config.price_month_rub} ₽</b>. Выберите способ оплаты:",
        reply_markup=payment_methods_kb("renew"),
    )
    await call.answer()


@router.callback_query(F.data == "topup_menu")
async def topup_menu(call: CallbackQuery):
    await call.message.answer(
        f"Пополнение баланса на <b>{config.price_month_rub} ₽</b>. Выберите способ оплаты:",
        reply_markup=payment_methods_kb("topup"),
    )
    await call.answer()


async def create_payment_for_user(call: CallbackQuery, payment_method: int, purpose: str):
    if not payments.enabled:
        await call.answer("Платежи пока не настроены. Заполните PLATEGA_MERCHANT_ID и PLATEGA_SECRET.", show_alert=True)
        return
    amount = float(config.price_month_rub)
    payload = f"{purpose}:{call.from_user.id}:{int(datetime.now().timestamp())}"
    try:
        result = await payments.create_payment(
            amount=amount,
            currency="RUB",
            description=f"AeroVPN — {purpose}",
            payment_method=payment_method,
            payload=payload,
        )
        transaction_id = result["transactionId"]
        redirect = result.get("redirect")
        status = result.get("status", "PENDING")
        await db.save_payment(transaction_id, call.from_user.id, payment_method, amount, purpose, status, payload, redirect)
        method_name = "СБП" if payment_method == 2 else "Криптовалюта"
        await call.message.answer(
            f"✅ Платёж создан\n\n"
            f"Способ: <b>{method_name}</b>\n"
            f"Сумма: <b>{rub(amount)}</b>\n"
            f"ID: <code>{transaction_id}</code>\n\n"
            f"Нажмите «Оплатить», затем «Проверить оплату».",
            reply_markup=payment_action_kb(transaction_id, redirect),
        )
    except Exception as e:
        logger.exception("create payment failed")
        await call.message.answer(f"Не удалось создать платёж: <code>{e}</code>")
    await call.answer()


@router.callback_query(F.data.startswith("renew:"))
async def renew_payment(call: CallbackQuery):
    payment_method = int(call.data.split(":")[1])
    await create_payment_for_user(call, payment_method, "subscription")


@router.callback_query(F.data.startswith("topup:"))
async def topup_payment(call: CallbackQuery):
    payment_method = int(call.data.split(":")[1])
    await create_payment_for_user(call, payment_method, "balance")


async def process_confirmed_payment(transaction_id: str, notify_user: bool = True):
    payment = await db.get_payment(transaction_id)
    if not payment or payment["status"] == "CONFIRMED_PROCESSED":
        return
    user_id = payment["user_id"]
    if payment["purpose"] == "balance":
        await db.add_balance(user_id, payment["amount"])
    elif payment["purpose"] == "subscription":
        await db.extend_subscription(user_id, 30)
    await db.distribute_referral_rewards(user_id, payment["amount"], transaction_id)
    await db.update_payment_status(transaction_id, "CONFIRMED_PROCESSED")
    if notify_user:
        try:
            bot = Bot(config.bot_token, parse_mode=ParseMode.HTML)
            if payment["purpose"] == "balance":
                await bot.send_message(user_id, f"✅ Оплата подтверждена. Баланс пополнен на {rub(payment['amount'])}.")
            else:
                user = await db.get_user(user_id)
                await bot.send_message(
                    user_id,
                    f"✅ Оплата подтверждена. Подписка продлена до <b>{format_date(user['subscription_until'])}</b>.",
                )
            await bot.session.close()
        except Exception:
            logger.exception("notify user failed")


@router.callback_query(F.data.startswith("check_payment:"))
async def check_payment(call: CallbackQuery):
    transaction_id = call.data.split(":", 1)[1]
    payment = await db.get_payment(transaction_id)
    if not payment:
        await call.answer("Платёж не найден", show_alert=True)
        return
    try:
        result = await payments.get_payment(transaction_id)
        status = result.get("status", "PENDING")
        await db.update_payment_status(transaction_id, status)
        if status == "CONFIRMED":
            await process_confirmed_payment(transaction_id)
            await call.message.answer("✅ Оплата подтверждена и обработана.")
        elif status == "PENDING":
            await call.message.answer("⏳ Платёж пока не подтвержден. Попробуйте позже.")
        elif status == "CANCELED":
            await call.message.answer("❌ Платёж отменен.")
        elif status == "CHARGEBACKED":
            await call.message.answer("⚠️ По платежу произошёл chargeback.")
        else:
            await call.message.answer(f"Статус платежа: <b>{status}</b>")
    except Exception as e:
        logger.exception("check payment failed")
        await call.message.answer(f"Не удалось проверить платёж: <code>{e}</code>")
    await call.answer()


@router.callback_query(F.data == "payment_history")
async def payment_history(call: CallbackQuery):
    rows = await db.fetchall(
        "SELECT transaction_id, amount, purpose, status, created_at FROM payments WHERE user_id = ? ORDER BY created_at DESC LIMIT 10",
        (call.from_user.id,),
    )
    if not rows:
        text = "История пополнений пока пустая."
    else:
        lines = ["📊 <b>Последние платежи:</b>"]
        for row in rows:
            lines.append(
                f"• <code>{row['transaction_id'][:8]}</code> | {row['purpose']} | {rub(row['amount'])} | {row['status']}"
            )
        text = "\n".join(lines)
    await call.message.answer(text, reply_markup=back_to_menu_inline())
    await call.answer()


@router.callback_query(F.data == "coupon_stub")
async def coupon_stub(call: CallbackQuery):
    await call.answer("Купоны можно добавить позже. Сейчас функция заглушена.", show_alert=True)


# ---------- Admin ----------
def is_admin(user_id: int) -> bool:
    return user_id in config.admin_ids


@router.message(Command("admin"))
async def admin_panel(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Нет доступа.")
        return
    await message.answer("⚙️ <b>Админ-панель</b>", reply_markup=admin_panel_kb())


@router.callback_query(F.data == "admin_add_balance")
async def admin_add_balance(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return await call.answer("Нет доступа", show_alert=True)
    await state.set_state(AdminStates.wait_add_balance)
    await call.message.answer("Отправьте: <code>user_id сумма</code>\nПример: <code>123456789 100</code>")
    await call.answer()


@router.callback_query(F.data == "admin_sub_balance")
async def admin_sub_balance(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return await call.answer("Нет доступа", show_alert=True)
    await state.set_state(AdminStates.wait_sub_balance)
    await call.message.answer("Отправьте: <code>user_id сумма</code>")
    await call.answer()


@router.callback_query(F.data == "admin_grant_sub")
async def admin_grant_sub(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return await call.answer("Нет доступа", show_alert=True)
    await state.set_state(AdminStates.wait_grant_sub)
    await call.message.answer("Отправьте: <code>user_id дни</code>\nПример: <code>123456789 30</code>")
    await call.answer()


@router.callback_query(F.data == "admin_broadcast")
async def admin_broadcast(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return await call.answer("Нет доступа", show_alert=True)
    await state.set_state(AdminStates.wait_broadcast)
    await call.message.answer("Отправьте текст рассылки одним сообщением.")
    await call.answer()


@router.callback_query(F.data == "admin_stats")
async def admin_stats(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return await call.answer("Нет доступа", show_alert=True)
    stats = await db.stats()
    text = (
        f"📊 <b>Статистика</b>\n\n"
        f"Пользователей: <b>{stats['users']}</b>\n"
        f"Активных подписок: <b>{stats['active']}</b>\n"
        f"Подтвержденных оплат: <b>{rub(stats['paid'])}</b>"
    )
    await call.message.answer(text)
    await call.answer()


@router.message(AdminStates.wait_add_balance)
async def admin_add_balance_message(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        user_id_str, amount_str = message.text.split(maxsplit=1)
        user_id = int(user_id_str)
        amount = float(amount_str.replace(",", "."))
        if not await db.get_user(user_id):
            await message.answer("Пользователь не найден в базе.")
        else:
            await db.add_balance(user_id, amount)
            await message.answer(f"✅ Баланс пополнен на {rub(amount)} для {user_id}")
    except Exception:
        await message.answer("Неверный формат. Нужно: <code>user_id сумма</code>")
    finally:
        await state.clear()


@router.message(AdminStates.wait_sub_balance)
async def admin_sub_balance_message(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        user_id_str, amount_str = message.text.split(maxsplit=1)
        user_id = int(user_id_str)
        amount = float(amount_str.replace(",", "."))
        if not await db.get_user(user_id):
            await message.answer("Пользователь не найден в базе.")
        else:
            await db.subtract_balance(user_id, amount)
            await message.answer(f"✅ Списано {rub(amount)} у {user_id}")
    except Exception:
        await message.answer("Неверный формат. Нужно: <code>user_id сумма</code>")
    finally:
        await state.clear()


@router.message(AdminStates.wait_grant_sub)
async def admin_grant_sub_message(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        user_id_str, days_str = message.text.split(maxsplit=1)
        user_id = int(user_id_str)
        days = int(days_str)
        if not await db.get_user(user_id):
            await message.answer("Пользователь не найден в базе.")
        else:
            new_until = await db.extend_subscription(user_id, days)
            await message.answer(f"✅ Подписка выдана до {new_until.astimezone().strftime('%d.%m.%Y %H:%M')} для {user_id}")
    except Exception:
        await message.answer("Неверный формат. Нужно: <code>user_id дни</code>")
    finally:
        await state.clear()


@router.message(AdminStates.wait_broadcast)
async def admin_broadcast_message(message: Message, state: FSMContext, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    users = await db.fetchall("SELECT user_id FROM users")
    success = 0
    failed = 0
    for row in users:
        try:
            await bot.send_message(row["user_id"], message.text)
            success += 1
            await asyncio.sleep(0.03)
        except Exception:
            failed += 1
    await message.answer(f"✅ Рассылка завершена. Успешно: {success}, ошибок: {failed}")
    await state.clear()


# ---------- Background task ----------
async def payment_watcher(bot: Bot):
    while True:
        try:
            if payments.enabled:
                pending = await db.get_pending_payments()
                for row in pending:
                    try:
                        result = await payments.get_payment(row["transaction_id"])
                        status = result.get("status", "PENDING")
                        if status != row["status"]:
                            await db.update_payment_status(row["transaction_id"], status)
                        if status == "CONFIRMED":
                            await process_confirmed_payment(row["transaction_id"])
                    except Exception:
                        logger.exception("watcher failed for payment %s", row["transaction_id"])
        except Exception:
            logger.exception("payment watcher loop failed")
        await asyncio.sleep(30)


async def main():
    await db.init()
    bot = Bot(token=config.bot_token, parse_mode=ParseMode.HTML)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    asyncio.create_task(payment_watcher(bot))
    logger.info("Bot started")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
