import asyncio
import base64
import io
import logging
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import aiohttp
import qrcode
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ChatMemberStatus, ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    BufferedInputFile,
    Message,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

# =============================
# HARD-CODED CONFIG
# =============================
BOT_TOKEN = os.getenv("BOT_TOKEN") or "8701402250:AAH9HdX2STWEl_CPx2L_Ab9cuOYXSHfMx4I"
BOT_USERNAME = os.getenv("BOT_USERNAME") or "AeroVpnPlus_bot"
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME") or "@AeroVPNpro"
CHANNEL_URL = os.getenv("CHANNEL_URL") or "https://t.me/AeroVPNpro"
SUBSCRIPTION_URL = os.getenv("SUBSCRIPTION_URL") or "http://62.60.235.194/sub.txt"
POLICY_URL = os.getenv("POLICY_URL") or "https://telegra.ph/Politika-konfidencialnosti-08-15-17"
AGREEMENT_URL = os.getenv("AGREEMENT_URL") or "https://telegra.ph/Polzovatelskoe-soglashenie-08-15-10"

PLATEGA_BASE_URL = (os.getenv("PLATEGA_BASE_URL") or "https://app.platega.io").rstrip("/")
PLATEGA_MERCHANT_ID = os.getenv("PLATEGA_MERCHANT_ID") or "6c6e3a35-258f-4c3f-8427-4081e0c6cc40"
PLATEGA_SECRET = os.getenv("PLATEGA_SECRET") or "24Yh7jaA617DW16rFRnp2qrFctcsn5BoMokxeu1MZ8daxZGBB1wL4CAMHND4s7xXKCWK5qZWGEJlXiii5icfZFKOweCeHiCErJXB"

PRICE_MONTH_RUB = int(os.getenv("PRICE_MONTH_RUB") or "100")
DB_PATH = os.getenv("DB_PATH") or "aerovpn.db"
ADMIN_IDS = {7105046320}

TZ = timezone.utc
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("aerovpn_bot")


# =============================
# DATABASE
# =============================
class Database:
    def __init__(self, path: str):
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.init()

    def init(self):
        cur = self.conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            balance INTEGER NOT NULL DEFAULT 0,
            subscription_until TEXT,
            referrer_id INTEGER,
            referrals_count INTEGER NOT NULL DEFAULT 0,
            referrals_income INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            local_id INTEGER PRIMARY KEY AUTOINCREMENT,
            transaction_id TEXT UNIQUE,
            user_id INTEGER NOT NULL,
            amount INTEGER NOT NULL,
            currency TEXT NOT NULL DEFAULT 'RUB',
            payment_method INTEGER NOT NULL,
            target TEXT NOT NULL,
            status TEXT NOT NULL,
            redirect_url TEXT,
            expires_in TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            payload TEXT
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS referral_rewards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_user_id INTEGER NOT NULL,
            to_user_id INTEGER NOT NULL,
            level INTEGER NOT NULL,
            amount INTEGER NOT NULL,
            created_at TEXT NOT NULL
        )
        """)
        self.conn.commit()

    def upsert_user(self, user_id: int, username: str | None, first_name: str | None, referrer_id: int | None = None):
        now = utc_now_iso()
        cur = self.conn.cursor()
        cur.execute("SELECT user_id, referrer_id FROM users WHERE user_id=?", (user_id,))
        row = cur.fetchone()
        if row:
            if row["referrer_id"] is None and referrer_id and referrer_id != user_id:
                cur.execute(
                    "UPDATE users SET username=?, first_name=?, referrer_id=?, updated_at=? WHERE user_id=?",
                    (username, first_name, referrer_id, now, user_id)
                )
                self._increment_ref_count_chain(referrer_id)
            else:
                cur.execute(
                    "UPDATE users SET username=?, first_name=?, updated_at=? WHERE user_id=?",
                    (username, first_name, now, user_id)
                )
        else:
            cur.execute(
                "INSERT INTO users(user_id, username, first_name, balance, subscription_until, referrer_id, referrals_count, referrals_income, created_at, updated_at) VALUES (?, ?, ?, 0, NULL, ?, 0, 0, ?, ?)",
                (user_id, username, first_name, referrer_id if referrer_id != user_id else None, now, now)
            )
            if referrer_id and referrer_id != user_id:
                self._increment_ref_count_chain(referrer_id)
        self.conn.commit()

    def _increment_ref_count_chain(self, referrer_id: int):
        # count direct invite only for referrer
        cur = self.conn.cursor()
        cur.execute("UPDATE users SET referrals_count = referrals_count + 1, updated_at=? WHERE user_id=?", (utc_now_iso(), referrer_id))

    def get_user(self, user_id: int):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM users WHERE user_id=?", (user_id,))
        return cur.fetchone()

    def set_balance(self, user_id: int, new_balance: int):
        cur = self.conn.cursor()
        cur.execute("UPDATE users SET balance=?, updated_at=? WHERE user_id=?", (new_balance, utc_now_iso(), user_id))
        self.conn.commit()

    def add_balance(self, user_id: int, amount: int):
        cur = self.conn.cursor()
        cur.execute("UPDATE users SET balance = balance + ?, updated_at=? WHERE user_id=?", (amount, utc_now_iso(), user_id))
        self.conn.commit()

    def get_subscription_until(self, user_id: int) -> Optional[datetime]:
        user = self.get_user(user_id)
        if not user or not user["subscription_until"]:
            return None
        try:
            return datetime.fromisoformat(user["subscription_until"])
        except Exception:
            return None

    def extend_subscription(self, user_id: int, days: int):
        now = datetime.now(TZ)
        current = self.get_subscription_until(user_id)
        if current and current > now:
            new_until = current + timedelta(days=days)
        else:
            new_until = now + timedelta(days=days)
        cur = self.conn.cursor()
        cur.execute("UPDATE users SET subscription_until=?, updated_at=? WHERE user_id=?", (new_until.isoformat(), utc_now_iso(), user_id))
        self.conn.commit()
        return new_until

    def create_transaction(self, transaction_id: str, user_id: int, amount: int, payment_method: int, target: str, status: str, redirect_url: str | None, expires_in: str | None, payload: str | None):
        now = utc_now_iso()
        cur = self.conn.cursor()
        cur.execute("""
        INSERT OR REPLACE INTO transactions(transaction_id, user_id, amount, currency, payment_method, target, status, redirect_url, expires_in, created_at, updated_at, payload)
        VALUES (?, ?, ?, 'RUB', ?, ?, ?, ?, ?, ?, ?, ?)
        """, (transaction_id, user_id, amount, payment_method, target, status, redirect_url, expires_in, now, now, payload))
        self.conn.commit()

    def update_transaction_status(self, transaction_id: str, status: str):
        cur = self.conn.cursor()
        cur.execute("UPDATE transactions SET status=?, updated_at=? WHERE transaction_id=?", (status, utc_now_iso(), transaction_id))
        self.conn.commit()

    def get_pending_transactions(self):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM transactions WHERE status='PENDING'")
        return cur.fetchall()

    def get_transaction(self, transaction_id: str):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM transactions WHERE transaction_id=?", (transaction_id,))
        return cur.fetchone()

    def get_stats(self):
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM users")
        users = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM users WHERE subscription_until IS NOT NULL")
        subscribed = cur.fetchone()[0]
        cur.execute("SELECT COALESCE(SUM(amount), 0) FROM transactions WHERE status='CONFIRMED'")
        paid = cur.fetchone()[0]
        return {"users": users, "subscribed": subscribed, "paid": paid}

    def add_referral_reward(self, from_user_id: int, to_user_id: int, level: int, amount: int):
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO referral_rewards(from_user_id, to_user_id, level, amount, created_at) VALUES (?, ?, ?, ?, ?)",
            (from_user_id, to_user_id, level, amount, utc_now_iso())
        )
        cur.execute(
            "UPDATE users SET referrals_income = referrals_income + ?, balance = balance + ?, updated_at=? WHERE user_id=?",
            (amount, amount, utc_now_iso(), to_user_id)
        )
        self.conn.commit()


db = Database(DB_PATH)

# =============================
# HELPERS
# =============================
def utc_now_iso() -> str:
    return datetime.now(TZ).isoformat()

def fmt_money(value: int) -> str:
    return f"{value} ₽"

def parse_referrer(arg: str | None) -> Optional[int]:
    if not arg:
        return None
    arg = arg.strip()
    for prefix in ("ref_", "referral_", "referral"):
        if arg.startswith(prefix):
            digits = re.sub(r"\D", "", arg)
            return int(digits) if digits else None
    if arg.isdigit():
        return int(arg)
    digits = re.sub(r"\D", "", arg)
    return int(digits) if digits else None

def is_subscription_active(user_row) -> bool:
    if not user_row or not user_row["subscription_until"]:
        return False
    try:
        until = datetime.fromisoformat(user_row["subscription_until"])
        return until > datetime.now(TZ)
    except Exception:
        return False

async def is_subscribed(bot: Bot, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(CHANNEL_USERNAME, user_id)
        return member.status in {
            ChatMemberStatus.CREATOR,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.RESTRICTED,
        }
    except Exception as e:
        logger.warning("Subscription check failed: %s", e)
        # allow access only if bot cannot check? better deny with clear message
        return False

def build_qr_bytes(text: str) -> bytes:
    qr = qrcode.QRCode(box_size=10, border=3)
    qr.add_data(text)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()

def main_menu_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🛒 Купить VPN", callback_data="menu:buy")
    kb.button(text="🔐 Моя подписка", callback_data="menu:subscription")
    kb.button(text="💳 Баланс", callback_data="menu:balance")
    kb.button(text="👥 Пригласить", callback_data="menu:invite")
    kb.button(text="ℹ️ О сервисе", callback_data="menu:about")
    kb.adjust(1, 2, 2)
    return kb.as_markup()

def back_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🏠 Личный кабинет", callback_data="menu:home")
    return kb.as_markup()

def sub_required_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="📢 Подписаться", url=CHANNEL_URL)
    kb.button(text="✅ Я подписался", callback_data="check_sub")
    kb.adjust(1)
    return kb.as_markup()

def subscription_kb(active: bool) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    if active:
        kb.button(text="🪄 Подключиться к VPN", callback_data="sub:connect")
        kb.button(text="📷 Показать QR-код", callback_data="sub:qr")
        kb.button(text="⏳ Продлить подписку", callback_data="sub:renew")
    else:
        kb.button(text="🛒 Купить VPN", callback_data="sub:buy")
    kb.button(text="🏠 Личный кабинет", callback_data="menu:home")
    kb.adjust(1)
    return kb.as_markup()

def balance_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="💰 Пополнить баланс", callback_data="balance:topup")
    kb.button(text="🎟 Активировать купон", callback_data="coupon:not_ready")
    kb.button(text="🏠 Личный кабинет", callback_data="menu:home")
    kb.adjust(1)
    return kb.as_markup()

def topup_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="💸 СБП", callback_data="pay:sbp:balance")
    kb.button(text="🪙 Криптовалюта", callback_data="pay:crypto:balance")
    kb.button(text="🏠 Личный кабинет", callback_data="menu:home")
    kb.adjust(1)
    return kb.as_markup()

def renew_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="💸 Купить через СБП", callback_data="pay:sbp:subscription")
    kb.button(text="🪙 Купить криптой", callback_data="pay:crypto:subscription")
    kb.button(text="✅ Проверить оплату", callback_data="pay:check_last")
    kb.button(text="🏠 Личный кабинет", callback_data="menu:home")
    kb.adjust(1)
    return kb.as_markup()

def invite_kb(user_id: int) -> InlineKeyboardMarkup:
    ref_link = f"https://t.me/{BOT_USERNAME}?start=referral_{user_id}"
    kb = InlineKeyboardBuilder()
    kb.button(text="📷 QR-код", callback_data="invite:qr")
    kb.button(text="🔗 Открыть ссылку", url=ref_link)
    kb.button(text="🏠 Личный кабинет", callback_data="menu:home")
    kb.adjust(1)
    return kb.as_markup()

def about_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="📄 Пользовательское соглашение", url=AGREEMENT_URL)
    kb.button(text="🔒 Политика конфиденциальности", url=POLICY_URL)
    kb.button(text="📢 Канал", url=CHANNEL_URL)
    kb.button(text="🏠 Назад", callback_data="menu:home")
    kb.adjust(1)
    return kb.as_markup()

def admin_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="📊 Статистика", callback_data="admin:stats")
    kb.button(text="💵 Выдать баланс", callback_data="admin:help_balance")
    kb.button(text="📅 Выдать подписку", callback_data="admin:help_sub")
    kb.button(text="📣 Рассылка", callback_data="admin:broadcast_help")
    kb.adjust(1)
    return kb.as_markup()

def payment_result_kb(url: str | None) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    if url:
        kb.button(text="💳 Перейти к оплате", url=url)
    kb.button(text="✅ Проверить оплату", callback_data="pay:check_last")
    kb.button(text="🏠 Личный кабинет", callback_data="menu:home")
    kb.adjust(1)
    return kb.as_markup()

# =============================
# PAYMENT
# =============================
class PlategaClient:
    def __init__(self):
        self.base_url = PLATEGA_BASE_URL
        self.headers = {
            "X-MerchantId": PLATEGA_MERCHANT_ID,
            "X-Secret": PLATEGA_SECRET,
            "Content-Type": "application/json",
        }

    async def create_payment(self, amount: int, method: int, description: str, payload: str) -> dict:
        body = {
            "paymentMethod": method,
            "paymentDetails": {
                "amount": amount,
                "currency": "RUB"
            },
            "description": description,
            "payload": payload
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self.base_url}/transaction/process", json=body, headers=self.headers, timeout=30) as resp:
                text = await resp.text()
                if resp.status >= 400:
                    raise RuntimeError(f"Platega create error {resp.status}: {text}")
                try:
                    return await resp.json()
                except Exception:
                    raise RuntimeError(f"Platega bad json: {text}")

    async def get_status(self, transaction_id: str) -> dict:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.base_url}/transaction/{transaction_id}", headers=self.headers, timeout=30) as resp:
                text = await resp.text()
                if resp.status >= 400:
                    raise RuntimeError(f"Platega status error {resp.status}: {text}")
                try:
                    return await resp.json()
                except Exception:
                    raise RuntimeError(f"Platega bad json: {text}")

payments = PlategaClient()
last_payment_by_user: dict[int, str] = {}
REF_PERCENTS = [25, 10, 6, 5, 4]

async def apply_transaction_effect(tx_row):
    if not tx_row or tx_row["status"] != "PENDING":
        return False
    status_data = await payments.get_status(tx_row["transaction_id"])
    status = (status_data.get("status") or "").upper()
    db.update_transaction_status(tx_row["transaction_id"], status)
    if status != "CONFIRMED":
        return False

    user_id = tx_row["user_id"]
    target = tx_row["target"]
    amount = int(tx_row["amount"])

    if target == "balance":
        db.add_balance(user_id, amount)
    elif target == "subscription":
        db.extend_subscription(user_id, 30)

    # 5-level referral rewards
    current_user = db.get_user(user_id)
    parent_id = current_user["referrer_id"] if current_user else None
    for level, percent in enumerate(REF_PERCENTS, start=1):
        if not parent_id:
            break
        reward = round(amount * percent / 100)
        if reward > 0:
            db.add_referral_reward(user_id, parent_id, level, reward)
        parent_row = db.get_user(parent_id)
        parent_id = parent_row["referrer_id"] if parent_row else None

    return True

async def payment_watcher(bot: Bot):
    while True:
        try:
            pending = db.get_pending_transactions()
            for tx in pending:
                try:
                    confirmed = await apply_transaction_effect(tx)
                    if confirmed:
                        try:
                            if tx["target"] == "subscription":
                                await bot.send_message(tx["user_id"], "✅ Оплата подтверждена. Подписка активирована на 30 дней.", reply_markup=main_menu_kb())
                            else:
                                await bot.send_message(tx["user_id"], f"✅ Оплата подтверждена. Баланс пополнен на {fmt_money(tx['amount'])}.", reply_markup=main_menu_kb())
                        except Exception as e:
                            logger.warning("Failed notify user %s: %s", tx["user_id"], e)
                except Exception as e:
                    logger.error("Payment watcher tx %s failed: %s", tx["transaction_id"], e)
        except Exception as e:
            logger.error("Payment watcher error: %s", e)

        await asyncio.sleep(30)

# =============================
# BOT
# =============================
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

async def ensure_user(message_or_cb, start_arg: str | None = None):
    user = message_or_cb.from_user
    referrer_id = parse_referrer(start_arg)
    db.upsert_user(user.id, user.username, user.first_name, referrer_id=referrer_id)

async def require_subscription_obj(obj) -> bool:
    user_id = obj.from_user.id
    ok = await is_subscribed(bot, user_id)
    if not ok:
        text = (
            "Для использования бота подпишитесь на наш канал.\n\n"
            "После подписки нажмите <b>Я подписался</b>."
        )
        if isinstance(obj, Message):
            await obj.answer(text, reply_markup=sub_required_kb())
        else:
            await obj.message.edit_text(text, reply_markup=sub_required_kb())
            await obj.answer()
    return ok

async def send_home(target):
    user = db.get_user(target.from_user.id)
    active = is_subscription_active(user)
    sub_count = 1 if active else 0
    text = (
        f"<b>AeroVPN</b>\n"
        f"Обход блокировок и глушилок, быстрый интернет.\n\n"
        f"🆔 ID: <code>{target.from_user.id}</code>\n"
        f"💰 Баланс: <b>{fmt_money(user['balance']) if user else '0 ₽'}</b>\n"
        f"🔐 Подписок: <b>{sub_count}</b>\n"
    )
    if isinstance(target, Message):
        await target.answer(text, reply_markup=main_menu_kb())
    else:
        await target.message.edit_text(text, reply_markup=main_menu_kb())
        await target.answer()

@dp.message(CommandStart())
async def cmd_start(message: Message, command: CommandStart):
    await ensure_user(message, command.args)
    if not await require_subscription_obj(message):
        return
    await send_home(message)

@dp.callback_query(F.data == "check_sub")
async def cb_check_sub(call: CallbackQuery):
    await ensure_user(call)
    if not await require_subscription_obj(call):
        return
    await send_home(call)

@dp.callback_query(F.data == "menu:home")
async def cb_home(call: CallbackQuery):
    await ensure_user(call)
    if not await require_subscription_obj(call):
        return
    await send_home(call)

@dp.callback_query(F.data == "menu:subscription")
async def cb_subscription(call: CallbackQuery):
    await ensure_user(call)
    if not await require_subscription_obj(call):
        return
    user = db.get_user(call.from_user.id)
    active = is_subscription_active(user)
    until = user["subscription_until"] if user and user["subscription_until"] else None
    if active:
        text = (
            "<b>🔐 Ваша подписка</b>\n\n"
            "✅ Подписка активна.\n"
            f"📅 Действует до: <b>{until}</b>\n"
            f"💸 Тариф: <b>{PRICE_MONTH_RUB} ₽ / месяц</b>\n\n"
            "Ссылка на подписку скрыта. Нажмите <b>Подключиться к VPN</b>, чтобы получить инструкцию и ссылку."
        )
    else:
        text = (
            "<b>❌ Подписка недействительна.</b>\n\n"
            "Возможно, срок просто истёк ⏰\n"
            "Продли AeroVPN — и снова будь под защитой.\n\n"
            f"💸 Стоимость: <b>{PRICE_MONTH_RUB} ₽ / месяц</b>"
        )
    await call.message.edit_text(text, reply_markup=subscription_kb(active))
    await call.answer()

@dp.callback_query(F.data == "sub:connect")
async def cb_connect(call: CallbackQuery):
    user = db.get_user(call.from_user.id)
    if not is_subscription_active(user):
        await call.answer("Сначала купите VPN", show_alert=True)
        await cb_subscription(call)
        return
    text = (
        "<b>🪄 Подключиться к VPN</b>\n\n"
        "1. Скачайте приложение <b>HAPP</b>\n"
        "2. Откройте приложение\n"
        "3. Найдите раздел <b>Import subscription / Подписка</b>\n"
        f"4. Вставьте ссылку:\n<code>{SUBSCRIPTION_URL}</code>\n"
        "5. Сохраните и подключитесь"
    )
    await call.message.edit_text(text, reply_markup=back_kb())
    await call.answer()

@dp.callback_query(F.data == "sub:buy")
@dp.callback_query(F.data == "menu:buy")
async def cb_buy(call: CallbackQuery):
    text = (
        "<b>🛒 Купить VPN</b>\n\n"
        f"Тариф: <b>{PRICE_MONTH_RUB} ₽ / месяц</b>\n"
        "Безлимитный доступ, быстрые сервера, обход блокировок.\n\n"
        "Выберите способ оплаты:"
    )
    await call.message.edit_text(text, reply_markup=renew_kb())
    await call.answer()

@dp.callback_query(F.data == "sub:qr")
async def cb_sub_qr(call: CallbackQuery):
    user = db.get_user(call.from_user.id)
    if not is_subscription_active(user):
        await call.answer("QR доступен только при активной подписке", show_alert=True)
        return
    img = build_qr_bytes(SUBSCRIPTION_URL)
    await call.message.answer_photo(
        BufferedInputFile(img, filename="subscription_qr.png"),
        caption=f"QR-код для ссылки подписки:\n<code>{SUBSCRIPTION_URL}</code>",
        reply_markup=back_kb()
    )
    await call.answer("QR-код отправлен")

@dp.callback_query(F.data == "sub:renew")
async def cb_sub_renew(call: CallbackQuery):
    text = (
        "<b>🛒 Купить VPN</b>\n\n"
        f"Цена: <b>{PRICE_MONTH_RUB} ₽</b> за 30 дней.\n"
        "Выберите способ оплаты:"
    )
    await call.message.edit_text(text, reply_markup=renew_kb())
    await call.answer()

@dp.callback_query(F.data == "menu:balance")
async def cb_balance(call: CallbackQuery):
    user = db.get_user(call.from_user.id)
    text = (
        "<b>💳 Баланс</b>\n\n"
        f"Ваш баланс: <b>{fmt_money(user['balance'] if user else 0)}</b>\n\n"
        "Пополнение доступно через СБП и криптовалюту."
    )
    await call.message.edit_text(text, reply_markup=balance_kb())
    await call.answer()

@dp.callback_query(F.data == "balance:topup")
async def cb_topup(call: CallbackQuery):
    text = (
        "<b>💰 Пополнение баланса</b>\n\n"
        f"Минимальное пополнение сейчас: <b>{PRICE_MONTH_RUB} ₽</b>\n"
        "Выберите способ оплаты:"
    )
    await call.message.edit_text(text, reply_markup=topup_kb())
    await call.answer()

async def start_payment(call: CallbackQuery, payment_method: int, target: str):
    user_id = call.from_user.id
    amount = PRICE_MONTH_RUB
    method_name = "СБП" if payment_method == 2 else "Криптовалюта"
    payload = f"user:{user_id};target:{target};method:{payment_method};ts:{int(datetime.now().timestamp())}"
    description = f"AeroVPN {'подписка' if target == 'subscription' else 'пополнение'} для user {user_id}"
    try:
        data = await payments.create_payment(amount=amount, method=payment_method, description=description, payload=payload)
    except Exception as e:
        await call.message.answer(f"❌ Не удалось создать платеж.\n{e}")
        await call.answer()
        return

    tx_id = data.get("transactionId")
    status = data.get("status", "PENDING")
    redirect = data.get("redirect")
    expires_in = data.get("expiresIn")
    db.create_transaction(
        transaction_id=tx_id,
        user_id=user_id,
        amount=amount,
        payment_method=payment_method,
        target=target,
        status=status,
        redirect_url=redirect,
        expires_in=expires_in,
        payload=payload,
    )
    last_payment_by_user[user_id] = tx_id

    text = (
        f"<b>💳 Платеж создан</b>\n\n"
        f"Способ: <b>{method_name}</b>\n"
        f"Сумма: <b>{amount} ₽</b>\n"
        f"Назначение: <b>{'Продление подписки' if target == 'subscription' else 'Пополнение баланса'}</b>\n"
        f"Статус: <b>{status}</b>\n"
        f"Транзакция: <code>{tx_id}</code>\n"
    )
    if expires_in:
        text += f"Действует: <b>{expires_in}</b>\n"
    text += "\nНажмите кнопку ниже, чтобы оплатить, затем проверьте статус."
    await call.message.edit_text(text, reply_markup=payment_result_kb(redirect))
    await call.answer()

@dp.callback_query(F.data.startswith("pay:"))
async def cb_pay_router(call: CallbackQuery):
    parts = call.data.split(":")
    if call.data == "pay:check_last":
        tx_id = last_payment_by_user.get(call.from_user.id)
        if not tx_id:
            await call.answer("Нет последнего платежа", show_alert=True)
            return
        tx = db.get_transaction(tx_id)
        if not tx:
            await call.answer("Платеж не найден", show_alert=True)
            return
        try:
            changed = await apply_transaction_effect(tx)
            tx = db.get_transaction(tx_id)
            status = tx["status"] if tx else "UNKNOWN"
            if status == "CONFIRMED":
                await call.message.answer("✅ Оплата подтверждена.", reply_markup=main_menu_kb())
            else:
                await call.answer(f"Статус: {status}", show_alert=True)
            return
        except Exception as e:
            await call.answer(f"Ошибка проверки: {e}", show_alert=True)
            return

    if len(parts) == 3:
        _, method, target = parts
        payment_method = 2 if method == "sbp" else 13
        await start_payment(call, payment_method, target)

@dp.callback_query(F.data == "menu:invite")
async def cb_invite(call: CallbackQuery):
    user = db.get_user(call.from_user.id)
    ref_link = f"https://t.me/{BOT_USERNAME}?start=referral_{call.from_user.id}"
    text = (
        "<b>👥 Реферальная система</b>\n\n"
        "Уровни бонусов:\n"
        "1 уровень — 25%\n"
        "2 уровень — 10%\n"
        "3 уровень — 6%\n"
        "4 уровень — 5%\n"
        "5 уровень — 4%\n\n"
        f"Ваша ссылка:\n<code>{ref_link}</code>\n\n"
        f"Приглашено: <b>{user['referrals_count'] if user else 0}</b>\n"
        f"Доход с рефералов: <b>{fmt_money(user['referrals_income'] if user else 0)}</b>"
    )
    await call.message.edit_text(text, reply_markup=invite_kb(call.from_user.id))
    await call.answer()

@dp.callback_query(F.data == "invite:qr")
async def cb_invite_qr(call: CallbackQuery):
    ref_link = f"https://t.me/{BOT_USERNAME}?start=referral_{call.from_user.id}"
    img = build_qr_bytes(ref_link)
    await call.message.answer_photo(
        BufferedInputFile(img, filename="invite_qr.png"),
        caption=f"QR-код для реферальной ссылки:\n<code>{ref_link}</code>",
        reply_markup=back_kb()
    )
    await call.answer("QR-код отправлен")

@dp.callback_query(F.data == "menu:about")
async def cb_about(call: CallbackQuery):
    text = (
        "<b>ℹ️ О сервисе</b>\n\n"
        "AeroVPN — это быстрый и надежный VPN-сервис.\n\n"
        "🚀 Высокая скорость — сервера в разных странах\n"
        "🔒 Защита данных и шифрование\n"
        "🌍 Обход блокировок и глушилок\n\n"
        "⚠️ Никогда не передавайте свою ссылку подписки другим людям."
    )
    await call.message.edit_text(text, reply_markup=about_kb())
    await call.answer()

@dp.callback_query(F.data == "coupon:not_ready")
async def cb_coupon(call: CallbackQuery):
    await call.answer("Купоны пока не подключены", show_alert=True)

# =============================
# ADMIN
# =============================
@dp.message(Command("admin"))
async def cmd_admin(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔ У вас нет доступа.")
        return
    help_text = (
        "<b>Админ-панель AeroVPN</b>\n\n"
        "Команды:\n"
        "/give_balance user_id amount\n"
        "/take_balance user_id amount\n"
        "/give_sub user_id days\n"
        "/broadcast текст\n"
    )
    await message.answer(help_text, reply_markup=admin_kb())

@dp.callback_query(F.data == "admin:stats")
async def cb_admin_stats(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("Нет доступа", show_alert=True)
        return
    s = db.get_stats()
    await call.message.answer(
        f"<b>Статистика</b>\n\n"
        f"Пользователей: <b>{s['users']}</b>\n"
        f"Покупок подтверждено: <b>{fmt_money(s['paid'])}</b>\n"
        f"Пользователей с подпиской: <b>{s['subscribed']}</b>"
    )
    await call.answer()

@dp.callback_query(F.data == "admin:help_balance")
async def cb_admin_help_balance(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("Нет доступа", show_alert=True)
        return
    await call.message.answer("Формат:\n<code>/give_balance user_id amount</code>\n<code>/take_balance user_id amount</code>")
    await call.answer()

@dp.callback_query(F.data == "admin:help_sub")
async def cb_admin_help_sub(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("Нет доступа", show_alert=True)
        return
    await call.message.answer("Формат:\n<code>/give_sub user_id days</code>")
    await call.answer()

@dp.callback_query(F.data == "admin:broadcast_help")
async def cb_admin_broadcast_help(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("Нет доступа", show_alert=True)
        return
    await call.message.answer("Формат:\n<code>/broadcast текст сообщения</code>")
    await call.answer()

@dp.message(Command("give_balance"))
async def cmd_give_balance(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3 or not parts[1].isdigit() or not parts[2].isdigit():
        await message.answer("Используй: /give_balance user_id amount")
        return
    user_id, amount = int(parts[1]), int(parts[2])
    db.upsert_user(user_id, None, None)
    db.add_balance(user_id, amount)
    await message.answer(f"✅ Баланс {amount} ₽ выдан пользователю {user_id}")

@dp.message(Command("take_balance"))
async def cmd_take_balance(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3 or not parts[1].isdigit() or not parts[2].isdigit():
        await message.answer("Используй: /take_balance user_id amount")
        return
    user_id, amount = int(parts[1]), int(parts[2])
    user = db.get_user(user_id)
    if not user:
        await message.answer("Пользователь не найден")
        return
    new_balance = max(0, int(user["balance"]) - amount)
    db.set_balance(user_id, new_balance)
    await message.answer(f"✅ У пользователя {user_id} списано {amount} ₽")

@dp.message(Command("give_sub"))
async def cmd_give_sub(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3 or not parts[1].isdigit() or not parts[2].isdigit():
        await message.answer("Используй: /give_sub user_id days")
        return
    user_id, days = int(parts[1]), int(parts[2])
    db.upsert_user(user_id, None, None)
    until = db.extend_subscription(user_id, days)
    await message.answer(f"✅ Подписка выдана до {until.isoformat()} пользователю {user_id}")

@dp.message(Command("broadcast"))
async def cmd_broadcast(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    text = message.text.removeprefix("/broadcast").strip()
    if not text:
        await message.answer("Используй: /broadcast текст")
        return
    cur = db.conn.cursor()
    cur.execute("SELECT user_id FROM users")
    rows = cur.fetchall()
    ok = 0
    bad = 0
    for row in rows:
        try:
            await bot.send_message(row["user_id"], text)
            ok += 1
        except Exception:
            bad += 1
    await message.answer(f"✅ Рассылка завершена.\nУспешно: {ok}\nОшибок: {bad}")

# =============================
# FALLBACK
# =============================
@dp.message()
async def fallback(message: Message):
    await ensure_user(message)
    if not await require_subscription_obj(message):
        return
    await send_home(message)

async def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не задан")
    logger.info("Bot starting")
    asyncio.create_task(payment_watcher(bot))
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
