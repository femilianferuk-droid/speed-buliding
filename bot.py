"""
Speed Building Bot — Telegram-бот для продажи Lego деталей поштучно.

Стек:
  • aiogram 3.x   (async Telegram framework)
  • asyncpg       (драйвер PostgreSQL)
  • aiohttp       (HTTP клиент для CryptoBot API)

Переменные окружения (обязательные):
  BOT_TOKEN         — токен Telegram-бота
  DATABASE_URL      — DSN PostgreSQL, postgres://user:pass@host:5432/dbname
  CRYPTO_BOT_TOKEN  — токен CryptoPay (@CryptoBot → Crypto Pay → My Apps)

Необязательные:
  ADMIN_ID          — Telegram ID админа (по умолчанию 7973988177)
  ADMIN_USERNAME    — username админа без @ для кнопки «Поддержка»
  RUB_PER_USDT      — курс (по умолчанию 90)
  PER_PAGE          — товаров на странице каталога (по умолчанию 8)

Запуск:
  pip install -r requirements.txt
  export BOT_TOKEN=... DATABASE_URL=... CRYPTO_BOT_TOKEN=...
  python bot.py
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import suppress
from decimal import Decimal, ROUND_UP, InvalidOperation
from typing import Optional, List

import aiohttp
import asyncpg
from aiogram import Bot, Dispatcher, F, types
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

# =====================================================================
# КОНФИГУРАЦИЯ
# =====================================================================

BOT_TOKEN: str = os.environ["BOT_TOKEN"]
DATABASE_URL: str = os.environ["DATABASE_URL"]
CRYPTO_BOT_TOKEN: str = os.environ["CRYPTO_BOT_TOKEN"]
ADMIN_ID: int = int(os.getenv("ADMIN_ID", "7973988177"))
ADMIN_USERNAME: str = os.getenv("ADMIN_USERNAME", "SpeedBuildingSupport").lstrip("@")
RUB_PER_USDT: Decimal = Decimal(os.getenv("RUB_PER_USDT", "90"))
PER_PAGE: int = int(os.getenv("PER_PAGE", "8"))

CRYPTO_BOT_API = "https://pay.crypt.bot/api"
SHOP_NAME = "Speed Building"

# =====================================================================
# ЛОГИРОВАНИЕ
# =====================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("speed_building")

# =====================================================================
# БАЗА ДАННЫХ
# =====================================================================

pool: Optional[asyncpg.Pool] = None


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS users (
    id          BIGINT PRIMARY KEY,
    username    TEXT,
    full_name   TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS categories (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    description TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS products (
    id           SERIAL PRIMARY KEY,
    category_id  INT NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
    name         TEXT NOT NULL,
    description  TEXT,
    price_rub    NUMERIC(12, 2) NOT NULL CHECK (price_rub >= 0),
    stock        INT  NOT NULL DEFAULT 0 CHECK (stock >= 0),
    photo_id     TEXT,
    is_original  BOOLEAN NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS cart_items (
    user_id     BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    product_id  INT    NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    quantity    INT    NOT NULL DEFAULT 1 CHECK (quantity > 0),
    added_at    TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, product_id)
);

CREATE TABLE IF NOT EXISTS orders (
    id                SERIAL PRIMARY KEY,
    user_id           BIGINT NOT NULL REFERENCES users(id),
    total_rub         NUMERIC(12, 2) NOT NULL,
    total_usdt        NUMERIC(12, 4) NOT NULL,
    status            TEXT NOT NULL DEFAULT 'pending',
    crypto_invoice_id BIGINT,
    crypto_pay_url    TEXT,
    created_at        TIMESTAMPTZ DEFAULT NOW(),
    paid_at           TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS order_items (
    id           SERIAL PRIMARY KEY,
    order_id     INT  NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_id   INT  REFERENCES products(id) ON DELETE SET NULL,
    product_name TEXT NOT NULL,
    quantity     INT  NOT NULL,
    price_rub    NUMERIC(12, 2) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_products_category ON products(category_id);
CREATE INDEX IF NOT EXISTS idx_orders_user       ON orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_status     ON orders(status);
"""


async def init_db() -> asyncpg.Pool:
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA_SQL)
    log.info("Database ready")
    return pool


async def close_db() -> None:
    if pool:
        await pool.close()


# ---------- helpers ----------
async def upsert_user(user: types.User) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO users (id, username, full_name)
               VALUES ($1, $2, $3)
               ON CONFLICT (id) DO UPDATE
                   SET username  = EXCLUDED.username,
                       full_name = EXCLUDED.full_name""",
            user.id, user.username, user.full_name,
        )


async def get_user(user_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM users WHERE id = $1", user_id)


async def get_user_stats(user_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            """SELECT u.*,
                      (SELECT COUNT(*) FROM orders
                        WHERE user_id = u.id) AS orders_count,
                      (SELECT COALESCE(SUM(total_rub),0) FROM orders
                        WHERE user_id = u.id AND status='paid') AS spent_rub
               FROM users u WHERE u.id = $1""",
            user_id,
        )


async def get_categories():
    async with pool.acquire() as conn:
        return await conn.fetch("SELECT * FROM categories ORDER BY id")


async def get_category(cat_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM categories WHERE id = $1", cat_id,
        )


async def add_category(name: str, description: str = "") -> bool:
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO categories (name, description) VALUES ($1, $2)",
                name, description,
            )
        return True
    except asyncpg.UniqueViolationError:
        return False


async def delete_category(cat_id: int) -> bool:
    async with pool.acquire() as conn:
        # ON DELETE CASCADE уберёт товары, корзины и позиции заказов
        result = await conn.execute("DELETE FROM categories WHERE id = $1", cat_id)
        return result.endswith("1")


async def get_products_by_category(cat_id: int):
    async with pool.acquire() as conn:
        return await conn.fetch(
            "SELECT * FROM products WHERE category_id = $1 ORDER BY id", cat_id,
        )


async def get_product(prod_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM products WHERE id = $1", prod_id,
        )


async def get_all_products():
    async with pool.acquire() as conn:
        return await conn.fetch("SELECT * FROM products ORDER BY id")


async def add_product(category_id: int, name: str, description: str,
                      price_rub: Decimal, stock: int, photo_id: Optional[str],
                      is_original: bool) -> int:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            """INSERT INTO products
                 (category_id, name, description, price_rub,
                  stock, photo_id, is_original)
               VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id""",
            category_id, name, description, price_rub,
            stock, photo_id, is_original,
        )


async def update_product(prod_id: int, **fields) -> None:
    if not fields:
        return
    cols = ", ".join(f"{k} = ${i+2}" for i, k in enumerate(fields))
    values = list(fields.values())
    async with pool.acquire() as conn:
        await conn.execute(
            f"UPDATE products SET {cols} WHERE id = $1", prod_id, *values,
        )


async def delete_product(prod_id: int) -> None:
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM products WHERE id = $1", prod_id)


async def update_stock(prod_id: int, stock: int) -> None:
    await update_product(prod_id, stock=stock)


async def add_to_cart(user_id: int, product_id: int, qty: int = 1) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO cart_items (user_id, product_id, quantity)
               VALUES ($1,$2,$3)
               ON CONFLICT (user_id, product_id)
               DO UPDATE SET quantity = cart_items.quantity + EXCLUDED.quantity""",
            user_id, product_id, qty,
        )


async def set_cart_qty(user_id: int, product_id: int, qty: int) -> None:
    async with pool.acquire() as conn:
        if qty <= 0:
            await conn.execute(
                "DELETE FROM cart_items WHERE user_id=$1 AND product_id=$2",
                user_id, product_id,
            )
        else:
            await conn.execute(
                """UPDATE cart_items SET quantity = $3
                   WHERE user_id=$1 AND product_id=$2""",
                user_id, product_id, qty,
            )


async def get_cart(user_id: int):
    async with pool.acquire() as conn:
        return await conn.fetch(
            """SELECT c.product_id, c.quantity,
                      p.name, p.price_rub, p.stock, p.photo_id, p.is_original,
                      cat.name AS category_name
               FROM cart_items c
               JOIN products  p   ON p.id   = c.product_id
               JOIN categories cat ON cat.id = p.category_id
               WHERE c.user_id = $1
               ORDER BY c.added_at""",
            user_id,
        )


async def clear_cart(user_id: int) -> None:
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM cart_items WHERE user_id = $1", user_id)


async def create_order(user_id: int, total_rub: Decimal,
                       total_usdt: Decimal) -> int:
    async with pool.acquire() as conn:
        async with conn.transaction():
            order_id = await conn.fetchval(
                """INSERT INTO orders (user_id, total_rub, total_usdt, status)
                   VALUES ($1,$2,$3,'pending') RETURNING id""",
                user_id, total_rub, total_usdt,
            )
            cart = await conn.fetch(
                """SELECT c.product_id, c.quantity, p.name, p.price_rub
                   FROM cart_items c
                   JOIN products p ON p.id = c.product_id
                   WHERE c.user_id = $1""",
                user_id,
            )
            for it in cart:
                await conn.execute(
                    """INSERT INTO order_items
                         (order_id, product_id, product_name, quantity, price_rub)
                       VALUES ($1,$2,$3,$4,$5)""",
                    order_id, it["product_id"], it["name"],
                    it["quantity"], it["price_rub"],
                )
            return order_id


async def attach_invoice(order_id: int, invoice_id: int, pay_url: str) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """UPDATE orders
               SET crypto_invoice_id = $2, crypto_pay_url = $3
               WHERE id = $1""",
            order_id, invoice_id, pay_url,
        )


async def get_order(order_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM orders WHERE id = $1", order_id,
        )


async def get_user_orders(user_id: int, limit: int = 50):
    async with pool.acquire() as conn:
        return await conn.fetch(
            """SELECT * FROM orders
               WHERE user_id = $1
               ORDER BY id DESC LIMIT $2""",
            user_id, limit,
        )


async def get_all_orders(status: Optional[str] = None, limit: int = 50):
    async with pool.acquire() as conn:
        if status:
            return await conn.fetch(
                """SELECT o.*, u.username, u.full_name
                   FROM orders o LEFT JOIN users u ON u.id = o.user_id
                   WHERE o.status = $1
                   ORDER BY o.id DESC LIMIT $2""",
                status, limit,
            )
        return await conn.fetch(
            """SELECT o.*, u.username, u.full_name
               FROM orders o LEFT JOIN users u ON u.id = o.user_id
               ORDER BY o.id DESC LIMIT $1""",
            limit,
        )


async def get_order_items(order_id: int):
    async with pool.acquire() as conn:
        return await conn.fetch(
            "SELECT * FROM order_items WHERE order_id = $1", order_id,
        )


async def mark_order_paid(order_id: int) -> bool:
    """Помечает заказ оплаченным и списывает товары со склада.
    Возвращает True, если статус изменился."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            updated = await conn.fetchval(
                """UPDATE orders SET status='paid', paid_at=NOW()
                   WHERE id = $1 AND status='pending' RETURNING id""",
                order_id,
            )
            if not updated:
                return False
            items = await conn.fetch(
                "SELECT product_id, quantity FROM order_items WHERE order_id=$1",
                order_id,
            )
            for it in items:
                if it["product_id"] is None:
                    continue
                await conn.execute(
                    """UPDATE products SET stock = GREATEST(0, stock - $2)
                       WHERE id = $1""",
                    it["product_id"], it["quantity"],
                )
            return True


async def mark_order_unpaid(order_id: int) -> bool:
    """Снимает отметку оплаты (для админа — откат ручного подтверждения)."""
    async with pool.acquire() as conn:
        result = await conn.execute(
            """UPDATE orders SET status='pending', paid_at=NULL
               WHERE id = $1 AND status='paid'""",
            order_id,
        )
        return result.endswith("1")


async def get_admin_stats():
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            """SELECT
                 (SELECT COUNT(*) FROM orders)                                AS orders_total,
                 (SELECT COUNT(*) FROM orders WHERE status='paid')            AS orders_paid,
                 (SELECT COUNT(*) FROM orders WHERE status='pending')         AS orders_pending,
                 (SELECT COALESCE(SUM(total_rub),0) FROM orders
                    WHERE status='paid')                                       AS revenue_total,
                 (SELECT COALESCE(SUM(total_rub),0) FROM orders
                    WHERE status='paid'
                      AND paid_at > NOW() - INTERVAL '24 hours')               AS revenue_day,
                 (SELECT COUNT(*) FROM orders
                    WHERE status='paid'
                      AND paid_at > NOW() - INTERVAL '24 hours')               AS orders_day,
                 (SELECT COALESCE(SUM(total_rub),0) FROM orders
                    WHERE status='paid'
                      AND paid_at > NOW() - INTERVAL '7 days')                 AS revenue_week,
                 (SELECT COUNT(*) FROM orders
                    WHERE status='paid'
                      AND paid_at > NOW() - INTERVAL '7 days')                 AS orders_week,
                 (SELECT COUNT(DISTINCT user_id) FROM orders)                 AS customers,
                 (SELECT COUNT(*) FROM products)                               AS products,
                 (SELECT COUNT(*) FROM categories)                             AS categories"""
        )


# =====================================================================
# CRYPTOBOT API
# =====================================================================

class CryptoBotError(Exception):
    """Ошибка CryptoBot API с кодом и именем."""

    def __init__(self, code: int, name: str):
        self.code = code
        self.name = name
        super().__init__(f"CryptoBot {code} {name}")


# Человекочитаемые сообщения об ошибках
ERR_USER_MESSAGES = {
    401: ("Платёжный сервис временно недоступен "
          "(ошибка авторизации). Администратор уже уведомлён — "
          "попробуй через несколько минут или свяжись с поддержкой."),
    403: "Приложение оплаты заблокировано. Обратись в поддержку.",
    429: "Слишком много запросов к платёжному сервису. Подожди минуту и попробуй снова.",
    400: "Некорректные параметры платежа. Обратись в поддержку.",
}


def cryptobot_user_message(e: CryptoBotError) -> str:
    if e.code in ERR_USER_MESSAGES:
        return ERR_USER_MESSAGES[e.code]
    if 500 <= e.code < 600:
        return "Сервер оплаты сейчас недоступен. Попробуй через пару минут."
    return (f"Не удалось создать платёж (код {e.code}). "
            "Попробуй позже или обратись в поддержку.")


async def cryptobot(method: str, params: Optional[dict] = None) -> dict:
    """Базовый вызов CryptoPay API. Бросает CryptoBotError при ошибке."""
    headers = {"Crypto-Pay-Token": CRYPTO_BOT_TOKEN}
    url = f"{CRYPTO_BOT_API}/{method}"
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url, headers=headers, params=params or {}) as r:
            try:
                data = await r.json()
            except Exception as e:
                log.exception("CryptoBot bad json")
                raise CryptoBotError(0, f"BAD_RESPONSE: {e}") from e
            if not data.get("ok"):
                err = data.get("error", {})
                raise CryptoBotError(err.get("code", 0), err.get("name", "UNKNOWN"))
            return data["result"]


async def cryptobot_self_check() -> Optional[str]:
    """Возвращает None если токен рабочий, иначе строку с описанием ошибки."""
    try:
        await cryptobot("getMe")
        return None
    except CryptoBotError as e:
        return f"код {e.code} ({e.name})"
    except Exception as e:
        return f"ошибка: {e}"


async def create_invoice(amount_usdt: Decimal, description: str,
                         payload: str) -> dict:
    amount = str(amount_usdt.quantize(Decimal("0.01"), rounding=ROUND_UP))
    if Decimal(amount) < Decimal("0.01"):
        raise CryptoBotError(400, "AMOUNT_TOO_SMALL")
    params = {
        "currency_type": "crypto",
        "asset": "USDT",
        "amount": amount,
        "description": description[:1024],
        "payload": payload[:128],
        "paid_btn_name": "viewItem",
    }
    return await cryptobot("createInvoice", params)


async def get_invoice(invoice_id: int) -> dict:
    return await cryptobot("getInvoices", {"invoice_ids": invoice_id})


# =====================================================================
# FSM СОСТОЯНИЯ
# =====================================================================

class AdminStates(StatesGroup):
    # Добавление категории
    add_cat_name        = State()
    add_cat_description = State()

    # Добавление товара
    add_prod_category   = State()
    add_prod_name       = State()
    add_prod_description = State()
    add_prod_price      = State()
    add_prod_stock      = State()
    add_prod_photo      = State()
    add_prod_original   = State()

    # Редактирование наличия
    edit_stock_pick_cat = State()
    edit_stock_pick_prod = State()
    edit_stock_value    = State()

    # Удаление товара
    delete_pick_cat    = State()
    delete_pick_prod   = State()
    delete_confirm     = State()

    # Удаление категории
    delete_cat_pick    = State()
    delete_cat_confirm = State()

    # Редактирование товара
    edit_prod_pick_cat  = State()
    edit_prod_pick_prod = State()
    edit_prod_field     = State()
    edit_prod_name      = State()
    edit_prod_desc      = State()
    edit_prod_price     = State()
    edit_prod_original  = State()

    # Ручное подтверждение оплаты
    manual_pay_pick     = State()
    manual_pay_confirm  = State()


# =====================================================================
# КЛАВИАТУРЫ
# =====================================================================

def main_menu_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🛒 Каталог",      callback_data="menu:catalog")
    kb.button(text="🧺 Корзина",      callback_data="menu:cart")
    kb.button(text="👤 Профиль",      callback_data="menu:profile")
    kb.button(text="📦 Мои заказы",   callback_data="menu:orders")
    kb.button(text="ℹ️ О магазине",   callback_data="menu:about")
    kb.adjust(2, 2, 1)
    return kb.as_markup()


def support_kb() -> list[list[InlineKeyboardButton]]:
    """Кнопка «Поддержка» — ссылка на админа (если задан username)."""
    rows = []
    if ADMIN_USERNAME:
        rows.append([InlineKeyboardButton(
            text="💬 Поддержка", url=f"https://t.me/{ADMIN_USERNAME}",
        )])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:home")])
    return rows


def admin_main_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Категория",       callback_data="adm:add_cat")
    kb.button(text="➕ Товар",           callback_data="adm:add_prod")
    kb.button(text="✏️ Изменить наличие", callback_data="adm:edit_stock")
    kb.button(text="✏️ Редактировать товар", callback_data="adm:edit_prod")
    kb.button(text="🗑 Удалить товар",   callback_data="adm:delete_prod")
    kb.button(text="🗑 Удалить категорию", callback_data="adm:delete_cat")
    kb.button(text="📦 Все заказы",      callback_data="adm:orders")
    kb.button(text="💸 Статистика",      callback_data="adm:stats")
    kb.button(text="🔧 Тест CryptoBot",  callback_data="adm:test_crypto")
    kb.button(text="📋 Товары / Категории", callback_data="adm:lists")
    kb.adjust(2, 2, 2, 2, 1, 1)
    return kb.as_markup()


def cancel_kb(back_to: str = "menu:home") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отмена", callback_data=back_to)],
    ])


# =====================================================================
# ФОРМАТИРОВАНИЕ
# =====================================================================

def fmt_money(amount: Decimal, suffix: str = "₽") -> str:
    s = f"{amount:.2f}"
    int_part, dec_part = s.split(".")
    sign = ""
    if int_part.startswith("-"):
        sign = "-"
        int_part = int_part[1:]
    groups = []
    while len(int_part) > 3:
        groups.append(int_part[-3:])
        int_part = int_part[:-3]
    groups.append(int_part)
    return f"{sign}{' '.join(reversed(groups))}.{dec_part} {suffix}"


def rub_to_usdt(rub: Decimal) -> Decimal:
    return (rub / RUB_PER_USDT).quantize(Decimal("0.01"), rounding=ROUND_UP)


def product_caption(p, full: bool = True) -> str:
    cat_name = p["category_name"] if "category_name" in p.keys() else ""
    kind = "✅ Оригинал" if p["is_original"] else "♻️ Аналог"
    lines = [
        f"🧱 <b>{p['name']}</b>",
        f"Категория: {cat_name}" if cat_name else "",
        f"{kind}",
        f"💰 Цена: <b>{fmt_money(p['price_rub'])}</b>",
    ]
    if full:
        lines.append(f"📦 В наличии: <b>{p['stock']} шт.</b>")
        if p["description"]:
            lines.append("")
            lines.append(p["description"])
    return "\n".join([x for x in lines if x])


def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


# =====================================================================
# БОТ И ДИСПЕТЧЕР
# =====================================================================

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())


async def notify_admin(text: str) -> None:
    """Шлёт сообщение админу, подавляя любые ошибки (чтобы не ломать основной flow)."""
    with suppress(Exception):
        await bot.send_message(ADMIN_ID, text)


# =====================================================================
# ХЭНДЛЕРЫ — ОБЩИЕ
# =====================================================================

@dp.message(CommandStart())
async def cmd_start(message: Message):
    await upsert_user(message.from_user)
    text = (
        f"👋 Привет, <b>{message.from_user.first_name or 'друг'}</b>!\n\n"
        f"Мы — <b>{SHOP_NAME}</b> 🧱\n"
        "Продаём детали <b>Lego поштучно</b>: "
        "есть как <b>оригинальные</b>, так и качественные <b>аналоги</b>.\n\n"
        "Выбирай раздел и погнали:"
    )
    await message.answer(text, reply_markup=main_menu_kb())


@dp.message(Command("admin"))
async def cmd_admin(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Доступ запрещён.")
        return
    await message.answer(
        f"⚙️ <b>Админ-панель {SHOP_NAME}</b>\n"
        "Выбери действие:",
        reply_markup=admin_main_kb(),
    )


@dp.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    if await state.get_state():
        await state.clear()
        await message.answer("Действие отменено.", reply_markup=main_menu_kb())


@dp.callback_query(F.data == "menu:home")
async def cb_home(call: CallbackQuery, state: FSMContext):
    await state.clear()
    text = (
        f"👋 <b>{SHOP_NAME}</b> — детали Lego поштучно.\n"
        "Оригиналы и аналоги. Выбирай раздел:"
    )
    try:
        await call.message.edit_text(text, reply_markup=main_menu_kb())
    except Exception:
        await call.message.answer(text, reply_markup=main_menu_kb())
    await call.answer()


@dp.callback_query(F.data == "menu:about")
async def cb_about(call: CallbackQuery):
    text = (
        f"ℹ️ <b>О магазине {SHOP_NAME}</b>\n\n"
        "• Продаём Lego-детали поштучно\n"
        "• Есть оригиналы и качественные аналоги\n"
        "• Оплата — USDT через CryptoBot\n"
        f"• Курс: 1 USDT = {RUB_PER_USDT}₽\n\n"
        "Возникли вопросы — напиши в поддержку 👇"
    )
    await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(
        inline_keyboard=support_kb()
    ))
    await call.answer()


# =====================================================================
# КАТАЛОГ
# =====================================================================

@dp.callback_query(F.data == "menu:catalog")
async def cb_catalog(call: CallbackQuery):
    cats = await get_categories()
    if not cats:
        await call.message.edit_text(
            "😔 Пока нет ни одной категории.\n"
            "Загляни позже или напиши администратору.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:home")],
            ]),
        )
        return
    kb = InlineKeyboardBuilder()
    for c in cats:
        kb.button(text=f"📁 {c['name']}", callback_data=f"cat:{c['id']}:0")
    kb.button(text="⬅️ Назад", callback_data="menu:home")
    kb.adjust(1)
    await call.message.edit_text("📂 <b>Категории</b>", reply_markup=kb.as_markup())
    await call.answer()


@dp.callback_query(F.data.startswith("cat:"))
async def cb_category(call: CallbackQuery):
    """Просмотр категории с пагинацией: cat:{id}:{page}"""
    parts = call.data.split(":")
    cat_id = int(parts[1])
    page = int(parts[2]) if len(parts) > 2 else 0

    cat = await get_category(cat_id)
    if not cat:
        await call.answer("Категория не найдена", show_alert=True)
        return

    products = await get_products_by_category(cat_id)
    total = len(products)
    pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
    page = max(0, min(page, pages - 1))
    start = page * PER_PAGE
    chunk = products[start:start + PER_PAGE]

    if not chunk:
        await call.message.edit_text(
            f"В категории <b>{cat['name']}</b> пока пусто.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ К категориям",
                                      callback_data="menu:catalog")],
            ]),
        )
        return

    kb = InlineKeyboardBuilder()
    for p in chunk:
        kind = "✅" if p["is_original"] else "♻️"
        kb.button(
            text=f"{kind} {p['name']} — {fmt_money(p['price_rub'])}",
            callback_data=f"prod:{p['id']}",
        )
    kb.adjust(1)

    if pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(
                text="◀️", callback_data=f"cat:{cat_id}:{page-1}",
            ))
        nav.append(InlineKeyboardButton(
            text=f"{page+1}/{pages}", callback_data="noop",
        ))
        if page + 1 < pages:
            nav.append(InlineKeyboardButton(
                text="▶️", callback_data=f"cat:{cat_id}:{page+1}",
            ))
        kb.row(*nav)

    kb.row(InlineKeyboardButton(text="⬅️ К категориям",
                                callback_data="menu:catalog"))
    await call.message.edit_text(
        f"📂 <b>{cat['name']}</b>\nТоваров: {total}. Стр. {page+1}/{pages}",
        reply_markup=kb.as_markup(),
    )
    await call.answer()


@dp.callback_query(F.data.startswith("prod:"))
async def cb_product(call: CallbackQuery):
    prod_id = int(call.data.split(":")[1])
    p = await get_product(prod_id)
    if not p:
        await call.answer("Товар не найден", show_alert=True)
        return
    cat = await get_category(p["category_id"])
    p = dict(p)
    p["category_name"] = cat["name"] if cat else ""

    caption = product_caption(p, full=True)
    kb = InlineKeyboardBuilder()
    if p["stock"] > 0:
        kb.button(text="🛒 В корзину", callback_data=f"cart:add:{prod_id}")
    else:
        kb.button(text="❌ Нет в наличии", callback_data="noop")
    kb.button(text="⬅️ К категории",
              callback_data=f"cat:{p['category_id']}:0")
    kb.adjust(1)

    if p["photo_id"]:
        await call.message.answer_photo(
            p["photo_id"], caption=caption, reply_markup=kb.as_markup(),
        )
        with suppress(Exception):
            await call.message.delete()
    else:
        await call.message.edit_text(caption, reply_markup=kb.as_markup())
    await call.answer()


@dp.callback_query(F.data == "noop")
async def cb_noop(call: CallbackQuery):
    await call.answer("Нет в наличии", show_alert=True)


# =====================================================================
# КОРЗИНА
# =====================================================================

async def render_cart(user_id: int, message: Message, edit: bool = False):
    cart = await get_cart(user_id)
    if not cart:
        text = "🧺 Корзина пуста.\nДобавь товары из каталога."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛒 В каталог",
                                  callback_data="menu:catalog")],
            [InlineKeyboardButton(text="⬅️ Назад",
                                  callback_data="menu:home")],
        ])
        if edit:
            await message.edit_text(text, reply_markup=kb)
        else:
            await message.answer(text, reply_markup=kb)
        return

    total = Decimal("0")
    lines = ["🧺 <b>Ваша корзина</b>\n"]
    for it in cart:
        sub = it["price_rub"] * it["quantity"]
        total += sub
        kind = "✅" if it["is_original"] else "♻️"
        lines.append(
            f"• {kind} <b>{it['name']}</b>\n"
            f"   {it['quantity']} × {fmt_money(it['price_rub'])} = "
            f"<b>{fmt_money(sub)}</b>"
        )
    lines.append(f"\n💳 Итого: <b>{fmt_money(total)}</b>")
    lines.append(f"≈ <b>{rub_to_usdt(total)} USDT</b> (1 USDT = {RUB_PER_USDT}₽)")

    kb = InlineKeyboardBuilder()
    for it in cart:
        kb.button(
            text=f"➖ {it['name'][:18]} ({it['quantity']})",
            callback_data=f"cart:dec:{it['product_id']}",
        )
        kb.button(
            text="➕", callback_data=f"cart:inc:{it['product_id']}",
        )
        kb.button(
            text="❌", callback_data=f"cart:rm:{it['product_id']}",
        )
    kb.button(text="✅ Оформить заказ", callback_data="cart:checkout")
    kb.button(text="🗑 Очистить",        callback_data="cart:clear")
    kb.button(text="⬅️ Назад",           callback_data="menu:home")
    kb.adjust(3, 1, 1)

    text = "\n".join(lines)
    if edit:
        await message.edit_text(text, reply_markup=kb.as_markup())
    else:
        await message.answer(text, reply_markup=kb.as_markup())


@dp.callback_query(F.data == "menu:cart")
async def cb_cart(call: CallbackQuery):
    await render_cart(call.from_user.id, call.message, edit=True)
    await call.answer()


@dp.callback_query(F.data.startswith("cart:add:"))
async def cb_cart_add(call: CallbackQuery):
    prod_id = int(call.data.split(":")[2])
    p = await get_product(prod_id)
    if not p or p["stock"] <= 0:
        await call.answer("Нет в наличии", show_alert=True)
        return
    await upsert_user(call.from_user)
    await add_to_cart(call.from_user.id, prod_id, 1)
    await call.answer("Добавлено в корзину ✅")


@dp.callback_query(F.data.startswith("cart:inc:"))
async def cb_cart_inc(call: CallbackQuery):
    prod_id = int(call.data.split(":")[2])
    p = await get_product(prod_id)
    cart = await get_cart(call.from_user.id)
    current = next((c["quantity"] for c in cart if c["product_id"] == prod_id), 0)
    if p and current + 1 > p["stock"]:
        await call.answer("Больше нет в наличии", show_alert=True)
        return
    await set_cart_qty(call.from_user.id, prod_id, current + 1)
    await render_cart(call.from_user.id, call.message, edit=True)
    await call.answer()


@dp.callback_query(F.data.startswith("cart:dec:"))
async def cb_cart_dec(call: CallbackQuery):
    prod_id = int(call.data.split(":")[2])
    cart = await get_cart(call.from_user.id)
    current = next((c["quantity"] for c in cart if c["product_id"] == prod_id), 0)
    await set_cart_qty(call.from_user.id, prod_id, current - 1)
    await render_cart(call.from_user.id, call.message, edit=True)
    await call.answer()


@dp.callback_query(F.data.startswith("cart:rm:"))
async def cb_cart_rm(call: CallbackQuery):
    prod_id = int(call.data.split(":")[2])
    await set_cart_qty(call.from_user.id, prod_id, 0)
    await render_cart(call.from_user.id, call.message, edit=True)
    await call.answer("Удалено")


@dp.callback_query(F.data == "cart:clear")
async def cb_cart_clear(call: CallbackQuery):
    await clear_cart(call.from_user.id)
    await render_cart(call.from_user.id, call.message, edit=True)
    await call.answer("Корзина очищена")


# =====================================================================
# ОФОРМЛЕНИЕ ЗАКАЗА И ОПЛАТА
# =====================================================================

@dp.callback_query(F.data == "cart:checkout")
async def cb_checkout(call: CallbackQuery):
    await upsert_user(call.from_user)
    cart = await get_cart(call.from_user.id)
    if not cart:
        await call.answer("Корзина пуста", show_alert=True)
        return

    for it in cart:
        p = await get_product(it["product_id"])
        if not p or p["stock"] < it["quantity"]:
            await call.answer(
                f"❗ Товара «{it['name']}» недостаточно на складе.",
                show_alert=True,
            )
            return

    total_rub = sum((it["price_rub"] * it["quantity"] for it in cart), Decimal("0"))
    total_usdt = rub_to_usdt(total_rub)
    order_id = await create_order(call.from_user.id, total_rub, total_usdt)

    # Уведомляем админа о новом заказе
    username = f"@{call.from_user.username}" if call.from_user.username else "—"
    await notify_admin(
        f"🆕 <b>Новый заказ #{order_id}</b>\n"
        f"👤 {call.from_user.full_name} ({username}, id {call.from_user.id})\n"
        f"💰 {fmt_money(total_rub)} ({total_usdt} USDT)\n"
        f"📦 Позиций: {len(cart)}"
    )

    try:
        invoice = await create_invoice(
            amount_usdt=total_usdt,
            description=f"Заказ #{order_id} в {SHOP_NAME}",
            payload=f"order:{order_id}:{call.from_user.id}",
        )
    except CryptoBotError as e:
        log.error("invoice create failed: %s", e)
        await notify_admin(
            f"⚠️ <b>CryptoBot недоступен!</b>\n"
            f"Ошибка: <code>{e}</code>\n"
            f"Заказ <b>#{order_id}</b> на {fmt_money(total_rub)} "
            f"({total_usdt} USDT) ждёт оплаты.\n"
            f"Юзер: {call.from_user.id} ({username})."
        )
        await call.message.edit_text(
            f"❌ <b>Не удалось создать платёж</b>\n\n"
            f"{cryptobot_user_message(e)}\n\n"
            f"Заказ <b>#{order_id}</b> сохранён — можно попробовать оплатить позже "
            f"через «📦 Мои заказы».",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📦 Мои заказы",
                                      callback_data="menu:orders")],
                [InlineKeyboardButton(text="⬅️ В меню",
                                      callback_data="menu:home")],
            ]),
        )
        await call.answer()
        return
    except Exception as e:
        log.exception("invoice create unexpected")
        await notify_admin(
            f"⚠️ Ошибка при создании инвойса для заказа #{order_id}: {e}"
        )
        await call.message.edit_text(
            "❌ Произошёл сбой. Попробуй позже или обратись в поддержку.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=support_kb()),
        )
        await call.answer()
        return

    await attach_invoice(order_id, invoice["invoice_id"], invoice["pay_url"])

    text = (
        f"🧾 <b>Заказ #{order_id}</b>\n\n"
        f"Сумма: <b>{fmt_money(total_rub)}</b>\n"
        f"К оплате: <b>{total_usdt} USDT</b>\n"
        f"(курс: 1 USDT = {RUB_PER_USDT}₽)\n\n"
        "💳 Оплата через <b>CryptoBot</b>.\n"
        "Открой счёт по кнопке ниже, оплати, а потом жми «Проверить оплату»."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Оплатить", url=invoice["pay_url"])],
        [InlineKeyboardButton(text="✅ Проверить оплату",
                              callback_data=f"pay:check:{order_id}")],
        [InlineKeyboardButton(text="❌ Отменить заказ",
                              callback_data=f"pay:cancel:{order_id}")],
    ])
    await call.message.edit_text(text, reply_markup=kb)
    await call.answer()


@dp.callback_query(F.data.startswith("pay:check:"))
async def cb_pay_check(call: CallbackQuery):
    order_id = int(call.data.split(":")[2])
    order = await get_order(order_id)
    if not order or order["user_id"] != call.from_user.id:
        await call.answer("Заказ не найден", show_alert=True)
        return

    if order["status"] == "paid":
        await clear_cart(call.from_user.id)
        await call.message.edit_text(
            f"✅ Заказ <b>#{order_id}</b> уже оплачен.\n"
            "Спасибо за покупку! 🧱",
            reply_markup=main_menu_kb(),
        )
        await call.answer()
        return

    if not order["crypto_invoice_id"]:
        await call.answer("Счёт не найден", show_alert=True)
        return

    try:
        data = await get_invoice(order["crypto_invoice_id"])
    except CryptoBotError as e:
        await call.answer(f"Ошибка проверки: {cryptobot_user_message(e)}",
                          show_alert=True)
        return
    except Exception:
        log.exception("invoice get failed")
        await call.answer("Ошибка проверки, попробуй позже", show_alert=True)
        return

    inv_list = data if isinstance(data, list) else data.get("items", [])
    if not inv_list:
        await call.answer("Счёт не найден", show_alert=True)
        return

    inv = inv_list[0]
    status = inv.get("status")

    if status == "paid":
        changed = await mark_order_paid(order_id)
        await clear_cart(call.from_user.id)
        await call.message.edit_text(
            f"✅ Оплата получена!\nЗаказ <b>#{order_id}</b> оплачен.\n"
            f"Спасибо, что выбрали {SHOP_NAME} 🧱",
            reply_markup=main_menu_kb(),
        )
        if changed:
            await notify_admin(
                f"💰 Оплачен заказ <b>#{order_id}</b> на "
                f"{fmt_money(order['total_rub'])} "
                f"({order['total_usdt']} USDT)."
            )
        await call.answer("Оплата подтверждена ✅")
        return

    if status == "active":
        await call.answer("⏳ Оплата ещё не поступила", show_alert=True)
        return

    await call.message.edit_text(
        f"❌ Счёт по заказу <b>#{order_id}</b> истёк или отменён.\n"
        "Оформи новый заказ.",
        reply_markup=main_menu_kb(),
    )
    await call.answer()


@dp.callback_query(F.data.startswith("pay:cancel:"))
async def cb_pay_cancel(call: CallbackQuery):
    order_id = int(call.data.split(":")[2])
    order = await get_order(order_id)
    if not order or order["user_id"] != call.from_user.id:
        await call.answer("Заказ не найден", show_alert=True)
        return
    if order["status"] == "paid":
        await call.answer("Заказ уже оплачен", show_alert=True)
        return
    await call.message.edit_text(
        f"🚫 Заказ <b>#{order_id}</b> отменён.\n"
        "Товар остался в корзине.",
        reply_markup=main_menu_kb(),
    )
    await call.answer()


# =====================================================================
# ПРОФИЛЬ
# =====================================================================

@dp.callback_query(F.data == "menu:profile")
async def cb_profile(call: CallbackQuery):
    user = await get_user_stats(call.from_user.id)
    if not user:
        await call.answer("Сначала нажми /start", show_alert=True)
        return
    name = user["full_name"] or "—"
    uname = f"@{user['username']}" if user["username"] else "—"
    created = user["created_at"].strftime("%d.%m.%Y %H:%M")
    text = (
        f"👤 <b>Профиль</b>\n\n"
        f"🆔 ID: <code>{user['id']}</code>\n"
        f"📛 Имя: <b>{name}</b>\n"
        f"🔗 Username: {uname}\n"
        f"📅 С нами с: {created}\n\n"
        f"📦 Заказов: <b>{user['orders_count']}</b>\n"
        f"💸 Потрачено: <b>{fmt_money(user['spent_rub'])}</b>"
    )
    await call.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=support_kb()),
    )
    await call.answer()


# =====================================================================
# МОИ ЗАКАЗЫ
# =====================================================================

@dp.callback_query(F.data == "menu:orders")
async def cb_orders(call: CallbackQuery):
    orders = await get_user_orders(call.from_user.id, limit=20)
    if not orders:
        await call.message.edit_text(
            "📭 У тебя пока нет заказов.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🛒 В каталог",
                                      callback_data="menu:catalog")],
                [InlineKeyboardButton(text="⬅️ Назад",
                                      callback_data="menu:home")],
            ]),
        )
        return

    lines = ["📦 <b>Твои заказы</b>\n"]
    kb = InlineKeyboardBuilder()
    for o in orders:
        status = "✅ Оплачен" if o["status"] == "paid" else "⏳ Ожидает"
        when = o["created_at"].strftime("%d.%m %H:%M")
        lines.append(
            f"• <b>#{o['id']}</b> — {fmt_money(o['total_rub'])} "
            f"({status}) — {when}"
        )
        kb.button(text=f"📄 Заказ #{o['id']}",
                  callback_data=f"order:view:{o['id']}")
    kb.button(text="⬅️ Назад", callback_data="menu:home")
    kb.adjust(1)
    await call.message.edit_text("\n".join(lines), reply_markup=kb.as_markup())
    await call.answer()


@dp.callback_query(F.data.startswith("order:view:"))
async def cb_order_view(call: CallbackQuery):
    order_id = int(call.data.split(":")[2])
    order = await get_order(order_id)
    if not order or order["user_id"] != call.from_user.id:
        await call.answer("Заказ не найден", show_alert=True)
        return

    items = await get_order_items(order_id)
    lines = [f"📄 <b>Заказ #{order_id}</b>\n"]
    for it in items:
        lines.append(
            f"• {it['product_name']} × {it['quantity']} = "
            f"{fmt_money(it['price_rub'] * it['quantity'])}"
        )
    status_text = "✅ Оплачен" if order["status"] == "paid" else "⏳ Ожидает оплаты"
    lines.append(f"\n💳 Сумма: <b>{fmt_money(order['total_rub'])}</b>")
    lines.append(f"📊 Статус: <b>{status_text}</b>")
    if order["paid_at"]:
        lines.append(f"🕒 Оплачен: {order['paid_at'].strftime('%d.%m.%Y %H:%M')}")

    kb_rows = []
    if order["status"] != "paid":
        if order["crypto_pay_url"]:
            kb_rows.append([InlineKeyboardButton(
                text="💳 Оплатить", url=order["crypto_pay_url"],
            )])
            kb_rows.append([InlineKeyboardButton(
                text="✅ Проверить оплату",
                callback_data=f"pay:check:{order_id}",
            )])
    kb_rows.append([InlineKeyboardButton(
        text="⬅️ К заказам", callback_data="menu:orders",
    )])
    await call.message.edit_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows),
    )
    await call.answer()


# =====================================================================
# АДМИНКА — ДОБАВЛЕНИЕ КАТЕГОРИИ
# =====================================================================

@dp.callback_query(F.data == "adm:add_cat")
async def adm_add_cat(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    await state.set_state(AdminStates.add_cat_name)
    await call.message.edit_text(
        "Введи <b>название</b> новой категории:",
        reply_markup=cancel_kb("adm:home"),
    )
    await call.answer()


@dp.message(AdminStates.add_cat_name)
async def adm_cat_name(message: Message, state: FSMContext):
    name = (message.text or "").strip()
    if not name or len(name) > 80:
        await message.answer("Название некорректное (1-80 символов). Попробуй ещё раз.")
        return
    await state.update_data(name=name)
    await state.set_state(AdminStates.add_cat_description)
    await message.answer(
        f"Категория: <b>{name}</b>\n"
        "Введи <b>описание</b> (или «-» чтобы пропустить):",
        reply_markup=cancel_kb("adm:home"),
    )


@dp.message(AdminStates.add_cat_description)
async def adm_cat_desc(message: Message, state: FSMContext):
    desc = (message.text or "").strip()
    if desc == "-":
        desc = ""
    data = await state.get_data()
    ok = await add_category(data["name"], desc)
    await state.clear()
    if ok:
        await message.answer(
            f"✅ Категория <b>{data['name']}</b> добавлена.",
            reply_markup=admin_main_kb(),
        )
    else:
        await message.answer(
            "⚠️ Такая категория уже существует.",
            reply_markup=admin_main_kb(),
        )


# =====================================================================
# АДМИНКА — ДОБАВЛЕНИЕ ТОВАРА
# =====================================================================

@dp.callback_query(F.data == "adm:add_prod")
async def adm_add_prod(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    cats = await get_categories()
    if not cats:
        await call.message.edit_text(
            "Сначала создай хотя бы одну категорию.",
            reply_markup=admin_main_kb(),
        )
        return
    kb = InlineKeyboardBuilder()
    for c in cats:
        kb.button(text=f"📁 {c['name']}", callback_data=f"apick:{c['id']}")
    kb.button(text="⬅️ Назад", callback_data="adm:home")
    kb.adjust(1)
    await state.set_state(AdminStates.add_prod_category)
    await call.message.edit_text(
        "Выбери <b>категорию</b> для нового товара:",
        reply_markup=kb.as_markup(),
    )
    await call.answer()


@dp.callback_query(F.data.startswith("apick:"))
async def adm_pick_cat(call: CallbackQuery, state: FSMContext):
    cat_id = int(call.data.split(":")[1])
    cat = await get_category(cat_id)
    if not cat:
        await call.answer("Категория не найдена", show_alert=True)
        return
    await state.update_data(category_id=cat_id, category_name=cat["name"])
    await state.set_state(AdminStates.add_prod_name)
    await call.message.edit_text(
        f"Категория: <b>{cat['name']}</b>\n\nВведи <b>название</b> товара:",
        reply_markup=cancel_kb("adm:home"),
    )
    await call.answer()


@dp.message(AdminStates.add_prod_name)
async def adm_prod_name(message: Message, state: FSMContext):
    name = (message.text or "").strip()
    if not name or len(name) > 120:
        await message.answer("Название некорректное (1-120 символов).")
        return
    await state.update_data(name=name)
    await state.set_state(AdminStates.add_prod_description)
    await message.answer(
        f"Название: <b>{name}</b>\n"
        "Введи <b>описание</b> (или «-» чтобы пропустить):",
        reply_markup=cancel_kb("adm:home"),
    )


@dp.message(AdminStates.add_prod_description)
async def adm_prod_desc(message: Message, state: FSMContext):
    desc = (message.text or "").strip()
    if desc == "-":
        desc = ""
    await state.update_data(description=desc)
    await state.set_state(AdminStates.add_prod_price)
    await message.answer(
        "Введи <b>цену в рублях</b> (например <code>499.90</code>):",
        reply_markup=cancel_kb("adm:home"),
    )


@dp.message(AdminStates.add_prod_price)
async def adm_prod_price(message: Message, state: FSMContext):
    raw = (message.text or "").strip().replace(",", ".")
    try:
        price = Decimal(raw)
        if price < 0:
            raise ValueError
    except (InvalidOperation, ValueError):
        await message.answer("Не похоже на цену. Введи число, например <code>499.90</code>.")
        return
    await state.update_data(price_rub=price)
    await state.set_state(AdminStates.add_prod_stock)
    await message.answer(
        f"Цена: <b>{fmt_money(price)}</b>\nВведи <b>количество на складе</b> (целое число):",
        reply_markup=cancel_kb("adm:home"),
    )


@dp.message(AdminStates.add_prod_stock)
async def adm_prod_stock(message: Message, state: FSMContext):
    raw = (message.text or "").strip()
    if not raw.isdigit():
        await message.answer("Нужно целое неотрицательное число.")
        return
    stock = int(raw)
    await state.update_data(stock=stock)
    await state.set_state(AdminStates.add_prod_photo)
    await message.answer(
        f"Наличие: <b>{stock}</b> шт.\n"
        "Отправь <b>фото</b> товара (или «-» чтобы пропустить):",
        reply_markup=cancel_kb("adm:home"),
    )


@dp.message(AdminStates.add_prod_photo, F.photo)
async def adm_prod_photo_file(message: Message, state: FSMContext):
    photo_id = message.photo[-1].file_id
    await state.update_data(photo_id=photo_id)
    await state.set_state(AdminStates.add_prod_original)
    await message.answer(
        "Фото сохранено ✅\nТовар <b>оригинальный</b>?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Оригинал", callback_data="orig:yes")],
            [InlineKeyboardButton(text="♻️ Аналог",   callback_data="orig:no")],
            [InlineKeyboardButton(text="⬅️ Назад",    callback_data="adm:home")],
        ]),
    )


@dp.message(AdminStates.add_prod_photo)
async def adm_prod_photo_text(message: Message, state: FSMContext):
    txt = (message.text or "").strip()
    if txt == "-":
        await state.update_data(photo_id=None)
        await state.set_state(AdminStates.add_prod_original)
        await message.answer(
            "Окей, без фото.\nТовар <b>оригинальный</b>?",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Оригинал", callback_data="orig:yes")],
                [InlineKeyboardButton(text="♻️ Аналог",   callback_data="orig:no")],
                [InlineKeyboardButton(text="⬅️ Назад",    callback_data="adm:home")],
            ]),
        )
        return
    await message.answer("Пришли фото или «-» чтобы пропустить.")


@dp.callback_query(F.data.startswith("orig:"))
async def adm_prod_original(call: CallbackQuery, state: FSMContext):
    is_orig = call.data.split(":")[1] == "yes"
    data = await state.get_data()
    prod_id = await add_product(
        category_id=data["category_id"],
        name=data["name"],
        description=data.get("description", ""),
        price_rub=data["price_rub"],
        stock=data["stock"],
        photo_id=data.get("photo_id"),
        is_original=is_orig,
    )
    await state.clear()
    kind = "✅ оригинал" if is_orig else "♻️ аналог"
    await call.message.edit_text(
        f"🎉 Товар добавлен!\n\n"
        f"<b>#{prod_id}</b> {data['name']}\n"
        f"Категория: {data.get('category_name','')}\n"
        f"{kind}, {fmt_money(data['price_rub'])}, {data['stock']} шт.",
        reply_markup=admin_main_kb(),
    )
    await call.answer()


# =====================================================================
# АДМИНКА — ГЛАВНОЕ МЕНЮ И ОБЩИЕ ОБРАБОТЧИКИ
# =====================================================================

@dp.callback_query(F.data == "adm:home")
async def cb_adm_home(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    await state.clear()
    await call.message.edit_text(
        f"⚙️ <b>Админ-панель {SHOP_NAME}</b>",
        reply_markup=admin_main_kb(),
    )
    await call.answer()


# =====================================================================
# АДМИНКА — ИЗМЕНЕНИЕ НАЛИЧИЯ
# =====================================================================

@dp.callback_query(F.data == "adm:edit_stock")
async def adm_edit_stock(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    cats = await get_categories()
    if not cats:
        await call.message.edit_text("Нет категорий.",
                                     reply_markup=admin_main_kb())
        return
    kb = InlineKeyboardBuilder()
    for c in cats:
        kb.button(text=f"📁 {c['name']}", callback_data=f"escat:{c['id']}")
    kb.button(text="⬅️ Назад", callback_data="adm:home")
    kb.adjust(1)
    await state.set_state(AdminStates.edit_stock_pick_cat)
    await call.message.edit_text(
        "Выбери категорию для изменения наличия:",
        reply_markup=kb.as_markup(),
    )
    await call.answer()


@dp.callback_query(F.data.startswith("escat:"))
async def adm_es_cat(call: CallbackQuery, state: FSMContext):
    cat_id = int(call.data.split(":")[1])
    products = await get_products_by_category(cat_id)
    if not products:
        await call.answer("В категории нет товаров", show_alert=True)
        return
    kb = InlineKeyboardBuilder()
    for p in products:
        kb.button(
            text=f"{p['name']} — сейчас {p['stock']} шт.",
            callback_data=f"esprod:{p['id']}",
        )
    kb.button(text="⬅️ Назад", callback_data="adm:edit_stock")
    kb.adjust(1)
    await state.set_state(AdminStates.edit_stock_pick_prod)
    await call.message.edit_text("Выбери товар:",
                                 reply_markup=kb.as_markup())
    await call.answer()


@dp.callback_query(F.data.startswith("esprod:"))
async def adm_es_pick(call: CallbackQuery, state: FSMContext):
    prod_id = int(call.data.split(":")[1])
    p = await get_product(prod_id)
    if not p:
        await call.answer("Товар не найден", show_alert=True)
        return
    await state.update_data(prod_id=prod_id)
    await state.set_state(AdminStates.edit_stock_value)
    await call.message.edit_text(
        f"Товар: <b>{p['name']}</b>\n"
        f"Сейчас на складе: <b>{p['stock']}</b> шт.\n\n"
        "Введи новое количество:",
        reply_markup=cancel_kb("adm:edit_stock"),
    )
    await call.answer()


@dp.message(AdminStates.edit_stock_value)
async def adm_es_value(message: Message, state: FSMContext):
    raw = (message.text or "").strip()
    if not raw.isdigit():
        await message.answer("Нужно целое неотрицательное число.")
        return
    new_stock = int(raw)
    data = await state.get_data()
    await update_stock(data["prod_id"], new_stock)
    await state.clear()
    await message.answer(
        f"✅ Наличие обновлено: <b>{new_stock}</b> шт.",
        reply_markup=admin_main_kb(),
    )


# =====================================================================
# АДМИНКА — РЕДАКТИРОВАНИЕ ТОВАРА
# =====================================================================

@dp.callback_query(F.data == "adm:edit_prod")
async def adm_edit_prod(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    cats = await get_categories()
    if not cats:
        await call.message.edit_text("Нет категорий.",
                                     reply_markup=admin_main_kb())
        return
    kb = InlineKeyboardBuilder()
    for c in cats:
        kb.button(text=f"📁 {c['name']}", callback_data=f"epcat:{c['id']}")
    kb.button(text="⬅️ Назад", callback_data="adm:home")
    kb.adjust(1)
    await state.set_state(AdminStates.edit_prod_pick_cat)
    await call.message.edit_text(
        "Выбери категорию для редактирования товара:",
        reply_markup=kb.as_markup(),
    )
    await call.answer()


@dp.callback_query(F.data.startswith("epcat:"))
async def adm_ep_cat(call: CallbackQuery, state: FSMContext):
    cat_id = int(call.data.split(":")[1])
    products = await get_products_by_category(cat_id)
    if not products:
        await call.answer("В категории нет товаров", show_alert=True)
        return
    kb = InlineKeyboardBuilder()
    for p in products:
        kb.button(text=f"✏️ {p['name']}",
                  callback_data=f"epprod:{p['id']}")
    kb.button(text="⬅️ Назад", callback_data="adm:edit_prod")
    kb.adjust(1)
    await state.set_state(AdminStates.edit_prod_pick_prod)
    await call.message.edit_text("Выбери товар:",
                                 reply_markup=kb.as_markup())
    await call.answer()


@dp.callback_query(F.data.startswith("epprod:"))
async def adm_ep_pick(call: CallbackQuery, state: FSMContext):
    prod_id = int(call.data.split(":")[1])
    p = await get_product(prod_id)
    if not p:
        await call.answer("Товар не найден", show_alert=True)
        return
    await state.update_data(prod_id=prod_id)
    await state.set_state(AdminStates.edit_prod_field)
    kind = "✅ оригинал" if p["is_original"] else "♻️ аналог"
    text = (
        f"✏️ <b>{p['name']}</b>\n"
        f"ID: #{prod_id}\n"
        f"Описание: <i>{p['description'] or '—'}</i>\n"
        f"Цена: {fmt_money(p['price_rub'])}\n"
        f"Тип: {kind}\n\n"
        "Что хочешь изменить?"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Название",
                              callback_data=f"epf:{prod_id}:name")],
        [InlineKeyboardButton(text="Описание",
                              callback_data=f"epf:{prod_id}:desc")],
        [InlineKeyboardButton(text="Цена",
                              callback_data=f"epf:{prod_id}:price")],
        [InlineKeyboardButton(text="Оригинал/Аналог",
                              callback_data=f"epf:{prod_id}:original")],
        [InlineKeyboardButton(text="⬅️ Назад",
                              callback_data="adm:edit_prod")],
    ])
    await call.message.edit_text(text, reply_markup=kb)
    await call.answer()


@dp.callback_query(F.data.startswith("epf:"))
async def adm_ep_field(call: CallbackQuery, state: FSMContext):
    parts = call.data.split(":")
    prod_id = int(parts[1])
    field = parts[2]
    await state.update_data(prod_id=prod_id, field=field)

    prompts = {
        "name":     "Введи новое <b>название</b> товара:",
        "desc":     "Введи новое <b>описание</b> (или «-» чтобы очистить):",
        "price":    "Введи новую <b>цену в рублях</b>:",
        "original": "Товар теперь:",
    }
    if field == "original":
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Оригинал",
                                  callback_data=f"eporig:{prod_id}:yes")],
            [InlineKeyboardButton(text="♻️ Аналог",
                                  callback_data=f"eporig:{prod_id}:no")],
            [InlineKeyboardButton(text="⬅️ Назад",
                                  callback_data=f"epprod:{prod_id}")],
        ])
        await call.message.edit_text(prompts[field], reply_markup=kb)
        await state.set_state(AdminStates.edit_prod_original)
    else:
        state_map = {
            "name":  AdminStates.edit_prod_name,
            "desc":  AdminStates.edit_prod_desc,
            "price": AdminStates.edit_prod_price,
        }
        await state.set_state(state_map[field])
        await call.message.edit_text(
            prompts[field],
            reply_markup=cancel_kb(f"epprod:{prod_id}"),
        )
    await call.answer()


@dp.message(AdminStates.edit_prod_name)
async def adm_ep_name(message: Message, state: FSMContext):
    name = (message.text or "").strip()
    if not name or len(name) > 120:
        await message.answer("Название некорректное (1-120).")
        return
    data = await state.get_data()
    await update_product(data["prod_id"], name=name)
    await state.clear()
    await message.answer(f"✅ Название обновлено: <b>{name}</b>",
                         reply_markup=admin_main_kb())


@dp.message(AdminStates.edit_prod_desc)
async def adm_ep_desc(message: Message, state: FSMContext):
    desc = (message.text or "").strip()
    if desc == "-":
        desc = ""
    data = await state.get_data()
    await update_product(data["prod_id"], description=desc)
    await state.clear()
    await message.answer("✅ Описание обновлено.",
                         reply_markup=admin_main_kb())


@dp.message(AdminStates.edit_prod_price)
async def adm_ep_price(message: Message, state: FSMContext):
    raw = (message.text or "").strip().replace(",", ".")
    try:
        price = Decimal(raw)
        if price < 0:
            raise ValueError
    except (InvalidOperation, ValueError):
        await message.answer("Не похоже на цену. Введи число, например <code>499.90</code>.")
        return
    data = await state.get_data()
    await update_product(data["prod_id"], price_rub=price)
    await state.clear()
    await message.answer(f"✅ Цена обновлена: {fmt_money(price)}",
                         reply_markup=admin_main_kb())


@dp.callback_query(F.data.startswith("eporig:"))
async def adm_ep_original(call: CallbackQuery, state: FSMContext):
    parts = call.data.split(":")
    prod_id = int(parts[1])
    is_orig = parts[2] == "yes"
    await update_product(prod_id, is_original=is_orig)
    await state.clear()
    await call.message.edit_text(
        f"✅ Тип товара обновлён: <b>{'оригинал' if is_orig else 'аналог'}</b>.",
        reply_markup=admin_main_kb(),
    )
    await call.answer()


# =====================================================================
# АДМИНКА — УДАЛЕНИЕ ТОВАРА
# =====================================================================

@dp.callback_query(F.data == "adm:delete_prod")
async def adm_del(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    cats = await get_categories()
    if not cats:
        await call.message.edit_text("Нет категорий.",
                                     reply_markup=admin_main_kb())
        return
    kb = InlineKeyboardBuilder()
    for c in cats:
        kb.button(text=f"📁 {c['name']}", callback_data=f"dcat:{c['id']}")
    kb.button(text="⬅️ Назад", callback_data="adm:home")
    kb.adjust(1)
    await state.set_state(AdminStates.delete_pick_cat)
    await call.message.edit_text(
        "Выбери категорию, из которой удалить товар:",
        reply_markup=kb.as_markup(),
    )
    await call.answer()


@dp.callback_query(F.data.startswith("dcat:"))
async def adm_del_cat(call: CallbackQuery, state: FSMContext):
    cat_id = int(call.data.split(":")[1])
    products = await get_products_by_category(cat_id)
    if not products:
        await call.answer("В категории нет товаров", show_alert=True)
        return
    kb = InlineKeyboardBuilder()
    for p in products:
        kb.button(text=f"🗑 {p['name']}", callback_data=f"dprod:{p['id']}")
    kb.button(text="⬅️ Назад", callback_data="adm:delete_prod")
    kb.adjust(1)
    await state.set_state(AdminStates.delete_pick_prod)
    await call.message.edit_text("Выбери товар для удаления:",
                                 reply_markup=kb.as_markup())
    await call.answer()


@dp.callback_query(F.data.startswith("dprod:"))
async def adm_del_prod(call: CallbackQuery, state: FSMContext):
    prod_id = int(call.data.split(":")[1])
    p = await get_product(prod_id)
    if not p:
        await call.answer("Товар не найден", show_alert=True)
        return
    await state.update_data(prod_id=prod_id)
    await state.set_state(AdminStates.delete_confirm)
    await call.message.edit_text(
        f"⚠️ Точно удалить <b>{p['name']}</b>?\n"
        f"Цена: {fmt_money(p['price_rub'])}, наличие: {p['stock']} шт.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, удалить",
                                  callback_data=f"ddel:{prod_id}")],
            [InlineKeyboardButton(text="❌ Отмена",
                                  callback_data="adm:delete_prod")],
        ]),
    )
    await call.answer()


@dp.callback_query(F.data.startswith("ddel:"))
async def adm_del_confirm(call: CallbackQuery, state: FSMContext):
    prod_id = int(call.data.split(":")[1])
    p = await get_product(prod_id)
    if not p:
        await call.answer("Уже удалён", show_alert=True)
        await state.clear()
        return
    await delete_product(prod_id)
    await state.clear()
    await call.message.edit_text(
        f"🗑 Товар <b>{p['name']}</b> удалён.",
        reply_markup=admin_main_kb(),
    )
    await call.answer()


# =====================================================================
# АДМИНКА — УДАЛЕНИЕ КАТЕГОРИИ
# =====================================================================

@dp.callback_query(F.data == "adm:delete_cat")
async def adm_delete_cat(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    cats = await get_categories()
    if not cats:
        await call.message.edit_text("Нет категорий.",
                                     reply_markup=admin_main_kb())
        return
    kb = InlineKeyboardBuilder()
    for c in cats:
        kb.button(text=f"📁 {c['name']}", callback_data=f"dccat:{c['id']}")
    kb.button(text="⬅️ Назад", callback_data="adm:home")
    kb.adjust(1)
    await state.set_state(AdminStates.delete_cat_pick)
    await call.message.edit_text(
        "⚠️ Какую категорию удалить?\n"
        "<i>Все товары в ней тоже будут удалены.</i>",
        reply_markup=kb.as_markup(),
    )
    await call.answer()


@dp.callback_query(F.data.startswith("dccat:"))
async def adm_delete_cat_pick(call: CallbackQuery, state: FSMContext):
    cat_id = int(call.data.split(":")[1])
    cat = await get_category(cat_id)
    if not cat:
        await call.answer("Категория не найдена", show_alert=True)
        return
    await state.update_data(cat_id=cat_id, cat_name=cat["name"])
    await state.set_state(AdminStates.delete_cat_confirm)
    await call.message.edit_text(
        f"Точно удалить категорию <b>{cat['name']}</b>?\n"
        "Будут удалены все её товары.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, удалить всё",
                                  callback_data=f"dcdel:{cat_id}")],
            [InlineKeyboardButton(text="❌ Отмена",
                                  callback_data="adm:home")],
        ]),
    )
    await call.answer()


@dp.callback_query(F.data.startswith("dcdel:"))
async def adm_delete_cat_confirm(call: CallbackQuery, state: FSMContext):
    cat_id = int(call.data.split(":")[1])
    data = await state.get_data()
    name = data.get("cat_name", "")
    ok = await delete_category(cat_id)
    await state.clear()
    if ok:
        await call.message.edit_text(
            f"🗑 Категория <b>{name}</b> и её товары удалены.",
            reply_markup=admin_main_kb(),
        )
    else:
        await call.message.edit_text(
            "⚠️ Не удалось удалить.",
            reply_markup=admin_main_kb(),
        )
    await call.answer()


# =====================================================================
# АДМИНКА — ВСЕ ЗАКАЗЫ + РУЧНОЕ ПОДТВЕРЖДЕНИЕ
# =====================================================================

@dp.callback_query(F.data == "adm:orders")
async def adm_orders(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏳ Pending", callback_data="adm:orders:pending")],
        [InlineKeyboardButton(text="✅ Paid",    callback_data="adm:orders:paid")],
        [InlineKeyboardButton(text="📋 Все",     callback_data="adm:orders:all")],
        [InlineKeyboardButton(text="⬅️ Назад",   callback_data="adm:home")],
    ])
    await call.message.edit_text("📦 <b>Заказы</b> — фильтр:",
                                 reply_markup=kb)
    await call.answer()


@dp.callback_query(F.data.startswith("adm:orders:"))
async def adm_orders_filter(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    flt = call.data.split(":")[2]
    status = None if flt == "all" else flt
    orders = await get_all_orders(status=status, limit=30)
    title = {"pending": "⏳ Ожидают оплаты",
             "paid": "✅ Оплаченные",
             "all": "📋 Все заказы"}[flt]
    if not orders:
        await call.message.edit_text(f"{title}\n\nПусто.",
                                     reply_markup=admin_main_kb())
        await call.answer()
        return

    lines = [f"<b>{title}</b>\n"]
    kb = InlineKeyboardBuilder()
    for o in orders:
        u = f"@{o['username']}" if o["username"] else (o["full_name"] or str(o["user_id"]))
        when = o["created_at"].strftime("%d.%m %H:%M")
        badge = "✅" if o["status"] == "paid" else "⏳"
        lines.append(
            f"{badge} <b>#{o['id']}</b> {fmt_money(o['total_rub'])} — "
            f"{u} — {when}"
        )
        kb.button(text=f"📄 #{o['id']} {fmt_money(o['total_rub'])}",
                  callback_data=f"adm:order:{o['id']}")
    kb.button(text="⬅️ Назад", callback_data="adm:orders")
    kb.adjust(1)
    await call.message.edit_text("\n".join(lines), reply_markup=kb.as_markup())
    await call.answer()


@dp.callback_query(F.data.startswith("adm:order:"))
async def adm_order_view(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    order_id = int(call.data.split(":")[2])
    order = await get_order(order_id)
    if not order:
        await call.answer("Заказ не найден", show_alert=True)
        return
    items = await get_order_items(order_id)
    user = await get_user(order["user_id"])

    u_name = user["full_name"] if user else "—"
    u_username = f"@{user['username']}" if user and user["username"] else "—"

    lines = [
        f"📄 <b>Заказ #{order_id}</b>",
        f"👤 {u_name} ({u_username}, id {order['user_id']})",
        f"💰 {fmt_money(order['total_rub'])} ({order['total_usdt']} USDT)",
        f"📊 Статус: <b>{order['status']}</b>",
        f"🕒 Создан: {order['created_at'].strftime('%d.%m.%Y %H:%M')}",
    ]
    if order["paid_at"]:
        lines.append(f"💸 Оплачен: {order['paid_at'].strftime('%d.%m.%Y %H:%M')}")
    if order["crypto_invoice_id"]:
        lines.append(f"🧾 Invoice ID: <code>{order['crypto_invoice_id']}</code>")
    lines.append("\n<b>Позиции:</b>")
    for it in items:
        lines.append(
            f"• {it['product_name']} × {it['quantity']} = "
            f"{fmt_money(it['price_rub'] * it['quantity'])}"
        )

    rows = []
    if order["status"] == "paid":
        rows.append([InlineKeyboardButton(
            text="↩️ Снять оплату",
            callback_data=f"mp:unpay:{order_id}",
        )])
    else:
        rows.append([InlineKeyboardButton(
            text="✅ Подтвердить оплату вручную",
            callback_data=f"mp:pay:{order_id}",
        )])
    if order["crypto_pay_url"]:
        rows.append([InlineKeyboardButton(
            text="💳 Открыть счёт CryptoBot",
            url=order["crypto_pay_url"],
        )])
    rows.append([InlineKeyboardButton(text="⬅️ К заказам",
                                      callback_data="adm:orders")])
    await call.message.edit_text("\n".join(lines),
                                 reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await call.answer()


@dp.callback_query(F.data.startswith("mp:pay:"))
async def adm_manual_pay(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    order_id = int(call.data.split(":")[2])
    changed = await mark_order_paid(order_id)
    if changed:
        order = await get_order(order_id)
        await clear_cart(order["user_id"])
        await call.answer("✅ Оплата подтверждена", show_alert=True)
        await call.message.edit_text(
            f"✅ Заказ <b>#{order_id}</b> помечен как оплаченный.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ К заказу",
                                      callback_data=f"adm:order:{order_id}")],
                [InlineKeyboardButton(text="⬅️ В админку",
                                      callback_data="adm:home")],
            ]),
        )
        await notify_admin(
            f"⚙️ Заказ <b>#{order_id}</b> подтверждён вручную админом."
        )
    else:
        await call.answer("Заказ уже оплачен", show_alert=True)


@dp.callback_query(F.data.startswith("mp:unpay:"))
async def adm_manual_unpay(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    order_id = int(call.data.split(":")[2])
    ok = await mark_order_unpaid(order_id)
    if ok:
        await call.answer("Оплата снята", show_alert=True)
        await call.message.edit_text(
            f"↩️ Заказ <b>#{order_id}</b> снова в статусе pending.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ К заказу",
                                      callback_data=f"adm:order:{order_id}")],
                [InlineKeyboardButton(text="⬅️ В админку",
                                      callback_data="adm:home")],
            ]),
        )
    else:
        await call.answer("Заказ не был оплачен", show_alert=True)


# =====================================================================
# АДМИНКА — СТАТИСТИКА
# =====================================================================

@dp.callback_query(F.data == "adm:stats")
async def adm_stats(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    s = await get_admin_stats()
    text = (
        "📊 <b>Статистика магазина</b>\n\n"
        f"📁 Категорий: <b>{s['categories']}</b>\n"
        f"🧱 Товаров: <b>{s['products']}</b>\n"
        f"👥 Покупателей: <b>{s['customers']}</b>\n\n"
        f"📦 Заказов всего: <b>{s['orders_total']}</b>\n"
        f"   ✅ Оплачено: <b>{s['orders_paid']}</b>\n"
        f"   ⏳ Pending: <b>{s['orders_pending']}</b>\n\n"
        f"💰 Выручка всего: <b>{fmt_money(s['revenue_total'])}</b>\n"
        f"📅 За 24 часа: <b>{fmt_money(s['revenue_day'])}</b> "
        f"(<b>{s['orders_day']}</b> заказов)\n"
        f"📅 За 7 дней: <b>{fmt_money(s['revenue_week'])}</b> "
        f"(<b>{s['orders_week']}</b> заказов)"
    )
    await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Обновить",
                                  callback_data="adm:stats")],
            [InlineKeyboardButton(text="⬅️ Назад",
                                  callback_data="adm:home")],
        ]
    ))
    await call.answer()


# =====================================================================
# АДМИНКА — ТЕСТ CRYPTOBOT
# =====================================================================

@dp.callback_query(F.data == "adm:test_crypto")
async def adm_test_crypto(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    await call.answer("Проверяю…")
    err = await cryptobot_self_check()
    if err is None:
        text = "✅ <b>CryptoBot работает</b>\n\nТокен валидный, API отвечает."
    else:
        text = (
            "❌ <b>CryptoBot недоступен</b>\n\n"
            f"Ошибка: <code>{err}</code>\n\n"
            "Проверь переменную <code>CRYPTO_BOT_TOKEN</code> в окружении.\n"
            "Токен берётся у @CryptoBot → Crypto Pay → My Apps."
        )
    await call.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Повторить",
                                  callback_data="adm:test_crypto")],
            [InlineKeyboardButton(text="⬅️ Назад",
                                  callback_data="adm:home")],
        ]),
    )


# =====================================================================
# АДМИНКА — СПИСКИ
# =====================================================================

@dp.callback_query(F.data == "adm:lists")
async def adm_lists(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📁 Категории",
                              callback_data="adm:list_categories")],
        [InlineKeyboardButton(text="🧱 Товары",
                              callback_data="adm:list_products")],
        [InlineKeyboardButton(text="⬅️ Назад",
                              callback_data="adm:home")],
    ])
    await call.message.edit_text("Что показать?", reply_markup=kb)
    await call.answer()


@dp.callback_query(F.data == "adm:list_categories")
async def adm_list_cats(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    cats = await get_categories()
    if not cats:
        await call.message.edit_text("Пока пусто.",
                                     reply_markup=admin_main_kb())
        return
    lines = ["📁 <b>Категории</b>\n"]
    for c in cats:
        lines.append(
            f"• <b>#{c['id']}</b> {c['name']}"
            + (f" — <i>{c['description']}</i>" if c['description'] else "")
        )
    await call.message.edit_text("\n".join(lines),
                                 reply_markup=admin_main_kb())
    await call.answer()


@dp.callback_query(F.data == "adm:list_products")
async def adm_list_prods(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    prods = await get_all_products()
    if not prods:
        await call.message.edit_text("Пока пусто.",
                                     reply_markup=admin_main_kb())
        return
    cats = {c["id"]: c["name"] for c in await get_categories()}
    lines = ["🧱 <b>Все товары</b>\n"]
    for p in prods:
        kind = "✅" if p["is_original"] else "♻️"
        lines.append(
            f"• {kind} <b>#{p['id']}</b> {p['name']} — "
            f"{fmt_money(p['price_rub'])} ({p['stock']} шт.) "
            f"— {cats.get(p['category_id'], '?')}"
        )
    await call.message.edit_text("\n".join(lines),
                                 reply_markup=admin_main_kb())
    await call.answer()


# =====================================================================
# ЗАПУСК
# =====================================================================

async def post_startup_checks():
    """Проверяет работоспособность CryptoPay и уведомляет админа."""
    err = await cryptobot_self_check()
    if err is None:
        log.info("CryptoBot OK")
        with suppress(Exception):
            await bot.send_message(
                ADMIN_ID,
                f"✅ Бот {SHOP_NAME} запущен.\nCryptoBot: OK",
            )
    else:
        log.error("CryptoBot недоступен: %s", err)
        with suppress(Exception):
            await bot.send_message(
                ADMIN_ID,
                f"⚠️ Бот {SHOP_NAME} запущен, но <b>CryptoBot недоступен</b>.\n"
                f"Ошибка: <code>{err}</code>\n"
                f"Проверь <code>CRYPTO_BOT_TOKEN</code>.\n"
                f"До получения рабочего токена оплата не работает.",
            )


async def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not set")
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    if not CRYPTO_BOT_TOKEN:
        raise RuntimeError("CRYPTO_BOT_TOKEN is not set")

    await init_db()

    # Запускаем поллинг + проверку CryptoBot
    asyncio.create_task(post_startup_checks())

    log.info("Starting bot…")
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await close_db()
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Stopped")
