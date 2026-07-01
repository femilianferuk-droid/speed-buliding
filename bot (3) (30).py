"""
Speed Building Bot — Telegram-бот для продажи Lego деталей поштучно.

Стек:
  • aiogram 3.x (async Telegram framework)
  • asyncpg   (драйвер PostgreSQL)
  • aiohttp   (HTTP клиент для CryptoBot API)

Конфигурация через переменные окружения:
  BOT_TOKEN          — токен Telegram-бота
  DATABASE_URL       — DSN PostgreSQL, например postgres://user:pass@host:5432/dbname
  CRYPTO_BOT_TOKEN   — токен CryptoPay (https://t.me/CryptoBot)
  ADMIN_ID           — Telegram ID администратора (по умолчанию 7973988177)

Запуск:
  python bot.py
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import suppress
from decimal import Decimal, ROUND_UP
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
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

# =====================================================================
# КОНФИГУРАЦИЯ
# =====================================================================

BOT_TOKEN: str = os.environ["BOT_TOKEN"]
DATABASE_URL: str = os.environ["DATABASE_URL"]
CRYPTO_BOT_TOKEN: str = os.environ["CRYPTO_BOT_TOKEN"]
ADMIN_ID: int = int(os.getenv("ADMIN_ID", "7973988177"))
RUB_PER_USDT: Decimal = Decimal(os.getenv("RUB_PER_USDT", "90"))

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
"""


async def init_db() -> asyncpg.Pool:
    """Создаёт пул соединений и применяет схему."""
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
            """
            INSERT INTO users (id, username, full_name)
            VALUES ($1, $2, $3)
            ON CONFLICT (id) DO UPDATE
                SET username  = EXCLUDED.username,
                    full_name = EXCLUDED.full_name
            """,
            user.id, user.username, user.full_name,
        )


async def get_user(user_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM users WHERE id = $1", user_id)


async def get_user_stats(user_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            """
            SELECT u.*,
                   (SELECT COUNT(*) FROM orders WHERE user_id = u.id) AS orders_count,
                   (SELECT COALESCE(SUM(total_rub),0) FROM orders
                       WHERE user_id = u.id AND status='paid') AS spent_rub
            FROM users u
            WHERE u.id = $1
            """,
            user_id,
        )


async def get_categories():
    async with pool.acquire() as conn:
        return await conn.fetch("SELECT * FROM categories ORDER BY id")


async def get_category(cat_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM categories WHERE id = $1", cat_id)


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


async def get_products_by_category(cat_id: int):
    async with pool.acquire() as conn:
        return await conn.fetch(
            "SELECT * FROM products WHERE category_id = $1 ORDER BY id", cat_id,
        )


async def get_product(prod_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM products WHERE id = $1", prod_id)


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


async def delete_product(prod_id: int) -> None:
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM products WHERE id = $1", prod_id)


async def update_stock(prod_id: int, stock: int) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE products SET stock = $2 WHERE id = $1", prod_id, stock,
        )


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
        return await conn.fetchrow("SELECT * FROM orders WHERE id = $1", order_id)


async def get_user_orders(user_id: int):
    async with pool.acquire() as conn:
        return await conn.fetch(
            "SELECT * FROM orders WHERE user_id = $1 ORDER BY id DESC", user_id,
        )


async def get_order_items(order_id: int):
    async with pool.acquire() as conn:
        return await conn.fetch(
            "SELECT * FROM order_items WHERE order_id = $1", order_id,
        )


async def mark_order_paid(order_id: int) -> bool:
    """Помечает заказ оплаченным и списывает товары со склада.
    Возвращает True, если статус изменился (т.е. заказ был pending)."""
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


# =====================================================================
# CRYPTOBOT API
# =====================================================================

async def cryptobot(method: str, params: Optional[dict] = None) -> dict:
    """Базовый вызов CryptoPay API."""
    headers = {"Crypto-Pay-Token": CRYPTO_BOT_TOKEN}
    url = f"{CRYPTO_BOT_API}/{method}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, params=params or {}) as r:
            data = await r.json()
            if not data.get("ok"):
                log.error("CryptoBot %s error: %s", method, data)
                raise RuntimeError(f"CryptoBot API error: {data}")
            return data["result"]


async def create_invoice(amount_usdt: Decimal, description: str,
                         payload: str) -> dict:
    """Создаёт инвойс в USDT. Минимальная сумма — 0.01 USDT."""
    amount = str(amount_usdt.quantize(Decimal("0.01"), rounding=ROUND_UP))
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
    add_cat_name = State()
    add_cat_description = State()

    # Добавление товара
    add_prod_category = State()
    add_prod_name = State()
    add_prod_description = State()
    add_prod_price = State()
    add_prod_stock = State()
    add_prod_photo = State()
    add_prod_original = State()

    # Редактирование наличия
    edit_stock_pick = State()
    edit_stock_value = State()

    # Удаление товара
    delete_pick_category = State()
    delete_pick_product = State()
    delete_confirm = State()


# =====================================================================
# КЛАВИАТУРЫ
# =====================================================================

def main_menu_kb() -> InlineKeyboardMarkup:
    """Главное меню бота."""
    kb = InlineKeyboardBuilder()
    kb.button(text="🛒 Каталог", callback_data="menu:catalog")
    kb.button(text="👤 Профиль", callback_data="menu:profile")
    kb.button(text="📦 Мои заказы", callback_data="menu:orders")
    kb.button(text="🧺 Корзина", callback_data="menu:cart")
    kb.adjust(2, 2)
    return kb.as_markup()


def admin_main_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Добавить категорию", callback_data="adm:add_cat")
    kb.button(text="➕ Добавить товар",      callback_data="adm:add_prod")
    kb.button(text="✏️ Изменить наличие",    callback_data="adm:edit_stock")
    kb.button(text="🗑 Удалить товар",       callback_data="adm:delete_prod")
    kb.button(text="📋 Все товары",          callback_data="adm:list_products")
    kb.button(text="📁 Все категории",       callback_data="adm:list_categories")
    kb.adjust(1)
    return kb.as_markup()


def cancel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")],
    ])


# =====================================================================
# ФОРМАТИРОВАНИЕ
# =====================================================================

def fmt_money(amount: Decimal, suffix: str = "₽") -> str:
    """Красивый вывод денег: 1 234.56₽"""
    s = f"{amount:.2f}"
    int_part, dec_part = s.split(".")
    # добавим пробелы как разделитель тысяч
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


def product_caption(p: asyncpg.Record, full: bool = True) -> str:
    kind = "✅ Оригинал" if p["is_original"] else "♻️ Аналог"
    stock_line = f"📦 В наличии: <b>{p['stock']} шт.</b>"
    lines = [
        f"🧱 <b>{p['name']}</b>",
        f"Категория: {p['category_name'] if 'category_name' in p.keys() else ''}",
        f"{kind}",
        f"💰 Цена: <b>{fmt_money(p['price_rub'])}</b>",
    ]
    if full:
        lines.append(stock_line)
        if p["description"]:
            lines.append("")
            lines.append(p["description"])
    return "\n".join(lines)


# =====================================================================
# ХЭНДЛЕРЫ — ОБЩИЕ
# =====================================================================

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())


def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


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


@dp.callback_query(F.data == "cancel")
async def cb_cancel(call: CallbackQuery, state: FSMContext):
    await state.clear()
    with suppress(Exception):
        await call.message.edit_reply_markup(reply_markup=None)
    await call.message.answer(
        "Действие отменено.", reply_markup=main_menu_kb(),
    )
    await call.answer()


@dp.callback_query(F.data == "menu:home")
async def cb_home(call: CallbackQuery):
    text = (
        f"👋 <b>{SHOP_NAME}</b> — детали Lego поштучно.\n"
        "Оригиналы и аналоги. Выбирай раздел:"
    )
    if call.message.photo:
        await call.message.answer(text, reply_markup=main_menu_kb())
    else:
        await call.message.edit_text(text, reply_markup=main_menu_kb())
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
        kb.button(text=f"📁 {c['name']}", callback_data=f"cat:{c['id']}")
    kb.button(text="⬅️ Назад", callback_data="menu:home")
    kb.adjust(1)
    await call.message.edit_text("📂 <b>Категории</b>", reply_markup=kb.as_markup())
    await call.answer()


@dp.callback_query(F.data.startswith("cat:"))
async def cb_category(call: CallbackQuery):
    cat_id = int(call.data.split(":")[1])
    products = await get_products_by_category(cat_id)
    cat = await get_category(cat_id)

    if not products:
        await call.message.edit_text(
            f"В категории <b>{cat['name']}</b> пока пусто.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ К категориям",
                                      callback_data="menu:catalog")],
            ]),
        )
        return

    kb = InlineKeyboardBuilder()
    for p in products:
        kind = "✅" if p["is_original"] else "♻️"
        kb.button(
            text=f"{kind} {p['name']} — {fmt_money(p['price_rub'])}",
            callback_data=f"prod:{p['id']}",
        )
    kb.button(text="⬅️ К категориям", callback_data="menu:catalog")
    kb.adjust(1)
    await call.message.edit_text(
        f"📂 <b>{cat['name']}</b>\nВыбери товар:",
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
    p["category_name"] = cat["name"]

    caption = product_caption(p, full=True)
    kb = InlineKeyboardBuilder()
    if p["stock"] > 0:
        kb.button(text="🛒 В корзину", callback_data=f"cart:add:{prod_id}")
    else:
        kb.button(text="❌ Нет в наличии", callback_data="noop")
    kb.button(text="⬅️ К категории",
              callback_data=f"cat:{p['category_id']}")
    kb.adjust(1)

    if p["photo_id"]:
        await call.message.answer_photo(
            p["photo_id"], caption=caption, reply_markup=kb.as_markup(),
        )
        # удалим «списочное» сообщение, чтобы не плодить мусор
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

async def render_cart(user_id: int, chat_id: int, message: Optional[Message] = None,
                      edit: bool = False):
    cart = await get_cart(user_id)
    if not cart:
        text = "🧺 Корзина пуста.\nДобавь товары из каталога."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛒 В каталог", callback_data="menu:catalog")],
            [InlineKeyboardButton(text="⬅️ Назад",      callback_data="menu:home")],
        ])
        if edit and message:
            await message.edit_text(text, reply_markup=kb)
        else:
            await message.answer(text, reply_markup=kb) if message else None
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
            text=f"➕",
            callback_data=f"cart:inc:{it['product_id']}",
        )
        kb.button(
            text=f"❌",
            callback_data=f"cart:rm:{it['product_id']}",
        )
    kb.button(text="✅ Оформить заказ", callback_data="cart:checkout")
    kb.button(text="🗑 Очистить",        callback_data="cart:clear")
    kb.button(text="⬅️ Назад",           callback_data="menu:home")
    kb.adjust(3, 1, 1)

    text = "\n".join(lines)
    if edit and message:
        await message.edit_text(text, reply_markup=kb.as_markup())
    else:
        if message:
            await message.answer(text, reply_markup=kb.as_markup())


@dp.callback_query(F.data == "menu:cart")
async def cb_cart(call: CallbackQuery):
    await render_cart(call.from_user.id, call.message.chat.id, call.message, edit=True)
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
    await call.answer("Добавлено в корзину ✅", show_alert=False)


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
    await render_cart(call.from_user.id, call.message.chat.id, call.message, edit=True)
    await call.answer()


@dp.callback_query(F.data.startswith("cart:dec:"))
async def cb_cart_dec(call: CallbackQuery):
    prod_id = int(call.data.split(":")[2])
    cart = await get_cart(call.from_user.id)
    current = next((c["quantity"] for c in cart if c["product_id"] == prod_id), 0)
    await set_cart_qty(call.from_user.id, prod_id, current - 1)
    await render_cart(call.from_user.id, call.message.chat.id, call.message, edit=True)
    await call.answer()


@dp.callback_query(F.data.startswith("cart:rm:"))
async def cb_cart_rm(call: CallbackQuery):
    prod_id = int(call.data.split(":")[2])
    await set_cart_qty(call.from_user.id, prod_id, 0)
    await render_cart(call.from_user.id, call.message.chat.id, call.message, edit=True)
    await call.answer("Удалено")


@dp.callback_query(F.data == "cart:clear")
async def cb_cart_clear(call: CallbackQuery):
    await clear_cart(call.from_user.id)
    await render_cart(call.from_user.id, call.message.chat.id, call.message, edit=True)
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

    # Проверим наличие
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

    try:
        invoice = await create_invoice(
            amount_usdt=total_usdt,
            description=f"Заказ #{order_id} в {SHOP_NAME}",
            payload=f"order:{order_id}:{call.from_user.id}",
        )
    except Exception as e:
        log.exception("invoice create failed")
        await call.message.answer(
            f"❌ Не удалось создать платёж: <code>{e}</code>\n"
            "Попробуй позже или напиши админу.",
            reply_markup=main_menu_kb(),
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
        "Нажми кнопку ниже, чтобы открыть счёт, "
        "а потом «Проверить оплату»."
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
    except Exception:
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
            "Спасибо, что выбрали Speed Building 🧱",
            reply_markup=main_menu_kb(),
        )
        if changed:
            with suppress(Exception):
                await bot.send_message(
                    ADMIN_ID,
                    f"💰 Оплачен заказ <b>#{order_id}</b> "
                    f"на {fmt_money(order['total_rub'])} "
                    f"({order['total_usdt']} USDT).",
                )
        await call.answer("Оплата подтверждена ✅")
        return

    if status == "active":
        await call.answer("⏳ Оплата ещё не поступила", show_alert=True)
        return

    # expired / другие статусы
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
    await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:home")],
        ]
    ))
    await call.answer()


# =====================================================================
# МОИ ЗАКАЗЫ
# =====================================================================

@dp.callback_query(F.data == "menu:orders")
async def cb_orders(call: CallbackQuery):
    orders = await get_user_orders(call.from_user.id)
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
    for o in orders[:20]:
        status = "✅ Оплачен" if o["status"] == "paid" else "⏳ Ожидает оплаты"
        when = o["created_at"].strftime("%d.%m %H:%M")
        lines.append(
            f"• <b>#{o['id']}</b> — {fmt_money(o['total_rub'])} "
            f"({status}) — {when}"
        )
        kb.button(text=f"📄 Заказ #{o['id']}", callback_data=f"order:view:{o['id']}")
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
    await call.message.edit_text("\n".join(lines),
                                 reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))
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
        reply_markup=cancel_kb(),
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
        reply_markup=cancel_kb(),
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
    kb.button(text="❌ Отмена", callback_data="cancel")
    kb.adjust(1)
    await state.set_state(AdminStates.add_prod_category)
    await call.message.edit_text(
        "Выбери <b>категорию</b> для нового товара:",
        reply_markup=kb.as_markup(),
    )
    await call.answer()


@dp.callback_query(F.data.startswith("apick:"), AdminStates.add_prod_category)
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
        reply_markup=cancel_kb(),
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
        reply_markup=cancel_kb(),
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
        reply_markup=cancel_kb(),
    )


@dp.message(AdminStates.add_prod_price)
async def adm_prod_price(message: Message, state: FSMContext):
    raw = (message.text or "").strip().replace(",", ".")
    try:
        price = Decimal(raw)
        if price < 0:
            raise ValueError
    except Exception:
        await message.answer("Не похоже на цену. Введи число, например <code>499.90</code>.")
        return
    await state.update_data(price_rub=price)
    await state.set_state(AdminStates.add_prod_stock)
    await message.answer(
        f"Цена: <b>{fmt_money(price)}</b>\nВведи <b>количество на складе</b> (целое число):",
        reply_markup=cancel_kb(),
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
        reply_markup=cancel_kb(),
    )


@dp.message(AdminStates.add_prod_photo, F.photo)
async def adm_prod_photo_file(message: Message, state: FSMContext):
    photo_id = message.photo[-1].file_id
    await state.update_data(photo_id=photo_id)
    await state.set_state(AdminStates.add_prod_original)
    await message.answer(
        "Фото сохранено ✅\nТовар <b>оригинальный</b>?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Оригинал",  callback_data="orig:yes")],
            [InlineKeyboardButton(text="♻️ Аналог",    callback_data="orig:no")],
            [InlineKeyboardButton(text="❌ Отмена",    callback_data="cancel")],
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
                [InlineKeyboardButton(text="✅ Оригинал",  callback_data="orig:yes")],
                [InlineKeyboardButton(text="♻️ Аналог",    callback_data="orig:no")],
                [InlineKeyboardButton(text="❌ Отмена",    callback_data="cancel")],
            ]),
        )
        return
    await message.answer("Пришли фото или «-» чтобы пропустить.")


@dp.callback_query(F.data.startswith("orig:"), AdminStates.add_prod_original)
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
        f"{kind}, {fmt_money(data['price_rub'])}, "
        f"{data['stock']} шт.",
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
        await call.message.edit_text(
            "Нет категорий.", reply_markup=admin_main_kb(),
        )
        return
    kb = InlineKeyboardBuilder()
    for c in cats:
        kb.button(text=f"📁 {c['name']}", callback_data=f"escat:{c['id']}")
    kb.button(text="⬅️ Назад", callback_data="adm:home")
    kb.adjust(1)
    await state.set_state(AdminStates.edit_stock_pick)
    await call.message.edit_text(
        "Выбери категорию для изменения наличия:",
        reply_markup=kb.as_markup(),
    )
    await call.answer()


@dp.callback_query(F.data == "adm:home")
async def cb_adm_home(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    await call.message.edit_text(
        f"⚙️ <b>Админ-панель {SHOP_NAME}</b>",
        reply_markup=admin_main_kb(),
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
    await state.update_data(cat_id=cat_id)
    await state.set_state(AdminStates.edit_stock_value)
    await call.message.edit_text(
        "Выбери товар:", reply_markup=kb.as_markup(),
    )
    await call.answer()


@dp.callback_query(F.data.startswith("esprod:"))
async def adm_es_pick(call: CallbackQuery, state: FSMContext):
    prod_id = int(call.data.split(":")[1])
    p = await get_product(prod_id)
    if not p:
        await call.answer("Товар не найден", show_alert=True)
        return
    await state.update_data(prod_id=prod_id)
    await call.message.edit_text(
        f"Товар: <b>{p['name']}</b>\n"
        f"Сейчас на складе: <b>{p['stock']}</b> шт.\n\n"
        "Введи новое количество:",
        reply_markup=cancel_kb(),
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
    await state.set_state(AdminStates.delete_pick_category)
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
    await state.set_state(AdminStates.delete_pick_product)
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
# АДМИНКА — СПИСКИ
# =====================================================================

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
        lines.append(f"• <b>#{c['id']}</b> {c['name']}"
                      + (f" — {c['description']}" if c['description'] else ""))
    await call.message.edit_text("\n".join(lines), reply_markup=admin_main_kb())
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
    await call.message.edit_text("\n".join(lines), reply_markup=admin_main_kb())
    await call.answer()


# =====================================================================
# ЗАПУСК
# =====================================================================

async def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not set")
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    if not CRYPTO_BOT_TOKEN:
        raise RuntimeError("CRYPTO_BOT_TOKEN is not set")

    await init_db()
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