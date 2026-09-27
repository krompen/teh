"""
Бот технической поддержки с системой тикетов, ролями и позывными специалистов.

ЛОГИКА ПЛАТФОРМЫ
=================
Роли:
  • Клиент       — любой пользователь, создаёт тикеты.
  • Специалист    — назначается владельцем, имеет "позывной" (алиас), который
                    показывается клиенту ВМЕСТО username, когда специалист отвечает.
  • Владелец      — ADMIN_ID из переменных окружения. Владелец — тоже специалист
                    (с позывным по умолчанию "Администрация"), плюс у него есть
                    админ-панель: добавление/переименование/удаление специалистов,
                    блокировка пользователей, статистика.

Жизненный цикл тикета:
  open (создан, никто не взял) → in_progress (взят специалистом, идёт диалог)
      → closed (закрыт, клиенту предложена оценка 👍/👎)

Ключевые механики:
  • При создании тикета уведомление с кнопкой "Взять в работу" рассылается
    ВСЕМ специалистам и владельцу. Кто первый нажал — тот и забрал (гонка
    разруливается атомарным UPDATE ... WHERE status='open'). У остальных
    кнопка автоматически гаснет ("уже взято специалистом #Позывной").
  • Клиент может иметь несколько тикетов одновременно. Если он просто пишет
    текст в чат: один открытый тикет — уходит туда; несколько — бот спросит,
    в какой; ноль — предложит создать новый.
  • Специалист отвечает через явный поток (кнопка "Взять"/"Ответить" → ввод
    текста), т.к. у одного специалиста тикетов может быть много одновременно.
  • Блокировка пользователя (клиента или специалиста) немедленно закрывает
    все его открытые тикеты и лишает его прав — даже если статус специалиста
    формально не снят.
  • Все сообщения по тикету логируются в БД (для истории/аудита).

Перед запуском:
    export BOT_TOKEN="токен_от_BotFather"
    export ADMIN_ID="ваш_telegram_id"
    python3 support_bot.py
"""

import asyncio
import html
import logging
import os
import re
import sqlite3
import time
from datetime import datetime

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command, BaseFilter, StateFilter
from aiogram.types import (
    Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup, any_state
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError

# ==================== КОНФИГУРАЦИЯ ====================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
_admin_env = os.getenv("ADMIN_ID", "").strip()

if not BOT_TOKEN:
    raise SystemExit("❌ Не задан BOT_TOKEN. export BOT_TOKEN=токен_от_BotFather")
if not _admin_env.lstrip("-").isdigit():
    raise SystemExit("❌ Не задан (или некорректен) ADMIN_ID. export ADMIN_ID=ваш_telegram_id")

ADMIN_ID = int(_admin_env)
DB_FILE = os.getenv("DB_FILE", "support_bot.sqlite")
DEFAULT_OWNER_TITLE = "Администрация"
TICKET_LIST_LIMIT = 15

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def esc(value) -> str:
    if value is None:
        return ""
    return html.escape(str(value), quote=False)


def clean_title(raw: str) -> str:
    """Приводит позывной к безопасному виду: буквы/цифры/_/- , до 24 символов."""
    raw = raw.strip().lstrip("#").strip()
    raw = re.sub(r"[^\w\-]", "", raw, flags=re.UNICODE)
    return raw[:24]


# ==================== БАЗА ДАННЫХ ====================

def _connect():
    conn = sqlite3.connect(DB_FILE, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def _init_db_sync():
    conn = _connect()
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            is_banned INTEGER DEFAULT 0,
            registered_at INTEGER
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS staff (
            user_id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            added_at INTEGER,
            added_by INTEGER
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS tickets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            category TEXT,
            status TEXT DEFAULT 'open',
            assigned_staff_id INTEGER,
            created_at INTEGER,
            closed_at INTEGER,
            rating TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS ticket_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticket_id INTEGER,
            sender_role TEXT,
            sender_id INTEGER,
            text TEXT,
            created_at INTEGER
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS ticket_notify (
            ticket_id INTEGER,
            staff_id INTEGER,
            chat_id INTEGER,
            message_id INTEGER
        )
    ''')
    # Владелец всегда существует как специалист с позывным по умолчанию.
    c.execute("SELECT 1 FROM staff WHERE user_id=?", (ADMIN_ID,))
    if not c.fetchone():
        c.execute(
            "INSERT INTO staff (user_id, title, added_at, added_by) VALUES (?, ?, ?, ?)",
            (ADMIN_ID, DEFAULT_OWNER_TITLE, int(time.time()), ADMIN_ID),
        )
    conn.commit()
    conn.close()


async def init_db():
    await asyncio.to_thread(_init_db_sync)


# ---- users ----

def _add_user_sync(user_id, username, full_name):
    conn = _connect()
    c = conn.cursor()
    c.execute("SELECT id FROM users WHERE id=?", (user_id,))
    if not c.fetchone():
        c.execute(
            "INSERT INTO users (id, username, full_name, is_banned, registered_at) VALUES (?, ?, ?, 0, ?)",
            (user_id, username, full_name, int(time.time())),
        )
    else:
        c.execute("UPDATE users SET username=?, full_name=? WHERE id=?", (username, full_name, user_id))
    conn.commit()
    conn.close()


async def db_add_user(user_id, username, full_name):
    await asyncio.to_thread(_add_user_sync, user_id, username, full_name)


def _get_user_sync(user_id):
    conn = _connect()
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE id=?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row


async def db_get_user(user_id):
    return await asyncio.to_thread(_get_user_sync, user_id)


def _set_ban_sync(user_id, value):
    conn = _connect()
    c = conn.cursor()
    c.execute("UPDATE users SET is_banned=? WHERE id=?", (value, user_id))
    conn.commit()
    conn.close()


async def db_set_ban(user_id, value: int):
    await asyncio.to_thread(_set_ban_sync, user_id, value)


def _stats_sync():
    conn = _connect()
    c = conn.cursor()
    stats = {}
    stats['users'] = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    stats['banned'] = c.execute("SELECT COUNT(*) FROM users WHERE is_banned=1").fetchone()[0]
    stats['staff'] = c.execute("SELECT COUNT(*) FROM staff").fetchone()[0]
    stats['open'] = c.execute("SELECT COUNT(*) FROM tickets WHERE status='open'").fetchone()[0]
    stats['in_progress'] = c.execute("SELECT COUNT(*) FROM tickets WHERE status='in_progress'").fetchone()[0]
    stats['closed'] = c.execute("SELECT COUNT(*) FROM tickets WHERE status='closed'").fetchone()[0]
    conn.close()
    return stats


async def db_get_stats():
    return await asyncio.to_thread(_stats_sync)


# ---- staff ----

def _get_staff_sync(user_id):
    conn = _connect()
    c = conn.cursor()
    c.execute("SELECT * FROM staff WHERE user_id=?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row


async def db_get_staff(user_id):
    return await asyncio.to_thread(_get_staff_sync, user_id)


def _get_all_staff_sync():
    conn = _connect()
    c = conn.cursor()
    c.execute("SELECT * FROM staff ORDER BY added_at ASC")
    rows = c.fetchall()
    conn.close()
    return rows


async def db_get_all_staff():
    return await asyncio.to_thread(_get_all_staff_sync)


def _upsert_staff_sync(user_id, title, added_by):
    conn = _connect()
    c = conn.cursor()
    c.execute("SELECT 1 FROM staff WHERE user_id=?", (user_id,))
    exists = c.fetchone()
    if exists:
        c.execute("UPDATE staff SET title=? WHERE user_id=?", (title, user_id))
    else:
        c.execute(
            "INSERT INTO staff (user_id, title, added_at, added_by) VALUES (?, ?, ?, ?)",
            (user_id, title, int(time.time()), added_by),
        )
    conn.commit()
    conn.close()
    return not exists  # True если это новый специалист


async def db_upsert_staff(user_id, title, added_by):
    return await asyncio.to_thread(_upsert_staff_sync, user_id, title, added_by)


def _remove_staff_sync(user_id):
    conn = _connect()
    c = conn.cursor()
    c.execute("DELETE FROM staff WHERE user_id=?", (user_id,))
    removed = c.rowcount > 0
    conn.commit()
    conn.close()
    return removed


async def db_remove_staff(user_id):
    return await asyncio.to_thread(_remove_staff_sync, user_id)


async def is_staff(user_id: int) -> bool:
    if user_id == ADMIN_ID:
        return True
    return bool(await db_get_staff(user_id))


async def get_title(user_id: int) -> str:
    row = await db_get_staff(user_id)
    return row['title'] if row else "Специалист"


# ---- tickets ----

def _create_ticket_sync(user_id, category):
    conn = _connect()
    c = conn.cursor()
    c.execute(
        "INSERT INTO tickets (user_id, category, status, created_at) VALUES (?, ?, 'open', ?)",
        (user_id, category, int(time.time())),
    )
    ticket_id = c.lastrowid
    conn.commit()
    conn.close()
    return ticket_id


async def db_create_ticket(user_id, category):
    return await asyncio.to_thread(_create_ticket_sync, user_id, category)


def _get_ticket_sync(ticket_id):
    conn = _connect()
    c = conn.cursor()
    c.execute("SELECT * FROM tickets WHERE id=?", (ticket_id,))
    row = c.fetchone()
    conn.close()
    return row


async def db_get_ticket(ticket_id):
    return await asyncio.to_thread(_get_ticket_sync, ticket_id)


def _claim_ticket_sync(ticket_id, staff_id):
    conn = _connect()
    c = conn.cursor()
    c.execute(
        "UPDATE tickets SET status='in_progress', assigned_staff_id=? WHERE id=? AND status='open'",
        (staff_id, ticket_id),
    )
    ok = c.rowcount > 0
    conn.commit()
    conn.close()
    return ok


async def db_claim_ticket(ticket_id, staff_id) -> bool:
    return await asyncio.to_thread(_claim_ticket_sync, ticket_id, staff_id)


def _close_ticket_sync(ticket_id):
    conn = _connect()
    c = conn.cursor()
    c.execute("UPDATE tickets SET status='closed', closed_at=? WHERE id=?", (int(time.time()), ticket_id))
    conn.commit()
    conn.close()


async def db_close_ticket(ticket_id):
    await asyncio.to_thread(_close_ticket_sync, ticket_id)


def _set_rating_sync(ticket_id, rating):
    conn = _connect()
    c = conn.cursor()
    c.execute("UPDATE tickets SET rating=? WHERE id=?", (rating, ticket_id))
    conn.commit()
    conn.close()


async def db_set_rating(ticket_id, rating):
    await asyncio.to_thread(_set_rating_sync, ticket_id, rating)


def _get_user_tickets_sync(user_id, open_only=False):
    conn = _connect()
    c = conn.cursor()
    if open_only:
        c.execute(
            "SELECT * FROM tickets WHERE user_id=? AND status!='closed' ORDER BY id DESC LIMIT ?",
            (user_id, TICKET_LIST_LIMIT),
        )
    else:
        c.execute("SELECT * FROM tickets WHERE user_id=? ORDER BY id DESC LIMIT ?", (user_id, TICKET_LIST_LIMIT))
    rows = c.fetchall()
    conn.close()
    return rows


async def db_get_user_tickets(user_id, open_only=False):
    return await asyncio.to_thread(_get_user_tickets_sync, user_id, open_only)


def _get_open_tickets_sync():
    conn = _connect()
    c = conn.cursor()
    c.execute("SELECT * FROM tickets WHERE status='open' ORDER BY id ASC LIMIT ?", (TICKET_LIST_LIMIT,))
    rows = c.fetchall()
    conn.close()
    return rows


async def db_get_open_tickets():
    return await asyncio.to_thread(_get_open_tickets_sync)


def _get_staff_tickets_sync(staff_id):
    conn = _connect()
    c = conn.cursor()
    c.execute(
        "SELECT * FROM tickets WHERE assigned_staff_id=? AND status='in_progress' ORDER BY id ASC LIMIT ?",
        (staff_id, TICKET_LIST_LIMIT),
    )
    rows = c.fetchall()
    conn.close()
    return rows


async def db_get_staff_tickets(staff_id):
    return await asyncio.to_thread(_get_staff_tickets_sync, staff_id)


def _get_active_tickets_sync():
    conn = _connect()
    c = conn.cursor()
    c.execute("SELECT * FROM tickets WHERE status!='closed' ORDER BY id ASC LIMIT ?", (TICKET_LIST_LIMIT,))
    rows = c.fetchall()
    conn.close()
    return rows


async def db_get_all_active_tickets():
    return await asyncio.to_thread(_get_active_tickets_sync)


def _force_close_user_tickets_sync(user_id):
    conn = _connect()
    c = conn.cursor()
    c.execute("SELECT id, assigned_staff_id FROM tickets WHERE user_id=? AND status!='closed'", (user_id,))
    rows = c.fetchall()
    c.execute(
        "UPDATE tickets SET status='closed', closed_at=? WHERE user_id=? AND status!='closed'",
        (int(time.time()), user_id),
    )
    conn.commit()
    conn.close()
    return rows


async def db_force_close_user_tickets(user_id):
    """Закрывает все незакрытые тикеты пользователя (при бане). Возвращает список (id, staff_id)."""
    return await asyncio.to_thread(_force_close_user_tickets_sync, user_id)


# ---- ticket messages (лог переписки) ----

def _add_message_sync(ticket_id, sender_role, sender_id, text):
    conn = _connect()
    c = conn.cursor()
    c.execute(
        "INSERT INTO ticket_messages (ticket_id, sender_role, sender_id, text, created_at) VALUES (?, ?, ?, ?, ?)",
        (ticket_id, sender_role, sender_id, text, int(time.time())),
    )
    conn.commit()
    conn.close()


async def db_log_message(ticket_id, sender_role, sender_id, text):
    await asyncio.to_thread(_add_message_sync, ticket_id, sender_role, sender_id, text)


def _last_message_sync(ticket_id):
    conn = _connect()
    c = conn.cursor()
    c.execute("SELECT * FROM ticket_messages WHERE ticket_id=? ORDER BY id DESC LIMIT 1", (ticket_id,))
    row = c.fetchone()
    conn.close()
    return row


async def db_get_last_message(ticket_id):
    return await asyncio.to_thread(_last_message_sync, ticket_id)


# ---- ticket notify (для гашения кнопок у остальных специалистов) ----

def _add_notify_sync(ticket_id, staff_id, chat_id, message_id):
    conn = _connect()
    c = conn.cursor()
    c.execute(
        "INSERT INTO ticket_notify (ticket_id, staff_id, chat_id, message_id) VALUES (?, ?, ?, ?)",
        (ticket_id, staff_id, chat_id, message_id),
    )
    conn.commit()
    conn.close()


async def db_add_notify(ticket_id, staff_id, chat_id, message_id):
    await asyncio.to_thread(_add_notify_sync, ticket_id, staff_id, chat_id, message_id)


def _get_notify_sync(ticket_id):
    conn = _connect()
    c = conn.cursor()
    c.execute("SELECT * FROM ticket_notify WHERE ticket_id=?", (ticket_id,))
    rows = c.fetchall()
    conn.close()
    return rows


async def db_get_notify(ticket_id):
    return await asyncio.to_thread(_get_notify_sync, ticket_id)


def _clear_notify_sync(ticket_id):
    conn = _connect()
    c = conn.cursor()
    c.execute("DELETE FROM ticket_notify WHERE ticket_id=?", (ticket_id,))
    conn.commit()
    conn.close()


async def db_clear_notify(ticket_id):
    await asyncio.to_thread(_clear_notify_sync, ticket_id)


# ==================== НАСТРОЙКА БОТА ====================

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())


class IsOwner(BaseFilter):
    async def __call__(self, message: Message) -> bool:
        return message.from_user.id == ADMIN_ID


CATEGORIES = {
    "cat_question": "❓ Общий вопрос",
    "cat_problem": "⚠️ Техническая проблема",
    "cat_billing": "💳 Оплата / возврат",
    "cat_other": "📝 Другое",
}


class ClientFSM(StatesGroup):
    choosing_category = State()
    writing_ticket = State()
    writing_reply = State()          # data: ticket_id


class StaffFSM(StatesGroup):
    replying = State()               # data: ticket_id


class AdminStaffFSM(StatesGroup):
    add_id = State()
    add_title = State()              # data: new_staff_id
    rename_id = State()
    rename_title = State()           # data: rename_id
    remove_id = State()


class BanFSM(StatesGroup):
    ban_id = State()
    unban_id = State()


# ==================== КЛАВИАТУРЫ ====================

async def get_main_kb(user_id: int) -> ReplyKeyboardMarkup:
    kb = [
        [KeyboardButton(text="🆕 Новый тикет"), KeyboardButton(text="📂 Мои тикеты")],
        [KeyboardButton(text="ℹ️ Как это работает"), KeyboardButton(text="🆔 Мой ID")],
    ]
    if await is_staff(user_id):
        kb.append([KeyboardButton(text="🎫 Тикеты поддержки")])
    if user_id == ADMIN_ID:
        kb.append([KeyboardButton(text="⚙️ Админ-панель")])
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


def get_admin_kb() -> ReplyKeyboardMarkup:
    kb = [
        [KeyboardButton(text="➕ Добавить специалиста"), KeyboardButton(text="🗑 Удалить специалиста")],
        [KeyboardButton(text="✏️ Переименовать специалиста"), KeyboardButton(text="📋 Список специалистов")],
        [KeyboardButton(text="🚫 Заблокировать"), KeyboardButton(text="✅ Разблокировать")],
        [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="📋 Активные тикеты")],
        [KeyboardButton(text="🔙 В главное меню")],
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


def get_categories_kb() -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton(text=CATEGORIES["cat_question"], callback_data="cat_question"),
         InlineKeyboardButton(text=CATEGORIES["cat_problem"], callback_data="cat_problem")],
        [InlineKeyboardButton(text=CATEGORIES["cat_billing"], callback_data="cat_billing"),
         InlineKeyboardButton(text=CATEGORIES["cat_other"], callback_data="cat_other")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cat_cancel")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)


STATUS_EMOJI = {"open": "🆕", "in_progress": "🔧", "closed": "✅"}


# ==================== ВСПОМОГАТЕЛЬНАЯ ЛОГИКА ТИКЕТОВ ====================

async def notify_staff_new_ticket(ticket_id: int):
    ticket = await db_get_ticket(ticket_id)
    if not ticket:
        return
    client = await db_get_user(ticket['user_id'])
    uname = f"@{esc(client['username'])}" if client and client['username'] else "скрыт"
    last_msg = await db_get_last_message(ticket_id)
    text = (
        f"🔔 <b>Новый тикет №{ticket_id}</b>\n"
        f"<b>Категория:</b> {ticket['category']}\n"
        f"<b>От:</b> {esc(client['full_name']) if client else '—'} ({uname})\n"
        f"<b>ID клиента:</b> <code>{ticket['user_id']}</code>\n\n"
        f"💬 {esc(last_msg['text']) if last_msg else ''}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🎯 Взять в работу", callback_data=f"take_{ticket_id}")]])

    staff_list = await db_get_all_staff()
    for s in staff_list:
        try:
            msg = await bot.send_message(s['user_id'], text, reply_markup=kb)
            await db_add_notify(ticket_id, s['user_id'], s['user_id'], msg.message_id)
        except Exception:
            continue


async def sweep_ticket_notifications(ticket_id: int, taken_by_title: str, exclude_staff_id: int):
    rows = await db_get_notify(ticket_id)
    for r in rows:
        if r['staff_id'] == exclude_staff_id:
            continue
        try:
            await bot.edit_message_text(
                chat_id=r['chat_id'],
                message_id=r['message_id'],
                text=f"🔒 <b>Тикет №{ticket_id}</b> уже взят в работу специалистом #{esc(taken_by_title)}.",
            )
        except Exception:
            pass
    await db_clear_notify(ticket_id)


async def send_to_client(ticket: sqlite3.Row, text: str, staff_title: str):
    footer = f"\n\n👨‍💻 <b>Тех.специалист:</b> #{esc(staff_title)}"
    try:
        await bot.send_message(ticket['user_id'], f"💬 <b>Ответ по тикету №{ticket['id']}:</b>\n\n{esc(text)}{footer}")
        return True
    except Exception:
        return False


async def send_to_staff(ticket: sqlite3.Row, text: str):
    if not ticket['assigned_staff_id']:
        return False
    client = await db_get_user(ticket['user_id'])
    name = esc(client['full_name']) if client else str(ticket['user_id'])
    try:
        await bot.send_message(
            ticket['assigned_staff_id'],
            f"✉️ <b>Клиент {name} (тикет №{ticket['id']}):</b>\n\n{esc(text)}",
        )
        return True
    except Exception:
        return False


def ticket_line(ticket: sqlite3.Row) -> str:
    dt = datetime.fromtimestamp(ticket['created_at']).strftime('%d.%m %H:%M')
    return f"{STATUS_EMOJI.get(ticket['status'], '❔')} №{ticket['id']} · {ticket['category']} · {dt}"


# ==================== ОБЩИЕ КОМАНДЫ ====================

@dp.message(CommandStart(), StateFilter(any_state))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    user = message.from_user
    await db_add_user(user.id, user.username, user.full_name)
    db_user = await db_get_user(user.id)

    if db_user and db_user['is_banned'] == 1:
        return await message.answer("🚫 <b>Доступ ограничен.</b> Ваш аккаунт заблокирован в поддержке.")

    role_note = ""
    staff_row = await db_get_staff(user.id)
    if staff_row:
        role_note = f"\n\nВы подключены как специалист поддержки с позывным <b>#{esc(staff_row['title'])}</b>."

    text = (
        f"👋 <b>Добро пожаловать в поддержку, {esc(user.full_name)}!</b>\n\n"
        f"Здесь вы можете создать тикет, и вам ответит специалист.{role_note}"
    )
    await message.answer(text, reply_markup=await get_main_kb(user.id))


@dp.message(Command("cancel"), StateFilter(any_state))
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Действие отменено.", reply_markup=await get_main_kb(message.from_user.id))


@dp.message(F.text == "🆔 Мой ID", StateFilter(any_state))
async def my_id(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(f"🆔 Ваш Telegram ID: <code>{message.from_user.id}</code>")


@dp.message(F.text == "ℹ️ Как это работает", StateFilter(any_state))
async def how_it_works(message: Message, state: FSMContext):
    await state.clear()
    text = (
        "ℹ️ <b>Как работает поддержка:</b>\n\n"
        "1️⃣ Нажмите «🆕 Новый тикет», выберите тему и опишите проблему.\n"
        "2️⃣ Тикет увидят специалисты поддержки, кто-то возьмёт его в работу.\n"
        "3️⃣ Дальше просто пишите сюда — ваши сообщения автоматически попадут "
        "в переписку по тикету.\n"
        "4️⃣ Специалист ответит вам под своим позывным (не username) — это нормально, "
        "так устроена анонимность нашей поддержки.\n"
        "5️⃣ После решения вопроса тикет закроют, и мы попросим оценить ответ 👍/👎."
    )
    await message.answer(text)


@dp.message(F.text == "🔙 В главное меню", StateFilter(any_state))
async def back_to_main(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Вы в главном меню.", reply_markup=await get_main_kb(message.from_user.id))


# ==================== СОЗДАНИЕ ТИКЕТА (КЛИЕНТ) ====================

@dp.message(F.text == "🆕 Новый тикет", StateFilter(any_state))
async def new_ticket_start(message: Message, state: FSMContext):
    await state.clear()
    db_user = await db_get_user(message.from_user.id)
    if not db_user or db_user['is_banned'] == 1:
        return await message.answer("🚫 Доступ ограничен. Ваш аккаунт заблокирован.")
    await message.answer("Выберите тему обращения:", reply_markup=get_categories_kb())
    await state.set_state(ClientFSM.choosing_category)


@dp.callback_query(F.data.startswith("cat_"), ClientFSM.choosing_category)
async def category_chosen(call: CallbackQuery, state: FSMContext):
    if call.data == "cat_cancel":
        await state.clear()
        await call.message.edit_text("❌ Создание тикета отменено.")
        return await call.answer()
    category = CATEGORIES.get(call.data, "📝 Другое")
    await state.update_data(category=category)
    await call.message.edit_text(f"Тема: <b>{category}</b>\n\n✏️ Опишите вашу проблему подробно одним сообщением:")
    await state.set_state(ClientFSM.writing_ticket)
    await call.answer()


@dp.message(ClientFSM.writing_ticket)
async def ticket_created(message: Message, state: FSMContext):
    if not message.text:
        return await message.answer("❌ Пожалуйста, опишите проблему текстом.")
    data = await state.get_data()
    category = data.get("category", "📝 Другое")

    ticket_id = await db_create_ticket(message.from_user.id, category)
    await db_log_message(ticket_id, "client", message.from_user.id, message.text)
    await state.clear()

    await message.answer(
        f"✅ <b>Тикет №{ticket_id} создан!</b>\nОжидайте — специалист скоро возьмёт его в работу.",
        reply_markup=await get_main_kb(message.from_user.id),
    )
    asyncio.create_task(notify_staff_new_ticket(ticket_id))


# ==================== "МОИ ТИКЕТЫ" (КЛИЕНТ) ====================

@dp.message(F.text == "📂 Мои тикеты", StateFilter(any_state))
async def my_tickets(message: Message, state: FSMContext):
    await state.clear()
    tickets = await db_get_user_tickets(message.from_user.id)
    if not tickets:
        return await message.answer("У вас пока нет тикетов. Нажмите «🆕 Новый тикет», чтобы создать первый.")

    rows = []
    lines = ["📂 <b>Ваши тикеты:</b>\n"]
    for t in tickets:
        lines.append(ticket_line(t))
        if t['status'] != 'closed':
            rows.append([InlineKeyboardButton(text=f"✏️ Написать в №{t['id']}", callback_data=f"myticket_{t['id']}")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows) if rows else None
    await message.answer("\n".join(lines), reply_markup=kb)


@dp.callback_query(F.data.startswith("myticket_"))
async def myticket_selected(call: CallbackQuery, state: FSMContext):
    ticket_id = int(call.data.split("_")[1])
    ticket = await db_get_ticket(ticket_id)
    if not ticket or ticket['user_id'] != call.from_user.id:
        return await call.answer("Тикет не найден.", show_alert=True)
    if ticket['status'] == 'closed':
        return await call.answer("Этот тикет уже закрыт.", show_alert=True)

    data = await state.get_data()
    pending_text = data.get("pending_text")

    if pending_text:
        await _route_client_message(call.from_user.id, ticket, pending_text)
        await state.clear()
        await call.message.edit_text(f"✅ Сообщение отправлено в тикет №{ticket_id}.")
    else:
        await state.set_state(ClientFSM.writing_reply)
        await state.update_data(ticket_id=ticket_id)
        await call.message.edit_text(f"✏️ Напишите сообщение по тикету №{ticket_id}:")
    await call.answer()


@dp.message(ClientFSM.writing_reply)
async def client_reply_to_selected_ticket(message: Message, state: FSMContext):
    if not message.text:
        return await message.answer("❌ Пожалуйста, отправьте текст.")
    data = await state.get_data()
    ticket_id = data.get("ticket_id")
    ticket = await db_get_ticket(ticket_id)
    if not ticket or ticket['status'] == 'closed':
        await state.clear()
        return await message.answer("❌ Тикет недоступен (возможно, уже закрыт).", reply_markup=await get_main_kb(message.from_user.id))
    await _route_client_message(message.from_user.id, ticket, message.text)
    await message.answer("✅ Отправлено.")


async def _route_client_message(client_id: int, ticket: sqlite3.Row, text: str):
    await db_log_message(ticket['id'], "client", client_id, text)
    if ticket['status'] == 'in_progress':
        await send_to_staff(ticket, text)
    # если тикет ещё 'open' (никто не взял) — сообщение просто добавляется в лог,
    # чтобы не спамить специалистов повторными уведомлениями.


# ==================== КАТЧ-ОЛЛ: СВОБОДНЫЙ ТЕКСТ ОТ КЛИЕНТА ====================
# Важно: регистрируется в самом конце файла (после всех остальных хендлеров),
# чтобы не перехватывать команды/кнопки меню.

async def client_freeform_text(message: Message, state: FSMContext):
    db_user = await db_get_user(message.from_user.id)
    if not db_user or db_user['is_banned'] == 1:
        return

    tickets = await db_get_user_tickets(message.from_user.id, open_only=True)
    if not tickets:
        return await message.answer(
            "У вас нет открытых тикетов. Нажмите «🆕 Новый тикет», чтобы создать обращение.",
            reply_markup=await get_main_kb(message.from_user.id),
        )

    if len(tickets) == 1:
        await _route_client_message(message.from_user.id, tickets[0], message.text)
        return await message.answer(f"✅ Сообщение добавлено в тикет №{tickets[0]['id']}.")

    # Несколько открытых тикетов — спрашиваем, в какой отправить.
    await state.update_data(pending_text=message.text)
    rows = [[InlineKeyboardButton(text=ticket_line(t), callback_data=f"myticket_{t['id']}")] for t in tickets]
    await message.answer("У вас несколько открытых тикетов. В какой отправить сообщение?", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))


# ==================== СПЕЦИАЛИСТЫ: ОЧЕРЕДЬ ТИКЕТОВ ====================

@dp.message(F.text == "🎫 Тикеты поддержки", StateFilter(any_state))
async def staff_queue(message: Message, state: FSMContext):
    await state.clear()
    if not await is_staff(message.from_user.id):
        return
    open_tickets = await db_get_open_tickets()
    my_tickets_list = await db_get_staff_tickets(message.from_user.id)

    lines = []
    rows = []
    if open_tickets:
        lines.append("🆕 <b>Новые (никто не взял):</b>")
        for t in open_tickets:
            lines.append(ticket_line(t))
            rows.append([InlineKeyboardButton(text=f"🎯 Взять №{t['id']}", callback_data=f"take_{t['id']}")])
    else:
        lines.append("🆕 Новых тикетов нет.")

    lines.append("")
    if my_tickets_list:
        lines.append("📌 <b>В работе у вас:</b>")
        for t in my_tickets_list:
            lines.append(ticket_line(t))
            rows.append([
                InlineKeyboardButton(text=f"💬 Ответить №{t['id']}", callback_data=f"reply_{t['id']}"),
                InlineKeyboardButton(text=f"✅ Закрыть №{t['id']}", callback_data=f"close_{t['id']}"),
            ])
    else:
        lines.append("📌 У вас нет тикетов в работе.")

    await message.answer("\n".join(lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=rows) if rows else None)


@dp.callback_query(F.data.startswith("take_"))
async def take_ticket(call: CallbackQuery, state: FSMContext):
    if not await is_staff(call.from_user.id):
        return await call.answer("Недостаточно прав.", show_alert=True)
    staff_user = await db_get_user(call.from_user.id)
    if staff_user and staff_user['is_banned'] == 1:
        return await call.answer("Вы заблокированы.", show_alert=True)

    ticket_id = int(call.data.split("_")[1])
    ticket = await db_get_ticket(ticket_id)
    if not ticket:
        return await call.answer("Тикет не найден.", show_alert=True)

    if ticket['status'] == 'closed':
        return await call.answer("Тикет уже закрыт.", show_alert=True)

    if ticket['status'] == 'in_progress':
        if ticket['assigned_staff_id'] == call.from_user.id:
            await state.set_state(StaffFSM.replying)
            await state.update_data(ticket_id=ticket_id)
            await call.message.edit_text(f"✏️ Тикет №{ticket_id} уже ваш. Напишите ответ клиенту:")
            return await call.answer()
        other_title = await get_title(ticket['assigned_staff_id'])
        return await call.answer(f"Уже взят специалистом #{other_title}.", show_alert=True)

    ok = await db_claim_ticket(ticket_id, call.from_user.id)
    if not ok:
        return await call.answer("Кто-то опередил вас с этим тикетом.", show_alert=True)

    my_title = await get_title(call.from_user.id)
    asyncio.create_task(sweep_ticket_notifications(ticket_id, my_title, exclude_staff_id=call.from_user.id))

    ticket = await db_get_ticket(ticket_id)
    try:
        await bot.send_message(ticket['user_id'], f"🔧 Ваш тикет №{ticket_id} взят в работу специалистом #{esc(my_title)}.")
    except Exception:
        pass

    await state.set_state(StaffFSM.replying)
    await state.update_data(ticket_id=ticket_id)
    await call.message.edit_text(f"✅ Тикет №{ticket_id} ваш.\n\n✏️ Напишите ответ клиенту:")
    await call.answer()


@dp.callback_query(F.data.startswith("reply_"))
async def reply_to_own_ticket(call: CallbackQuery, state: FSMContext):
    ticket_id = int(call.data.split("_")[1])
    ticket = await db_get_ticket(ticket_id)
    if not ticket or ticket['status'] != 'in_progress' or ticket['assigned_staff_id'] != call.from_user.id:
        return await call.answer("Этот тикет вам недоступен.", show_alert=True)
    await state.set_state(StaffFSM.replying)
    await state.update_data(ticket_id=ticket_id)
    await call.message.edit_text(f"✏️ Напишите ответ клиенту по тикету №{ticket_id}:")
    await call.answer()


@dp.message(StaffFSM.replying)
async def staff_send_reply(message: Message, state: FSMContext):
    if not message.text:
        return await message.answer("❌ Пожалуйста, отправьте текстовый ответ.")
    data = await state.get_data()
    ticket_id = data.get("ticket_id")
    ticket = await db_get_ticket(ticket_id)

    if not ticket or ticket['status'] == 'closed':
        await state.clear()
        return await message.answer("❌ Тикет уже закрыт или не найден.", reply_markup=await get_main_kb(message.from_user.id))
    if ticket['assigned_staff_id'] != message.from_user.id:
        await state.clear()
        return await message.answer("❌ Этот тикет закреплён за другим специалистом.")

    title = await get_title(message.from_user.id)
    await db_log_message(ticket_id, "staff", message.from_user.id, message.text)
    delivered = await send_to_client(ticket, message.text, title)

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Закрыть тикет", callback_data=f"close_{ticket_id}")]])
    note = "✅ Ответ отправлен клиенту." if delivered else "⚠️ Ответ сохранён, но клиенту доставить не удалось (заблокировал бота)."
    await message.answer(note, reply_markup=kb)
    # Состояние НЕ сбрасываем — специалист может продолжать переписку тем же текстом.


@dp.callback_query(F.data.startswith("close_"))
async def close_ticket(call: CallbackQuery, state: FSMContext):
    ticket_id = int(call.data.split("_")[1])
    ticket = await db_get_ticket(ticket_id)
    if not ticket:
        return await call.answer("Тикет не найден.", show_alert=True)
    if ticket['status'] == 'closed':
        return await call.answer("Уже закрыт.", show_alert=True)

    is_assigned_to_caller = ticket['assigned_staff_id'] == call.from_user.id
    if not (is_assigned_to_caller or call.from_user.id == ADMIN_ID):
        return await call.answer("Закрыть тикет может только владелец или назначенный специалист.", show_alert=True)

    await db_close_ticket(ticket_id)
    title = await get_title(call.from_user.id)
    await call.message.edit_text(f"✅ Тикет №{ticket_id} закрыт.")

    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="👍", callback_data=f"rate_{ticket_id}_up"),
        InlineKeyboardButton(text="👎", callback_data=f"rate_{ticket_id}_down"),
    ]])
    try:
        await bot.send_message(
            ticket['user_id'],
            f"✅ <b>Тикет №{ticket_id} закрыт</b> специалистом #{esc(title)}.\n\nОцените, пожалуйста, качество ответа:",
            reply_markup=kb,
        )
    except Exception:
        pass

    state_data = await state.get_data()
    if state_data.get("ticket_id") == ticket_id:
        await state.clear()
    await call.answer()


@dp.callback_query(F.data.startswith("rate_"))
async def rate_ticket(call: CallbackQuery):
    _, ticket_id_str, vote = call.data.split("_")
    ticket_id = int(ticket_id_str)
    ticket = await db_get_ticket(ticket_id)
    if not ticket or ticket['user_id'] != call.from_user.id:
        return await call.answer()
    await db_set_rating(ticket_id, vote)
    await call.message.edit_text(f"Спасибо за оценку {'👍' if vote == 'up' else '👎'}!")
    await call.answer()


# ==================== АДМИН-ПАНЕЛЬ (ВЛАДЕЛЕЦ) ====================

@dp.message(F.text == "⚙️ Админ-панель", IsOwner(), StateFilter(any_state))
async def admin_panel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("🔐 <b>Админ-панель поддержки</b>", reply_markup=get_admin_kb())


@dp.message(F.text == "📊 Статистика", IsOwner())
async def admin_stats(message: Message):
    s = await db_get_stats()
    text = (
        f"📊 <b>Статистика платформы</b>\n\n"
        f"👥 Пользователей: {s['users']} (заблокировано: {s['banned']})\n"
        f"🧑‍💻 Специалистов: {s['staff']}\n\n"
        f"🆕 Открытых тикетов: {s['open']}\n"
        f"🔧 В работе: {s['in_progress']}\n"
        f"✅ Закрытых: {s['closed']}"
    )
    await message.answer(text)


@dp.message(F.text == "📋 Список специалистов", IsOwner())
async def admin_staff_list(message: Message):
    staff_list = await db_get_all_staff()
    lines = ["👥 <b>Специалисты поддержки:</b>\n"]
    for s in staff_list:
        mark = " (владелец)" if s['user_id'] == ADMIN_ID else ""
        lines.append(f"#{esc(s['title'])} — <code>{s['user_id']}</code>{mark}")
    await message.answer("\n".join(lines))


@dp.message(F.text == "📋 Активные тикеты", IsOwner())
async def admin_active_tickets(message: Message):
    tickets = await db_get_all_active_tickets()
    if not tickets:
        return await message.answer("Активных тикетов нет.")
    rows = []
    lines = ["📋 <b>Активные тикеты платформы:</b>\n"]
    for t in tickets:
        assignee = f" · у #{await get_title(t['assigned_staff_id'])}" if t['assigned_staff_id'] else ""
        lines.append(ticket_line(t) + assignee)
        if t['status'] == 'open':
            rows.append([InlineKeyboardButton(text=f"🎯 Взять №{t['id']}", callback_data=f"take_{t['id']}")])
        else:
            rows.append([InlineKeyboardButton(text=f"✅ Закрыть №{t['id']}", callback_data=f"close_{t['id']}")])
    await message.answer("\n".join(lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))


@dp.message(F.text == "➕ Добавить специалиста", IsOwner())
async def admin_add_staff_start(message: Message, state: FSMContext):
    await message.answer("Введите Telegram ID нового специалиста:")
    await state.set_state(AdminStaffFSM.add_id)


@dp.message(AdminStaffFSM.add_id, IsOwner())
async def admin_add_staff_id(message: Message, state: FSMContext):
    if not message.text or not message.text.strip().lstrip("-").isdigit():
        return await message.answer("❌ ID должен быть числом.")
    await state.update_data(new_staff_id=int(message.text.strip()))
    await message.answer("Теперь введите позывной для этого специалиста (например: Олежа):")
    await state.set_state(AdminStaffFSM.add_title)


@dp.message(AdminStaffFSM.add_title, IsOwner())
async def admin_add_staff_title(message: Message, state: FSMContext):
    title = clean_title(message.text or "")
    if not title:
        return await message.answer("❌ Позывной должен содержать хотя бы одну букву или цифру. Попробуйте ещё раз:")
    data = await state.get_data()
    new_id = data.get("new_staff_id")

    is_new = await db_upsert_staff(new_id, title, message.from_user.id)
    verb = "добавлен" if is_new else "обновлён"
    await message.answer(f"✅ Специалист <code>{new_id}</code> {verb} с позывным #{esc(title)}.", reply_markup=get_admin_kb())

    try:
        await bot.send_message(
            new_id,
            f"🎉 Вам присвоен статус специалиста поддержки с позывным <b>#{esc(title)}</b>.\n"
            f"Отправьте /start, чтобы увидеть новые кнопки в меню.",
        )
    except Exception:
        await message.answer("⚠️ Не удалось уведомить специалиста лично (возможно, он не запускал бота).")
    await state.clear()


@dp.message(F.text == "✏️ Переименовать специалиста", IsOwner())
async def admin_rename_start(message: Message, state: FSMContext):
    await message.answer("Введите Telegram ID специалиста, которого нужно переименовать:")
    await state.set_state(AdminStaffFSM.rename_id)


@dp.message(AdminStaffFSM.rename_id, IsOwner())
async def admin_rename_id(message: Message, state: FSMContext):
    if not message.text or not message.text.strip().lstrip("-").isdigit():
        return await message.answer("❌ ID должен быть числом.")
    target_id = int(message.text.strip())
    if not await db_get_staff(target_id):
        await state.clear()
        return await message.answer("❌ Этот пользователь не является специалистом.", reply_markup=get_admin_kb())
    await state.update_data(rename_id=target_id)
    await message.answer("Введите новый позывной:")
    await state.set_state(AdminStaffFSM.rename_title)


@dp.message(AdminStaffFSM.rename_title, IsOwner())
async def admin_rename_title(message: Message, state: FSMContext):
    title = clean_title(message.text or "")
    if not title:
        return await message.answer("❌ Позывной должен содержать хотя бы одну букву или цифру. Попробуйте ещё раз:")
    data = await state.get_data()
    target_id = data.get("rename_id")
    await db_upsert_staff(target_id, title, message.from_user.id)
    await message.answer(f"✅ Новый позывной для <code>{target_id}</code>: #{esc(title)}.", reply_markup=get_admin_kb())
    try:
        await bot.send_message(target_id, f"ℹ️ Ваш позывной в поддержке изменён на <b>#{esc(title)}</b>.")
    except Exception:
        pass
    await state.clear()


@dp.message(F.text == "🗑 Удалить специалиста", IsOwner())
async def admin_remove_start(message: Message, state: FSMContext):
    await message.answer("Введите Telegram ID специалиста для удаления:")
    await state.set_state(AdminStaffFSM.remove_id)


@dp.message(AdminStaffFSM.remove_id, IsOwner())
async def admin_remove_id(message: Message, state: FSMContext):
    if not message.text or not message.text.strip().lstrip("-").isdigit():
        return await message.answer("❌ ID должен быть числом.")
    target_id = int(message.text.strip())
    if target_id == ADMIN_ID:
        await state.clear()
        return await message.answer("❌ Нельзя удалить владельца платформы.", reply_markup=get_admin_kb())

    removed = await db_remove_staff(target_id)
    await state.clear()
    if removed:
        await message.answer(f"✅ Специалист <code>{target_id}</code> удалён.", reply_markup=get_admin_kb())
        try:
            await bot.send_message(target_id, "ℹ️ Вы больше не являетесь специалистом поддержки.")
        except Exception:
            pass
    else:
        await message.answer("❌ Такой специалист не найден.", reply_markup=get_admin_kb())


@dp.message(F.text == "🚫 Заблокировать", IsOwner())
async def admin_ban_start(message: Message, state: FSMContext):
    await message.answer("Введите Telegram ID пользователя для блокировки:")
    await state.set_state(BanFSM.ban_id)


@dp.message(BanFSM.ban_id, IsOwner())
async def admin_ban_id(message: Message, state: FSMContext):
    if not message.text or not message.text.strip().lstrip("-").isdigit():
        return await message.answer("❌ ID должен быть числом.")
    target_id = int(message.text.strip())
    if target_id == ADMIN_ID:
        await state.clear()
        return await message.answer("❌ Нельзя заблокировать владельца платформы.", reply_markup=get_admin_kb())

    await db_set_ban(target_id, 1)
    closed = await db_force_close_user_tickets(target_id)
    for row in closed:
        if row['assigned_staff_id']:
            try:
                await bot.send_message(row['assigned_staff_id'], f"ℹ️ Тикет №{row['id']} закрыт автоматически: клиент заблокирован администрацией.")
            except Exception:
                pass

    await message.answer(
        f"✅ Пользователь <code>{target_id}</code> заблокирован."
        + (f" Закрыто тикетов: {len(closed)}." if closed else ""),
        reply_markup=get_admin_kb(),
    )
    try:
        await bot.send_message(target_id, "🚫 <b>Вы заблокированы в поддержке администрацией платформы.</b>")
    except Exception:
        pass
    await state.clear()


@dp.message(F.text == "✅ Разблокировать", IsOwner())
async def admin_unban_start(message: Message, state: FSMContext):
    await message.answer("Введите Telegram ID пользователя для разблокировки:")
    await state.set_state(BanFSM.unban_id)


@dp.message(BanFSM.unban_id, IsOwner())
async def admin_unban_id(message: Message, state: FSMContext):
    if not message.text or not message.text.strip().lstrip("-").isdigit():
        return await message.answer("❌ ID должен быть числом.")
    target_id = int(message.text.strip())
    await db_set_ban(target_id, 0)
    await message.answer(f"✅ Пользователь <code>{target_id}</code> разблокирован.", reply_markup=get_admin_kb())
    try:
        await bot.send_message(target_id, "✅ <b>Блокировка в поддержке снята.</b>")
    except Exception:
        pass
    await state.clear()


# ==================== КАТЧ-ОЛЛ (регистрируется последним!) ====================

dp.message.register(client_freeform_text, F.text, StateFilter(None))


# ==================== ЗАПУСК ====================

async def main():
    await init_db()
    try:
        me = await bot.get_me()
        logger.info(f"🚀 Бот поддержки запущен: @{me.username}")
    except Exception:
        logger.error("❌ НЕВЕРНЫЙ ТОКЕН БОТА! Проверьте переменную окружения BOT_TOKEN.")
        return
    try:
        await dp.start_polling(bot)
    finally:
        try:
            await bot.session.close()
        except Exception:
            pass
        logger.info("🛑 Бот поддержки остановлен.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
