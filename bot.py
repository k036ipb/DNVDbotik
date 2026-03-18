import os
import json
import logging
import asyncio

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.exceptions import (
    MessageNotModified,
    MessageToEditNotFound,
    MessageCantBeEdited,
    TelegramAPIError
)

logging.basicConfig(level=logging.INFO)

TOKEN = os.getenv("API_TOKEN")

bot = Bot(token=TOKEN)
dp = Dispatcher(bot)

DATA_FILE = "data.json"
db_lock = asyncio.Lock()


# =========================
# DATABASE
# =========================

def load_data():
    if not os.path.exists(DATA_FILE):
        return {"users": {}, "workspaces": {}}

    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {"users": {}, "workspaces": {}}

    data.setdefault("users", {})
    data.setdefault("workspaces", {})

    return data


async def save_data(data):
    async with db_lock:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)


# =========================
# SAFE EDIT
# =========================

async def safe_edit(message, text, keyboard=None):
    try:
        await message.edit_text(text, reply_markup=keyboard)
    except (MessageNotModified, MessageToEditNotFound, MessageCantBeEdited):
        pass


# =========================
# KEYBOARDS
# =========================

def main_keyboard(user_id, data):
    kb = InlineKeyboardMarkup(row_width=1)

    for ws_id in data["users"][user_id]["workspaces"]:
        ws = data["workspaces"].get(ws_id)
        if not ws:
            continue

        kb.add(
            InlineKeyboardButton(
                f"📂 {ws['name']}",
                callback_data=f"ws:{ws_id}"
            )
        )

    kb.add(InlineKeyboardButton("➕ Подключить workspace", callback_data="connect_help"))
    kb.add(InlineKeyboardButton("🔄 Обновить", callback_data="panel"))

    return kb


def workspace_keyboard(ws_id):
    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("➕ Создать компанию", callback_data=f"create:{ws_id}")
    )
    return kb


# =========================
# TEXT
# =========================

def workspace_text(user_id, data):
    text = "📂 Ваши workspace\n\n"

    ws_list = data["users"][user_id]["workspaces"]

    if not ws_list:
        return text + "Нет подключенных workspace"

    for ws_id in ws_list:
        ws = data["workspaces"].get(ws_id)
        if ws:
            text += f"• {ws['name']}\n"

    return text


# =========================
# START
# =========================

@dp.message_handler(commands=["start"])
async def start(message: types.Message):
    data = load_data()
    user_id = str(message.from_user.id)

    if user_id not in data["users"]:
        data["users"][user_id] = {"workspaces": []}
        await save_data(data)

    await message.answer(
        workspace_text(user_id, data),
        reply_markup=main_keyboard(user_id, data)
    )


# =========================
# PANEL
# =========================

@dp.callback_query_handler(lambda c: c.data == "panel")
async def panel(callback: types.CallbackQuery):
    data = load_data()
    user_id = str(callback.from_user.id)

    if user_id not in data["users"]:
        return

    await safe_edit(
        callback.message,
        workspace_text(user_id, data),
        main_keyboard(user_id, data)
    )

    await callback.answer()


# =========================
# CONNECT HELP (FIX)
# =========================

@dp.callback_query_handler(lambda c: c.data == "connect_help")
async def connect_help(callback: types.CallbackQuery):
    await callback.answer()

    await callback.message.answer(
        "📌 Как подключить workspace:\n\n"
        "1. Открой нужный тред (topic)\n"
        "2. Напиши туда команду:\n"
        "/connect\n\n"
        "После этого workspace появится здесь"
    )


# =========================
# CONNECT
# =========================

@dp.message_handler(commands=["connect"])
async def connect(message: types.Message):

    if message.chat.type == "private":
        await message.reply("Эту команду нужно писать в группе")
        return

    data = load_data()
    user_id = str(message.from_user.id)

    chat_id = message.chat.id
    thread_id = message.message_thread_id or 0
    ws_id = f"{chat_id}_{thread_id}"

    if ws_id not in data["workspaces"]:
        data["workspaces"][ws_id] = {
            "name": message.chat.title,
            "chat_id": chat_id,
            "thread_id": thread_id,
            "template": [
                "Создать договор",
                "Выставить счет",
                "Подготовить мебель"
            ],
            "companies": {},
            "awaiting": False
        }

    data["users"].setdefault(user_id, {"workspaces": []})

    if ws_id not in data["users"][user_id]["workspaces"]:
        data["users"][user_id]["workspaces"].append(ws_id)

    await save_data(data)

    await message.reply(f"✅ Workspace {message.chat.title} подключен")

    # меню
    await bot.send_message(
        chat_id,
        "📂 Меню workspace",
        message_thread_id=thread_id,
        reply_markup=workspace_keyboard(ws_id)
    )

    # просьба админки
    await bot.send_message(
        chat_id,
        "ℹ️ Дай админку, чтобы я мог чистить сообщения",
        message_thread_id=thread_id
    )

    # безопасная отправка в личку
    try:
        await bot.send_message(
            message.from_user.id,
            f"Workspace {message.chat.title} подключен"
        )
    except TelegramAPIError:
        pass


# =========================
# CREATE COMPANY BUTTON
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("create:"))
async def create_company(callback: types.CallbackQuery):

    ws_id = callback.data.split(":")[1]
    data = load_data()

    ws = data["workspaces"].get(ws_id)
    if not ws:
        return

    if ws.get("awaiting"):
        await callback.answer("Уже ждём название")
        return

    ws["awaiting"] = True
    await save_data(data)

    await callback.answer("Напиши название компании в чат")


# =========================
# HANDLE NAME
# =========================

@dp.message_handler(lambda m: m.chat.type != "private")
async def handle_name(message: types.Message):

    data = load_data()

    ws_id = f"{message.chat.id}_{message.message_thread_id or 0}"
    ws = data["workspaces"].get(ws_id)

    if not ws or not ws.get("awaiting"):
        return

    name = message.text.strip()
    if not name:
        return

    ws["awaiting"] = False

    tasks = [{"text": t, "done": False} for t in ws["template"]]
    ws["companies"][name] = {"tasks": tasks}

    await save_data(data)

    try:
        await message.delete()
    except TelegramAPIError:
        pass

    kb = InlineKeyboardMarkup(row_width=1)
    text = f"📁 Клиент: {name}\n\nЗадачи:\n"

    for i, t in enumerate(tasks):
        text += f"⬜ {t['text']}\n"
        kb.add(
            InlineKeyboardButton(
                f"⬜ {t['text']}",
                callback_data=f"task:{ws_id}:{name}:{i}"
            )
        )

    await bot.send_message(
        message.chat.id,
        message_thread_id=message.message_thread_id,
        text=text,
        reply_markup=kb
    )


# =========================
# TASK TOGGLE
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("task:"))
async def toggle(callback: types.CallbackQuery):

    try:
        _, ws_id, company, index = callback.data.split(":")
    except ValueError:
        return

    index = int(index)

    data = load_data()
    ws = data["workspaces"].get(ws_id)

    if not ws:
        return

    task = ws["companies"][company]["tasks"][index]
    task["done"] = not task["done"]

    await save_data(data)

    kb = InlineKeyboardMarkup(row_width=1)
    text = f"📁 Клиент: {company}\n\nЗадачи:\n"

    for i, t in enumerate(ws["companies"][company]["tasks"]):
        icon = "✔" if t["done"] else "⬜"
        text += f"{icon} {t['text']}\n"

        kb.add(
            InlineKeyboardButton(
                f"{icon} {t['text']}",
                callback_data=f"task:{ws_id}:{company}:{i}"
            )
        )

    await safe_edit(callback.message, text, kb)
    await callback.answer()


# =========================
# RUN
# =========================

if __name__ == "__main__":
    if not os.path.exists(DATA_FILE):
        with open(DATA_FILE, "w") as f:
            json.dump({"users": {}, "workspaces": {}}, f)

    executor.start_polling(
        dp,
        skip_updates=True,
        timeout=20,
        relax=0.1
    )
