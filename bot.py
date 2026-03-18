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
            return json.load(f)
    except:
        return {"users": {}, "workspaces": {}}


async def save_data(data):
    async with db_lock:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)


# =========================
# HELPERS
# =========================

def get_ws_id(chat_id, thread_id):
    return f"{chat_id}_{thread_id}"


def get_ws(message, data):
    return data["workspaces"].get(
        get_ws_id(message.chat.id, message.message_thread_id)
    )


# =========================
# SAFE EDIT
# =========================

async def safe_edit(message, text, kb=None):
    try:
        await message.edit_text(text, reply_markup=kb)
    except (MessageNotModified, MessageToEditNotFound, MessageCantBeEdited):
        pass


# =========================
# KEYBOARDS
# =========================

def main_keyboard(user_id, data):
    kb = InlineKeyboardMarkup(row_width=1)

    for ws_id in data["users"].get(user_id, {}).get("workspaces", []):
        ws = data["workspaces"].get(ws_id)
        if ws:
            kb.add(InlineKeyboardButton(ws["name"], callback_data="ws"))

    kb.add(InlineKeyboardButton("➕ Подключить workspace", callback_data="connect_help"))
    kb.add(InlineKeyboardButton("🔄 Обновить", callback_data="panel"))

    return kb


def workspace_keyboard(ws):
    kb = InlineKeyboardMarkup(row_width=1)

    for name in ws["companies"].keys():
        kb.add(InlineKeyboardButton(name, callback_data=f"open:{name}"))

    kb.add(InlineKeyboardButton("➕ Создать компанию", callback_data="create"))
    kb.add(InlineKeyboardButton("⚙️ Редактировать шаблон", callback_data="edit_template"))

    return kb


# =========================
# START
# =========================

@dp.message_handler(commands=["start"])
async def start(message: types.Message):
    data = load_data()
    user_id = str(message.from_user.id)

    data["users"].setdefault(user_id, {"workspaces": []})
    await save_data(data)

    await message.answer(
        "📂 Ваши workspace",
        reply_markup=main_keyboard(user_id, data)
    )


# =========================
# PANEL
# =========================

@dp.callback_query_handler(lambda c: c.data == "panel")
async def panel(callback: types.CallbackQuery):
    data = load_data()
    user_id = str(callback.from_user.id)

    await safe_edit(
        callback.message,
        "📂 Ваши workspace",
        main_keyboard(user_id, data)
    )
    await callback.answer()


# =========================
# WS MENU (ЛС)
# =========================

@dp.callback_query_handler(lambda c: c.data == "ws")
async def ws_menu(callback: types.CallbackQuery):
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🗑 Удалить workspace", callback_data="delete_ws"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data="panel"))

    await safe_edit(callback.message, "⚙️ Управление workspace", kb)
    await callback.answer()


# =========================
# CONNECT HELP
# =========================

@dp.callback_query_handler(lambda c: c.data == "connect_help")
async def connect_help(callback: types.CallbackQuery):
    await callback.message.answer(
        "📌 Перейди в тред и напиши:\n/connect"
    )
    await callback.answer()


# =========================
# CONNECT
# =========================

@dp.message_handler(commands=["connect"])
async def connect(message: types.Message):

    if message.chat.type == "private":
        return await message.answer("Используй в группе")

    data = load_data()
    user_id = str(message.from_user.id)

    ws_id = get_ws_id(message.chat.id, message.message_thread_id)

    data["workspaces"][ws_id] = {
        "name": message.chat.title,
        "template": [
            "Создать договор",
            "Выставить счет",
            "Подготовить мебель"
        ],
        "companies": {},
        "awaiting": False,
        "awaiting_msg": None
    }

    data["users"].setdefault(user_id, {"workspaces": []})
    if ws_id not in data["users"][user_id]["workspaces"]:
        data["users"][user_id]["workspaces"].append(ws_id)

    await save_data(data)

    await bot.send_message(
        message.chat.id,
        "📂 Меню workspace",
        message_thread_id=message.message_thread_id,
        reply_markup=workspace_keyboard(data["workspaces"][ws_id])
    )


# =========================
# CREATE COMPANY
# =========================

@dp.callback_query_handler(lambda c: c.data == "create")
async def create(callback: types.CallbackQuery):

    data = load_data()
    ws = get_ws(callback.message, data)

    if not ws:
        return await callback.answer("Сначала /connect", show_alert=True)

    if ws["awaiting"]:
        return await callback.answer("Уже ждём")

    msg = await callback.message.answer("✏️ Напиши название компании")

    ws["awaiting"] = True
    ws["awaiting_msg"] = msg.message_id

    await save_data(data)
    await callback.answer()


# =========================
# HANDLE NAME
# =========================

@dp.message_handler(lambda m: m.chat.type != "private")
async def handle_name(message: types.Message):

    data = load_data()
    ws = get_ws(message, data)

    if not ws or not ws["awaiting"] or not message.text:
        return

    name = message.text.strip()

    ws["awaiting"] = False

    tasks = [{"text": t, "done": False} for t in ws["template"]]
    ws["companies"][name] = {"tasks": tasks}

    await save_data(data)

    # удалить сообщения
    try:
        await message.delete()
    except:
        pass

    try:
        await bot.delete_message(message.chat.id, ws["awaiting_msg"])
    except:
        pass

    # задачи
    kb = InlineKeyboardMarkup(row_width=1)
    text = f"📁 {name}\n\n"

    for i, t in enumerate(tasks):
        text += f"⬜ {t['text']}\n"
        kb.add(InlineKeyboardButton(f"⬜ {t['text']}", callback_data=f"task:{i}:{name}"))

    await bot.send_message(
        message.chat.id,
        text,
        message_thread_id=message.message_thread_id,
        reply_markup=kb
    )

    # обновить меню
    await bot.send_message(
        message.chat.id,
        "📂 Меню workspace",
        message_thread_id=message.message_thread_id,
        reply_markup=workspace_keyboard(ws)
    )


# =========================
# TASK
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("task:"))
async def task(callback: types.CallbackQuery):

    _, i, name = callback.data.split(":")
    i = int(i)

    data = load_data()
    ws = get_ws(callback.message, data)

    task = ws["companies"][name]["tasks"][i]
    task["done"] = not task["done"]

    await save_data(data)

    kb = InlineKeyboardMarkup(row_width=1)
    text = f"📁 {name}\n\n"

    for idx, t in enumerate(ws["companies"][name]["tasks"]):
        icon = "✔" if t["done"] else "⬜"
        text += f"{icon} {t['text']}\n"
        kb.add(InlineKeyboardButton(f"{icon} {t['text']}", callback_data=f"task:{idx}:{name}"))

    await safe_edit(callback.message, text, kb)
    await callback.answer()


# =========================
# DELETE WS
# =========================

@dp.callback_query_handler(lambda c: c.data == "delete_ws")
async def delete_ws(callback: types.CallbackQuery):

    data = load_data()
    user_id = str(callback.from_user.id)

    for ws_id in data["users"].get(user_id, {}).get("workspaces", []):
        data["workspaces"].pop(ws_id, None)

    data["users"][user_id]["workspaces"] = []

    await save_data(data)

    await safe_edit(callback.message, "❌ Workspace удалён")
    await callback.answer()


# =========================
# RUN
# =========================

if __name__ == "__main__":
    if not os.path.exists(DATA_FILE):
        with open(DATA_FILE, "w") as f:
            json.dump({"users": {}, "workspaces": {}}, f)

    executor.start_polling(dp, skip_updates=True)
