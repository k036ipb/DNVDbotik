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
    MessageCantBeEdited
)

logging.basicConfig(level=logging.INFO)

TOKEN = os.getenv("API_TOKEN")

bot = Bot(TOKEN)
dp = Dispatcher(bot)

DATA_FILE = "data.json"

db_lock = asyncio.Lock()


# =====================
# DATABASE
# =====================

def load_data():

    if not os.path.exists(DATA_FILE):
        return {"users": {}, "workspaces": {}}

    try:
        with open(DATA_FILE, "r", encoding="utf8") as f:
            return json.load(f)
    except:
        return {"users": {}, "workspaces": {}}


async def save_data(data):

    async with db_lock:

        with open(DATA_FILE, "w", encoding="utf8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)


# =====================
# SAFE EDIT
# =====================

async def safe_edit(message, text, kb):

    try:
        await message.edit_text(
            text,
            reply_markup=kb
        )

    except (
        MessageNotModified,
        MessageToEditNotFound,
        MessageCantBeEdited
    ):
        pass


# =====================
# KEYBOARDS
# =====================

def workspace_menu():

    kb = InlineKeyboardMarkup()

    kb.add(
        InlineKeyboardButton(
            "➕ Создать компанию",
            callback_data="company_create"
        )
    )

    kb.add(
        InlineKeyboardButton(
            "📋 Список компаний",
            callback_data="company_list"
        )
    )

    return kb


# =====================
# START
# =====================

@dp.message_handler(commands=["start"])
async def start(message: types.Message):

    data = load_data()

    user_id = str(message.from_user.id)

    if user_id not in data["users"]:
        data["users"][user_id] = {"workspaces": []}
        await save_data(data)

    text = "📂 Ваши workspace\n\n"

    kb = InlineKeyboardMarkup()

    for ws_id in data["users"][user_id]["workspaces"]:

        ws = data["workspaces"].get(ws_id)

        if not ws:
            continue

        text += f"• {ws['name']}\n"

        kb.add(
            InlineKeyboardButton(
                ws["name"],
                callback_data=f"ws:{ws_id}"
            )
        )

    kb.add(
        InlineKeyboardButton(
            "➕ Подключить workspace",
            callback_data="connect_help"
        )
    )

    await message.answer(text, reply_markup=kb)


# =====================
# CONNECT
# =====================

@dp.message_handler(commands=["connect"])
async def connect(message: types.Message):

    if message.chat.type == "private":
        return

    data = load_data()

    chat_id = message.chat.id
    thread_id = message.message_thread_id or 0

    ws_id = f"{chat_id}_{thread_id}"

    if ws_id not in data["workspaces"]:

        data["workspaces"][ws_id] = {

            "name": message.chat.title,
            "chat_id": chat_id,
            "thread_id": thread_id,
            "menu_message_id": None,

            "template": [
                "Создать договор",
                "Выставить счет",
                "Подготовить мебель"
            ],

            "companies": {}
        }

    user_id = str(message.from_user.id)

    if user_id not in data["users"]:
        data["users"][user_id] = {"workspaces": []}

    if ws_id not in data["users"][user_id]["workspaces"]:
        data["users"][user_id]["workspaces"].append(ws_id)

    await save_data(data)

    await message.reply("✅ Workspace подключен")

    menu = await bot.send_message(
        chat_id,
        "📂 Меню workspace",
        message_thread_id=thread_id,
        reply_markup=workspace_menu()
    )

    data = load_data()

    data["workspaces"][ws_id]["menu_message_id"] = menu.message_id

    await save_data(data)


# =====================
# COMPANY CREATE
# =====================

@dp.message_handler(lambda m: m.text and m.text.startswith("/company"))
async def company_create(message: types.Message):

    if message.chat.type == "private":
        return

    name = message.text.replace("/company", "").strip()

    if not name:
        return

    data = load_data()

    chat_id = message.chat.id
    thread_id = message.message_thread_id or 0

    ws_id = f"{chat_id}_{thread_id}"

    ws = data["workspaces"].get(ws_id)

    if not ws:
        return

    tasks = []

    for t in ws["template"]:

        tasks.append({
            "text": t,
            "done": False
        })

    ws["companies"][name] = {
        "tasks": tasks
    }

    await save_data(data)

    kb = InlineKeyboardMarkup()

    text = f"📁 {name}\n\n"

    for i, t in enumerate(tasks):

        text += f"⬜ {t['text']}\n"

        kb.add(
            InlineKeyboardButton(
                f"⬜ {t['text']}",
                callback_data=f"task:{ws_id}:{name}:{i}"
            )
        )

    await message.answer(text, reply_markup=kb)


# =====================
# TASK TOGGLE
# =====================

@dp.callback_query_handler(lambda c: c.data.startswith("task:"))
async def toggle(callback: types.CallbackQuery):

    _, ws_id, company, i = callback.data.split(":")
    i = int(i)

    data = load_data()

    ws = data["workspaces"].get(ws_id)

    if not ws:
        return

    task = ws["companies"][company]["tasks"][i]

    task["done"] = not task["done"]

    await save_data(data)

    kb = InlineKeyboardMarkup()

    text = f"📁 {company}\n\n"

    for index, t in enumerate(ws["companies"][company]["tasks"]):

        icon = "✔" if t["done"] else "⬜"

        text += f"{icon} {t['text']}\n"

        kb.add(
            InlineKeyboardButton(
                f"{icon} {t['text']}",
                callback_data=f"task:{ws_id}:{company}:{index}"
            )
        )

    await safe_edit(callback.message, text, kb)

    await callback.answer()


# =====================
# BOT REMOVED
# =====================

@dp.my_chat_member_handler()
async def removed(event: types.ChatMemberUpdated):

    if event.new_chat_member.status == "kicked":

        data = load_data()

        chat_id = event.chat.id

        for ws_id in list(data["workspaces"].keys()):

            if str(chat_id) in ws_id:
                del data["workspaces"][ws_id]

        await save_data(data)


# =====================
# RUN
# =====================

if __name__ == "__main__":

    executor.start_polling(
        dp,
        skip_updates=True
    )
