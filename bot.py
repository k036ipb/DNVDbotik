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

    if "users" not in data:
        data["users"] = {}
    if "workspaces" not in data:
        data["workspaces"] = {}
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
            InlineKeyboardButton(f"📂 {ws['name']}", callback_data=f"ws:{ws_id}")
        )
    kb.add(InlineKeyboardButton("➕ Подключить workspace", callback_data="connect_help"))
    kb.add(InlineKeyboardButton("🔄 Обновить", callback_data="panel"))
    return kb


def workspace_actions_keyboard(ws_id):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("➕ Создать компанию", callback_data=f"company_create:{ws_id}")
    )
    return kb


def back_keyboard(callback_data="panel"):
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("◀ Назад", callback_data=callback_data))
    return kb


# =========================
# TEXT
# =========================
def workspace_text(user_id, data):
    text = "📂 Ваши workspace\n\n"
    ws_list = data["users"][user_id]["workspaces"]
    if not ws_list:
        text += "Нет подключенных workspace"
        return text
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
# PANEL REFRESH
# =========================
@dp.callback_query_handler(lambda c: c.data == "panel")
async def panel(callback: types.CallbackQuery):
    data = load_data()
    user_id = str(callback.from_user.id)
    if user_id not in data["users"]:
        return
    await safe_edit(callback.message, workspace_text(user_id, data), main_keyboard(user_id, data))
    await callback.answer()


# =========================
# CONNECT HELP
# =========================
@dp.callback_query_handler(lambda c: c.data == "connect_help")
async def connect_help(callback: types.CallbackQuery):
    text = (
        "📌 Как подключить workspace\n\n"
        "1️⃣ Добавьте бота в группу\n"
        "2️⃣ Откройте нужный topic\n"
        "3️⃣ Нажмите кнопку подключения workspace"
    )
    await safe_edit(callback.message, text, back_keyboard("panel"))
    await callback.answer()


# =========================
# CONNECT WORKSPACE
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
            "menu_message_id": None,
            "template": ["Создать договор", "Выставить счет", "Подготовить мебель"],
            "companies": {},
            "awaiting_company_name": False
        }

    if user_id not in data["users"]:
        data["users"][user_id] = {"workspaces": []}
    if ws_id not in data["users"][user_id]["workspaces"]:
        data["users"][user_id]["workspaces"].append(ws_id)

    await save_data(data)

    await message.reply(f"✅ Workspace {message.chat.title} подключен")

    # Создаём меню workspace в треде
    menu = await bot.send_message(
        chat_id=chat_id,
        message_thread_id=thread_id,
        text="📂 Меню workspace",
        reply_markup=workspace_actions_keyboard(ws_id)
    )
    data = load_data()
    data["workspaces"][ws_id]["menu_message_id"] = menu.message_id
    await save_data(data)

    # Сообщение о правах
    await bot.send_message(
        chat_id=chat_id,
        message_thread_id=thread_id,
        text="ℹ️ Дай боту админку, чтобы я мог пылесосить тред от мусора"
    )

    # уведомление пользователю в личку
    await bot.send_message(message.from_user.id, f"Workspace {message.chat.title} подключен")


# =========================
# CREATE COMPANY
# =========================
@dp.callback_query_handler(lambda c: c.data.startswith("company_create:"))
async def create_company_menu(callback: types.CallbackQuery):
    ws_id = callback.data.split(":")[1]
    data = load_data()
    ws = data["workspaces"].get(ws_id)
    if not ws:
        await callback.answer("Workspace не найден")
        return
    ws["awaiting_company_name"] = True
    await save_data(data)
    await callback.answer("📌 Напишите название новой компании в этом треде")


@dp.message_handler(lambda m: m.chat.type != "private")
async def handle_new_company_name(message: types.Message):
    chat_id = message.chat.id
    thread_id = message.message_thread_id or 0
    ws_id = f"{chat_id}_{thread_id}"
    data = load_data()
    ws = data["workspaces"].get(ws_id)
    if not ws or not ws.get("awaiting_company_name", False):
        return

    name = message.text.strip()
    if not name:
        return

    # Создаём задачи
    tasks = [{"text": t, "done": False} for t in ws["template"]]
    ws["companies"][name] = {"tasks": tasks}
    ws["awaiting_company_name"] = False
    await save_data(data)

    # Удаляем сообщение пользователя, если есть права
    try:
        await message.delete()
    except TelegramAPIError:
        pass  # просто игнорируем, если нельзя удалить

    # Формируем сообщение с задачами
    kb = InlineKeyboardMarkup(row_width=1)
    text = f"📁 Клиент: {name}\n\nЗадачи:\n"
    for i, task in enumerate(tasks):
        text += f"⬜ {task['text']}\n"
        kb.add(
            InlineKeyboardButton(f"⬜ {task['text']}", callback_data=f"task:{ws_id}:{name}:{i}")
        )
    await bot.send_message(chat_id=chat_id, message_thread_id=thread_id, text=text, reply_markup=kb)


# =========================
# TOGGLE TASK
# =========================
@dp.callback_query_handler(lambda c: c.data.startswith("task:"))
async def toggle_task(callback: types.CallbackQuery):
    _, ws_id, company, task_index = callback.data.split(":")
    task_index = int(task_index)
    data = load_data()
    ws = data["workspaces"].get(ws_id)
    if not ws:
        return

    task = ws["companies"][company]["tasks"][task_index]
    task["done"] = not task["done"]
    await save_data(data)

    kb = InlineKeyboardMarkup(row_width=1)
    text = f"📁 Клиент: {company}\n\nЗадачи:\n"
    for i, t in enumerate(ws["companies"][company]["tasks"]):
        icon = "✔" if t["done"] else "⬜"
        text += f"{icon} {t['text']}\n"
        kb.add(
            InlineKeyboardButton(f"{icon} {t['text']}", callback_data=f"task:{ws_id}:{company}:{i}")
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
    executor.start_polling(dp, skip_updates=True)
