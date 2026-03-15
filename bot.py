import os
import json
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.exceptions import MessageNotModified

TOKEN = os.getenv("API_TOKEN")

bot = Bot(token=TOKEN)
dp = Dispatcher(bot)

DATA_FILE = "data.json"


# ----------------
# DATA
# ----------------

def load_data():
    if not os.path.exists(DATA_FILE):
        return {"users": {}}

    with open(DATA_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_data(data):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_user(user_id):
    data = load_data()

    if str(user_id) not in data["users"]:
        data["users"][str(user_id)] = {
            "workspaces": {}
        }
        save_data(data)

    return data["users"][str(user_id)]


def update_user(user_id, user):
    data = load_data()
    data["users"][str(user_id)] = user
    save_data(data)


# ----------------
# KEYBOARDS
# ----------------

def main_keyboard(user):

    kb = InlineKeyboardMarkup(row_width=1)

    for ws_id, ws in user["workspaces"].items():
        kb.add(
            InlineKeyboardButton(
                f"📂 {ws['name']}",
                callback_data=f"ws:{ws_id}"
            )
        )

    kb.add(
        InlineKeyboardButton(
            "➕ Подключить workspace",
            callback_data="connect_help"
        )
    )

    kb.add(
        InlineKeyboardButton(
            "🔄 Обновить",
            callback_data="refresh"
        )
    )

    return kb


def back_keyboard():

    kb = InlineKeyboardMarkup()

    kb.add(
        InlineKeyboardButton(
            "⬅ Назад",
            callback_data="panel"
        )
    )

    return kb


def workspace_actions(ws_id):

    kb = InlineKeyboardMarkup()

    kb.add(
        InlineKeyboardButton(
            "🗑 Удалить workspace",
            callback_data=f"delete:{ws_id}"
        )
    )

    kb.add(
        InlineKeyboardButton(
            "⬅ Назад",
            callback_data="panel"
        )
    )

    return kb


# ----------------
# TEXT
# ----------------

def workspace_text(user):

    text = "📂 Ваши workspace\n\n"

    if not user["workspaces"]:
        text += "Нет подключенных workspace"

    for ws in user["workspaces"].values():
        text += f"• {ws['name']}\n"

    return text


# ----------------
# START
# ----------------

@dp.message_handler(commands=["start"])
async def start(message: types.Message):

    user = get_user(message.from_user.id)

    await message.answer(
        workspace_text(user),
        reply_markup=main_keyboard(user)
    )


# ----------------
# CALLBACKS
# ----------------

@dp.callback_query_handler(lambda c: c.data == "panel")
async def panel(callback: types.CallbackQuery):

    user = get_user(callback.from_user.id)

    try:
        await callback.message.edit_text(
            workspace_text(user),
            reply_markup=main_keyboard(user)
        )
    except MessageNotModified:
        pass

    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "refresh")
async def refresh(callback: types.CallbackQuery):

    user = get_user(callback.from_user.id)

    try:
        await callback.message.edit_text(
            workspace_text(user),
            reply_markup=main_keyboard(user)
        )
    except MessageNotModified:
        pass

    await callback.answer("Обновлено")


@dp.callback_query_handler(lambda c: c.data == "connect_help")
async def connect_help(callback: types.CallbackQuery):

    text = (
        "📌 Как подключить workspace\n\n"
        "1️⃣ Добавьте бота в нужную конфу\n"
        "2️⃣ Откройте нужный topic\n"
        "3️⃣ Напишите там:\n\n"
        "/connect"
    )

    try:
        await callback.message.edit_text(
            text,
            reply_markup=back_keyboard()
        )
    except MessageNotModified:
        pass

    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("ws:"))
async def workspace_menu(callback: types.CallbackQuery):

    ws_id = callback.data.split(":")[1]

    try:
        await callback.message.edit_text(
            "Управление workspace",
            reply_markup=workspace_actions(ws_id)
        )
    except MessageNotModified:
        pass

    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("delete:"))
async def delete_workspace(callback: types.CallbackQuery):

    ws_id = callback.data.split(":")[1]

    user = get_user(callback.from_user.id)

    if ws_id in user["workspaces"]:

        ws_name = user["workspaces"][ws_id]["name"]

        del user["workspaces"][ws_id]

        update_user(callback.from_user.id, user)

        await bot.send_message(
            callback.from_user.id,
            f"Workspace {ws_name} удален"
        )

    try:
        await callback.message.edit_text(
            workspace_text(user),
            reply_markup=main_keyboard(user)
        )
    except MessageNotModified:
        pass

    await callback.answer()


# ----------------
# CONNECT
# ----------------

@dp.message_handler(commands=["connect"])
async def connect(message: types.Message):

    if message.chat.type == "private":
        await message.reply("Эту команду нужно писать в группе")
        return

    user = get_user(message.from_user.id)

    chat_id = message.chat.id
    thread_id = message.message_thread_id or 0

    ws_id = f"{chat_id}_{thread_id}"

    if ws_id in user["workspaces"]:
        await message.reply("Workspace уже подключен")
        return

    user["workspaces"][ws_id] = {
        "name": message.chat.title
    }

    update_user(message.from_user.id, user)

    await message.reply(
        f"✅ Workspace {message.chat.title} подключен"
    )

    await bot.send_message(
        message.from_user.id,
        f"Workspace {message.chat.title} подключен"
    )


# ----------------
# RUN
# ----------------

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
