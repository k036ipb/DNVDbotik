import json
import os

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

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

    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {"users": {}}


def save_data(data):

    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_user(uid):

    data = load_data()

    if str(uid) not in data["users"]:

        data["users"][str(uid)] = {
            "workspaces": {}
        }

        save_data(data)

    return data["users"][str(uid)]


def update_user(uid, user):

    data = load_data()
    data["users"][str(uid)] = user
    save_data(data)


# ----------------
# UI
# ----------------

def panel_keyboard():

    kb = InlineKeyboardMarkup()

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


def tasks_keyboard(company):

    kb = InlineKeyboardMarkup()

    for i, task in enumerate(company["tasks"]):

        mark = "✔" if task["done"] else "⬜"

        kb.add(
            InlineKeyboardButton(
                f"{mark} {task['text']}",
                callback_data=f"task:{i}"
            )
        )

    return kb


# ----------------
# TEXT
# ----------------

def workspace_text(user):

    text = "📂 Ваши workspace\n\n"

    for ws in user["workspaces"].values():

        companies = len(ws["companies"])

        total_tasks = sum(len(c["tasks"]) for c in ws["companies"].values())

        done = sum(
            sum(1 for t in c["tasks"] if t["done"])
            for c in ws["companies"].values()
        )

        text += f"{ws['name']}\n"
        text += f"Компаний: {companies}\n"
        text += f"Задач: {total_tasks}\n"
        text += f"Выполнено: {done}\n\n"

    if not user["workspaces"]:
        text += "Нет workspace"

    return text


def company_text(name, company):

    text = f"📁 Клиент: {name}\n\n"

    for i, task in enumerate(company["tasks"]):

        mark = "✔" if task["done"] else "⬜"

        text += f"{i+1}. {mark} {task['text']}\n"

    return text


# ----------------
# START
# ----------------

@dp.message_handler(commands=["start"])
async def start(message: types.Message):

    user = get_user(message.from_user.id)

    await message.answer(
        workspace_text(user),
        reply_markup=panel_keyboard()
    )


# ----------------
# PANEL CALLBACKS
# ----------------

@dp.callback_query_handler(lambda c: c.data == "connect_help")
async def connect_help(callback: types.CallbackQuery):

    text = (
        "Чтобы подключить workspace:\n\n"
        "1️⃣ Открой нужный Telegram topic\n"
        "2️⃣ Напиши там команду:\n\n"
        "/connect"
    )

    await callback.message.edit_text(
        text,
        reply_markup=panel_keyboard()
    )

    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "refresh")
async def refresh(callback: types.CallbackQuery):

    user = get_user(callback.from_user.id)

    await callback.message.edit_text(
        workspace_text(user),
        reply_markup=panel_keyboard()
    )

    await callback.answer()


# ----------------
# CONNECT
# ----------------

@dp.message_handler(commands=["connect"])
async def connect(message: types.Message):

    if message.chat.type == "private":
        return

    user = get_user(message.from_user.id)

    chat_id = message.chat.id
    thread_id = message.message_thread_id or 0

    ws_id = f"{chat_id}_{thread_id}"

    if ws_id in user["workspaces"]:

        await message.reply("Workspace уже подключен")
        return

    user["workspaces"][ws_id] = {

        "name": message.chat.title,

        "chat_id": chat_id,
        "thread_id": thread_id,

        "template": [
            "Создать договор",
            "Выставить счет",
            "Подготовить мебель"
        ],

        "companies": {}
    }

    update_user(message.from_user.id, user)

    await message.reply("Workspace подключен")

    await bot.send_message(
        message.from_user.id,
        "Workspace подключен"
    )


# ----------------
# COMPANY
# ----------------

@dp.message_handler(commands=["company"])
async def company(message: types.Message):

    if message.chat.type == "private":
        return

    name = message.get_args()

    if not name:
        return

    data = load_data()

    chat_id = message.chat.id
    thread_id = message.message_thread_id or 0

    for user in data["users"].values():

        for ws in user["workspaces"].values():

            if ws["chat_id"] == chat_id and ws["thread_id"] == thread_id:

                company = {

                    "tasks": [
                        {"text": t, "done": False}
                        for t in ws["template"]
                    ],

                    "message_id": None
                }

                ws["companies"][name] = company

                msg = await message.answer(
                    company_text(name, company),
                    reply_markup=tasks_keyboard(company)
                )

                company["message_id"] = msg.message_id

                save_data(data)

                return


# ----------------
# TASK TOGGLE
# ----------------

@dp.callback_query_handler(lambda c: c.data.startswith("task:"))
async def toggle_task(callback: types.CallbackQuery):

    index = int(callback.data.split(":")[1])

    data = load_data()

    chat_id = callback.message.chat.id
    thread_id = callback.message.message_thread_id or 0

    for user in data["users"].values():

        for ws in user["workspaces"].values():

            if ws["chat_id"] == chat_id and ws["thread_id"] == thread_id:

                for cname, company in ws["companies"].items():

                    if company["message_id"] == callback.message.message_id:

                        company["tasks"][index]["done"] = not company["tasks"][index]["done"]

                        await callback.message.edit_text(
                            company_text(cname, company),
                            reply_markup=tasks_keyboard(company)
                        )

                        save_data(data)

                        await callback.answer()

                        return


# ----------------
# RUN
# ----------------

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
