import json
import os

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

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
# KEYBOARD
# ----------------

def private_keyboard():

    kb = ReplyKeyboardMarkup(resize_keyboard=True)

    kb.add(KeyboardButton("➕ Подключить workspace"))
    kb.add(KeyboardButton("🔄 Обновить"))

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

        text += f"{i+1} {mark} {task['text']}\n"

    return text


# ----------------
# START
# ----------------

@dp.message_handler(commands=["start"])
async def start(message: types.Message):

    user = get_user(message.from_user.id)

    await message.answer(
        workspace_text(user),
        reply_markup=private_keyboard()
    )


# ----------------
# CONNECT
# ----------------

@dp.message_handler(commands=["connect"])
async def connect(message: types.Message):

    if message.chat.type == "private":

        await message.reply("Эту команду нужно писать в группе.")
        return

    user = get_user(message.from_user.id)

    chat_id = message.chat.id
    thread_id = message.message_thread_id or 0

    ws_id = f"{chat_id}_{thread_id}"

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

    await message.reply("✅ Workspace подключен")

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
        await message.reply("Напиши /company НАЗВАНИЕ")
        return

    data = load_data()

    for user in data["users"].values():

        for ws in user["workspaces"].values():

            if ws["chat_id"] == message.chat.id and ws["thread_id"] == message.message_thread_id:

                company = {

                    "tasks": [
                        {"text": t, "done": False}
                        for t in ws["template"]
                    ],

                    "message_id": None
                }

                ws["companies"][name] = company

                msg = await message.answer(
                    company_text(name, company)
                )

                company["message_id"] = msg.message_id

                save_data(data)

                return


# ----------------
# TASK
# ----------------

@dp.message_handler(commands=["task"])
async def task(message: types.Message):

    n = message.get_args()

    if not n:
        return

    n = int(n) - 1

    data = load_data()

    for user in data["users"].values():

        for ws in user["workspaces"].values():

            if ws["chat_id"] == message.chat.id and ws["thread_id"] == message.message_thread_id:

                for cname, company in ws["companies"].items():

                    if company["message_id"] == message.reply_to_message.message_id:

                        company["tasks"][n]["done"] = not company["tasks"][n]["done"]

                        await bot.edit_message_text(
                            company_text(cname, company),
                            chat_id=message.chat.id,
                            message_id=company["message_id"]
                        )

                        save_data(data)

                        return


# ----------------
# RUN
# ----------------

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
