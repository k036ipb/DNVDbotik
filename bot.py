import json
import os

from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils import executor

TOKEN = os.getenv("API_TOKEN")

bot = Bot(token=TOKEN)
dp = Dispatcher(bot)

DATA_FILE = "data.json"


# -------------------------
# DATA
# -------------------------

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
            "workspaces": {},
            "current_workspace": None,
            "current_company": None
        }

        save_data(data)

    return data["users"][str(user_id)]


def update_user(user_id, user):

    data = load_data()
    data["users"][str(user_id)] = user
    save_data(data)


# -------------------------
# TEXT
# -------------------------

def company_text(name, company):

    text = f"📁 Клиент: {name}\n\n"

    for i, task in enumerate(company["tasks"]):

        if task["done"]:
            text += f"{i+1} ✔ ~~{task['text']}~~\n"
        else:
            text += f"{i+1} ⬜ {task['text']}\n"

    return text


# -------------------------
# KEYBOARDS
# -------------------------

def workspace_keyboard(user):

    kb = InlineKeyboardMarkup()

    for ws_id, ws in user["workspaces"].items():

        kb.add(
            InlineKeyboardButton(
                text=f"▶ {ws['name']}",
                callback_data=f"ws:{ws_id}"
            )
        )

    kb.add(
        InlineKeyboardButton(
            text="➕ Добавить новую конфу",
            callback_data="ws_new"
        )
    )

    return kb


def companies_keyboard(ws):

    kb = InlineKeyboardMarkup()

    for cname in ws["companies"]:

        kb.add(
            InlineKeyboardButton(
                text=cname,
                callback_data=f"company:{cname}"
            )
        )

    kb.add(InlineKeyboardButton("➕ Добавить компанию", callback_data="company_add"))
    kb.add(InlineKeyboardButton("⚙ Редактор шаблона", callback_data="template"))

    return kb


def tasks_keyboard(company):

    kb = InlineKeyboardMarkup()

    for i, task in enumerate(company["tasks"]):

        mark = "✔" if task["done"] else "⬜"

        kb.add(
            InlineKeyboardButton(
                text=f"{i+1} {mark} {task['text']}",
                callback_data=f"task:{i}"
            )
        )

    kb.add(InlineKeyboardButton("➕ Добавить задачу", callback_data="task_add"))
    kb.add(InlineKeyboardButton("🤖 Добавить дубликат", callback_data="dup_add"))
    kb.add(InlineKeyboardButton("🗑 Удалить список", callback_data="company_delete"))
    kb.add(InlineKeyboardButton("🔙 Назад", callback_data="companies"))

    return kb


def template_keyboard(template):

    kb = InlineKeyboardMarkup()

    for i, t in enumerate(template):

        kb.add(
            InlineKeyboardButton(
                text=f"{i+1} {t}",
                callback_data=f"template_task:{i}"
            )
        )

    kb.add(InlineKeyboardButton("➕ Добавить", callback_data="template_add"))
    kb.add(InlineKeyboardButton("🔙 Назад", callback_data="companies"))

    return kb


# -------------------------
# RENDER COMPANY
# -------------------------

async def render_company(ws, cname):

    company = ws["companies"][cname]

    text = company_text(cname, company)

    try:

        await bot.edit_message_text(
            chat_id=ws["chat_id"],
            message_id=company["message_id"],
            text=text,
            reply_markup=tasks_keyboard(company)
        )

    except:
        pass

    for dup in company.get("duplicates", []):

        try:

            await bot.edit_message_text(
                chat_id=dup["chat_id"],
                message_id=dup["message_id"],
                text=text
            )

        except:
            pass


# -------------------------
# CREATE COMPANY
# -------------------------

async def create_company(ws, cname):

    company = {

        "tasks": [
            {"text": t, "done": False}
            for t in ws["template"]
        ],

        "duplicates": [],
        "message_id": None
    }

    ws["companies"][cname] = company

    msg = await bot.send_message(
        ws["chat_id"],
        company_text(cname, company),
        message_thread_id=ws["thread_id"],
        reply_markup=tasks_keyboard(company)
    )

    company["message_id"] = msg.message_id

# -------------------------
# /start
# -------------------------
@dp.message_handler(commands=["start"])
async def start(message: types.Message):
    user = get_user(message.from_user.id)

    # Если есть workspace, показываем выбор
    if user["workspaces"]:
        await message.reply(
            "Выберите рабочую конфигурацию:",
            reply_markup=workspace_keyboard(user)
        )
        return

    # Если workspace нет, предлагаем подключить тред
    user["await_forward"] = True
    update_user(message.from_user.id, user)

    await message.reply(
        "Добавим первую конфигурацию.\n"
        "Перешлите любое сообщение из нужной темы (треда) группы, "
        "или напишите команду /connect прямо в этой теме."
    )

# -------------------------
# CALLBACKS
# -------------------------

@dp.callback_query_handler(lambda c: True)
async def callbacks(cq: types.CallbackQuery):

    user = get_user(cq.from_user.id)
    data = cq.data


# WORKSPACE

    if data.startswith("ws:"):

        ws_id = data.split(":")[1]

        user["current_workspace"] = ws_id
        update_user(cq.from_user.id, user)

        ws = user["workspaces"][ws_id]

        await bot.send_message(
            ws["chat_id"],
            "Компании:",
            message_thread_id=ws["thread_id"],
            reply_markup=companies_keyboard(ws)
        )

        return


    if data == "ws_new":

        user["await_forward"] = True
        update_user(cq.from_user.id, user)

        await cq.message.edit_text("Перешлите сообщение из нужного треда.")
        return


# COMPANIES

    ws = user["workspaces"].get(user["current_workspace"])

    if not ws:
        return


    if data == "companies":

        await cq.message.edit_text(
            "Компании:",
            reply_markup=companies_keyboard(ws)
        )
        return


    if data == "company_add":

        user["await_company"] = True
        update_user(cq.from_user.id, user)

        await cq.message.reply("Введите название компании")
        return


    if data.startswith("company:"):

        cname = data.split(":")[1]

        user["current_company"] = cname
        update_user(cq.from_user.id, user)

        company = ws["companies"][cname]

        await cq.message.edit_text(
            company_text(cname, company),
            reply_markup=tasks_keyboard(company)
        )

        return


# TASKS

    cname = user.get("current_company")

    if not cname:
        return

    company = ws["companies"][cname]


    if data.startswith("task:"):

        idx = int(data.split(":")[1])

        company["tasks"][idx]["done"] = not company["tasks"][idx]["done"]

        update_user(cq.from_user.id, user)

        await render_company(ws, cname)

        return


    if data == "task_add":

        user["await_task"] = True
        update_user(cq.from_user.id, user)

        await cq.message.reply("Введите текст задачи")
        return


    if data == "dup_add":

        user["await_dup"] = True
        update_user(cq.from_user.id, user)

        await cq.message.reply("Отправьте chat_id дубликата")
        return

# -------------------------
# TEXT HANDLER
# -------------------------
@dp.message_handler(lambda m: True)
async def text_handler(message: types.Message):

    user = get_user(message.from_user.id)

    # -------------------------
    # CREATE WORKSPACE
    # -------------------------

    if user.get("await_forward"):

        # нельзя создавать workspace из лички
        if message.chat.type == "private":
            await message.reply("Отправьте сообщение из группы или темы (треда).")
            return

        chat_id = message.chat.id
        thread_id = message.message_thread_id or 0

        ws_id = f"{chat_id}_{thread_id}"

        user["workspaces"][ws_id] = {
            "name": message.chat.title or "Без названия",
            "chat_id": chat_id,
            "thread_id": thread_id,
            "template": [
                "Создать договор",
                "Выставить счет",
                "Подготовить мебель"
            ],
            "companies": {}
        }

        user["current_workspace"] = ws_id
        user.pop("await_forward")

        update_user(message.from_user.id, user)

        ws = user["workspaces"][ws_id]

        # отправляем меню компаний прямо в тред
        await bot.send_message(
            chat_id,
            "Компании:",
            message_thread_id=thread_id,
            reply_markup=companies_keyboard(ws)
        )

        await message.reply(
            f"Конфа '{ws['name']}' добавлена!"
        )

        return

    # -------------------------
    # ADD COMPANY
    # -------------------------

    if user.get("await_company"):

        cname = message.text.strip()

        ws = user["workspaces"][user["current_workspace"]]

        await create_company(ws, cname)

        user.pop("await_company")

        update_user(message.from_user.id, user)

        await message.reply("Компания создана")

        return

    # -------------------------
    # ADD TASK
    # -------------------------

    if user.get("await_task"):

        cname = user["current_company"]
        ws = user["workspaces"][user["current_workspace"]]

        company = ws["companies"][cname]

        company["tasks"].append({
            "text": message.text,
            "done": False
        })

        user.pop("await_task")

        update_user(message.from_user.id, user)

        await render_company(ws, cname)

        return

    # -------------------------
    # ADD DUPLICATE
    # -------------------------

    if user.get("await_dup"):

        cname = user["current_company"]
        ws = user["workspaces"][user["current_workspace"]]

        company = ws["companies"][cname]

        chat_id = int(message.text)

        msg = await bot.send_message(
            chat_id,
            company_text(cname, company)
        )

        company.setdefault("duplicates", []).append({
            "chat_id": chat_id,
            "message_id": msg.message_id
        })

        user.pop("await_dup")

        update_user(message.from_user.id, user)

        await message.reply("Дубликат создан")

        return
        
# -------------------------
# RUN
# -------------------------

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
