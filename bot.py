import json, os
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

TOKEN = "YOUR_BOT_TOKEN_HERE"
bot = Bot(token=TOKEN)
dp = Dispatcher(bot)
DATA_FILE = "data.json"

# -------------------------
# Data helpers
# -------------------------
def load_data():
    if not os.path.exists(DATA_FILE):
        return {"users": {}}
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def save_data(data):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def get_user_data(user_id):
    data = load_data()
    if str(user_id) not in data["users"]:
        data["users"][str(user_id)] = {"workspaces": {}, "known_chats": []}
        save_data(data)
    return data["users"][str(user_id)]

def update_user_data(user_id, user_data):
    data = load_data()
    data["users"][str(user_id)] = user_data
    save_data(data)

# -------------------------
# Keyboards
# -------------------------
def chats_keyboard(user_data):
    kb = InlineKeyboardMarkup()
    if not user_data.get("known_chats"):
        return None
    for chat_id in user_data["known_chats"]:
        kb.add(InlineKeyboardButton(text=f"Чат {chat_id}", callback_data=f"select_chat:{chat_id}"))
    return kb

def company_keyboard(ws):
    kb = InlineKeyboardMarkup()
    for cname in ws.get("companies", {}):
        kb.add(InlineKeyboardButton(text=cname, callback_data=f"company:{cname}"))
    kb.add(InlineKeyboardButton(text="➕ Добавить компанию", callback_data="add_company"))
    kb.add(InlineKeyboardButton(text="⚙ Редактор шаблона", callback_data="edit_template"))
    return kb

def task_keyboard(company):
    kb = InlineKeyboardMarkup()
    for i, task in enumerate(company["tasks"]):
        check = "✔" if task["done"] else "⬜"
        kb.add(InlineKeyboardButton(text=f"{i+1} {check} {task['text']}", callback_data=f"task:{i}"))
    kb.add(InlineKeyboardButton(text="➕ Добавить задачу", callback_data="add_task"))
    kb.add(InlineKeyboardButton(text="🤖 Добавить дублирующий чат", callback_data="add_duplicate"))
    kb.add(InlineKeyboardButton(text="🔌 Отвязать чат", callback_data="remove_duplicate"))
    kb.add(InlineKeyboardButton(text="🗑 Удалить список", callback_data="delete_company"))
    kb.add(InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_companies"))
    return kb

def single_task_keyboard():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(text="✔ Выполнить", callback_data="mark_done"))
    kb.add(InlineKeyboardButton(text="🟡 Снять выполнение", callback_data="unmark_done"))
    kb.add(InlineKeyboardButton(text="✏ Переименовать", callback_data="rename_task"))
    kb.add(InlineKeyboardButton(text="❌ Удалить", callback_data="delete_task"))
    kb.add(InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_company"))
    return kb

def template_keyboard(template):
    kb = InlineKeyboardMarkup()
    for i, task in enumerate(template):
        kb.add(InlineKeyboardButton(text=f"{i+1} {task}", callback_data=f"template_task:{i}"))
    kb.add(InlineKeyboardButton(text="➕ Добавить", callback_data="add_template_task"))
    kb.add(InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_companies"))
    return kb

# -------------------------
# Text helpers
# -------------------------
def company_text(name, company):
    text = f"📁 Клиент: {name}\n"
    for i, task in enumerate(company["tasks"]):
        check = "✔" if task["done"] else "⬜"
        text += f"{i+1} {check} {task['text']}\n"
    return text

# -------------------------
# Duplicate functions
# -------------------------
async def update_duplicates(company_data, company_name):
    for dup in company_data.get("duplicates", []):
        try:
            text = company_text(company_name, company_data)
            await bot.edit_message_text(text=text, chat_id=dup["chat_id"], message_id=dup["message_id"])
        except Exception:
            pass

async def add_duplicate_chat(company_data, company_name, duplicate_chat_id):
    msg = await bot.send_message(chat_id=duplicate_chat_id, text=company_text(company_name, company_data))
    company_data["duplicates"].append({"chat_id": duplicate_chat_id, "message_id": msg.message_id})

# -------------------------
# /start handler
# -------------------------
@dp.message_handler(commands=["start"])
async def start(message: types.Message):
    user_data = get_user_data(message.from_user.id)
    if not user_data.get("known_chats"):
        await message.reply("Привет! Добавьте бота хотя бы в один чат и перешлите сообщение из него.")
        return
    kb = chats_keyboard(user_data)
    await message.reply("Выберите чат для привязки треда:", reply_markup=kb)

@dp.callback_query_handler(lambda c: True)
async def callbacks(cq: types.CallbackQuery):
    user_data = get_user_data(cq.from_user.id)
    ws = next(iter(user_data["workspaces"].values()), None)
    if not ws:
        await cq.answer("Нет workspace.")
        return
    data = cq.data

    # ---------- Companies ----------
    if data == "add_company":
        await cq.message.reply("Отправьте название новой компании:")
        user_data["awaiting_company_name"] = True
        update_user_data(cq.from_user.id, user_data)
        return
    if data.startswith("company:"):
        cname = data.split(":")[1]
        company = ws["companies"].get(cname)
        if company:
            await cq.message.edit_text(company_text(cname, company), reply_markup=task_keyboard(company))
        return
    if data == "back_to_companies":
        await cq.message.edit_text("Компании:", reply_markup=company_keyboard(ws))
        return
    if data == "edit_template":
        await cq.message.edit_text("Шаблон задач:", reply_markup=template_keyboard(ws["template"]))
        return

    # ---------- Tasks ----------
    if data.startswith("task:"):
        idx = int(data.split(":")[1])
        cname = next(iter(ws["companies"]))
        company = ws["companies"][cname]
        company["current_task"] = idx
        update_user_data(cq.from_user.id, user_data)
        await cq.message.edit_text(f"Задача: {company['tasks'][idx]['text']}", reply_markup=single_task_keyboard())
        return
    if data == "mark_done":
        cname = next(iter(ws["companies"]))
        company = ws["companies"][cname]
        idx = company.get("current_task", 0)
        company["tasks"][idx]["done"] = True
        update_user_data(cq.from_user.id, user_data)
        await cq.message.edit_text(company_text(cname, company), reply_markup=task_keyboard(company))
        await update_duplicates(company, cname)
        return
    if data == "unmark_done":
        cname = next(iter(ws["companies"]))
        company = ws["companies"][cname]
        idx = company.get("current_task", 0)
        company["tasks"][idx]["done"] = False
        update_user_data(cq.from_user.id, user_data)
        await cq.message.edit_text(company_text(cname, company), reply_markup=task_keyboard(company))
        await update_duplicates(company, cname)
        return
    if data == "back_to_company":
        cname = next(iter(ws["companies"]))
        company = ws["companies"][cname]
        await cq.message.edit_text(company_text(cname, company), reply_markup=task_keyboard(company))
        return
    if data == "rename_task":
        cname = next(iter(ws["companies"]))
        company = ws["companies"][cname]
        idx = company.get("current_task", 0)
        user_data["awaiting_task_rename"] = idx
        update_user_data(cq.from_user.id, user_data)
        await cq.message.reply(f"Отправьте новое название для задачи: {company['tasks'][idx]['text']}")
        return
    if data == "delete_task":
        cname = next(iter(ws["companies"]))
        company = ws["companies"][cname]
        idx = company.get("current_task", 0)
        company["tasks"].pop(idx)
        update_user_data(cq.from_user.id, user_data)
        await cq.message.edit_text(company_text(cname, company), reply_markup=task_keyboard(company))
        await update_duplicates(company, cname)
        return
    if data.startswith("template_task:"):
        idx = int(data.split(":")[1])
        user_data["awaiting_template_rename"] = idx
        update_user_data(cq.from_user.id, user_data)
        await cq.message.reply(f"Отправьте новое название для шаблонной задачи: {ws['template'][idx]}")
        return
    if data == "add_template_task":
        user_data["awaiting_template_add"] = True
        update_user_data(cq.from_user.id, user_data)
        await cq.message.reply("Отправьте название новой задачи для шаблона")
        return

# -------------------------
# Text handler for all inputs
# -------------------------
@dp.message_handler(lambda m: True)
async def text_handler_full(message: types.Message):
    user_data = get_user_data(message.from_user.id)
    ws = next(iter(user_data["workspaces"].values()), None)
    if not ws:
        return

    # ----- Company name -----
    if user_data.get("awaiting_company_name"):
        cname = message.text.strip()
        ws["companies"][cname] = {"tasks":[{"text": t,"done":False} for t in ws["template"]],"message_id":None,"duplicates":[]}
        user_data.pop("awaiting_company_name")
        update_user_data(message.from_user.id, user_data)
        await message.reply(f"Компания '{cname}' создана!", reply_markup=company_keyboard(ws))
        return

    # ----- Task rename -----
    if user_data.get("awaiting_task_rename") is not None:
        idx = user_data.pop("awaiting_task_rename")
        cname = next(iter(ws["companies"]))
        company = ws["companies"][cname]
        company["tasks"][idx]["text"] = message.text.strip()
        update_user_data(message.from_user.id, user_data)
        await message.reply(f"Задача переименована!", reply_markup=task_keyboard(company))
        await update_duplicates(company, cname)
        return

    # ----- Template rename -----
    if user_data.get("awaiting_template_rename") is not None:
        idx = user_data.pop("awaiting_template_rename")
        ws["template"][idx] = message.text.strip()
        update_user_data(message.from_user.id, user_data)
        await message.reply("Шаблон обновлён", reply_markup=template_keyboard(ws["template"]))
        return

    # ----- Template add -----
    if user_data.get("awaiting_template_add"):
        user_data.pop("awaiting_template_add")
        ws["template"].append(message.text.strip())
        update_user_data(message.from_user.id, user_data)
        await message.reply("Задача добавлена в шаблон", reply_markup=template_keyboard(ws["template"]))
        return

# -------------------------
# Run
# -------------------------
if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
