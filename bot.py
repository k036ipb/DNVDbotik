import json, os
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.exceptions import MessageNotModified

TOKEN = "YOUR_BOT_TOKEN_HERE"

bot = Bot(token=TOKEN)
dp = Dispatcher(bot)
DATA_FILE = "data.json"

# -------------------------
# Data functions
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
        data["users"][str(user_id)] = {"workspaces": {}}
        save_data(data)
    return data["users"][str(user_id)]

def update_user_data(user_id, user_data):
    data = load_data()
    data["users"][str(user_id)] = user_data
    save_data(data)

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
# Keyboards
# -------------------------
def company_keyboard(workspace):
    kb = InlineKeyboardMarkup()
    for cname in workspace.get("companies", {}):
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
# Duplicate functions
# -------------------------
async def update_duplicates(company_data, company_name):
    for dup in company_data.get("duplicates", []):
        try:
            text = company_text(company_name, company_data)
            await bot.edit_message_text(
                text=text,
                chat_id=dup["chat_id"],
                message_id=dup["message_id"]
            )
        except Exception:
            pass

async def add_duplicate_chat(company_data, company_name, duplicate_chat_id):
    msg = await bot.send_message(
        chat_id=duplicate_chat_id,
        text=company_text(company_name, company_data)
    )
    company_data["duplicates"].append({
        "chat_id": duplicate_chat_id,
        "message_id": msg.message_id
    })

# -------------------------
# Commands
# -------------------------
@dp.message_handler(commands=["start"])
async def start(message: types.Message):
    user_data = get_user_data(message.from_user.id)
    if not user_data.get("workspaces"):
        user_data["workspaces"]["main"] = {
            "chat_id": message.chat.id,
            "thread_id": None,
            "mode": "main",
            "template": ["Создать договор","Выставить счет","Подготовить мебель"],
            "companies": {}
        }
        update_user_data(message.from_user.id, user_data)
@dp.message_handler(commands=["start"])
async def start(message: types.Message):
    user_data = get_user_data(message.from_user.id)
    
    # Если нет workspace, создаём main
    if not user_data.get("workspaces"):
        user_data["workspaces"]["main"] = {
            "chat_id": message.chat.id,
            "thread_id": None,
            "mode": "main",
            "template": ["Создать договор","Выставить счет","Подготовить мебель"],
            "companies": {}
        }
        update_user_data(message.from_user.id, user_data)
    
    workspace = list(user_data["workspaces"].values())[0]
    
    if not workspace.get("companies"):  # Если компаний пока нет
        await message.reply(
            "У вас пока нет компаний. Добавьте первую через кнопку ➕ Добавить компанию.",
            reply_markup=company_keyboard(workspace)
        )
    else:
        await message.reply("Выберите компанию:", reply_markup=company_keyboard(workspace))

@dp.message_handler(commands=["thread"])
async def show_thread(message: types.Message):
    user_data = get_user_data(message.from_user.id)
    text = "Ваши рабочие пространства:\n"
    for ws_id, ws in user_data.get("workspaces", {}).items():
        text += f"Chat ID: {ws_id}\nThread ID: {ws.get('thread_id')}\nКомпании: {list(ws.get('companies', {}).keys())}\n\n"
    await message.reply(text or "Нет рабочих пространств.")

# -------------------------
# Text handler
# -------------------------
@dp.message_handler(lambda m: True)
async def text_handler(message: types.Message):
    user_data = get_user_data(message.from_user.id)
    workspace = list(user_data["workspaces"].values())[0]

    # Add company
    if workspace.get("adding_company"):
        cname = message.text.strip()
        if cname in workspace["companies"]:
            await message.reply("Такая компания уже есть.")
        else:
            workspace["companies"][cname] = {
                "tasks": [{"text": t,"done": False} for t in workspace.get("template", [])],
                "message_id": None,
                "duplicates": [],
                "selected_task": None
            }
            update_user_data(message.from_user.id, user_data)
            kb = task_keyboard(workspace["companies"][cname])
            msg = await message.reply(company_text(cname, workspace["companies"][cname]), reply_markup=kb)
            workspace["companies"][cname]["message_id"] = msg.message_id
            update_user_data(message.from_user.id, user_data)
        workspace.pop("adding_company")
        return

    # Add task
    if workspace.get("adding_task"):
        cname = workspace["adding_task"]
        tname = message.text.strip()
        workspace["companies"][cname]["tasks"].append({"text": tname,"done": False})
        update_user_data(message.from_user.id, user_data)
        kb = task_keyboard(workspace["companies"][cname])
        try:
            await bot.edit_message_text(company_text(cname, workspace["companies"][cname]),
                                        chat_id=workspace["chat_id"],
                                        message_id=workspace["companies"][cname]["message_id"],
                                        reply_markup=kb)
        except MessageNotModified:
            pass
        await update_duplicates(workspace["companies"][cname], cname)
        workspace.pop("adding_task")
        return

    # Add template task
    if workspace.get("adding_template_task"):
        tname = message.text.strip()
        workspace["template"].append(tname)
        update_user_data(message.from_user.id, user_data)
        await message.reply(f"Задача '{tname}' добавлена в шаблон.", reply_markup=template_keyboard(workspace["template"]))
        workspace.pop("adding_template_task")
        return

# -------------------------
# Callback handler
# -------------------------
@dp.callback_query_handler(lambda c: c.data)
async def callback_handler(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    user_data = get_user_data(user_id)
    workspace = list(user_data["workspaces"].values())[0]
    data = callback_query.data

    if data == "back_to_companies":
        await callback_query.message.edit_text("Выберите компанию:", reply_markup=company_keyboard(workspace))
        return
    if data == "add_company":
        workspace["adding_company"] = True
        update_user_data(user_id, user_data)
        await callback_query.message.answer("Введите название новой компании:")
        return
    if data.startswith("company:"):
        cname = data.split(":",1)[1]
        workspace["current_company"] = cname
        update_user_data(user_id, user_data)
        kb = task_keyboard(workspace["companies"][cname])
        await callback_query.message.edit_text(company_text(cname, workspace["companies"][cname]), reply_markup=kb)
        return
    if data == "add_task":
        cname = workspace.get("current_company")
        if cname:
            workspace["adding_task"] = cname
            update_user_data(user_id, user_data)
            await callback_query.message.answer("Введите название задачи:")
        return
    if data.startswith("task:"):
        idx = int(data.split(":")[1])
        cname = workspace.get("current_company")
        if cname:
            workspace["companies"][cname]["selected_task"] = idx
            update_user_data(user_id, user_data)
            await callback_query.message.edit_text(f"Задача: {workspace['companies'][cname]['tasks'][idx]['text']}", reply_markup=single_task_keyboard())
        return
    if data == "mark_done":
        cname = workspace.get("current_company")
        idx = workspace["companies"][cname]["selected_task"]
        workspace["companies"][cname]["tasks"][idx]["done"] = True
        update_user_data(user_id, user_data)
        kb = task_keyboard(workspace["companies"][cname])
        await callback_query.message.edit_text(company_text(cname, workspace["companies"][cname]), reply_markup=kb)
        await update_duplicates(workspace["companies"][cname], cname)
        return
    if data == "unmark_done":
        cname = workspace.get("current_company")
        idx = workspace["companies"][cname]["selected_task"]
        workspace["companies"][cname]["tasks"][idx]["done"] = False
        update_user_data(user_id, user_data)
        kb = task_keyboard(workspace["companies"][cname])
        await callback_query.message.edit_text(company_text(cname, workspace["companies"][cname]), reply_markup=kb)
        await update_duplicates(workspace["companies"][cname], cname)
        return
    if data == "delete_task":
        cname = workspace.get("current_company")
        idx = workspace["companies"][cname]["selected_task"]
        workspace["companies"][cname]["tasks"].pop(idx)
        workspace["companies"][cname]["selected_task"] = None
        update_user_data(user_id, user_data)
        kb = task_keyboard(workspace["companies"][cname])
        await callback_query.message.edit_text(company_text(cname, workspace["companies"][cname]), reply_markup=kb)
        await update_duplicates(workspace["companies"][cname], cname)
        return
    if data == "back_to_company":
        cname = workspace.get("current_company")
        kb = task_keyboard(workspace["companies"][cname])
        await callback_query.message.edit_text(company_text(cname, workspace["companies"][cname]), reply_markup=kb)
        return
    if data == "delete_company":
        cname = workspace.get("current_company")
        del workspace["companies"][cname]
        workspace["current_company"] = None
        update_user_data(user_id, user_data)
        await callback_query.message.edit_text("Компания удалена.", reply_markup=company_keyboard(workspace))
        return
    if data == "edit_template":
        await callback_query.message.edit_text("Редактор шаблона:", reply_markup=template_keyboard(workspace["template"]))
        return
    if data == "add_template_task":
        workspace["adding_template_task"] = True
        update_user_data(user_id, user_data)
        await callback_query.message.answer("Введите название задачи для шаблона:")
        return
    if data == "add_duplicate":
        cname = workspace.get("current_company")
        if cname:
            workspace["awaiting_duplicate"] = cname
            update_user_data(user_id, user_data)
            await callback_query.message.answer("Перешлите сообщение из чата, куда хотите дублировать список (forward или отправьте chat_id).")
        return

# -------------------------
# Handle forwarded message for duplicates
# -------------------------
@dp.message_handler(lambda m: m.forward_from_chat or hasattr(m, "chat"))
async def duplicate_chat_handler(message: types.Message):
    user_data = get_user_data(message.from_user.id)
    workspace = list(user_data["workspaces"].values())[0]
    if "awaiting_duplicate" in workspace:
        cname = workspace["awaiting_duplicate"]
        duplicate_chat_id = message.forward_from_chat.id if message.forward_from_chat else message.chat.id
        company_data = workspace["companies"][cname]
        await add_duplicate_chat(company_data, cname, duplicate_chat_id)
        await message.reply(f"Дублирующий чат добавлен для компании {cname}.")
        workspace.pop("awaiting_duplicate")
        update_user_data(message.from_user.id, user_data)

# -------------------------
# Run
# -------------------------
if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
