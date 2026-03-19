import os
import json
import asyncio

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.exceptions import TelegramAPIError

TOKEN = os.getenv("API_TOKEN")

bot = Bot(token=TOKEN)
dp = Dispatcher(bot)

DATA_FILE = "data.json"
lock = asyncio.Lock()


# =========================
# DB
# =========================

def load():
    if not os.path.exists(DATA_FILE):
        return {"users": {}, "workspaces": {}}
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {"users": {}, "workspaces": {}}


async def save(data):
    async with lock:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)


def ws_id(chat_id, thread_id):
    return f"{chat_id}_{thread_id or 0}"


# =========================
# MAIN MENU
# =========================

def main_kb(user_id, data):
    kb = InlineKeyboardMarkup(row_width=1)
    for wid in data["users"].get(user_id, {}).get("workspaces", []):
        ws = data["workspaces"].get(wid)
        if ws:
            kb.add(InlineKeyboardButton(ws["name"], callback_data=f"ws:{wid}"))
    kb.add(InlineKeyboardButton("➕ Подключить workspace", callback_data="help"))
    kb.add(InlineKeyboardButton("🔄 Обновить", callback_data="refresh"))
    return kb


def main_text(user_id, data):
    text = "📂 Ваши workspace\n\n"
    wss = data["users"].get(user_id, {}).get("workspaces", [])
    if not wss:
        return text + "Нет workspace"
    for wid in wss:
        ws = data["workspaces"].get(wid)
        if ws:
            text += f"• {ws['name']}\n"
    return text


# =========================
# WORKSPACE KB
# =========================

def ws_kb(wid, ws):
    kb = InlineKeyboardMarkup(row_width=1)
    for c in ws["companies"]:
        kb.add(InlineKeyboardButton(c, callback_data=f"company:{wid}:{c}"))
    kb.add(InlineKeyboardButton("➕ Создать компанию", callback_data=f"create:{wid}"))
    kb.add(InlineKeyboardButton("⚙️ Шаблон задач", callback_data=f"template:{wid}"))
    return kb


def template_kb(wid, ws):
    kb = InlineKeyboardMarkup(row_width=1)
    for i, t in enumerate(ws["template"]):
        kb.add(InlineKeyboardButton(t, callback_data=f"t_open:{wid}:{i}"))
    kb.add(InlineKeyboardButton("➕ Добавить задачу", callback_data=f"t_add:{wid}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"back:{wid}"))
    return kb


def company_kb(wid, company_name):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"ws_back:{wid}"))
    return kb


# =========================
# START
# =========================

@dp.message_handler(commands=["start"])
async def start(m: types.Message):
    data = load()
    uid = str(m.from_user.id)
    data["users"].setdefault(uid, {"workspaces": []})
    await save(data)
    await m.answer(main_text(uid, data), reply_markup=main_kb(uid, data))


# =========================
# REFRESH
# =========================

@dp.callback_query_handler(lambda c: c.data == "refresh")
async def refresh(cb: types.CallbackQuery):
    data = load()
    uid = str(cb.from_user.id)
    await cb.message.edit_text(main_text(uid, data), reply_markup=main_kb(uid, data))
    await cb.answer()


# =========================
# HELP CONNECT
# =========================

@dp.callback_query_handler(lambda c: c.data == "help")
async def help_connect(cb: types.CallbackQuery):
    await cb.message.edit_text(
        "📌 Как подключить workspace:\n\n"
        "1. Перейди в тред группы\n"
        "2. Напиши команду:\n/connect\n"
        "После этого workspace появится в списке"
    )
    await cb.answer()


# =========================
# CONNECT
# =========================

@dp.message_handler(commands=["connect"])
async def connect(m: types.Message):
    if m.chat.type == "private":
        return

    data = load()
    uid = str(m.from_user.id)
    thread_id = m.message_thread_id or 0
    wid = ws_id(m.chat.id, thread_id)

    data["workspaces"][wid] = {
        "name": m.chat.title,
        "template": ["Создать договор", "Выставить счет"],
        "companies": {},
        "awaiting": None
    }

    data["users"].setdefault(uid, {"workspaces": []})
    if wid not in data["users"][uid]["workspaces"]:
        data["users"][uid]["workspaces"].append(wid)

    await save(data)

    # меню workspace в треде
    await bot.send_message(
        m.chat.id,
        "📂 Workspace подключен",
        message_thread_id=thread_id,
        reply_markup=ws_kb(wid, data["workspaces"][wid])
    )

    # сообщение в личку
    try:
        await bot.send_message(uid, f"Workspace {m.chat.title} подключен")
    except:
        pass


# =========================
# CREATE COMPANY
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("create:"))
async def create(cb: types.CallbackQuery):
    _, wid = cb.data.split(":")
    data = load()
    msg = await cb.message.answer("✏️ Напиши название компании")
    data["workspaces"][wid]["awaiting"] = {"type": "company", "msg": msg.message_id}
    await save(data)
    await cb.answer()


# =========================
# TEMPLATE MENU
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("template:"))
async def template(cb: types.CallbackQuery):
    _, wid = cb.data.split(":")
    data = load()
    ws = data["workspaces"][wid]
    await cb.message.edit_text("⚙️ Шаблон задач", reply_markup=template_kb(wid, ws))
    await cb.answer()


# =========================
# OPEN TEMPLATE TASK
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("t_open:"))
async def t_open(cb: types.CallbackQuery):
    _, wid, i = cb.data.split(":")
    i = int(i)
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🗑 Удалить", callback_data=f"t_del:{wid}:{i}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"template:{wid}"))
    await cb.message.edit_text("⚙️ Действие с задачей", reply_markup=kb)
    await cb.answer()


# =========================
# DELETE TEMPLATE TASK
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("t_del:"))
async def t_del(cb: types.CallbackQuery):
    _, wid, i = cb.data.split(":")
    i = int(i)
    data = load()
    ws = data["workspaces"][wid]
    ws["template"].pop(i)
    await save(data)
    await cb.message.edit_text("⚙️ Шаблон задач", reply_markup=template_kb(wid, ws))
    await cb.answer("Удалено")


# =========================
# ADD TEMPLATE TASK
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("t_add:"))
async def t_add(cb: types.CallbackQuery):
    _, wid = cb.data.split(":")
    data = load()
    msg = await cb.message.answer("Введите новую задачу")
    data["workspaces"][wid]["awaiting"] = {"type": "template", "msg": msg.message_id}
    await save(data)
    await cb.answer()


# =========================
# BACK
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("back:"))
async def back(cb: types.CallbackQuery):
    _, wid = cb.data.split(":")
    data = load()
    await cb.message.edit_text("📂 Workspace", reply_markup=ws_kb(wid, data["workspaces"][wid]))
    await cb.answer()


# =========================
# OPEN COMPANY
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("company:"))
async def open_company(cb: types.CallbackQuery):
    _, wid, cname = cb.data.split(":")
    data = load()
    ws = data["workspaces"][wid]
    if cname not in ws["companies"]:
        await cb.answer("Компания не найдена")
        return
    # формируем список задач с чекбоксами
    text = f"📁 {cname}\n\n"
    kb = InlineKeyboardMarkup(row_width=1)
    for i, t in enumerate(ws["companies"][cname]["tasks"]):
        icon = "✔" if t["done"] else "⬜"
        text += f"{icon} {t['text']}\n"
        kb.add(InlineKeyboardButton(f"{icon} {t['text']}", callback_data=f"task:{wid}:{cname}:{i}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"back:{wid}"))
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()


# =========================
# TOGGLE TASK
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("task:"))
async def toggle_task(cb: types.CallbackQuery):
    _, wid, cname, i = cb.data.split(":")
    i = int(i)
    data = load()
    ws = data["workspaces"][wid]
    task = ws["companies"][cname]["tasks"][i]
    task["done"] = not task["done"]
    await save(data)
    # обновляем список задач
    text = f"📁 {cname}\n\n"
    kb = InlineKeyboardMarkup(row_width=1)
    for j, t in enumerate(ws["companies"][cname]["tasks"]):
        icon = "✔" if t["done"] else "⬜"
        text += f"{icon} {t['text']}\n"
        kb.add(InlineKeyboardButton(f"{icon} {t['text']}", callback_data=f"task:{wid}:{cname}:{j}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"back:{wid}"))
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()


# =========================
# HANDLE INPUT
# =========================

@dp.message_handler(lambda m: m.chat.type != "private")
async def handle_input(m: types.Message):
    data = load()
    tid = m.message_thread_id or 0
    wid = ws_id(m.chat.id, tid)
    ws = data["workspaces"].get(wid)
    if not ws or not ws["awaiting"] or not m.text:
        return

    mode = ws["awaiting"]["type"]
    msg_id = ws["awaiting"]["msg"]
    text = m.text.strip()
    ws["awaiting"] = None

    if mode == "company":
        if text in ws["companies"]:
            return
        ws["companies"][text] = {"tasks": [{"text": t, "done": False} for t in ws["template"]]}
    elif mode == "template":
        ws["template"].append(text)

    await save(data)
    try:
        await m.delete()
    except:
        pass
    try:
        await bot.delete_message(m.chat.id, msg_id)
    except:
        pass
    await bot.send_message(m.chat.id, "📂 Workspace", message_thread_id=tid, reply_markup=ws_kb(wid, ws))


# =========================
# RUN
# =========================

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
