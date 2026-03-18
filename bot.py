import os
import json
import asyncio

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

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
    return f"{chat_id}_{thread_id}"


# =========================
# KEYBOARDS
# =========================

def ws_kb(ws_id, ws):
    kb = InlineKeyboardMarkup(row_width=1)

    for c in ws["companies"]:
        kb.add(InlineKeyboardButton(c, callback_data=f"company:{ws_id}:{c}"))

    kb.add(InlineKeyboardButton("➕ Создать компанию", callback_data=f"create:{ws_id}"))
    kb.add(InlineKeyboardButton("⚙️ Шаблон задач", callback_data=f"template:{ws_id}"))

    return kb


def template_kb(ws_id, ws):
    kb = InlineKeyboardMarkup(row_width=1)

    for i, t in enumerate(ws["template"]):
        kb.add(InlineKeyboardButton(t, callback_data=f"t_edit:{ws_id}:{i}"))

    kb.add(InlineKeyboardButton("➕ Добавить задачу", callback_data=f"t_add:{ws_id}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"back:{ws_id}"))

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

    await m.answer("Готов")


# =========================
# CONNECT
# =========================

@dp.message_handler(commands=["connect"])
async def connect(m: types.Message):

    if m.chat.type == "private":
        return

    data = load()
    uid = str(m.from_user.id)

    wid = ws_id(m.chat.id, m.message_thread_id)

    data["workspaces"][wid] = {
        "name": m.chat.title,
        "template": ["Задача 1", "Задача 2"],
        "companies": {},
        "awaiting": None
    }

    data["users"].setdefault(uid, {"workspaces": []})

    if wid not in data["users"][uid]["workspaces"]:
        data["users"][uid]["workspaces"].append(wid)

    await save(data)

    await bot.send_message(
        m.chat.id,
        "📂 Workspace",
        message_thread_id=m.message_thread_id,
        reply_markup=ws_kb(wid, data["workspaces"][wid])
    )


# =========================
# CREATE COMPANY
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("create:"))
async def create(cb: types.CallbackQuery):

    _, wid = cb.data.split(":")
    data = load()
    ws = data["workspaces"][wid]

    if ws["awaiting"]:
        return await cb.answer("Уже ждём")

    msg = await cb.message.answer("Напиши название компании")

    ws["awaiting"] = {
        "type": "company",
        "msg_id": msg.message_id
    }

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

    await cb.message.edit_text(
        "⚙️ Шаблон задач",
        reply_markup=template_kb(wid, ws)
    )

    await cb.answer()


# =========================
# ADD TEMPLATE TASK
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("t_add:"))
async def t_add(cb: types.CallbackQuery):

    _, wid = cb.data.split(":")
    data = load()

    msg = await cb.message.answer("Введите задачу")

    data["workspaces"][wid]["awaiting"] = {
        "type": "template",
        "msg_id": msg.message_id
    }

    await save(data)
    await cb.answer()


# =========================
# DELETE TEMPLATE TASK
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("t_edit:"))
async def t_edit(cb: types.CallbackQuery):

    _, wid, i = cb.data.split(":")
    i = int(i)

    data = load()
    ws = data["workspaces"][wid]

    ws["template"].pop(i)

    await save(data)

    await cb.message.edit_text(
        "⚙️ Шаблон задач",
        reply_markup=template_kb(wid, ws)
    )

    await cb.answer("Удалено")


# =========================
# BACK
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("back:"))
async def back(cb: types.CallbackQuery):

    _, wid = cb.data.split(":")
    data = load()

    await cb.message.edit_text(
        "📂 Workspace",
        reply_markup=ws_kb(wid, data["workspaces"][wid])
    )

    await cb.answer()


# =========================
# HANDLE INPUT
# =========================

@dp.message_handler(lambda m: m.chat.type != "private")
async def handle(m: types.Message):

    data = load()
    wid = ws_id(m.chat.id, m.message_thread_id)

    ws = data["workspaces"].get(wid)
    if not ws or not ws["awaiting"]:
        return

    text = m.text.strip()

    mode = ws["awaiting"]["type"]
    msg_id = ws["awaiting"]["msg_id"]

    ws["awaiting"] = None

    if mode == "company":
        tasks = [{"text": t, "done": False} for t in ws["template"]]
        ws["companies"][text] = {"tasks": tasks}

    elif mode == "template":
        ws["template"].append(text)

    await save(data)

    # удаление
    try:
        await m.delete()
    except:
        pass

    try:
        await bot.delete_message(m.chat.id, msg_id)
    except:
        pass

    # обновление меню
    await bot.send_message(
        m.chat.id,
        "📂 Workspace",
        message_thread_id=m.message_thread_id,
        reply_markup=ws_kb(wid, ws)
    )


# =========================
# RUN
# =========================

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
