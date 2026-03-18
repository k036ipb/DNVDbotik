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
    return f"{chat_id}_{thread_id}"


# =========================
# MAIN MENU (ЛС)
# =========================

def main_kb(user_id, data):
    kb = InlineKeyboardMarkup(row_width=1)

    for wid in data["users"].get(user_id, {}).get("workspaces", []):
        ws = data["workspaces"].get(wid)
        if ws:
            kb.add(InlineKeyboardButton(ws["name"], callback_data=f"ws:{wid}"))

    kb.add(InlineKeyboardButton("➕ Подключить workspace", callback_data="help"))

    return kb


def main_text(user_id, data):
    text = "📂 Ваши workspace\n\n"

    wss = data["users"].get(user_id, {}).get("workspaces", [])

    if not wss:
        return text + "Нет подключенных workspace"

    for wid in wss:
        ws = data["workspaces"].get(wid)
        if ws:
            text += f"• {ws['name']}\n"

    return text


# =========================
# WORKSPACE KEYBOARDS
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
        kb.add(InlineKeyboardButton(t, callback_data=f"t_del:{wid}:{i}"))

    kb.add(InlineKeyboardButton("➕ Добавить задачу", callback_data=f"t_add:{wid}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"back:{wid}"))

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

    await m.answer(
        main_text(uid, data),
        reply_markup=main_kb(uid, data)
    )


# =========================
# MAIN NAVIGATION
# =========================

@dp.callback_query_handler(lambda c: c.data == "help")
async def help_connect(cb: types.CallbackQuery):
    await cb.message.answer("📌 Перейди в тред группы и напиши:\n/connect")
    await cb.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("ws:"))
async def open_ws(cb: types.CallbackQuery):
    _, wid = cb.data.split(":")

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🗑 Удалить workspace", callback_data=f"del:{wid}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data="back_main"))

    await cb.message.edit_text("⚙️ Управление workspace", reply_markup=kb)
    await cb.answer()


@dp.callback_query_handler(lambda c: c.data == "back_main")
async def back_main(cb: types.CallbackQuery):
    data = load()
    uid = str(cb.from_user.id)

    await cb.message.edit_text(
        main_text(uid, data),
        reply_markup=main_kb(uid, data)
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

    wid = ws_id(m.chat.id, m.message_thread_id)

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

    msg = await cb.message.answer("✏️ Напиши название компании")

    ws["awaiting"] = {
        "type": "company",
        "msg_id": msg.message_id
    }

    await save(data)
    await cb.answer()


# =========================
# TEMPLATE
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("template:"))
async def template(cb: types.CallbackQuery):

    _, wid = cb.data.split(":")
    data = load()

    await cb.message.edit_text(
        "⚙️ Шаблон задач",
        reply_markup=template_kb(wid, data["workspaces"][wid])
    )
    await cb.answer()


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


@dp.callback_query_handler(lambda c: c.data.startswith("t_del:"))
async def t_del(cb: types.CallbackQuery):

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
    if not ws or not ws["awaiting"] or not m.text:
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

    try:
        await m.delete()
    except:
        pass

    try:
        await bot.delete_message(m.chat.id, msg_id)
    except:
        pass

    await bot.send_message(
        m.chat.id,
        "📂 Workspace",
        message_thread_id=m.message_thread_id,
        reply_markup=ws_kb(wid, ws)
    )


# =========================
# DELETE WORKSPACE
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("del:"))
async def delete_ws(cb: types.CallbackQuery):

    _, wid = cb.data.split(":")
    data = load()
    uid = str(cb.from_user.id)

    data["workspaces"].pop(wid, None)

    if wid in data["users"].get(uid, {}).get("workspaces", []):
        data["users"][uid]["workspaces"].remove(wid)

    await save(data)

    await cb.message.edit_text("❌ Workspace удалён")
    await cb.answer()


# =========================
# RUN
# =========================

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
