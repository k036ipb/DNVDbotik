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

def ws_id(chat_id: int, thread_id: int) -> str:
    return f"{chat_id}_{thread_id or 0}"

# -------- Функции загрузки и сохранения данных --------
def load_data():
    if not os.path.exists(DATA_FILE):
        return {"users": {}, "workspaces": {}}
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {"users": {}, "workspaces": {}}

async def save_data(data):
    async with lock:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

# -------- Главный интерфейс (ЛС) --------
def main_kb(uid, data):
    kb = InlineKeyboardMarkup(row_width=1)
    for wid in data["users"].get(uid, {}).get("workspaces", []):
        ws = data["workspaces"].get(wid)
        if ws:
            # Кнопка с названием workspace
            kb.add(InlineKeyboardButton(ws["name"], callback_data=f"ws:{wid}"))
    kb.add(InlineKeyboardButton("➕ Подключить workspace", callback_data="help"))
    kb.add(InlineKeyboardButton("🔄 Обновить", callback_data="refresh"))
    return kb

def main_text(uid, data):
    text = "📂 Ваши workspace\n\n"
    wss = data["users"].get(uid, {}).get("workspaces", [])
    if not wss:
        return text + "Нет workspace"
    for wid in wss:
        ws = data["workspaces"].get(wid)
        if ws:
            text += f"• {ws['name']}\n"
    return text

@dp.message_handler(commands=["start"])
async def cmd_start(m: types.Message):
    uid = str(m.from_user.id)
    data = load_data()
    data["users"].setdefault(uid, {"workspaces": []})
    await save_data(data)
    await m.answer(main_text(uid, data), reply_markup=main_kb(uid, data))

@dp.callback_query_handler(lambda c: c.data == "refresh")
async def cb_refresh(cb: types.CallbackQuery):
    data = load_data()
    uid = str(cb.from_user.id)
    await cb.message.edit_text(main_text(uid, data), reply_markup=main_kb(uid, data))
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data == "help")
async def cb_help(cb: types.CallbackQuery):
    # Показываем инструкцию по подключению workspace
    text = (
        "📌 Как подключить workspace:\n\n"
        "1️⃣ Перейди в тред группы\n"
        "2️⃣ Напиши команду:\n    /connect\n"
        "После этого workspace появится в списке твоих workspace."
    )
    await cb.message.edit_text(text)
    await cb.answer()

# Открыть меню workspace в ЛС
@dp.callback_query_handler(lambda c: c.data.startswith("ws:"))
async def cb_open_workspace_pm(cb: types.CallbackQuery):
    _, wid = cb.data.split(":")
    data = load_data()
    ws = data["workspaces"].get(wid)
    if not ws:
        await cb.answer("Workspace не найден")
        return
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("🗑 Удалить workspace", callback_data=f"delete_ws:{wid}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data="back_main"))
    await cb.message.edit_text(f"📂 Workspace: {ws['name']}", reply_markup=kb)
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data == "back_main")
async def cb_back_main(cb: types.CallbackQuery):
    data = load_data()
    uid = str(cb.from_user.id)
    await cb.message.edit_text(main_text(uid, data), reply_markup=main_kb(uid, data))
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("delete_ws:"))
async def cb_delete_workspace(cb: types.CallbackQuery):
    _, wid = cb.data.split(":")
    data = load_data()
    uid = str(cb.from_user.id)
    if wid in data["users"].get(uid, {}).get("workspaces", []):
        data["users"][uid]["workspaces"].remove(wid)
    await save_data(data)
    # Обновляем главное меню ЛС
    await cb.message.edit_text(main_text(uid, data), reply_markup=main_kb(uid, data))
    await cb.answer("Workspace удалён")

# -------- Подключение workspace в треде --------
@dp.message_handler(commands=["connect"])
async def cmd_connect(m: types.Message):
    if m.chat.type == "private":
        # Игнорируем в ЛС
        return
    data = load_data()
    uid = str(m.from_user.id)
    thread_id = m.message_thread_id or 0
    wid = ws_id(m.chat.id, thread_id)
    # Создаём или обновляем workspace
    if wid not in data["workspaces"]:
        data["workspaces"][wid] = {
            "name": m.chat.title or "Чат",
            "chat_id": m.chat.id,
            "thread_id": thread_id,
            "menu_msg_id": None,
            "template": ["Создать договор", "Выставить счет"],  # начальный шаблон
            "companies": [],
            "awaiting": None
        }
    data["users"].setdefault(uid, {"workspaces": []})
    if wid not in data["users"][uid]["workspaces"]:
        data["users"][uid]["workspaces"].append(wid)
    await save_data(data)
    # Отправляем меню workspace в тред (новое или заменяемое)
    menu_msg = await bot.send_message(
        m.chat.id,
        "📂 Workspace подключен",
        message_thread_id=thread_id,
        reply_markup=InlineKeyboardMarkup()  # пока без кнопок, добавим ниже
    )
    # Обновим menu_msg_id
    data = load_data()
    data["workspaces"][wid]["menu_msg_id"] = menu_msg.message_id
    await save_data(data)
    # В ЛС пишем уведомление (которое позже удалим)
    try:
        info_msg = await bot.send_message(uid, f"Workspace «{m.chat.title}» подключен")
        # Удалим уведомление через неделю (примерно)
        async def delete_notice(chat, msg_id):
            await asyncio.sleep(7*24*3600)
            try:
                await bot.delete_message(chat, msg_id)
            except:
                pass
        asyncio.create_task(delete_notice(uid, info_msg.message_id))
    except:
        pass  # если нельзя отправить
    await m.answer("Workspace подключен", reply_markup=main_kb(uid, data))
    await menu_msg.edit_reply_markup(reply_markup=ws_kb(wid, data["workspaces"][wid]))

# Клавиатура workspace (в треде): список компаний + кнопки
def ws_kb(wid, ws):
    kb = InlineKeyboardMarkup(row_width=1)
    for idx, comp in enumerate(ws["companies"]):
        kb.add(InlineKeyboardButton(comp["name"], callback_data=f"company:{wid}:{idx}"))
    kb.add(InlineKeyboardButton("➕ Создать компанию", callback_data=f"create:{wid}"))
    kb.add(InlineKeyboardButton("⚙️ Шаблон задач", callback_data=f"template:{wid}"))
    return kb

# -------- Создание компании --------
@dp.callback_query_handler(lambda c: c.data.startswith("create:"))
async def cb_create_company(cb: types.CallbackQuery):
    _, wid = cb.data.split(":")
    data = load_data()
    ws = data["workspaces"].get(wid)
    if not ws:
        await cb.answer("Workspace не найден")
        return
    msg = await cb.message.answer("✏️ Напиши название компании")
    ws["awaiting"] = {"type": "new_company", "msg": msg.message_id}
    await save_data(data)
    await cb.answer()

# -------- Шаблон задач --------
@dp.callback_query_handler(lambda c: c.data.startswith("template:"))
async def cb_template(cb: types.CallbackQuery):
    _, wid = cb.data.split(":")
    data = load_data()
    ws = data["workspaces"].get(wid)
    if not ws:
        await cb.answer("Workspace не найден")
        return
    text = "⚙️ Шаблон задач"
    kb = InlineKeyboardMarkup(row_width=1)
    for idx, task in enumerate(ws["template"]):
        kb.add(InlineKeyboardButton(task, callback_data=f"t_open:{wid}:{idx}"))
    kb.add(InlineKeyboardButton("➕ Добавить задачу", callback_data=f"t_add:{wid}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"back:{wid}"))
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("t_open:"))
async def cb_template_open(cb: types.CallbackQuery):
    _, wid, i_str = cb.data.split(":")
    i = int(i_str)
    data = load_data()
    ws = data["workspaces"].get(wid)
    if not ws or i < 0 or i >= len(ws["template"]):
        await cb.answer("Задача не найдена")
        return
    text = f"⚙️ Задача шаблона: «{ws['template'][i]}»"
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("✍️ Переименовать", callback_data=f"t_rename:{wid}:{i}"))
    kb.add(InlineKeyboardButton("🗑 Удалить", callback_data=f"t_del:{wid}:{i}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"template:{wid}"))
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("t_del:"))
async def cb_template_del(cb: types.CallbackQuery):
    _, wid, i_str = cb.data.split(":")
    i = int(i_str)
    data = load_data()
    ws = data["workspaces"].get(wid)
    if not ws or i<0 or i>=len(ws["template"]):
        await cb.answer("Ошибка")
        return
    ws["template"].pop(i)
    await save_data(data)
    # Обновляем меню шаблона
    text = "⚙️ Шаблон задач"
    kb = InlineKeyboardMarkup(row_width=1)
    for idx, task in enumerate(ws["template"]):
        kb.add(InlineKeyboardButton(task, callback_data=f"t_open:{wid}:{idx}"))
    kb.add(InlineKeyboardButton("➕ Добавить задачу", callback_data=f"t_add:{wid}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"back:{wid}"))
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer("Удалено")

@dp.callback_query_handler(lambda c: c.data.startswith("t_add:"))
async def cb_template_add(cb: types.CallbackQuery):
    _, wid = cb.data.split(":")
    data = load_data()
    ws = data["workspaces"].get(wid)
    if not ws:
        await cb.answer("Ошибка")
        return
    msg = await cb.message.answer("✏️ Введите новую задачу шаблона")
    ws["awaiting"] = {"type": "new_template", "msg": msg.message_id}
    await save_data(data)
    await cb.answer()

# -------- Кнопка «Назад» (возврат в меню workspace) --------
@dp.callback_query_handler(lambda c: c.data.startswith("back:"))
async def cb_back(cb: types.CallbackQuery):
    _, wid = cb.data.split(":")
    data = load_data()
    ws = data["workspaces"].get(wid)
    if not ws:
        await cb.answer("Workspace не найден")
        return
    await cb.message.edit_text("📂 Workspace", reply_markup=ws_kb(wid, ws))
    await cb.answer()

# -------- Открыть компанию (меню задач компании) --------
@dp.callback_query_handler(lambda c: c.data.startswith("company:"))
async def cb_open_company(cb: types.CallbackQuery):
    _, wid, comp_str = cb.data.split(":")
    comp_idx = int(comp_str)
    data = load_data()
    ws = data["workspaces"].get(wid)
    if not ws or comp_idx<0 or comp_idx>=len(ws["companies"]):
        await cb.answer("Компания не найдена")
        return
    comp = ws["companies"][comp_idx]
    # Формируем текст списка задач
    text = f"📁 {comp['name']}\n\n"
    kb = InlineKeyboardMarkup(row_width=1)
    for i, t in enumerate(comp["tasks"]):
        icon = "✔" if t["done"] else "⬜"
        text += f"{icon} {t['text']}\n"
        kb.add(InlineKeyboardButton(f"{icon} {t['text']}", callback_data=f"task_menu:{wid}:{comp_idx}:{i}"))
    kb.add(InlineKeyboardButton("➕ Добавить задачу", callback_data=f"add_task:{wid}:{comp_idx}"))
    kb.add(InlineKeyboardButton("✍️ Переименовать компанию", callback_data=f"rename_company:{wid}:{comp_idx}"))
    kb.add(InlineKeyboardButton("🗑 Удалить компанию", callback_data=f"delete_company:{wid}:{comp_idx}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"back:{wid}"))
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()

# -------- Меню управления задачей --------
@dp.callback_query_handler(lambda c: c.data.startswith("task_menu:"))
async def cb_task_menu(cb: types.CallbackQuery):
    _, wid, comp_str, task_str = cb.data.split(":")
    comp_idx = int(comp_str); task_idx = int(task_str)
    data = load_data()
    ws = data["workspaces"].get(wid)
    if not ws:
        await cb.answer("Workspace не найден")
        return
    tasks = ws["companies"][comp_idx]["tasks"]
    if task_idx<0 or task_idx>=len(tasks):
        await cb.answer("Задача не найдена")
        return
    task = tasks[task_idx]
    icon = "✔" if task["done"] else "⬜"
    text = f"📋 {task['text']}\n\nВыберите действие:"
    kb = InlineKeyboardMarkup(row_width=1)
    if task["done"]:
        kb.add(InlineKeyboardButton("❌ Отметить невыполненной", callback_data=f"task_done:{wid}:{comp_idx}:{task_idx}"))
    else:
        kb.add(InlineKeyboardButton("✔ Отметить выполненной", callback_data=f"task_done:{wid}:{comp_idx}:{task_idx}"))
    kb.add(InlineKeyboardButton("✍️ Переименовать", callback_data=f"task_rename:{wid}:{comp_idx}:{task_idx}"))
    kb.add(InlineKeyboardButton("🗑 Удалить задачу", callback_data=f"task_del:{wid}:{comp_idx}:{task_idx}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"company:{wid}:{comp_idx}"))
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("task_done:"))
async def cb_task_done(cb: types.CallbackQuery):
    _, wid, comp_str, task_str = cb.data.split(":")
    comp_idx = int(comp_str); task_idx = int(task_str)
    data = load_data()
    ws = data["workspaces"].get(wid)
    comp = ws["companies"][comp_idx]
    tasks = comp["tasks"]
    if 0 <= task_idx < len(tasks):
        tasks[task_idx]["done"] = not tasks[task_idx]["done"]
    await save_data(data)
    # Обновляем карточку и меню задач
    chat_id = ws["chat_id"]; thread_id = ws["thread_id"]
    # Новый текст для меню задач
    text = f"📁 {comp['name']}\n\n"
    kb = InlineKeyboardMarkup(row_width=1)
    for i, t in enumerate(tasks):
        icon = "✔" if t["done"] else "⬜"
        text += f"{icon} {t['text']}\n"
        kb.add(InlineKeyboardButton(f"{icon} {t['text']}", callback_data=f"task_menu:{wid}:{comp_idx}:{i}"))
    kb.add(InlineKeyboardButton("➕ Добавить задачу", callback_data=f"add_task:{wid}:{comp_idx}"))
    kb.add(InlineKeyboardButton("✍️ Переименовать компанию", callback_data=f"rename_company:{wid}:{comp_idx}"))
    kb.add(InlineKeyboardButton("🗑 Удалить компанию", callback_data=f"delete_company:{wid}:{comp_idx}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"back:{wid}"))
    await cb.message.edit_text(text, reply_markup=kb)
    # Обновляем карточку компании
    card_text = f"📁 {comp['name']}:\n"
    for t in tasks:
        icon = "✔" if t["done"] else "⬜"
        card_text += f"{icon} {t['text']}\n"
    card_id = comp["card_msg_id"]
    try:
        await bot.edit_message_text(card_text, chat_id, message_id=card_id)
    except:
        pass
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("task_del:"))
async def cb_task_del(cb: types.CallbackQuery):
    _, wid, comp_str, task_str = cb.data.split(":")
    comp_idx = int(comp_str); task_idx = int(task_str)
    data = load_data()
    ws = data["workspaces"].get(wid)
    comp = ws["companies"][comp_idx]
    if 0 <= task_idx < len(comp["tasks"]):
        comp["tasks"].pop(task_idx)
    await save_data(data)
    # Обновляем карточку и меню
    chat_id = ws["chat_id"]
    text = f"📁 {comp['name']}\n\n"
    kb = InlineKeyboardMarkup(row_width=1)
    for i, t in enumerate(comp["tasks"]):
        icon = "✔" if t["done"] else "⬜"
        text += f"{icon} {t['text']}\n"
        kb.add(InlineKeyboardButton(f"{icon} {t['text']}", callback_data=f"task_menu:{wid}:{comp_idx}:{i}"))
    kb.add(InlineKeyboardButton("➕ Добавить задачу", callback_data=f"add_task:{wid}:{comp_idx}"))
    kb.add(InlineKeyboardButton("✍️ Переименовать компанию", callback_data=f"rename_company:{wid}:{comp_idx}"))
    kb.add(InlineKeyboardButton("🗑 Удалить компанию", callback_data=f"delete_company:{wid}:{comp_idx}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"back:{wid}"))
    await cb.message.edit_text(text, reply_markup=kb)
    # Обновляем карточку компании
    card_text = f"📁 {comp['name']}:\n"
    for t in comp["tasks"]:
        icon = "✔" if t["done"] else "⬜"
        card_text += f"{icon} {t['text']}\n"
    try:
        await bot.edit_message_text(card_text, chat_id, message_id=comp["card_msg_id"])
    except:
        pass
    await cb.answer("Задача удалена")

@dp.callback_query_handler(lambda c: c.data.startswith("task_rename:"))
async def cb_task_rename(cb: types.CallbackQuery):
    _, wid, comp_str, task_str = cb.data.split(":")
    comp_idx = int(comp_str); task_idx = int(task_str)
    data = load_data()
    ws = data["workspaces"].get(wid)
    if not ws or comp_idx<0 or task_idx<0:
        await cb.answer("Ошибка")
        return
    msg = await cb.message.answer("✏️ Введите новое название задачи")
    ws["awaiting"] = {"type": "rename_task", "company_idx": comp_idx, "task_idx": task_idx, "msg": msg.message_id}
    await save_data(data)
    await cb.answer()

# -------- Переименование компании --------
@dp.callback_query_handler(lambda c: c.data.startswith("rename_company:"))
async def cb_rename_company(cb: types.CallbackQuery):
    _, wid, comp_str = cb.data.split(":")
    comp_idx = int(comp_str)
    data = load_data()
    ws = data["workspaces"].get(wid)
    if not ws:
        await cb.answer("Ошибка")
        return
    msg = await cb.message.answer("✏️ Введите новое название компании")
    ws["awaiting"] = {"type": "rename_company", "company_idx": comp_idx, "msg": msg.message_id}
    await save_data(data)
    await cb.answer()

# -------- Удаление компании --------
@dp.callback_query_handler(lambda c: c.data.startswith("delete_company:"))
async def cb_delete_company(cb: types.CallbackQuery):
    _, wid, comp_str = cb.data.split(":")
    comp_idx = int(comp_str)
    data = load_data()
    ws = data["workspaces"].get(wid)
    if not ws or comp_idx<0 or comp_idx>=len(ws["companies"]):
        await cb.answer("Ошибка")
        return
    comp = ws["companies"].pop(comp_idx)
    await save_data(data)
    # Удаляем карточку в чате
    try:
        await bot.delete_message(ws["chat_id"], comp["card_msg_id"])
    except:
        pass
    # Показываем меню workspace
    await cb.message.edit_text("📂 Workspace", reply_markup=ws_kb(wid, ws))
    await cb.answer("Компания удалена")

# -------- Обработка текста (ожидания) --------
@dp.message_handler(lambda m: m.chat.type != "private")
async def handle_input(m: types.Message):
    data = load_data()
    tid = m.message_thread_id or 0
    wid = ws_id(m.chat.id, tid)
    ws = data["workspaces"].get(wid)
    if not ws or not ws["awaiting"] or not m.text:
        return
    awaiting = ws["awaiting"]
    mode = awaiting["type"]
    text = m.text.strip()
    # Очищаем состояние ожидания
    ws["awaiting"] = None

    chat_id = ws["chat_id"]
    if mode == "new_company":
        # Создание новой компании
        # Проверка на дубликат:
        if any(comp["name"] == text for comp in ws["companies"]):
            # Дубли не создаём
            await m.delete()
            await bot.delete_message(chat_id, awaiting["msg"], message_thread_id=tid)
            await bot.send_message(chat_id, f"Компания «{text}» уже существует", message_thread_id=tid)
        else:
            # Формируем задачи из шаблона
            tasks = [{"text": t, "done": False} for t in ws["template"]]
            card_msg = await bot.send_message(
                chat_id,
                f"📁 {text}:\n" + "\n".join(f"⬜ {t['text']}" for t in tasks),
                message_thread_id=tid
            )
            ws["companies"].append({"name": text, "tasks": tasks, "card_msg_id": card_msg.message_id})
            await save_data(data)
            # Удаляем сообщения запроса и пользователя
            try: await m.delete()
            except: pass
            try: await bot.delete_message(chat_id, awaiting["msg"], message_thread_id=tid)
            except: pass
            # Обновляем меню workspace: удаляем старое и отправляем новое снизу
            old_menu = ws["menu_msg_id"]
            try: await bot.delete_message(chat_id, old_menu, message_thread_id=tid)
            except: pass
            new_kb = ws_kb(wid, ws)
            menu_msg = await bot.send_message(chat_id, "📂 Workspace", message_thread_id=tid, reply_markup=new_kb)
            ws["menu_msg_id"] = menu_msg.message_id
            await save_data(data)

    elif mode == "add_task":
        comp_idx = awaiting.get("company_idx")
        if comp_idx is not None and 0 <= comp_idx < len(ws["companies"]):
            ws["companies"][comp_idx]["tasks"].append({"text": text, "done": False})
            await save_data(data)
            # Удаляем запрос и ответ
            try: await m.delete()
            except: pass
            try: await bot.delete_message(chat_id, awaiting["msg"], message_thread_id=tid)
            except: pass
            # Обновляем меню задач компании
            comp = ws["companies"][comp_idx]
            new_text = f"📁 {comp['name']}\n\n"
            kb = InlineKeyboardMarkup(row_width=1)
            for i, t in enumerate(comp["tasks"]):
                icon = "✔" if t["done"] else "⬜"
                new_text += f"{icon} {t['text']}\n"
                kb.add(InlineKeyboardButton(f"{icon} {t['text']}", callback_data=f"task_menu:{wid}:{comp_idx}:{i}"))
            kb.add(InlineKeyboardButton("➕ Добавить задачу", callback_data=f"add_task:{wid}:{comp_idx}"))
            kb.add(InlineKeyboardButton("✍️ Переименовать компанию", callback_data=f"rename_company:{wid}:{comp_idx}"))
            kb.add(InlineKeyboardButton("🗑 Удалить компанию", callback_data=f"delete_company:{wid}:{comp_idx}"))
            kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"back:{wid}"))
            # Редактируем меню (сообщение с клавиатурой)
            try:
                await bot.edit_message_text(new_text, chat_id, message_id=ws["menu_msg_id"], reply_markup=kb)
            except:
                pass
            # Обновляем карточку компании
            card_text = f"📁 {comp['name']}:\n"
            for t in comp["tasks"]:
                icon = "✔" if t["done"] else "⬜"
                card_text += f"{icon} {t['text']}\n"
            try:
                await bot.edit_message_text(card_text, chat_id, message_id=comp["card_msg_id"])
            except:
                pass

    elif mode == "rename_company":
        comp_idx = awaiting.get("company_idx")
        if comp_idx is not None and 0 <= comp_idx < len(ws["companies"]):
            # Проверка дубликата
            if any(c["name"] == text for i,c in enumerate(ws["companies"]) if i != comp_idx):
                # Если название занято, игнорируем
                await m.delete(); await bot.delete_message(chat_id, awaiting["msg"], message_thread_id=tid)
            else:
                ws["companies"][comp_idx]["name"] = text
                await save_data(data)
                # Удаляем запрос и ответ
                try: await m.delete()
                except: pass
                try: await bot.delete_message(chat_id, awaiting["msg"], message_thread_id=tid)
                except: pass
                comp = ws["companies"][comp_idx]
                # Обновляем карточку
                card_text = f"📁 {comp['name']}:\n"
                for t in comp["tasks"]:
                    icon = "✔" if t["done"] else "⬜"
                    card_text += f"{icon} {t['text']}\n"
                try:
                    await bot.edit_message_text(card_text, chat_id, message_id=comp["card_msg_id"])
                except: pass
                # Обновляем меню задач (заголовок)
                new_text = f"📁 {comp['name']}\n\n"
                kb = InlineKeyboardMarkup(row_width=1)
                for i, t in enumerate(comp["tasks"]):
                    icon = "✔" if t["done"] else "⬜"
                    new_text += f"{icon} {t['text']}\n"
                    kb.add(InlineKeyboardButton(f"{icon} {t['text']}", callback_data=f"task_menu:{wid}:{comp_idx}:{i}"))
                kb.add(InlineKeyboardButton("➕ Добавить задачу", callback_data=f"add_task:{wid}:{comp_idx}"))
                kb.add(InlineKeyboardButton("✍️ Переименовать компанию", callback_data=f"rename_company:{wid}:{comp_idx}"))
                kb.add(InlineKeyboardButton("🗑 Удалить компанию", callback_data=f"delete_company:{wid}:{comp_idx}"))
                kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"back:{wid}"))
                try:
                    await bot.edit_message_text(new_text, chat_id, message_id=ws["menu_msg_id"], reply_markup=kb)
                except: pass

    elif mode == "rename_task":
        comp_idx = awaiting.get("company_idx")
        task_idx = awaiting.get("task_idx")
        if comp_idx is not None and task_idx is not None:
            comp = ws["companies"][comp_idx]
            if 0 <= task_idx < len(comp["tasks"]):
                comp["tasks"][task_idx]["text"] = text
        await save_data(data)
        # Удаляем запрос и ответ
        try: await m.delete()
        except: pass
        try: await bot.delete_message(chat_id, awaiting["msg"], message_thread_id=tid)
        except: pass
        # Обновляем карточку и меню
        comp = ws["companies"][comp_idx]
        card_text = f"📁 {comp['name']}:\n"
        for t in comp["tasks"]:
            icon = "✔" if t["done"] else "⬜"
            card_text += f"{icon} {t['text']}\n"
        try:
            await bot.edit_message_text(card_text, chat_id, message_id=comp["card_msg_id"])
        except: pass
        new_text = f"📁 {comp['name']}\n\n"
        kb = InlineKeyboardMarkup(row_width=1)
        for i, t in enumerate(comp["tasks"]):
            icon = "✔" if t["done"] else "⬜"
            new_text += f"{icon} {t['text']}\n"
            kb.add(InlineKeyboardButton(f"{icon} {t['text']}", callback_data=f"task_menu:{wid}:{comp_idx}:{i}"))
        kb.add(InlineKeyboardButton("➕ Добавить задачу", callback_data=f"add_task:{wid}:{comp_idx}"))
        kb.add(InlineKeyboardButton("✍️ Переименовать компанию", callback_data=f"rename_company:{wid}:{comp_idx}"))
        kb.add(InlineKeyboardButton("🗑 Удалить компанию", callback_data=f"delete_company:{wid}:{comp_idx}"))
        kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"back:{wid}"))
        try:
            await bot.edit_message_text(new_text, chat_id, message_id=ws["menu_msg_id"], reply_markup=kb)
        except: pass

    elif mode == "new_template":
        ws["template"].append(text)
        await save_data(data)
        try: await m.delete()
        except: pass
        try: await bot.delete_message(chat_id, awaiting["msg"], message_thread_id=tid)
        except: pass
        # Обновляем меню шаблона
        text_template = "⚙️ Шаблон задач"
        kb = InlineKeyboardMarkup(row_width=1)
        for idx, task in enumerate(ws["template"]):
            kb.add(InlineKeyboardButton(task, callback_data=f"t_open:{wid}:{idx}"))
        kb.add(InlineKeyboardButton("➕ Добавить задачу", callback_data=f"t_add:{wid}"))
        kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"back:{wid}"))
        try:
            await bot.edit_message_text(text_template, chat_id, message_id=ws["menu_msg_id"], reply_markup=kb)
        except: pass

    elif mode == "rename_template":
        idx = awaiting.get("task_idx")
        if idx is not None and 0 <= idx < len(ws["template"]):
            ws["template"][idx] = text
        await save_data(data)
        try: await m.delete()
        except: pass
        try: await bot.delete_message(chat_id, awaiting["msg"], message_thread_id=tid)
        except: pass
        # Обновляем меню шаблона
        text_template = "⚙️ Шаблон задач"
        kb = InlineKeyboardMarkup(row_width=1)
        for i, t in enumerate(ws["template"]):
            kb.add(InlineKeyboardButton(t, callback_data=f"t_open:{wid}:{i}"))
        kb.add(InlineKeyboardButton("➕ Добавить задачу", callback_data=f"t_add:{wid}"))
        kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"back:{wid}"))
        try:
            await bot.edit_message_text(text_template, chat_id, message_id=ws["menu_msg_id"], reply_markup=kb)
        except: pass

    # Сохраняем изменения (если что-то добавляли/переименовывали)
    await save_data(data)

# -------- Запуск бота --------
if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
