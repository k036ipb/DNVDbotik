import os, json, asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils import executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup

TOKEN = os.getenv("API_TOKEN")  # формат токена: строка (не указано подробностей)
bot = Bot(token=TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
DATA_FILE = "data.json"
lock = asyncio.Lock()

# FSM-состояния
class BotState(StatesGroup):
    new_company = State()
    add_task = State()
    rename_company = State()
    rename_task = State()
    new_template = State()
    rename_template = State()

def ws_id(chat_id, thread_id):
    return f"{chat_id}_{thread_id or 0}"

async def load_data():
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

# Клавиатуры
def main_kb(uid, data):
    kb = InlineKeyboardMarkup(row_width=1)
    for wid in data["users"].get(uid, {}).get("workspaces", []):
        ws = data["workspaces"].get(wid)
        if ws:
            kb.add(InlineKeyboardButton(ws["name"], callback_data=f"ws:{wid}"))
    kb.add(InlineKeyboardButton("➕ Подключить workspace", callback_data="help"))
    kb.add(InlineKeyboardButton("🔄 Обновить", callback_data="refresh"))
    return kb

def ws_kb(wid, ws):
    kb = InlineKeyboardMarkup(row_width=1)
    for i, comp in enumerate(ws["companies"]):
        kb.add(InlineKeyboardButton(comp["name"], callback_data=f"company:{wid}:{i}"))
    kb.add(InlineKeyboardButton("➕ Создать компанию", callback_data=f"create:{wid}"))
    kb.add(InlineKeyboardButton("⚙️ Шаблон задач", callback_data=f"template:{wid}"))
    return kb

def template_kb(wid, ws):
    kb = InlineKeyboardMarkup(row_width=1)
    for i, task in enumerate(ws["template"]):
        kb.add(InlineKeyboardButton(task, callback_data=f"t_open:{wid}:{i}"))
    kb.add(InlineKeyboardButton("➕ Добавить задачу", callback_data=f"t_add:{wid}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"back:{wid}"))
    return kb

# /start в ЛС
@dp.message_handler(commands=["start"])
async def cmd_start(m: types.Message):
    if m.chat.type != "private":
        return
    data = await load_data()
    uid = str(m.from_user.id)
    data["users"].setdefault(uid, {"workspaces": []})
    await save_data(data)
    text = "📂 Ваши workspace:\n"
    if not data["users"][uid]["workspaces"]:
        text += "Нет workspace"
    else:
        for wid in data["users"][uid]["workspaces"]:
            ws = data["workspaces"].get(wid)
            if ws:
                text += f"• {ws['name']}\n"
    await m.answer(text, reply_markup=main_kb(uid, data))

# Обновление меню ЛС
@dp.callback_query_handler(lambda c: c.data == "refresh")
async def cb_refresh(cb: types.CallbackQuery):
    await cb.answer()
    data = await load_data()
    uid = str(cb.from_user.id)
    text = "📂 Ваши workspace:\n"
    wss = data["users"].get(uid, {}).get("workspaces", [])
    if not wss:
        text += "Нет workspace"
    else:
        for wid in wss:
            ws = data["workspaces"].get(wid)
            if ws:
                text += f"• {ws['name']}\n"
    await cb.message.edit_text(text, reply_markup=main_kb(uid, data))

# Подсказка подключения (отдельное сообщение)
@dp.callback_query_handler(lambda c: c.data == "help")
async def cb_help(cb: types.CallbackQuery):
    await cb.answer()
    data = await load_data()
    uid = str(cb.from_user.id)
    # Высылаем инструкцию
    hint_msg = await cb.message.answer(
        "📌 Как подключить workspace:\n\n"
        "1) Перейдите в нужный тред группы\n"
        "2) Отправьте команду /connect"
    )
    # Сохраняем ID подсказки, чтобы удалить позже
    # (берём ws_id = uid_0, временно, или сохраняем в отдельную структуру)
    # Здесь просто удалим в /connect после работы

# /connect в теме
@dp.message_handler(commands=["connect"])
async def cmd_connect(m: types.Message):
    if m.chat.type == "private":
        return
    data = await load_data()
    uid = str(m.from_user.id)
    thread_id = m.message_thread_id or 0
    wid = ws_id(m.chat.id, thread_id)
    # Создаём/обновляем workspace
    data["workspaces"].setdefault(wid, {
        "name": m.chat.title or "Группа",
        "chat_id": m.chat.id,
        "thread_id": thread_id,
        "menu_msg_id": None,
        "template": ["Создать договор", "Выставить счёт"],
        "companies": [],
        "awaiting": None
    })
    data["users"].setdefault(uid, {"workspaces": []})
    if wid not in data["users"][uid]["workspaces"]:
        data["users"][uid]["workspaces"].append(wid)
    await save_data(data)
    # Удаляем подсказку (предполагаем, что она только что была отправлена)
    try:
        await bot.delete_message(m.chat.id, m.message_id - 1, message_thread_id=thread_id)
    except: pass
    # Отправляем меню Workspace в треде
    ws = data["workspaces"][wid]
    menu_msg = await bot.send_message(
        m.chat.id, "📂 Workspace", reply_markup=ws_kb(wid, ws),
        message_thread_id=thread_id
    )
    ws["menu_msg_id"] = menu_msg.message_id
    await save_data(data)
    # Уведомление в ЛС (удалится через неделю)
    try:
        info = await bot.send_message(uid, f"Workspace «{m.chat.title}» подключён")
        async def delete_notice(chat, msg_id):
            await asyncio.sleep(7*24*3600)
            try: await bot.delete_message(chat, msg_id)
            except: pass
        asyncio.create_task(delete_notice(uid, info.message_id))
    except: pass

# Открыть управление workspace в ЛС
@dp.callback_query_handler(lambda c: c.data.startswith("ws:"))
async def cb_open_ws(cb: types.CallbackQuery):
    await cb.answer()
    data = await load_data()
    uid = str(cb.from_user.id)
    _, wid = cb.data.split(":")
    ws = data["workspaces"].get(wid)
    if not ws:
        await cb.answer("Workspace не найден")
        return
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("🗑 Удалить workspace", callback_data=f"delete_ws:{wid}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data="refresh"))
    await cb.message.edit_text(f"📂 {ws['name']}", reply_markup=kb)

# Удалить workspace
@dp.callback_query_handler(lambda c: c.data.startswith("delete_ws:"))
async def cb_delete_ws(cb: types.CallbackQuery):
    await cb.answer()
    data = await load_data()
    uid = str(cb.from_user.id)
    _, wid = cb.data.split(":")
    ws = data["workspaces"].get(wid)
    if ws:
        # Удаляем меню из треда
        chat_id = ws["chat_id"]; thread_id = ws["thread_id"]
        old_menu = ws.get("menu_msg_id")
        if old_menu:
            try: await bot.delete_message(chat_id, old_menu, message_thread_id=thread_id)
            except: pass
        # Удаляем из списка пользователя
        if wid in data["users"][uid]["workspaces"]:
            data["users"][uid]["workspaces"].remove(wid)
    await save_data(data)
    # Показываем обновлённый список workspace
    text = "📂 Ваши workspace:\n"
    wss = data["users"].get(uid, {}).get("workspaces", [])
    if not wss:
        text += "Нет workspace"
    else:
        for wid2 in wss:
            ws2 = data["workspaces"].get(wid2)
            if ws2:
                text += f"• {ws2['name']}\n"
    await cb.message.edit_text(text, reply_markup=main_kb(uid, data))

# Создать компанию (запрос названия)
@dp.callback_query_handler(lambda c: c.data.startswith("create:"))
async def cb_create(cb: types.CallbackQuery):
    await cb.answer()
    data = await load_data()
    _, wid = cb.data.split(":")
    ws = data["workspaces"].get(wid)
    if not ws:
        await cb.answer("Ошибка")
        return
    await BotState.new_company.set()
    await cb.message.answer("✏️ Напишите название компании:")

# Ввести название компании
@dp.message_handler(state=BotState.new_company, content_types=types.ContentTypes.TEXT)
async def process_new_company(message: types.Message, state: FSMContext):
    text = message.text.lstrip("/").strip()
    data = await load_data()
    tid = message.message_thread_id or 0
    wid = ws_id(message.chat.id, tid)
    ws = data["workspaces"].get(wid)
    if ws is None:
        await state.finish()
        return
    # Проверка дублей
    if any(c["name"] == text for c in ws["companies"]):
        await message.answer("Такая компания уже существует.")
        await state.finish()
        return
    # Создаём карточку компании
    tasks = [{"text": t, "done": False} for t in ws["template"]]
    card_msg = await message.answer(f"📁 {text}:\n" + "\n".join(f"⬜ {t['text']}" for t in tasks))
    ws["companies"].append({"name": text, "tasks": tasks, "card_msg_id": card_msg.message_id})
    await save_data(data)
    # Удаляем запрос и текст пользователя
    try: await message.delete()
    except: pass
    try: await bot.delete_message(message.chat.id, message.message_id - 1, message_thread_id=tid)
    except: pass
    # Пересоздаём меню workspace
    old_menu = ws.get("menu_msg_id")
    if old_menu:
        try: await bot.delete_message(message.chat.id, old_menu, message_thread_id=tid)
        except: pass
    menu_msg = await bot.send_message(message.chat.id, "📂 Workspace", reply_markup=ws_kb(wid, ws), message_thread_id=tid)
    ws["menu_msg_id"] = menu_msg.message_id
    await save_data(data)
    await state.finish()

# Открыть список задач компании
@dp.callback_query_handler(lambda c: c.data.startswith("company:"))
async def cb_open_company(cb: types.CallbackQuery):
    await cb.answer()
    data = await load_data()
    _, wid, idx = cb.data.split(":")
    comp_idx = int(idx)
    ws = data["workspaces"].get(wid)
    if not ws or comp_idx >= len(ws["companies"]):
        await cb.answer("Компания не найдена")
        return
    comp = ws["companies"][comp_idx]
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

# Добавить задачу (запрос)
@dp.callback_query_handler(lambda c: c.data.startswith("add_task:"))
async def cb_add_task(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, comp_idx = cb.data.split(":")
    ws = (await load_data())["workspaces"].get(wid)
    if not ws or int(comp_idx) >= len(ws["companies"]):
        return
    await BotState.add_task.set()
    await cb.message.answer("✏️ Введите текст новой задачи:")

# Ввести задачу
@dp.message_handler(state=BotState.add_task, content_types=types.ContentTypes.TEXT)
async def process_add_task(message: types.Message, state: FSMContext):
    text = message.text
    data = await load_data()
    tid = message.message_thread_id or 0
    wid = ws_id(message.chat.id, tid)
    ws = data["workspaces"].get(wid)
    comp_idx = ws["awaiting"]["company_idx"] if ws.get("awaiting") else None
    if not ws or comp_idx is None: 
        await state.finish(); return
    comp = ws["companies"][comp_idx]
    comp["tasks"].append({"text": text, "done": False})
    await save_data(data)
    # Удаляем запрос и ввод
    try: await message.delete()
    except: pass
    # Обновляем меню задач
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
    await bot.edit_message_text(new_text, message.chat.id, message_id=ws["menu_msg_id"], reply_markup=kb)
    # Обновляем карточку
    card_text = f"📁 {comp['name']}:\n"
    for t in comp["tasks"]:
        icon = "✔" if t["done"] else "⬜"
        card_text += f"{icon} {t['text']}\n"
    try:
        await bot.edit_message_text(card_text, message.chat.id, message_id=comp["card_msg_id"])
    except: pass
    await state.finish()

# Удалить компанию
@dp.callback_query_handler(lambda c: c.data.startswith("delete_company:"))
async def cb_delete_company(cb: types.CallbackQuery):
    await cb.answer()
    data = await load_data()
    _, wid, comp_idx = cb.data.split(":")
    comp_idx = int(comp_idx)
    ws = data["workspaces"].get(wid)
    if not ws or comp_idx >= len(ws["companies"]):
        return
    # Удаляем карточку
    comp = ws["companies"].pop(comp_idx)
    try:
        await bot.delete_message(ws["chat_id"], comp["card_msg_id"], message_thread_id=ws["thread_id"])
    except: pass
    await save_data(data)
    # Пересоздаём меню workspace
    old_menu = ws.get("menu_msg_id")
    if old_menu:
        try: await bot.delete_message(ws["chat_id"], old_menu, message_thread_id=ws["thread_id"])
        except: pass
    menu_msg = await bot.send_message(ws["chat_id"], "📂 Workspace", reply_markup=ws_kb(wid, ws), message_thread_id=ws["thread_id"])
    ws["menu_msg_id"] = menu_msg.message_id

# Переименовать задачу (запрос нового названия)
@dp.callback_query_handler(lambda c: c.data.startswith("task_rename:"))
async def cb_task_rename(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, comp_idx, task_idx = cb.data.split(":")
    await BotState.rename_task.set()
    await cb.message.answer("✏️ Введите новое название задачи:")

@dp.message_handler(state=BotState.rename_task, content_types=types.ContentTypes.TEXT)
async def process_rename_task(message: types.Message, state: FSMContext):
    text = message.text
    data = await load_data()
    tid = message.message_thread_id or 0
    wid = ws_id(message.chat.id, tid)
    ws = data["workspaces"].get(wid)
    comp_idx = ws["awaiting"]["company_idx"] if ws.get("awaiting") else None
    task_idx = ws["awaiting"]["task_idx"] if ws.get("awaiting") else None
    if ws and comp_idx is not None and task_idx is not None:
        comp = ws["companies"][comp_idx]
        comp["tasks"][task_idx]["text"] = text
        await save_data(data)
        await message.answer("✅ Название задачи обновлено")
        try: await message.delete()
        except: pass
        # Обновляем карточку
        card_text = f"📁 {comp['name']}:\n"
        for t in comp["tasks"]:
            icon = "✔" if t["done"] else "⬜"
            card_text += f"{icon} {t['text']}\n"
        try:
            await bot.edit_message_text(card_text, message.chat.id, message_id=comp["card_msg_id"])
        except: pass
        # Обновляем меню задач
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
        await bot.edit_message_text(new_text, message.chat.id, message_id=ws["menu_msg_id"], reply_markup=kb)
    await state.finish()

# Обновить статус задачи
@dp.callback_query_handler(lambda c: c.data.startswith("task_done:"))
async def cb_task_done(cb: types.CallbackQuery):
    await cb.answer()
    data = await load_data()
    _, wid, comp_idx, task_idx = cb.data.split(":")
    comp_idx = int(comp_idx); task_idx = int(task_idx)
    ws = data["workspaces"].get(wid)
    if not ws: return
    comp = ws["companies"][comp_idx]
    comp["tasks"][task_idx]["done"] = not comp["tasks"][task_idx]["done"]
    await save_data(data)
    # Перестроение меню и карточки аналогично выше
    await cb.answer("Статус задачи обновлён")
