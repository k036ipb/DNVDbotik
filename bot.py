import os, json, asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup

# --- FSM States ---
class BotState(StatesGroup):
    new_company = State()
    add_task = State()
    rename_company = State()
    rename_task = State()
    new_template = State()
    rename_template = State()

TOKEN = os.getenv("API_TOKEN")  # токен бота (строка, не указано точно)
bot = Bot(token=TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

DATA_FILE = "data.json"
lock = asyncio.Lock()

def ws_id(chat_id, thread_id):
    return f"{chat_id}_{thread_id or 0}"

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

# --- Клавиатуры ---
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
    for idx, task in enumerate(ws["template"]):
        kb.add(InlineKeyboardButton(task, callback_data=f"t_open:{wid}:{idx}"))
    kb.add(InlineKeyboardButton("➕ Добавить задачу", callback_data=f"t_add:{wid}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"back:{wid}"))
    return kb

# --- Команда /start ---
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    uid = str(message.from_user.id)
    data = load_data()
    data["users"].setdefault(uid, {"workspaces": []})
    await save_data(data)
    await message.answer("📂 Ваши workspace:\n" +
                         ("\n".join(f"• {load_data()['workspaces'][wid]['name']}" for wid in data["users"][uid]["workspaces"]) or "Нет workspace"),
                         reply_markup=main_kb(uid, data))

# --- Обновление главного меню ---
@dp.callback_query_handler(lambda c: c.data == "refresh")
async def cb_refresh(cb: types.CallbackQuery):
    data = load_data()
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
    await cb.answer()

# --- Инструкция подключения ---
@dp.callback_query_handler(lambda c: c.data == "help")
async def cb_help(cb: types.CallbackQuery):
    # Высылаем подсказку отдельным сообщением
    await cb.answer()  # важно убрать "крутилку"
    text = ("📌 Как подключить workspace:\n\n"
            "1. Перейдите в нужный тред группы\n"
            "2. Напишите команду /connect\n"
            "После этого workspace появится в списке ваших.")
    hint_msg = await cb.message.answer(text)
    # Мы не редактируем текущее сообщение, так как оно меню
    await asyncio.sleep(0)  # ничего не делаем дальше

# --- Подключение workspace ---
@dp.message_handler(commands=["connect"])
async def cmd_connect(m: types.Message):
    if m.chat.type == "private":
        return  # не подключаем в ЛС
    data = load_data()
    uid = str(m.from_user.id)
    thread_id = m.message_thread_id or 0
    wid = ws_id(m.chat.id, thread_id)
    # Инициализируем workspace
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
    # Отправляем меню workspace в тред (новое сообщение)
    ws = data["workspaces"][wid]
    menu_msg = await bot.send_message(
        m.chat.id, "📂 Workspace", reply_markup=ws_kb(wid, ws),
        message_thread_id=thread_id
    )
    # Сохраним ID меню
    data["workspaces"][wid]["menu_msg_id"] = menu_msg.message_id
    await save_data(data)
    # Уведомление в ЛС
    try:
        info = await bot.send_message(uid, f"Workspace «{m.chat.title}» подключён")
        # Автоудаление через неделю
        async def delete_after(chat, msg_id):
            await asyncio.sleep(7*24*3600)
            try:
                await bot.delete_message(chat, msg_id)
            except:
                pass
        asyncio.create_task(delete_after(uid, info.message_id))
    except:
        pass

# --- Открыть меню workspace в ЛС ---
@dp.callback_query_handler(lambda c: c.data.startswith("ws:"))
async def cb_open_ws_in_pm(cb: types.CallbackQuery):
    await cb.answer()
    data = load_data()
    uid = str(cb.from_user.id)
    _, wid = cb.data.split(":")
    ws = data["workspaces"].get(wid)
    if not ws:
        await cb.answer("Workspace не найден")
        return
    # Показываем меню в ЛС (удаление workspace)
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("🗑 Удалить workspace", callback_data=f"delete_ws:{wid}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data="refresh"))
    await cb.message.edit_text(f"📂 {ws['name']}", reply_markup=kb)

# --- Удалить workspace ---
@dp.callback_query_handler(lambda c: c.data.startswith("delete_ws:"))
async def cb_delete_ws(cb: types.CallbackQuery):
    await cb.answer()
    data = load_data()
    uid = str(cb.from_user.id)
    _, wid = cb.data.split(":")
    data["users"].get(uid, {"workspaces": []})["workspaces"] = [
        x for x in data["users"].get(uid, {}).get("workspaces", []) if x != wid
    ]
    await save_data(data)
    # Возвращаем главное меню ЛС
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

# --- Создать компанию (запрос названия) ---
@dp.callback_query_handler(lambda c: c.data.startswith("create:"))
async def cb_create_company(cb: types.CallbackQuery):
    await cb.answer()
    data = load_data()
    _, wid = cb.data.split(":")
    ws = data["workspaces"].get(wid)
    if not ws:
        await cb.answer("Ошибка")
        return
    await BotState.new_company.set()
    data["workspaces"][wid]["awaiting"] = {"type": "new_company"}
    await save_data(data)
    await cb.message.answer("✏️ Напишите название компании:")

# --- Добавить компанию: обработка ввода ---
@dp.message_handler(state=BotState.new_company, content_types=types.ContentTypes.TEXT)
async def process_new_company(message: types.Message, state: FSMContext):
    text = message.text.strip()
    tid = message.message_thread_id or 0
    wid = ws_id(message.chat.id, tid)
    data = load_data()
    ws = data["workspaces"].get(wid)
    if not ws:
        await message.answer("Ошибка.")
        await state.finish()
        return
    # Проверяем дубликаты
    if any(comp["name"] == text for comp in ws["companies"]):
        await message.answer(f"Компания «{text}» уже есть.")
        await state.finish()
        return
    # Создаём компанию
    tasks = [{"text": t, "done": False} for t in ws["template"]]
    card = await message.answer(f"📁 {text}:\n" + "\n".join(f"⬜ {t['text']}" for t in tasks))
    ws["companies"].append({"name": text, "tasks": tasks, "card_msg_id": card.message_id})
    await save_data(data)
    # Удаляем запрос и сообщение пользователя
    try: await message.delete()
    except: pass
    try: await bot.delete_message(message.chat.id, card.message_id - 1, message_thread_id=tid)  # предположим, запрос было предыдущее
    except: pass
    # Обновляем меню (пересоздаём под карточкой)
    old_menu = ws["menu_msg_id"]
    try: await bot.delete_message(message.chat.id, old_menu, message_thread_id=tid)
    except: pass
    new_menu = await bot.send_message(message.chat.id, "📂 Workspace", reply_markup=ws_kb(wid, ws), message_thread_id=tid)
    ws["menu_msg_id"] = new_menu.message_id
    await save_data(data)
    await state.finish()

# --- Открыть задачи компании ---
@dp.callback_query_handler(lambda c: c.data.startswith("company:"))
async def cb_open_company(cb: types.CallbackQuery):
    await cb.answer()
    data = load_data()
    _, wid, comp_str = cb.data.split(":")
    comp_idx = int(comp_str)
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

# --- Добавить задачу в компанию: запрос названия ---
@dp.callback_query_handler(lambda c: c.data.startswith("add_task:"))
async def cb_add_task(cb: types.CallbackQuery):
    await cb.answer()
    data = load_data()
    _, wid, comp_str = cb.data.split(":")
    comp_idx = int(comp_str)
    ws = data["workspaces"].get(wid)
    if not ws or comp_idx >= len(ws["companies"]):
        return
    await BotState.add_task.set()
    data["workspaces"][wid]["awaiting"] = {"type": "add_task", "company_idx": comp_idx}
    await save_data(data)
    await cb.message.answer("✏️ Введите текст задачи:")

@dp.message_handler(state=BotState.add_task, content_types=types.ContentTypes.TEXT)
async def process_add_task(message: types.Message, state: FSMContext):
    text = message.text.strip()
    tid = message.message_thread_id or 0
    wid = ws_id(message.chat.id, tid)
    data = load_data()
    ws = data["workspaces"].get(wid)
    comp_idx = ws["awaiting"].get("company_idx", None) if ws.get("awaiting") else None
    if ws and comp_idx is not None and comp_idx < len(ws["companies"]):
        ws["companies"][comp_idx]["tasks"].append({"text": text, "done": False})
        await save_data(data)
        # Удаляем запрос и ввод
        try: await message.delete()
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
        await bot.edit_message_text(new_text, message.chat.id, message_id=ws["menu_msg_id"], reply_markup=kb)
        # Обновляем карточку компании
        card_text = f"📁 {comp['name']}:\n"
        for t in comp["tasks"]:
            icon = "✔" if t["done"] else "⬜"
            card_text += f"{icon} {t['text']}\n"
        try:
            await bot.edit_message_text(card_text, message.chat.id, message_id=comp["card_msg_id"])
        except:
            pass
    await state.finish()

# --- Удалить компанию ---
@dp.callback_query_handler(lambda c: c.data.startswith("delete_company:"))
async def cb_delete_company(cb: types.CallbackQuery):
    await cb.answer()
    data = load_data()
    _, wid, comp_str = cb.data.split(":")
    comp_idx = int(comp_str)
    ws = data["workspaces"].get(wid)
    if not ws or comp_idx >= len(ws["companies"]):
        return
    comp = ws["companies"].pop(comp_idx)
    await save_data(data)
    # Удаляем карточку компании
    try:
        await bot.delete_message(ws["chat_id"], comp["card_msg_id"], message_thread_id=ws["thread_id"])
    except:
        pass
    # Переходим в меню workspace
    menu_msg = await bot.send_message(ws["chat_id"], "📂 Workspace", reply_markup=ws_kb(wid, ws), message_thread_id=ws["thread_id"])
    ws["menu_msg_id"] = menu_msg.message_id
    await save_data(data)

# --- Переименовать компанию: запрос нового названия ---
@dp.callback_query_handler(lambda c: c.data.startswith("rename_company:"))
async def cb_rename_company(cb: types.CallbackQuery):
    await cb.answer()
    data = load_data()
    _, wid, comp_str = cb.data.split(":")
    comp_idx = int(comp_str)
    ws = data["workspaces"].get(wid)
    if not ws or comp_idx >= len(ws["companies"]):
        return
    await BotState.rename_company.set()
    data["workspaces"][wid]["awaiting"] = {"type": "rename_company", "company_idx": comp_idx}
    await save_data(data)
    await cb.message.answer("✏️ Введите новое название компании:")

@dp.message_handler(state=BotState.rename_company, content_types=types.ContentTypes.TEXT)
async def process_rename_company(message: types.Message, state: FSMContext):
    text = message.text.strip()
    tid = message.message_thread_id or 0
    wid = ws_id(message.chat.id, tid)
    data = load_data()
    ws = data["workspaces"].get(wid)
    comp_idx = ws["awaiting"].get("company_idx", None) if ws.get("awaiting") else None
    if ws and comp_idx is not None and comp_idx < len(ws["companies"]):
        ws["companies"][comp_idx]["name"] = text
        await save_data(data)
        # Удаляем запрос и ввод
        try: await message.delete()
        except: pass
        # Обновляем карточку компании
        comp = ws["companies"][comp_idx]
        card_text = f"📁 {comp['name']}:\n"
        for t in comp["tasks"]:
            icon = "✔" if t["done"] else "⬜"
            card_text += f"{icon} {t['text']}\n"
        try:
            await bot.edit_message_text(card_text, message.chat.id, message_id=comp["card_msg_id"])
        except:
            pass
        # Обновляем меню задач компании (текущий открытый)
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
            await bot.edit_message_text(new_text, message.chat.id, message_id=ws["menu_msg_id"], reply_markup=kb)
        except:
            pass
    await state.finish()

# --- Меню управления задачей ---
@dp.callback_query_handler(lambda c: c.data.startswith("task_menu:"))
async def cb_task_menu(cb: types.CallbackQuery):
    await cb.answer()
    data = load_data()
    _, wid, comp_str, task_str = cb.data.split(":")
    comp_idx = int(comp_str); task_idx = int(task_str)
    ws = data["workspaces"].get(wid)
    if not ws or comp_idx>=len(ws["companies"]):
        return
    tasks = ws["companies"][comp_idx]["tasks"]
    if task_idx>=len(tasks):
        return
    task = tasks[task_idx]
    icon = "✔" if task["done"] else "⬜"
    text = f"📋 {task['text']}\nВыберите действие:"
    kb = InlineKeyboardMarkup(row_width=1)
    if task["done"]:
        kb.add(InlineKeyboardButton("❌ Отметить невыполненной", callback_data=f"task_done:{wid}:{comp_idx}:{task_idx}"))
    else:
        kb.add(InlineKeyboardButton("✔ Отметить выполненной", callback_data=f"task_done:{wid}:{comp_idx}:{task_idx}"))
    kb.add(InlineKeyboardButton("✍️ Переименовать", callback_data=f"task_rename:{wid}:{comp_idx}:{task_idx}"))
    kb.add(InlineKeyboardButton("🗑 Удалить задачу", callback_data=f"task_del:{wid}:{comp_idx}:{task_idx}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"company:{wid}:{comp_idx}"))
    await cb.message.edit_text(text, reply_markup=kb)

# --- Отметить задачу выполненной/не выполненной ---
@dp.callback_query_handler(lambda c: c.data.startswith("task_done:"))
async def cb_task_done(cb: types.CallbackQuery):
    await cb.answer()
    data = load_data()
    _, wid, comp_str, task_str = cb.data.split(":")
    comp_idx = int(comp_str); task_idx = int(task_str)
    ws = data["workspaces"].get(wid)
    if not ws:
        return
    comp = ws["companies"][comp_idx]
    # Переключаем статус
    comp["tasks"][task_idx]["done"] = not comp["tasks"][task_idx]["done"]
    await save_data(data)
    # Обновляем карточку и меню задач
    icon = "✔" if comp["tasks"][task_idx]["done"] else "⬜"
    # Перестроим меню задач полностью
    new_text = f"📁 {comp['name']}\n\n"
    kb = InlineKeyboardMarkup(row_width=1)
    for i, t in enumerate(comp["tasks"]):
        ic = "✔" if t["done"] else "⬜"
        new_text += f"{ic} {t['text']}\n"
        kb.add(InlineKeyboardButton(f"{ic} {t['text']}", callback_data=f"task_menu:{wid}:{comp_idx}:{i}"))
    kb.add(InlineKeyboardButton("➕ Добавить задачу", callback_data=f"add_task:{wid}:{comp_idx}"))
    kb.add(InlineKeyboardButton("✍️ Переименовать компанию", callback_data=f"rename_company:{wid}:{comp_idx}"))
    kb.add(InlineKeyboardButton("🗑 Удалить компанию", callback_data=f"delete_company:{wid}:{comp_idx}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"back:{wid}"))
    await cb.message.edit_text(new_text, reply_markup=kb)
    # Обновляем карточку
    card_text = f"📁 {comp['name']}:\n"
    for t in comp["tasks"]:
        ic = "✔" if t["done"] else "⬜"
        card_text += f"{ic} {t['text']}\n"
    try:
        await bot.edit_message_text(card_text, comp["card_msg_id"], message_id=comp["card_msg_id"])
    except:
        pass

# --- Удалить задачу ---
@dp.callback_query_handler(lambda c: c.data.startswith("task_del:"))
async def cb_task_del(cb: types.CallbackQuery):
    await cb.answer()
    data = load_data()
    _, wid, comp_str, task_str = cb.data.split(":")
    comp_idx = int(comp_str); task_idx = int(task_str)
    ws = data["workspaces"].get(wid)
    if not ws:
        return
    comp = ws["companies"][comp_idx]
    if task_idx < len(comp["tasks"]):
        comp["tasks"].pop(task_idx)
    await save_data(data)
    # Обновляем меню и карточку аналогично task_done
    await cb.message.answer("Задача удалена")
    await cb.message.delete()

# --- Переименовать задачу: запрос нового названия ---
@dp.callback_query_handler(lambda c: c.data.startswith("task_rename:"))
async def cb_task_rename(cb: types.CallbackQuery):
    await cb.answer()
    data = load_data()
    _, wid, comp_str, task_str = cb.data.split(":")
    comp_idx = int(comp_str); task_idx = int(task_str)
    ws = data["workspaces"].get(wid)
    if not ws or comp_idx>=len(ws["companies"]):
        return
    await BotState.rename_task.set()
    data["workspaces"][wid]["awaiting"] = {"type": "rename_task", "company_idx": comp_idx, "task_idx": task_idx}
    await save_data(data)
    await cb.message.answer("✏️ Введите новое название задачи:")

@dp.message_handler(state=BotState.rename_task, content_types=types.ContentTypes.TEXT)
async def process_rename_task(message: types.Message, state: FSMContext):
    text = message.text.strip()
    tid = message.message_thread_id or 0
    wid = ws_id(message.chat.id, tid)
    data = load_data()
    ws = data["workspaces"].get(wid)
    comp_idx = ws["awaiting"].get("company_idx", None) if ws.get("awaiting") else None
    task_idx = ws["awaiting"].get("task_idx", None) if ws.get("awaiting") else None
    if ws and comp_idx is not None and task_idx is not None:
        ws["companies"][comp_idx]["tasks"][task_idx]["text"] = text
        await save_data(data)
        # Удаляем запрос/ввод
        try: await message.delete()
        except: pass
        # Обновляем карточку
        comp = ws["companies"][comp_idx]
        card_text = f"📁 {comp['name']}:\n"
        for t in comp["tasks"]:
            ic = "✔" if t["done"] else "⬜"
            card_text += f"{ic} {t['text']}\n"
        try:
            await bot.edit_message_text(card_text, message.chat.id, message_id=comp["card_msg_id"])
        except:
            pass
        # Обновляем меню задач компании
        new_text = f"📁 {comp['name']}\n\n"
        kb = InlineKeyboardMarkup(row_width=1)
        for i, t in enumerate(comp["tasks"]):
            ic = "✔" if t["done"] else "⬜"
            new_text += f"{ic} {t['text']}\n"
            kb.add(InlineKeyboardButton(f"{ic} {t['text']}", callback_data=f"task_menu:{wid}:{comp_idx}:{i}"))
        kb.add(InlineKeyboardButton("➕ Добавить задачу", callback_data=f"add_task:{wid}:{comp_idx}"))
        kb.add(InlineKeyboardButton("✍️ Переименовать компанию", callback_data=f"rename_company:{wid}:{comp_idx}"))
        kb.add(InlineKeyboardButton("🗑 Удалить компанию", callback_data=f"delete_company:{wid}:{comp_idx}"))
        kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"back:{wid}"))
        try:
            await bot.edit_message_text(new_text, message.chat.id, message_id=ws["menu_msg_id"], reply_markup=kb)
        except:
            pass
    await state.finish()

# --- Шаблон задач ---
@dp.callback_query_handler(lambda c: c.data.startswith("t_open:"))
async def cb_template_open(cb: types.CallbackQuery):
    await cb.answer()
    data = load_data()
    _, wid, i_str = cb.data.split(":")
    i = int(i_str)
    ws = data["workspaces"].get(wid)
    if not ws or i>=len(ws["template"]):
        return
    text = f"⚙️ Шаблон: «{ws['template'][i]}»"
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("✍️ Переименовать", callback_data=f"t_rename:{wid}:{i}"))
    kb.add(InlineKeyboardButton("🗑 Удалить", callback_data=f"t_del:{wid}:{i}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"template:{wid}"))
    await cb.message.edit_text(text, reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith("t_del:"))
async def cb_template_del(cb: types.CallbackQuery):
    await cb.answer()
    data = load_data()
    _, wid, i_str = cb.data.split(":")
    i = int(i_str)
    ws = data["workspaces"].get(wid)
    if not ws or i>=len(ws["template"]):
        return
    ws["template"].pop(i)
    await save_data(data)
    # Обновляем меню шаблона
    await cb.message.edit_text("⚙️ Шаблон задач", reply_markup=template_kb(wid, ws))

@dp.callback_query_handler(lambda c: c.data.startswith("t_add:"))
async def cb_template_add(cb: types.CallbackQuery):
    await cb.answer()
    _, wid = cb.data.split(":")
    data = load_data()
    ws = data["workspaces"].get(wid)
    if not ws:
        return
    await BotState.new_template.set()
    data["workspaces"][wid]["awaiting"] = {"type": "new_template"}
    await save_data(data)
    await cb.message.answer("✏️ Введите новую задачу для шаблона:")

@dp.message_handler(state=BotState.new_template, content_types=types.ContentTypes.TEXT)
async def process_new_template(message: types.Message, state: FSMContext):
    text = message.text.strip()
    tid = message.message_thread_id or 0
    wid = ws_id(message.chat.id, tid)
    data = load_data()
    ws = data["workspaces"].get(wid)
    if ws:
        ws["template"].append(text)
        await save_data(data)
        try: await message.delete()
        except: pass
        # Обновляем меню шаблона
        try:
            await bot.edit_message_text("⚙️ Шаблон задач", message.chat.id, message_id=ws["menu_msg_id"],
                                        reply_markup=template_kb(wid, ws))
        except:
            pass
    await state.finish()

@dp.callback_query_handler(lambda c: c.data.startswith("t_rename:"))
async def cb_template_rename(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, i_str = cb.data.split(":")
    i = int(i_str)
    data = load_data()
    ws = data["workspaces"].get(wid)
    if not ws or i>=len(ws["template"]):
        return
    await BotState.rename_template.set()
    data["workspaces"][wid]["awaiting"] = {"type": "rename_template", "task_idx": i}
    await save_data(data)
    await cb.message.answer("✏️ Введите новое название задачи шаблона:")

@dp.message_handler(state=BotState.rename_template, content_types=types.ContentTypes.TEXT)
async def process_rename_template(message: types.Message, state: FSMContext):
    text = message.text.strip()
    tid = message.message_thread_id or 0
    wid = ws_id(message.chat.id, tid)
    data = load_data()
    ws = data["workspaces"].get(wid)
    idx = ws["awaiting"].get("task_idx", None) if ws.get("awaiting") else None
    if ws and idx is not None:
        ws["template"][idx] = text
        await save_data(data)
        try: await message.delete()
        except: pass
        # Обновляем меню шаблона
        try:
            await bot.edit_message_text("⚙️ Шаблон задач", message.chat.id, message_id=ws["menu_msg_id"],
                                        reply_markup=template_kb(wid, ws))
        except:
            pass
    await state.finish()

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
