import os
import json
import asyncio

from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils import executor
from aiogram.utils.exceptions import MessageNotModified

TOKEN = os.getenv("API_TOKEN")
if not TOKEN:
    raise RuntimeError("API_TOKEN is not set")

bot = Bot(token=TOKEN)
dp = Dispatcher(bot)

DATA_FILE = "data.json"
lock = asyncio.Lock()


# =========================
# DATA
# =========================

def default_data():
    return {"users": {}, "workspaces": {}}


async def load_data():
    if not os.path.exists(DATA_FILE):
        return default_data()
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default_data()


async def save_data(data):
    async with lock:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)


def ensure_user(data, user_id: str):
    data["users"].setdefault(
        user_id,
        {
            "workspaces": [],
            "pm_menu_msg_id": None,
            "help_msg_id": None,
        },
    )
    return data["users"][user_id]


def make_ws_id(chat_id: int, thread_id: int | None):
    return f"{chat_id}_{thread_id or 0}"


def thread_kwargs(thread_id: int):
    return {"message_thread_id": thread_id} if thread_id else {}


def clean_text(text: str) -> str:
    return (text or "").strip().lstrip("/").strip()


def company_card_text(company: dict) -> str:
    lines = [f"📁 {company['name']}:"]
    if company["tasks"]:
        for task in company["tasks"]:
            icon = "✔" if task["done"] else "⬜"
            lines.append(f"{icon} {task['text']}")
    else:
        lines.append("—")
    return "\n".join(lines)


def pm_main_text(user_id: str, data: dict) -> str:
    lines = ["📂 Ваши workspace:"]
    workspaces = data["users"].get(user_id, {}).get("workspaces", [])
    if not workspaces:
        lines.append("Нет workspace")
    else:
        for wid in workspaces:
            ws = data["workspaces"].get(wid)
            if ws:
                lines.append(f"• {ws['name']}")
    return "\n".join(lines)


# =========================
# KEYBOARDS
# =========================

def pm_main_kb(user_id: str, data: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    for wid in data["users"].get(user_id, {}).get("workspaces", []):
        ws = data["workspaces"].get(wid)
        if ws:
            kb.add(InlineKeyboardButton(ws["name"], callback_data=f"pmws:{wid}"))
    kb.add(InlineKeyboardButton("➕ Подключить workspace", callback_data="pmhelp:root"))
    kb.add(InlineKeyboardButton("🔄 Обновить", callback_data="pmrefresh:root"))
    return kb


def pm_ws_manage_kb(wid: str):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("🗑 Удалить workspace", callback_data=f"pmwsdel:{wid}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data="pmrefresh:root"))
    return kb


def ws_home_kb(wid: str, ws: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    for idx, company in enumerate(ws["companies"]):
        kb.add(InlineKeyboardButton(company["name"], callback_data=f"cmp:{wid}:{idx}"))
    kb.add(InlineKeyboardButton("➕ Создать компанию", callback_data=f"cmpnew:{wid}"))
    kb.add(InlineKeyboardButton("⚙️ Шаблон задач", callback_data=f"tpl:{wid}"))
    return kb


def company_menu_kb(wid: str, company_idx: int, company: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    for task_idx, task in enumerate(company["tasks"]):
        icon = "✔" if task["done"] else "⬜"
        kb.add(
            InlineKeyboardButton(
                f"{icon} {task['text']}",
                callback_data=f"task:{wid}:{company_idx}:{task_idx}",
            )
        )
    kb.add(InlineKeyboardButton("➕ Добавить задачу", callback_data=f"tasknew:{wid}:{company_idx}"))
    kb.add(InlineKeyboardButton("✍️ Переименовать компанию", callback_data=f"cmpren:{wid}:{company_idx}"))
    kb.add(InlineKeyboardButton("🗑 Удалить компанию", callback_data=f"cmpdel:{wid}:{company_idx}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"backws:{wid}"))
    return kb


def task_menu_kb(wid: str, company_idx: int, task_idx: int, task: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    if task["done"]:
        kb.add(InlineKeyboardButton("❌ Отменить выполнение", callback_data=f"taskdone:{wid}:{company_idx}:{task_idx}"))
    else:
        kb.add(InlineKeyboardButton("✔ Отметить выполненной", callback_data=f"taskdone:{wid}:{company_idx}:{task_idx}"))
    kb.add(InlineKeyboardButton("✍️ Переименовать", callback_data=f"taskren:{wid}:{company_idx}:{task_idx}"))
    kb.add(InlineKeyboardButton("🗑 Удалить задачу", callback_data=f"taskdel:{wid}:{company_idx}:{task_idx}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"cmp:{wid}:{company_idx}"))
    return kb


def template_menu_kb(wid: str, ws: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    for idx, task in enumerate(ws["template"]):
        kb.add(InlineKeyboardButton(task, callback_data=f"tplitem:{wid}:{idx}"))
    kb.add(InlineKeyboardButton("➕ Добавить задачу", callback_data=f"tplnew:{wid}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"backws:{wid}"))
    return kb


def template_item_kb(wid: str, template_idx: int):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("✍️ Переименовать", callback_data=f"tplren:{wid}:{template_idx}"))
    kb.add(InlineKeyboardButton("🗑 Удалить", callback_data=f"tpldel:{wid}:{template_idx}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"tpl:{wid}"))
    return kb


def prompt_kb(wid: str):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"cancel:{wid}"))
    return kb


# =========================
# SAFE HELPERS
# =========================

async def safe_delete_message(chat_id: int, message_id: int | None):
    if not message_id:
        return
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception:
        pass


async def safe_edit_text(chat_id: int, message_id: int, text: str, reply_markup=None):
    try:
        await bot.edit_message_text(text, chat_id, message_id, reply_markup=reply_markup)
    except MessageNotModified:
        pass
    except Exception:
        pass


async def send_temp_message(chat_id: int, text: str, thread_id: int = 0, delay: int = 8):
    msg = await bot.send_message(chat_id, text, **thread_kwargs(thread_id))

    async def remover():
        await asyncio.sleep(delay)
        try:
            await bot.delete_message(chat_id, msg.message_id)
        except Exception:
            pass

    asyncio.create_task(remover())


async def send_week_notice_pm(user_id: str, text: str):
    msg = await bot.send_message(int(user_id), text)

    async def remover():
        await asyncio.sleep(7 * 24 * 3600)
        try:
            await bot.delete_message(int(user_id), msg.message_id)
        except Exception:
            pass

    asyncio.create_task(remover())


async def update_pm_menu(user_id: str, data: dict):
    user = ensure_user(data, user_id)
    text = pm_main_text(user_id, data)
    kb = pm_main_kb(user_id, data)

    if user.get("pm_menu_msg_id"):
        try:
            await bot.edit_message_text(
                text,
                int(user_id),
                user["pm_menu_msg_id"],
                reply_markup=kb,
            )
            return
        except Exception:
            user["pm_menu_msg_id"] = None

    try:
        msg = await bot.send_message(int(user_id), text, reply_markup=kb)
        user["pm_menu_msg_id"] = msg.message_id
    except Exception:
        pass


async def update_company_card(ws: dict, company_idx: int):
    if company_idx < 0 or company_idx >= len(ws["companies"]):
        return
    company = ws["companies"][company_idx]
    if not company.get("card_msg_id"):
        return
    await safe_edit_text(
        ws["chat_id"],
        company["card_msg_id"],
        company_card_text(company),
    )


async def delete_old_prompt_if_any(ws: dict):
    awaiting = ws.get("awaiting")
    if awaiting and awaiting.get("prompt_msg_id"):
        await safe_delete_message(ws["chat_id"], awaiting["prompt_msg_id"])


async def set_prompt(ws: dict, prompt_text: str, awaiting_payload: dict):
    await delete_old_prompt_if_any(ws)
    msg = await bot.send_message(
        ws["chat_id"],
        prompt_text,
        reply_markup=prompt_kb(ws["id"]),
        **thread_kwargs(ws["thread_id"]),
    )
    awaiting_payload["prompt_msg_id"] = msg.message_id
    ws["awaiting"] = awaiting_payload


async def send_or_replace_ws_home_menu(data: dict, wid: str):
    ws = data["workspaces"].get(wid)
    if not ws:
        return

    old_menu_id = ws.get("menu_msg_id")
    if old_menu_id:
        await safe_delete_message(ws["chat_id"], old_menu_id)

    msg = await bot.send_message(
        ws["chat_id"],
        "📂 Меню workspace",
        reply_markup=ws_home_kb(wid, ws),
        **thread_kwargs(ws["thread_id"]),
    )
    ws["menu_msg_id"] = msg.message_id


async def edit_ws_home_menu(data: dict, wid: str):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("menu_msg_id"):
        return
    await safe_edit_text(
        ws["chat_id"],
        ws["menu_msg_id"],
        "📂 Меню workspace",
        reply_markup=ws_home_kb(wid, ws),
    )


async def edit_company_menu(data: dict, wid: str, company_idx: int):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("menu_msg_id"):
        return
    if company_idx < 0 or company_idx >= len(ws["companies"]):
        await edit_ws_home_menu(data, wid)
        return

    company = ws["companies"][company_idx]
    text_lines = [f"📁 {company['name']}", ""]
    if company["tasks"]:
        for task in company["tasks"]:
            icon = "✔" if task["done"] else "⬜"
            text_lines.append(f"{icon} {task['text']}")
    else:
        text_lines.append("—")

    await safe_edit_text(
        ws["chat_id"],
        ws["menu_msg_id"],
        "\n".join(text_lines),
        reply_markup=company_menu_kb(wid, company_idx, company),
    )


async def edit_task_menu(data: dict, wid: str, company_idx: int, task_idx: int):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("menu_msg_id"):
        return
    if company_idx < 0 or company_idx >= len(ws["companies"]):
        await edit_ws_home_menu(data, wid)
        return

    company = ws["companies"][company_idx]
    if task_idx < 0 or task_idx >= len(company["tasks"]):
        await edit_company_menu(data, wid, company_idx)
        return

    task = company["tasks"][task_idx]
    text = f"📌 {task['text']}"
    await safe_edit_text(
        ws["chat_id"],
        ws["menu_msg_id"],
        text,
        reply_markup=task_menu_kb(wid, company_idx, task_idx, task),
    )


async def edit_template_menu(data: dict, wid: str):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("menu_msg_id"):
        return
    await safe_edit_text(
        ws["chat_id"],
        ws["menu_msg_id"],
        "⚙️ Шаблон задач",
        reply_markup=template_menu_kb(wid, ws),
    )


async def edit_template_item_menu(data: dict, wid: str, template_idx: int):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("menu_msg_id"):
        return
    if template_idx < 0 or template_idx >= len(ws["template"]):
        await edit_template_menu(data, wid)
        return

    text = f"⚙️ {ws['template'][template_idx]}"
    await safe_edit_text(
        ws["chat_id"],
        ws["menu_msg_id"],
        text,
        reply_markup=template_item_kb(wid, template_idx),
    )


def company_exists(ws: dict, name: str, exclude_idx: int | None = None) -> bool:
    target = name.casefold()
    for idx, company in enumerate(ws["companies"]):
        if exclude_idx is not None and idx == exclude_idx:
            continue
        if company["name"].casefold() == target:
            return True
    return False


# =========================
# PM HANDLERS
# =========================

@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    if message.chat.type != "private":
        return

    data = await load_data()
    uid = str(message.from_user.id)
    user = ensure_user(data, uid)

    text = pm_main_text(uid, data)
    msg = await message.answer(text, reply_markup=pm_main_kb(uid, data))
    user["pm_menu_msg_id"] = msg.message_id
    await save_data(data)


@dp.callback_query_handler(lambda c: c.data == "pmrefresh:root")
async def pm_refresh(cb: types.CallbackQuery):
    await cb.answer()
    if cb.message.chat.type != "private":
        return

    data = await load_data()
    uid = str(cb.from_user.id)
    user = ensure_user(data, uid)
    user["pm_menu_msg_id"] = cb.message.message_id
    await save_data(data)

    await cb.message.edit_text(
        pm_main_text(uid, data),
        reply_markup=pm_main_kb(uid, data),
    )


@dp.callback_query_handler(lambda c: c.data == "pmhelp:root")
async def pm_help(cb: types.CallbackQuery):
    await cb.answer()
    if cb.message.chat.type != "private":
        return

    data = await load_data()
    uid = str(cb.from_user.id)
    user = ensure_user(data, uid)

    if user.get("help_msg_id"):
        await safe_delete_message(int(uid), user["help_msg_id"])

    msg = await cb.message.answer(
        "📌 Как подключить workspace:\n\n"
        "1) Перейдите в нужный тред группы\n"
        "2) Отправьте команду /connect"
    )
    user["help_msg_id"] = msg.message_id
    await save_data(data)


@dp.callback_query_handler(lambda c: c.data.startswith("pmws:"))
async def pm_open_workspace(cb: types.CallbackQuery):
    await cb.answer()
    if cb.message.chat.type != "private":
        return

    data = await load_data()
    uid = str(cb.from_user.id)
    wid = cb.data.split(":", 1)[1]

    ws = data["workspaces"].get(wid)
    if not ws or wid not in data["users"].get(uid, {}).get("workspaces", []):
        await cb.answer("Workspace не найден", show_alert=False)
        await cb.message.edit_text(
            pm_main_text(uid, data),
            reply_markup=pm_main_kb(uid, data),
        )
        return

    await cb.message.edit_text(
        f"📂 {ws['name']}",
        reply_markup=pm_ws_manage_kb(wid),
    )


@dp.callback_query_handler(lambda c: c.data.startswith("pmwsdel:"))
async def pm_delete_workspace(cb: types.CallbackQuery):
    await cb.answer()
    if cb.message.chat.type != "private":
        return

    data = await load_data()
    uid = str(cb.from_user.id)
    wid = cb.data.split(":", 1)[1]
    ws = data["workspaces"].get(wid)

    if not ws:
        await cb.message.edit_text(
            pm_main_text(uid, data),
            reply_markup=pm_main_kb(uid, data),
        )
        return

    # Удаляем prompt/menu/cards в треде
    await safe_delete_message(ws["chat_id"], ws.get("menu_msg_id"))
    if ws.get("awaiting", {}).get("prompt_msg_id"):
        await safe_delete_message(ws["chat_id"], ws["awaiting"]["prompt_msg_id"])
    for company in ws["companies"]:
        await safe_delete_message(ws["chat_id"], company.get("card_msg_id"))

    # Удаляем ws у всех пользователей
    for user_id, user in data["users"].items():
        if wid in user.get("workspaces", []):
            user["workspaces"].remove(wid)

    ws_name = ws["name"]
    thread_id = ws["thread_id"]
    chat_id = ws["chat_id"]

    data["workspaces"].pop(wid, None)
    await save_data(data)

    # Уведомления
    await cb.message.edit_text(
        pm_main_text(uid, data),
        reply_markup=pm_main_kb(uid, data),
    )
    await send_temp_message(int(uid), f"🗑 Workspace «{ws_name}» удалён", 0, delay=10)
    await send_temp_message(chat_id, f"🗑 Workspace «{ws_name}» удалён", thread_id, delay=10)


# =========================
# CONNECT
# =========================

@dp.message_handler(commands=["connect"])
async def cmd_connect(message: types.Message):
    if message.chat.type == "private":
        return

    data = await load_data()
    uid = str(message.from_user.id)
    ensure_user(data, uid)

    thread_id = message.message_thread_id or 0
    wid = make_ws_id(message.chat.id, thread_id)

    existing_ws = data["workspaces"].get(wid)
    if existing_ws:
        await safe_delete_message(existing_ws["chat_id"], existing_ws.get("menu_msg_id"))
        if existing_ws.get("awaiting", {}).get("prompt_msg_id"):
            await safe_delete_message(existing_ws["chat_id"], existing_ws["awaiting"]["prompt_msg_id"])

    ws = data["workspaces"].setdefault(
        wid,
        {
            "id": wid,
            "name": message.chat.title or "Workspace",
            "chat_id": message.chat.id,
            "thread_id": thread_id,
            "menu_msg_id": None,
            "template": ["Создать договор", "Выставить счёт"],
            "companies": [],
            "awaiting": None,
        },
    )

    ws["id"] = wid
    ws["name"] = message.chat.title or "Workspace"
    ws["chat_id"] = message.chat.id
    ws["thread_id"] = thread_id
    ws["awaiting"] = None

    if wid not in data["users"][uid]["workspaces"]:
        data["users"][uid]["workspaces"].append(wid)

    # удаляем help в ЛС
    help_msg_id = data["users"][uid].get("help_msg_id")
    if help_msg_id:
        await safe_delete_message(int(uid), help_msg_id)
        data["users"][uid]["help_msg_id"] = None

    await send_or_replace_ws_home_menu(data, wid)
    await update_pm_menu(uid, data)
    await save_data(data)

    try:
        await send_week_notice_pm(uid, f"Workspace «{ws['name']}» подключён")
    except Exception:
        pass


# =========================
# GROUP MENU NAVIGATION
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("backws:"))
async def back_to_ws(cb: types.CallbackQuery):
    await cb.answer()
    wid = cb.data.split(":", 1)[1]
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws:
        await cb.answer("Workspace удалён", show_alert=False)
        return
    await edit_ws_home_menu(data, wid)


@dp.callback_query_handler(lambda c: c.data.startswith("cmp:"))
async def open_company(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx = cb.data.split(":")
    company_idx = int(company_idx)

    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws:
        await cb.answer("Workspace удалён", show_alert=False)
        return

    await edit_company_menu(data, wid, company_idx)


@dp.callback_query_handler(lambda c: c.data.startswith("task:"))
async def open_task_menu(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx, task_idx = cb.data.split(":")
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws:
        await cb.answer("Workspace удалён", show_alert=False)
        return

    await edit_task_menu(data, wid, int(company_idx), int(task_idx))


@dp.callback_query_handler(lambda c: c.data.startswith("tpl:"))
async def open_template_menu(cb: types.CallbackQuery):
    await cb.answer()
    wid = cb.data.split(":", 1)[1]
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws:
        await cb.answer("Workspace удалён", show_alert=False)
        return

    await edit_template_menu(data, wid)


@dp.callback_query_handler(lambda c: c.data.startswith("tplitem:"))
async def open_template_item(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, template_idx = cb.data.split(":")
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws:
        await cb.answer("Workspace удалён", show_alert=False)
        return

    await edit_template_item_menu(data, wid, int(template_idx))


# =========================
# PROMPT / CANCEL
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("cancel:"))
async def cancel_input(cb: types.CallbackQuery):
    await cb.answer()
    wid = cb.data.split(":", 1)[1]

    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws:
        return

    awaiting = ws.get("awaiting")
    if awaiting:
        await safe_delete_message(ws["chat_id"], awaiting.get("prompt_msg_id"))
        back_to = awaiting.get("back_to", {"view": "ws"})
        ws["awaiting"] = None
        await save_data(data)

        if back_to["view"] == "company":
            await edit_company_menu(data, wid, back_to["company_idx"])
        elif back_to["view"] == "template":
            await edit_template_menu(data, wid)
        else:
            await edit_ws_home_menu(data, wid)


# =========================
# CREATE / RENAME / DELETE COMPANY
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("cmpnew:"))
async def create_company_prompt(cb: types.CallbackQuery):
    await cb.answer()
    wid = cb.data.split(":", 1)[1]

    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws:
        await cb.answer("Workspace удалён", show_alert=False)
        return

    await set_prompt(
        ws,
        "✏️ Напишите название компании:",
        {
            "type": "new_company",
            "back_to": {"view": "ws"},
        },
    )
    await save_data(data)


@dp.callback_query_handler(lambda c: c.data.startswith("cmpren:"))
async def rename_company_prompt(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx = cb.data.split(":")
    company_idx = int(company_idx)

    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws:
        return

    await set_prompt(
        ws,
        "✏️ Введите новое название компании:",
        {
            "type": "rename_company",
            "company_idx": company_idx,
            "back_to": {"view": "company", "company_idx": company_idx},
        },
    )
    await save_data(data)


@dp.callback_query_handler(lambda c: c.data.startswith("cmpdel:"))
async def delete_company(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx = cb.data.split(":")
    company_idx = int(company_idx)

    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws:
        return
    if company_idx < 0 or company_idx >= len(ws["companies"]):
        return

    company = ws["companies"].pop(company_idx)
    await safe_delete_message(ws["chat_id"], company.get("card_msg_id"))
    await send_or_replace_ws_home_menu(data, wid)
    await save_data(data)


# =========================
# TASK ACTIONS
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("tasknew:"))
async def add_task_prompt(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx = cb.data.split(":")
    company_idx = int(company_idx)

    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws:
        return

    await set_prompt(
        ws,
        "✏️ Введите текст новой задачи:",
        {
            "type": "new_task",
            "company_idx": company_idx,
            "back_to": {"view": "company", "company_idx": company_idx},
        },
    )
    await save_data(data)


@dp.callback_query_handler(lambda c: c.data.startswith("taskren:"))
async def rename_task_prompt(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx, task_idx = cb.data.split(":")
    company_idx = int(company_idx)
    task_idx = int(task_idx)

    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws:
        return

    await set_prompt(
        ws,
        "✏️ Введите новое название задачи:",
        {
            "type": "rename_task",
            "company_idx": company_idx,
            "task_idx": task_idx,
            "back_to": {"view": "company", "company_idx": company_idx},
        },
    )
    await save_data(data)


@dp.callback_query_handler(lambda c: c.data.startswith("taskdel:"))
async def delete_task(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx, task_idx = cb.data.split(":")
    company_idx = int(company_idx)
    task_idx = int(task_idx)

    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws:
        return
    if company_idx < 0 or company_idx >= len(ws["companies"]):
        return

    company = ws["companies"][company_idx]
    if task_idx < 0 or task_idx >= len(company["tasks"]):
        return

    company["tasks"].pop(task_idx)
    await save_data(data)

    await update_company_card(ws, company_idx)
    await edit_company_menu(data, wid, company_idx)


@dp.callback_query_handler(lambda c: c.data.startswith("taskdone:"))
async def toggle_task_done(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx, task_idx = cb.data.split(":")
    company_idx = int(company_idx)
    task_idx = int(task_idx)

    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws:
        return
    if company_idx < 0 or company_idx >= len(ws["companies"]):
        return

    company = ws["companies"][company_idx]
    if task_idx < 0 or task_idx >= len(company["tasks"]):
        return

    company["tasks"][task_idx]["done"] = not company["tasks"][task_idx]["done"]
    await save_data(data)

    await update_company_card(ws, company_idx)
    await edit_task_menu(data, wid, company_idx, task_idx)


# =========================
# TEMPLATE ACTIONS
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("tplnew:"))
async def add_template_task_prompt(cb: types.CallbackQuery):
    await cb.answer()
    wid = cb.data.split(":", 1)[1]

    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws:
        return

    await set_prompt(
        ws,
        "✏️ Введите название новой задачи шаблона:",
        {
            "type": "new_template_task",
            "back_to": {"view": "template"},
        },
    )
    await save_data(data)


@dp.callback_query_handler(lambda c: c.data.startswith("tplren:"))
async def rename_template_task_prompt(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, template_idx = cb.data.split(":")
    template_idx = int(template_idx)

    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws:
        return

    await set_prompt(
        ws,
        "✏️ Введите новое название задачи шаблона:",
        {
            "type": "rename_template_task",
            "template_idx": template_idx,
            "back_to": {"view": "template"},
        },
    )
    await save_data(data)


@dp.callback_query_handler(lambda c: c.data.startswith("tpldel:"))
async def delete_template_task(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, template_idx = cb.data.split(":")
    template_idx = int(template_idx)

    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws:
        return
    if template_idx < 0 or template_idx >= len(ws["template"]):
        return

    ws["template"].pop(template_idx)
    await save_data(data)
    await edit_template_menu(data, wid)


# =========================
# GROUP TEXT INPUT HANDLER
# =========================

@dp.message_handler(content_types=types.ContentTypes.TEXT)
async def handle_group_text(message: types.Message):
    if message.chat.type == "private":
        return

    data = await load_data()
    wid = make_ws_id(message.chat.id, message.message_thread_id or 0)
    ws = data["workspaces"].get(wid)

    if not ws or not ws.get("awaiting"):
        return

    awaiting = ws["awaiting"]
    mode = awaiting.get("type")
    text = clean_text(message.text)

    if not text:
        return

    prompt_msg_id = awaiting.get("prompt_msg_id")

    # ========= NEW COMPANY =========
    if mode == "new_company":
        if company_exists(ws, text):
            await send_temp_message(ws["chat_id"], "Такая компания уже существует.", ws["thread_id"], delay=6)
            return

        company = {
            "name": text,
            "tasks": [{"text": t, "done": False} for t in ws["template"]],
            "card_msg_id": None,
        }

        card_msg = await bot.send_message(
            ws["chat_id"],
            company_card_text(company),
            **thread_kwargs(ws["thread_id"]),
        )
        company["card_msg_id"] = card_msg.message_id
        ws["companies"].append(company)
        ws["awaiting"] = None

        await save_data(data)
        await safe_delete_message(ws["chat_id"], prompt_msg_id)
        try:
            await message.delete()
        except Exception:
            pass

        await send_or_replace_ws_home_menu(data, wid)
        await save_data(data)
        return

    # ========= RENAME COMPANY =========
    if mode == "rename_company":
        company_idx = awaiting["company_idx"]
        if company_idx < 0 or company_idx >= len(ws["companies"]):
            ws["awaiting"] = None
            await save_data(data)
            return

        if company_exists(ws, text, exclude_idx=company_idx):
            await send_temp_message(ws["chat_id"], "Такая компания уже существует.", ws["thread_id"], delay=6)
            return

        ws["companies"][company_idx]["name"] = text
        ws["awaiting"] = None
        await save_data(data)

        await safe_delete_message(ws["chat_id"], prompt_msg_id)
        try:
            await message.delete()
        except Exception:
            pass

        await update_company_card(ws, company_idx)
        await edit_company_menu(data, wid, company_idx)
        await send_temp_message(ws["chat_id"], "✅ Новое название компании сохранено", ws["thread_id"], delay=6)
        return

    # ========= NEW TASK =========
    if mode == "new_task":
        company_idx = awaiting["company_idx"]
        if company_idx < 0 or company_idx >= len(ws["companies"]):
            ws["awaiting"] = None
            await save_data(data)
            return

        ws["companies"][company_idx]["tasks"].append({"text": text, "done": False})
        ws["awaiting"] = None
        await save_data(data)

        await safe_delete_message(ws["chat_id"], prompt_msg_id)
        try:
            await message.delete()
        except Exception:
            pass

        await update_company_card(ws, company_idx)
        await edit_company_menu(data, wid, company_idx)
        return

    # ========= RENAME TASK =========
    if mode == "rename_task":
        company_idx = awaiting["company_idx"]
        task_idx = awaiting["task_idx"]

        if company_idx < 0 or company_idx >= len(ws["companies"]):
            ws["awaiting"] = None
            await save_data(data)
            return

        company = ws["companies"][company_idx]
        if task_idx < 0 or task_idx >= len(company["tasks"]):
            ws["awaiting"] = None
            await save_data(data)
            return

        company["tasks"][task_idx]["text"] = text
        ws["awaiting"] = None
        await save_data(data)

        await safe_delete_message(ws["chat_id"], prompt_msg_id)
        try:
            await message.delete()
        except Exception:
            pass

        await update_company_card(ws, company_idx)
        await edit_company_menu(data, wid, company_idx)
        await send_temp_message(ws["chat_id"], "✅ Название задачи обновлено", ws["thread_id"], delay=6)
        return

    # ========= NEW TEMPLATE TASK =========
    if mode == "new_template_task":
        ws["template"].append(text)
        ws["awaiting"] = None
        await save_data(data)

        await safe_delete_message(ws["chat_id"], prompt_msg_id)
        try:
            await message.delete()
        except Exception:
            pass

        await edit_template_menu(data, wid)
        return

    # ========= RENAME TEMPLATE TASK =========
    if mode == "rename_template_task":
        template_idx = awaiting["template_idx"]
        if template_idx < 0 or template_idx >= len(ws["template"]):
            ws["awaiting"] = None
            await save_data(data)
            return

        ws["template"][template_idx] = text
        ws["awaiting"] = None
        await save_data(data)

        await safe_delete_message(ws["chat_id"], prompt_msg_id)
        try:
            await message.delete()
        except Exception:
            pass

        await edit_template_menu(data, wid)
        return


# =========================
# RUN
# =========================

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
