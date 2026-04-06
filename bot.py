import os
import json
import asyncio
import uuid
from typing import Optional

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
menu_locks: dict[str, asyncio.Lock] = {}
live_menu_ids: dict[str, int] = {}


# =========================
# DATA
# =========================


def default_data():
    return {
        "users": {},
        "workspaces": {},
        "mirror_tokens": {},
    }



def make_task(text: str, done: bool = False, category_id: Optional[str] = None):
    return {
        "id": uuid.uuid4().hex,
        "text": text,
        "done": done,
        "category_id": category_id,
    }



def make_category(name: str):
    return {
        "id": uuid.uuid4().hex,
        "name": name,
    }



def normalize_template(template):
    if isinstance(template, list):
        return {
            "tasks": [make_task(str(item), False, None) for item in template],
            "categories": [],
        }

    if not isinstance(template, dict):
        template = {}

    template.setdefault("tasks", [])
    template.setdefault("categories", [])

    if not isinstance(template["tasks"], list):
        template["tasks"] = []
    if not isinstance(template["categories"], list):
        template["categories"] = []

    for idx, category in enumerate(template["categories"]):
        if not isinstance(category, dict):
            template["categories"][idx] = make_category(str(category))
            category = template["categories"][idx]
        category.setdefault("id", uuid.uuid4().hex)
        category.setdefault("name", "Категория")

    valid_category_ids = {cat["id"] for cat in template["categories"]}

    for idx, task in enumerate(template["tasks"]):
        if not isinstance(task, dict):
            template["tasks"][idx] = make_task(str(task), False, None)
            task = template["tasks"][idx]
        task.setdefault("id", uuid.uuid4().hex)
        task.setdefault("text", "")
        task.setdefault("done", False)
        task.setdefault("category_id", None)
        if task.get("category_id") not in valid_category_ids:
            task["category_id"] = None

    return template



def normalize_company(company):
    if not isinstance(company, dict):
        company = {}

    company.setdefault("id", uuid.uuid4().hex)
    company.setdefault("name", "Компания")
    company.setdefault("tasks", [])
    company.setdefault("categories", [])
    company.setdefault("card_msg_id", None)
    company.setdefault("mirror", None)
    company.setdefault("mirror_history", {})
    company.setdefault("pending_mirror_msg_id", None)

    if not isinstance(company["tasks"], list):
        company["tasks"] = []
    if not isinstance(company["categories"], list):
        company["categories"] = []
    if not isinstance(company["mirror_history"], dict):
        company["mirror_history"] = {}

    for idx, category in enumerate(company["categories"]):
        if not isinstance(category, dict):
            company["categories"][idx] = make_category(str(category))
            category = company["categories"][idx]
        category.setdefault("id", uuid.uuid4().hex)
        category.setdefault("name", "Категория")

    valid_category_ids = {cat["id"] for cat in company["categories"]}

    for idx, task in enumerate(company["tasks"]):
        if not isinstance(task, dict):
            company["tasks"][idx] = make_task(str(task), False, None)
            task = company["tasks"][idx]
        task.setdefault("id", uuid.uuid4().hex)
        task.setdefault("text", "")
        task.setdefault("done", False)
        task.setdefault("category_id", None)
        if task.get("category_id") not in valid_category_ids:
            task["category_id"] = None

    return company



def normalize_data(data: dict) -> dict:
    if not isinstance(data, dict):
        data = {}

    data.setdefault("users", {})
    data.setdefault("workspaces", {})
    data.setdefault("mirror_tokens", {})

    for uid, user in list(data["users"].items()):
        if not isinstance(user, dict):
            data["users"][uid] = {}
            user = data["users"][uid]
        user.setdefault("workspaces", [])
        user.setdefault("pm_menu_msg_id", None)
        user.setdefault("help_msg_id", None)

    for wid, ws in list(data["workspaces"].items()):
        if not isinstance(ws, dict):
            data["workspaces"][wid] = {}
            ws = data["workspaces"][wid]

        ws.setdefault("id", wid)
        ws.setdefault("name", "Workspace")
        ws.setdefault("chat_title", None)
        ws.setdefault("topic_title", None)
        ws.setdefault("chat_id", None)
        ws.setdefault("thread_id", 0)
        ws.setdefault("menu_msg_id", None)
        ws["template"] = normalize_template(ws.get("template", []))
        ws.setdefault("companies", [])
        ws.setdefault("awaiting", None)
        ws.setdefault("is_connected", True)

        if not isinstance(ws["companies"], list):
            ws["companies"] = []

        for idx, company in enumerate(ws["companies"]):
            ws["companies"][idx] = normalize_company(company)

    valid_tokens = {}
    for token, payload in list(data["mirror_tokens"].items()):
        if not isinstance(payload, dict):
            continue
        source_wid = payload.get("source_wid")
        if not source_wid:
            continue
        if payload.get("company_id"):
            valid_tokens[token] = payload
    data["mirror_tokens"] = valid_tokens
    return data


async def load_data_unlocked():
    if not os.path.exists(DATA_FILE):
        return default_data()
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return normalize_data(data)
    except Exception:
        return default_data()


async def save_data_unlocked(data):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(normalize_data(data), f, ensure_ascii=False, indent=2)


async def load_data():
    async with lock:
        return await load_data_unlocked()


async def save_data(data):
    async with lock:
        await save_data_unlocked(data)


# =========================
# BASICS
# =========================


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



def make_ws_id(chat_id: int, thread_id: Optional[int]):
    return f"{chat_id}_{thread_id or 0}"



def thread_kwargs(thread_id: int):
    return {"message_thread_id": thread_id} if thread_id else {}



def clean_text(text: str) -> str:
    return (text or "").strip().lstrip("/").strip()



def is_known_command(text: str) -> bool:
    if not text or not text.startswith("/"):
        return False
    head = text.split()[0].lower()
    return head in {"/start", "/connect", "/mirror"}



def workspace_full_name(chat_title: str, topic_title: Optional[str], thread_id: int) -> str:
    if thread_id:
        return f"{chat_title} - {(topic_title or f'Тред {thread_id}').strip()}"
    return chat_title



def extract_topic_title(message: types.Message) -> Optional[str]:
    if getattr(message, "forum_topic_created", None):
        return message.forum_topic_created.name
    if getattr(message, "forum_topic_edited", None):
        new_name = getattr(message.forum_topic_edited, "name", None)
        if new_name:
            return new_name
    reply = getattr(message, "reply_to_message", None)
    if reply:
        if getattr(reply, "forum_topic_created", None):
            return reply.forum_topic_created.name
        if getattr(reply, "forum_topic_edited", None):
            new_name = getattr(reply.forum_topic_edited, "name", None)
            if new_name:
                return new_name
    return None



def is_topic_service_message(message: types.Message) -> bool:
    return bool(getattr(message, "forum_topic_created", None) or getattr(message, "forum_topic_edited", None))



def generate_mirror_token() -> str:
    return uuid.uuid4().hex[:8].upper()



def make_mirror_target_key(chat_id: int, thread_id: int) -> str:
    return f"{chat_id}:{thread_id or 0}"



def get_menu_lock(wid: str) -> asyncio.Lock:
    if wid not in menu_locks:
        menu_locks[wid] = asyncio.Lock()
    return menu_locks[wid]



def find_company_index_by_id(ws: dict, company_id: str) -> Optional[int]:
    for idx, company in enumerate(ws.get("companies", [])):
        if company.get("id") == company_id:
            return idx
    return None



def find_category_index(categories: list, category_id: str) -> Optional[int]:
    for idx, category in enumerate(categories):
        if category.get("id") == category_id:
            return idx
    return None



def company_exists(ws: dict, name: str, exclude_idx: Optional[int] = None) -> bool:
    target = name.casefold()
    for idx, company in enumerate(ws.get("companies", [])):
        if exclude_idx is not None and idx == exclude_idx:
            continue
        if company.get("name", "").casefold() == target:
            return True
    return False



def category_exists(categories: list, name: str, exclude_idx: Optional[int] = None) -> bool:
    target = name.casefold()
    for idx, category in enumerate(categories):
        if exclude_idx is not None and idx == exclude_idx:
            continue
        if category.get("name", "").casefold() == target:
            return True
    return False



def tasks_in_category(tasks: list, category_id: Optional[str]):
    return [task for task in tasks if task.get("category_id") == category_id]



def task_button_text(task: dict) -> str:
    icon = "✔" if task.get("done") else "⬜"
    return f"{icon} {task.get('text', '')}"



def company_card_text(company: dict) -> str:
    lines = [f"{company_display_name(company)}:"]
    uncategorized = tasks_in_category(company["tasks"], None)
    for task in uncategorized:
        lines.append(task_button_text(task))

    for category in company.get("categories", []):
        lines.append(f"    {category_display_name(category)}:")
        cat_tasks = tasks_in_category(company["tasks"], category["id"])
        for task in cat_tasks:
            icon = "✔" if task.get("done") else "⬜"
            lines.append(f"        {icon} {task.get('text', '')}")

    if len(lines) == 1:
        lines.append("—")
    return "\n".join(lines)



def pm_main_text(user_id: str, data: dict) -> str:
    lines = ["📂 Ваши workspace:"]
    items = data["users"].get(user_id, {}).get("workspaces", [])
    active_items = [wid for wid in items if data["workspaces"].get(wid, {}).get("is_connected")]
    if not active_items:
        lines.append("Нет workspace")
    else:
        for wid in active_items:
            ws = data["workspaces"].get(wid)
            if ws:
                lines.append(f"• {ws['name']}")
    return "\n".join(lines)



def extract_emoji_input(text: str) -> Optional[str]:
    value = (text or "").strip()
    if not value:
        return None
    if value != value.split()[0]:
        return None
    if len(value) > 8:
        return None
    if any(ch.isalnum() for ch in value):
        return None
    return value



def split_leading_visual_prefix(value: str) -> tuple[str, str]:
    value = (value or "").strip()
    if not value:
        return "", ""
    i = 0
    while i < len(value) and not value[i].isalnum():
        i += 1
    return value[:i].strip(), value[i:].strip()



def company_display_name(company: dict) -> str:
    prefix, rest = split_leading_visual_prefix(company.get("name") or "")
    rest = rest or "Компания"
    if prefix:
        return f"{prefix}{rest}"
    return f"📁{rest}"



def category_display_name(category: dict) -> str:
    prefix, rest = split_leading_visual_prefix(category.get("name") or "")
    rest = rest or "Категория"
    if prefix:
        return f"{prefix}{rest}"
    return f"📁{rest}"



def reassign_leading_emoji(name: str, emoji: str) -> str:
    _, rest = split_leading_visual_prefix(name)
    if not rest:
        return emoji
    return f"{emoji}{rest}"



def clear_pending_mirror_tokens_for_company(data: dict, wid: str, company_id: str):
    to_delete = []
    for token, payload in data.get("mirror_tokens", {}).items():
        if payload.get("source_wid") == wid and payload.get("company_id") == company_id:
            to_delete.append(token)
    for token in to_delete:
        data["mirror_tokens"].pop(token, None)


# =========================
# KEYBOARDS
# =========================


def pm_main_kb(user_id: str, data: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    for wid in data["users"].get(user_id, {}).get("workspaces", []):
        ws = data["workspaces"].get(wid)
        if ws and ws.get("is_connected"):
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



def create_company_mode_kb(wid: str):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("📋 По шаблону", callback_data=f"cmpnewmode:{wid}:template"))
    kb.add(InlineKeyboardButton("📝 Пустую", callback_data=f"cmpnewmode:{wid}:empty"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"backws:{wid}"))
    return kb



def company_menu_kb(wid: str, company_idx: int, company: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    for task_idx, task in enumerate(company["tasks"]):
        if task.get("category_id") is None:
            kb.add(InlineKeyboardButton(task_button_text(task), callback_data=f"task:{wid}:{company_idx}:{task_idx}"))
    for cat_idx, category in enumerate(company.get("categories", [])):
        kb.add(InlineKeyboardButton(category_display_name(category), callback_data=f"cat:{wid}:{company_idx}:{cat_idx}"))
    kb.add(InlineKeyboardButton("➕ Добавить задачу", callback_data=f"tasknew:{wid}:{company_idx}:root"))
    kb.add(InlineKeyboardButton("➕ Добавить категорию", callback_data=f"catnew:{wid}:{company_idx}"))
    kb.add(InlineKeyboardButton("⚙️ Настройки компании", callback_data=f"cmpset:{wid}:{company_idx}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"backws:{wid}"))
    return kb

def company_settings_kb(wid: str, company_idx: int, company: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("✍️ Переименовать компанию", callback_data=f"cmpren:{wid}:{company_idx}"))
    kb.add(InlineKeyboardButton("😀 Переприсвоить смайлик", callback_data=f"cmpemoji:{wid}:{company_idx}"))
    if company.get("mirror"):
        kb.add(InlineKeyboardButton("🔌 Отвязать список", callback_data=f"mirroroff:{wid}:{company_idx}"))
    else:
        kb.add(InlineKeyboardButton("📤 Дублировать список", callback_data=f"mirroron:{wid}:{company_idx}"))
    kb.add(InlineKeyboardButton("🗑 Удалить компанию", callback_data=f"cmpdel:{wid}:{company_idx}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"cmp:{wid}:{company_idx}"))
    return kb

def category_menu_kb(wid: str, company_idx: int, category_idx: int, company: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    category_id = company["categories"][category_idx]["id"]
    for task_idx, task in enumerate(company["tasks"]):
        if task.get("category_id") == category_id:
            kb.add(InlineKeyboardButton(task_button_text(task), callback_data=f"task:{wid}:{company_idx}:{task_idx}"))
    kb.add(InlineKeyboardButton("➕ Добавить задачу", callback_data=f"tasknew:{wid}:{company_idx}:{category_idx}"))
    kb.add(InlineKeyboardButton("⚙️ Настройки категории", callback_data=f"catset:{wid}:{company_idx}:{category_idx}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"cmp:{wid}:{company_idx}"))
    return kb

def category_settings_index_kb(wid: str, company_idx: int, company: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    for cat_idx, category in enumerate(company.get("categories", [])):
        kb.add(InlineKeyboardButton(category_display_name(category), callback_data=f"catset:{wid}:{company_idx}:{cat_idx}"))
    kb.add(InlineKeyboardButton("➕ Добавить категорию", callback_data=f"catnew:{wid}:{company_idx}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"cmp:{wid}:{company_idx}"))
    return kb



def category_settings_item_kb(wid: str, company_idx: int, category_idx: int):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("✍️ Переименовать", callback_data=f"catren:{wid}:{company_idx}:{category_idx}"))
    kb.add(InlineKeyboardButton("😀 Переприсвоить смайлик", callback_data=f"catemoji:{wid}:{company_idx}:{category_idx}"))
    kb.add(InlineKeyboardButton("🗑 Удалить", callback_data=f"catdel:{wid}:{company_idx}:{category_idx}"))
    kb.add(InlineKeyboardButton("🗑 Удалить с задачами", callback_data=f"catdelall:{wid}:{company_idx}:{category_idx}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"cmpset:{wid}:{company_idx}"))
    return kb



def task_menu_kb(wid: str, company_idx: int, task_idx: int, task: dict, company: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    if task["done"]:
        kb.add(InlineKeyboardButton("❌ Отменить выполнение", callback_data=f"taskdone:{wid}:{company_idx}:{task_idx}"))
    else:
        kb.add(InlineKeyboardButton("✔ Отметить выполненной", callback_data=f"taskdone:{wid}:{company_idx}:{task_idx}"))
    kb.add(InlineKeyboardButton("✍️ Переименовать", callback_data=f"taskren:{wid}:{company_idx}:{task_idx}"))
    if company.get("categories"):
        if task.get("category_id"):
            kb.add(InlineKeyboardButton("📥 Перевсунуть", callback_data=f"taskmovepick:{wid}:{company_idx}:{task_idx}"))
        else:
            kb.add(InlineKeyboardButton("📥 Всунуть в категорию", callback_data=f"taskmovepick:{wid}:{company_idx}:{task_idx}"))
    kb.add(InlineKeyboardButton("🗑 Удалить задачу", callback_data=f"taskdel:{wid}:{company_idx}:{task_idx}"))
    back_to = f"cat:{wid}:{company_idx}:{find_category_index(company.get('categories', []), task.get('category_id'))}" if task.get("category_id") and find_category_index(company.get("categories", []), task.get("category_id")) is not None else f"cmp:{wid}:{company_idx}"
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=back_to))
    return kb

def task_move_category_kb(wid: str, company_idx: int, task_idx: int, company: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    current_category_id = company["tasks"][task_idx].get("category_id")
    for cat_idx, category in enumerate(company.get("categories", [])):
        if category.get("id") == current_category_id:
            continue
        kb.add(InlineKeyboardButton(category_display_name(category), callback_data=f"taskmoveto:{wid}:{company_idx}:{task_idx}:{cat_idx}"))
    if current_category_id:
        kb.add(InlineKeyboardButton("📤 Высунуть", callback_data=f"taskuncat:{wid}:{company_idx}:{task_idx}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"task:{wid}:{company_idx}:{task_idx}"))
    return kb

def template_menu_kb(wid: str, ws: dict):
    template = ws["template"]
    kb = InlineKeyboardMarkup(row_width=1)
    for task_idx, task in enumerate(template["tasks"]):
        if task.get("category_id") is None:
            kb.add(InlineKeyboardButton(task.get("text", ""), callback_data=f"tpltask:{wid}:{task_idx}"))
    for cat_idx, category in enumerate(template.get("categories", [])):
        kb.add(InlineKeyboardButton(category_display_name(category), callback_data=f"tplcat:{wid}:{cat_idx}"))
    kb.add(InlineKeyboardButton("➕ Добавить задачу", callback_data=f"tplnew:{wid}:root"))
    kb.add(InlineKeyboardButton("➕ Добавить категорию", callback_data=f"tplcatnew:{wid}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"backws:{wid}"))
    return kb



def template_category_menu_kb(wid: str, cat_idx: int, ws: dict):
    template = ws["template"]
    category_id = template["categories"][cat_idx]["id"]
    kb = InlineKeyboardMarkup(row_width=1)
    for task_idx, task in enumerate(template["tasks"]):
        if task.get("category_id") == category_id:
            kb.add(InlineKeyboardButton(task.get("text", ""), callback_data=f"tpltask:{wid}:{task_idx}"))
    kb.add(InlineKeyboardButton("➕ Добавить задачу", callback_data=f"tplnew:{wid}:{cat_idx}"))
    kb.add(InlineKeyboardButton("⚙️ Настройки категории", callback_data=f"tplcatset:{wid}:{cat_idx}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"tpl:{wid}"))
    return kb



def template_category_settings_index_kb(wid: str, ws: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    for cat_idx, category in enumerate(ws["template"].get("categories", [])):
        kb.add(InlineKeyboardButton(category_display_name(category), callback_data=f"tplcatset:{wid}:{cat_idx}"))
    kb.add(InlineKeyboardButton("➕ Добавить категорию", callback_data=f"tplcatnew:{wid}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"tpl:{wid}"))
    return kb



def template_category_settings_item_kb(wid: str, cat_idx: int):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("✍️ Переименовать", callback_data=f"tplcatren:{wid}:{cat_idx}"))
    kb.add(InlineKeyboardButton("😀 Переприсвоить смайлик", callback_data=f"tplcatemoji:{wid}:{cat_idx}"))
    kb.add(InlineKeyboardButton("🗑 Удалить", callback_data=f"tplcatdel:{wid}:{cat_idx}"))
    kb.add(InlineKeyboardButton("🗑 Удалить с задачами", callback_data=f"tplcatdelall:{wid}:{cat_idx}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"tplcat:{wid}:{cat_idx}"))
    return kb



def template_task_menu_kb(wid: str, task_idx: int, ws: dict):
    template = ws["template"]
    task = template["tasks"][task_idx]
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("✍️ Переименовать", callback_data=f"tplren:{wid}:{task_idx}"))
    if template.get("categories"):
        if task.get("category_id"):
            kb.add(InlineKeyboardButton("📤 Высунуть из категории", callback_data=f"tpltaskuncat:{wid}:{task_idx}"))
        else:
            kb.add(InlineKeyboardButton("📥 Всунуть в категорию", callback_data=f"tpltaskmovepick:{wid}:{task_idx}"))
    kb.add(InlineKeyboardButton("🗑 Удалить", callback_data=f"tpldel:{wid}:{task_idx}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"tpl:{wid}"))
    return kb



def template_task_move_category_kb(wid: str, task_idx: int, ws: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    for cat_idx, category in enumerate(ws["template"].get("categories", [])):
        kb.add(InlineKeyboardButton(category_display_name(category), callback_data=f"tpltaskmoveto:{wid}:{task_idx}:{cat_idx}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"tpltask:{wid}:{task_idx}"))
    return kb



def prompt_kb(wid: str):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"cancel:{wid}"))
    return kb


# =========================
# SAFE HELPERS
# =========================


async def safe_delete_message(chat_id: int, message_id: Optional[int]):
    if not message_id:
        return
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception:
        pass


async def try_edit_text(chat_id: int, message_id: int, text: str, reply_markup=None) -> bool:
    try:
        await bot.edit_message_text(text, chat_id, message_id, reply_markup=reply_markup)
        return True
    except MessageNotModified:
        return True
    except Exception:
        return False


async def safe_edit_text(chat_id: int, message_id: int, text: str, reply_markup=None):
    await try_edit_text(chat_id, message_id, text, reply_markup=reply_markup)


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


async def try_delete_user_message(message: types.Message):
    try:
        await message.delete()
    except Exception:
        pass


async def update_pm_menu(user_id: str, data: dict):
    user = ensure_user(data, user_id)
    text = pm_main_text(user_id, data)
    kb = pm_main_kb(user_id, data)

    if user.get("pm_menu_msg_id"):
        try:
            await bot.edit_message_text(text, int(user_id), user["pm_menu_msg_id"], reply_markup=kb)
            return
        except MessageNotModified:
            return
        except Exception:
            user["pm_menu_msg_id"] = None

    try:
        msg = await bot.send_message(int(user_id), text, reply_markup=kb)
        user["pm_menu_msg_id"] = msg.message_id
    except Exception:
        pass


async def upsert_company_card(ws: dict, company_idx: int):
    if company_idx < 0 or company_idx >= len(ws["companies"]):
        return
    company = ws["companies"][company_idx]
    text = company_card_text(company)
    card_msg_id = company.get("card_msg_id")
    if card_msg_id:
        ok = await try_edit_text(ws["chat_id"], card_msg_id, text)
        if ok:
            return
    msg = await bot.send_message(ws["chat_id"], text, **thread_kwargs(ws["thread_id"]))
    company["card_msg_id"] = msg.message_id


async def upsert_company_mirror(company: dict):
    mirror = company.get("mirror")
    if not mirror:
        return
    text = company_card_text(company)
    msg_id = mirror.get("message_id")
    if msg_id:
        ok = await try_edit_text(mirror["chat_id"], msg_id, text)
        if ok:
            target_key = make_mirror_target_key(mirror["chat_id"], mirror.get("thread_id") or 0)
            company.setdefault("mirror_history", {})[target_key] = msg_id
            return
    msg = await bot.send_message(mirror["chat_id"], text, **thread_kwargs(mirror.get("thread_id") or 0))
    mirror["message_id"] = msg.message_id
    target_key = make_mirror_target_key(mirror["chat_id"], mirror.get("thread_id") or 0)
    company.setdefault("mirror_history", {})[target_key] = msg.message_id


async def ensure_all_company_cards(ws: dict):
    for idx in range(len(ws.get("companies", []))):
        await upsert_company_card(ws, idx)


async def sync_company_everywhere(ws: dict, company_idx: int):
    await upsert_company_card(ws, company_idx)
    await upsert_company_mirror(ws["companies"][company_idx])


async def delete_old_prompt_if_any(ws: dict):
    awaiting = ws.get("awaiting") or {}
    if awaiting.get("prompt_msg_id"):
        await safe_delete_message(ws["chat_id"], awaiting["prompt_msg_id"])


async def set_prompt(ws: dict, prompt_text: str, awaiting_payload: dict):
    await delete_old_prompt_if_any(ws)
    msg = await bot.send_message(ws["chat_id"], prompt_text, reply_markup=prompt_kb(ws["id"]), **thread_kwargs(ws["thread_id"]))
    awaiting_payload["prompt_msg_id"] = msg.message_id
    ws["awaiting"] = awaiting_payload


async def upsert_ws_menu(ws: dict, text: str, reply_markup) -> bool:
    if not ws or not ws.get("is_connected"):
        return False
    wid = ws["id"]
    async with get_menu_lock(wid):
        runtime_msg_id = live_menu_ids.get(wid)
        if runtime_msg_id and ws.get("menu_msg_id") != runtime_msg_id:
            ws["menu_msg_id"] = runtime_msg_id

        menu_msg_id = ws.get("menu_msg_id")
        if menu_msg_id:
            ok = await try_edit_text(ws["chat_id"], menu_msg_id, text, reply_markup=reply_markup)
            if ok:
                live_menu_ids[wid] = menu_msg_id
                return False

        runtime_msg_id = live_menu_ids.get(wid)
        if runtime_msg_id and runtime_msg_id != menu_msg_id:
            ok = await try_edit_text(ws["chat_id"], runtime_msg_id, text, reply_markup=reply_markup)
            if ok:
                ws["menu_msg_id"] = runtime_msg_id
                return False

        msg = await bot.send_message(ws["chat_id"], text, reply_markup=reply_markup, **thread_kwargs(ws["thread_id"]))
        ws["menu_msg_id"] = msg.message_id
        live_menu_ids[wid] = msg.message_id
        return True


async def navigate(data: dict, wid: str, view: dict):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return False
    kind = view.get("view", "ws")
    if kind == "ws":
        created = await upsert_ws_menu(ws, "📂 Меню workspace", ws_home_kb(wid, ws))
    elif kind == "create_company":
        created = await upsert_ws_menu(ws, "➕ Создать компанию", create_company_mode_kb(wid))
    elif kind == "company":
        company_idx = view["company_idx"]
        if company_idx < 0 or company_idx >= len(ws["companies"]):
            return await navigate(data, wid, {"view": "ws"})
        company = ws["companies"][company_idx]
        created = await upsert_ws_menu(ws, company_display_name(company), company_menu_kb(wid, company_idx, company))
    elif kind == "company_settings":
        company_idx = view["company_idx"]
        if company_idx < 0 or company_idx >= len(ws["companies"]):
            return await navigate(data, wid, {"view": "ws"})
        company = ws["companies"][company_idx]
        created = await upsert_ws_menu(ws, f"⚙️ {company_display_name(company)}", company_settings_kb(wid, company_idx, company))
    elif kind == "category":
        company_idx = view["company_idx"]
        category_idx = view["category_idx"]
        if company_idx < 0 or company_idx >= len(ws["companies"]):
            return await navigate(data, wid, {"view": "ws"})
        company = ws["companies"][company_idx]
        if category_idx < 0 or category_idx >= len(company.get("categories", [])):
            return await navigate(data, wid, {"view": "company", "company_idx": company_idx})
        category = company["categories"][category_idx]
        created = await upsert_ws_menu(ws, category_display_name(category), category_menu_kb(wid, company_idx, category_idx, company))
    elif kind == "categories":
        company_idx = view["company_idx"]
        if company_idx < 0 or company_idx >= len(ws["companies"]):
            return await navigate(data, wid, {"view": "ws"})
        company = ws["companies"][company_idx]
        created = await upsert_ws_menu(ws, f"🗂 Категории • {company['name']}", category_settings_index_kb(wid, company_idx, company))
    elif kind == "category_settings":
        company_idx = view["company_idx"]
        category_idx = view["category_idx"]
        if company_idx < 0 or company_idx >= len(ws["companies"]):
            return await navigate(data, wid, {"view": "ws"})
        company = ws["companies"][company_idx]
        if category_idx < 0 or category_idx >= len(company.get("categories", [])):
            return await navigate(data, wid, {"view": "company_settings", "company_idx": company_idx})
        category = company["categories"][category_idx]
        created = await upsert_ws_menu(ws, f"⚙️ {category_display_name(category)}", category_settings_item_kb(wid, company_idx, category_idx))
    elif kind == "task":
        company_idx = view["company_idx"]
        task_idx = view["task_idx"]
        if company_idx < 0 or company_idx >= len(ws["companies"]):
            return await navigate(data, wid, {"view": "ws"})
        company = ws["companies"][company_idx]
        if task_idx < 0 or task_idx >= len(company["tasks"]):
            return await navigate(data, wid, {"view": "company", "company_idx": company_idx})
        task = company["tasks"][task_idx]
        created = await upsert_ws_menu(ws, f"{company_display_name(company)}/📌 {task['text']}", task_menu_kb(wid, company_idx, task_idx, task, company))
    elif kind == "task_move":
        company_idx = view["company_idx"]
        task_idx = view["task_idx"]
        if company_idx < 0 or company_idx >= len(ws["companies"]):
            return await navigate(data, wid, {"view": "ws"})
        company = ws["companies"][company_idx]
        if task_idx < 0 or task_idx >= len(company["tasks"]):
            return await navigate(data, wid, {"view": "company", "company_idx": company_idx})
        task = company["tasks"][task_idx]
        created = await upsert_ws_menu(ws, f"📥 Категория для: {task['text']}", task_move_category_kb(wid, company_idx, task_idx, company))
    elif kind == "template":
        created = await upsert_ws_menu(ws, "⚙️ Шаблон задач", template_menu_kb(wid, ws))
    elif kind == "template_category":
        cat_idx = view["category_idx"]
        if cat_idx < 0 or cat_idx >= len(ws["template"].get("categories", [])):
            return await navigate(data, wid, {"view": "template"})
        category = ws["template"]["categories"][cat_idx]
        created = await upsert_ws_menu(ws, category_display_name(category), template_category_menu_kb(wid, cat_idx, ws))
    elif kind == "template_categories":
        created = await upsert_ws_menu(ws, "🗂 Категории шаблона", template_category_settings_index_kb(wid, ws))
    elif kind == "template_category_settings":
        cat_idx = view["category_idx"]
        if cat_idx < 0 or cat_idx >= len(ws["template"].get("categories", [])):
            return await navigate(data, wid, {"view": "template"})
        category = ws["template"]["categories"][cat_idx]
        created = await upsert_ws_menu(ws, f"⚙️ {category_display_name(category)}", template_category_settings_item_kb(wid, cat_idx))
    elif kind == "template_task":
        task_idx = view["task_idx"]
        if task_idx < 0 or task_idx >= len(ws["template"]["tasks"]):
            return await navigate(data, wid, {"view": "template"})
        task = ws["template"]["tasks"][task_idx]
        created = await upsert_ws_menu(ws, f"Шаблон/📌 {task['text']}", template_task_menu_kb(wid, task_idx, ws))
    elif kind == "template_task_move":
        task_idx = view["task_idx"]
        if task_idx < 0 or task_idx >= len(ws["template"]["tasks"]):
            return await navigate(data, wid, {"view": "template"})
        task = ws["template"]["tasks"][task_idx]
        created = await upsert_ws_menu(ws, f"📥 Категория для: {task['text']}", template_task_move_category_kb(wid, task_idx, ws))
    else:
        created = await upsert_ws_menu(ws, "📂 Меню workspace", ws_home_kb(wid, ws))

    if created:
        await save_data(data)
    return True


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
    if user.get("pm_menu_msg_id"):
        ok = await try_edit_text(int(uid), user["pm_menu_msg_id"], pm_main_text(uid, data), reply_markup=pm_main_kb(uid, data))
        if ok:
            await save_data(data)
            return
    msg = await message.answer(pm_main_text(uid, data), reply_markup=pm_main_kb(uid, data))
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
    await safe_edit_text(int(uid), cb.message.message_id, pm_main_text(uid, data), reply_markup=pm_main_kb(uid, data))
    await save_data(data)


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
    msg = await cb.message.answer("📌 Как подключить workspace:\n\n1) Перейдите в нужный тред группы\n2) Отправьте команду /connect")
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
    if not ws or not ws.get("is_connected") or wid not in data["users"].get(uid, {}).get("workspaces", []):
        await safe_edit_text(int(uid), cb.message.message_id, pm_main_text(uid, data), reply_markup=pm_main_kb(uid, data))
        return
    await safe_edit_text(int(uid), cb.message.message_id, f"📂 {ws['name']}", reply_markup=pm_ws_manage_kb(wid))


@dp.callback_query_handler(lambda c: c.data.startswith("pmwsdel:"))
async def pm_delete_workspace(cb: types.CallbackQuery):
    await cb.answer()
    if cb.message.chat.type != "private":
        return

    data = await load_data()
    current_uid = str(cb.from_user.id)
    wid = cb.data.split(":", 1)[1]
    ws = data["workspaces"].get(wid)
    if not ws:
        await safe_edit_text(int(current_uid), cb.message.message_id, pm_main_text(current_uid, data), reply_markup=pm_main_kb(current_uid, data))
        return

    ws_name = ws["name"]
    chat_id = ws["chat_id"]
    thread_id = ws["thread_id"]

    await safe_delete_message(chat_id, ws.get("menu_msg_id"))
    awaiting = ws.get("awaiting") or {}
    if awaiting.get("prompt_msg_id"):
        await safe_delete_message(chat_id, awaiting["prompt_msg_id"])

    ws["menu_msg_id"] = None
    ws["awaiting"] = None
    ws["is_connected"] = False
    live_menu_ids.pop(wid, None)

    affected_users = []
    for uid, user in data["users"].items():
        if wid in user.get("workspaces", []):
            user["workspaces"].remove(wid)
            affected_users.append(uid)

    ensure_user(data, current_uid)
    data["users"][current_uid]["pm_menu_msg_id"] = cb.message.message_id
    await save_data(data)

    await safe_edit_text(int(current_uid), cb.message.message_id, pm_main_text(current_uid, data), reply_markup=pm_main_kb(current_uid, data))
    for uid in affected_users:
        if uid != current_uid:
            await update_pm_menu(uid, data)
    for uid in affected_users:
        await send_temp_message(int(uid), f"Workspace «{ws_name}» отключен", 0, delay=10)
    await send_temp_message(chat_id, f"Workspace «{ws_name}» отключен", thread_id, delay=10)


# =========================
# CONNECT + TOPIC TRACKING
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

    topic_title = extract_topic_title(message)
    if not topic_title and existing_ws:
        topic_title = existing_ws.get("topic_title")

    chat_title = message.chat.title or "Workspace"
    ws_name = workspace_full_name(chat_title, topic_title, thread_id)

    old_companies = existing_ws.get("companies", []) if existing_ws else []
    old_template = existing_ws.get("template") if existing_ws else {
        "tasks": [make_task("Создать договор"), make_task("Выставить счёт")],
        "categories": [],
    }

    old_menu_msg_id = existing_ws.get("menu_msg_id") if existing_ws else None
    old_prompt_msg_id = (existing_ws.get("awaiting") or {}).get("prompt_msg_id") if existing_ws else None

    data["workspaces"][wid] = {
        "id": wid,
        "name": ws_name,
        "chat_title": chat_title,
        "topic_title": topic_title,
        "chat_id": message.chat.id,
        "thread_id": thread_id,
        "menu_msg_id": None,
        "template": old_template,
        "companies": old_companies,
        "awaiting": None,
        "is_connected": True,
    }
    ws = data["workspaces"][wid]

    if wid not in data["users"][uid]["workspaces"]:
        data["users"][uid]["workspaces"].append(wid)

    help_msg_id = data["users"][uid].get("help_msg_id")
    data["users"][uid]["help_msg_id"] = None
    await save_data(data)

    if old_menu_msg_id:
        await safe_delete_message(message.chat.id, old_menu_msg_id)
    if old_prompt_msg_id:
        await safe_delete_message(message.chat.id, old_prompt_msg_id)
    if help_msg_id:
        await safe_delete_message(int(uid), help_msg_id)

    fresh = await load_data()
    ws = fresh["workspaces"][wid]
    await ensure_all_company_cards(ws)
    for company in ws["companies"]:
        if company.get("mirror"):
            await upsert_company_mirror(company)
    await navigate(fresh, wid, {"view": "ws"})
    await update_pm_menu(uid, fresh)
    await save_data(fresh)

    try:
        await send_week_notice_pm(uid, f"Workspace «{ws['name']}» подключён")
    except Exception:
        pass


@dp.message_handler(is_topic_service_message, content_types=types.ContentTypes.ANY)
async def track_forum_topic_updates(message: types.Message):
    if message.chat.type == "private":
        return
    thread_id = message.message_thread_id or 0
    if not thread_id:
        return
    topic_title = extract_topic_title(message)
    if not topic_title:
        return
    data = await load_data()
    wid = make_ws_id(message.chat.id, thread_id)
    ws = data["workspaces"].get(wid)
    if not ws:
        return
    ws["topic_title"] = topic_title
    ws["chat_title"] = message.chat.title or ws.get("chat_title") or "Workspace"
    ws["name"] = workspace_full_name(ws["chat_title"], ws["topic_title"], thread_id)
    await save_data(data)
    for uid, user in data["users"].items():
        if wid in user.get("workspaces", []):
            await update_pm_menu(uid, data)


# =========================
# MIRROR
# =========================


@dp.callback_query_handler(lambda c: c.data.startswith("mirroron:"))
async def mirror_on(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx = cb.data.split(":")
    company_idx = int(company_idx)
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    if company_idx < 0 or company_idx >= len(ws["companies"]):
        return

    company = ws["companies"][company_idx]
    if company.get("mirror"):
        await send_temp_message(ws["chat_id"], "Этот список уже дублируется.", ws["thread_id"], delay=8)
        return

    clear_pending_mirror_tokens_for_company(data, wid, company["id"])
    if company.get("pending_mirror_msg_id"):
        await safe_delete_message(ws["chat_id"], company["pending_mirror_msg_id"])
        company["pending_mirror_msg_id"] = None

    token = generate_mirror_token()
    data["mirror_tokens"][token] = {"source_wid": wid, "company_id": company["id"], "created_by": cb.from_user.id}
    prompt_msg = await bot.send_message(
        ws["chat_id"],
        "📤 Чтобы привязать дубликат:\n1) Перейдите в целевой чат/тред\n2) Отправьте команду:\n"
        f"/mirror {token}",
        **thread_kwargs(ws["thread_id"]),
    )
    company["pending_mirror_msg_id"] = prompt_msg.message_id
    await save_data(data)
    await navigate(data, wid, {"view": "company_settings", "company_idx": company_idx})


@dp.callback_query_handler(lambda c: c.data.startswith("mirroroff:"))
async def mirror_off(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx = cb.data.split(":")
    company_idx = int(company_idx)
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws or company_idx < 0 or company_idx >= len(ws["companies"]):
        return
    company = ws["companies"][company_idx]
    mirror = company.get("mirror") or {}
    if mirror.get("message_id"):
        await safe_delete_message(mirror["chat_id"], mirror["message_id"])
    company["mirror"] = None
    if company.get("pending_mirror_msg_id"):
        await safe_delete_message(ws["chat_id"], company["pending_mirror_msg_id"])
        company["pending_mirror_msg_id"] = None
    clear_pending_mirror_tokens_for_company(data, wid, company["id"])
    await save_data(data)
    await navigate(data, wid, {"view": "company_settings", "company_idx": company_idx})
    await send_temp_message(ws["chat_id"], "🔌 Список отвязан", ws["thread_id"], delay=8)


@dp.message_handler(commands=["mirror"])
async def cmd_mirror(message: types.Message):
    if message.chat.type == "private":
        return
    code = (message.get_args() or "").strip().upper()
    if not code:
        await send_temp_message(message.chat.id, "Укажите код: /mirror CODE", message.message_thread_id or 0, delay=10)
        await try_delete_user_message(message)
        return

    data = await load_data()
    payload = data.get("mirror_tokens", {}).get(code)
    if not payload:
        await send_temp_message(message.chat.id, "Код не найден или уже использован.", message.message_thread_id or 0, delay=10)
        await try_delete_user_message(message)
        return

    source_wid = payload["source_wid"]
    company_id = payload.get("company_id")
    ws = data["workspaces"].get(source_wid)
    if not ws:
        data["mirror_tokens"].pop(code, None)
        await save_data(data)
        await send_temp_message(message.chat.id, "Исходный workspace не найден.", message.message_thread_id or 0, delay=10)
        await try_delete_user_message(message)
        return

    company_idx = find_company_index_by_id(ws, company_id)
    if company_idx is None:
        data["mirror_tokens"].pop(code, None)
        await save_data(data)
        await send_temp_message(message.chat.id, "Компания не найдена.", message.message_thread_id or 0, delay=10)
        await try_delete_user_message(message)
        return

    company = ws["companies"][company_idx]
    target_chat_id = message.chat.id
    target_thread_id = message.message_thread_id or 0
    target_key = make_mirror_target_key(target_chat_id, target_thread_id)
    known_message_id = (company.get("mirror_history") or {}).get(target_key)

    company["mirror"] = {"chat_id": target_chat_id, "thread_id": target_thread_id, "message_id": known_message_id}
    await upsert_company_mirror(company)
    if company.get("pending_mirror_msg_id"):
        await safe_delete_message(ws["chat_id"], company["pending_mirror_msg_id"])
        company["pending_mirror_msg_id"] = None
    data["mirror_tokens"].pop(code, None)
    await save_data(data)
    await try_delete_user_message(message)
    await send_temp_message(ws["chat_id"], f"📤 Список «{company['name']}» дублируется в другой тред/чат", ws["thread_id"], delay=10)
    await navigate(data, source_wid, {"view": "company_settings", "company_idx": company_idx})


# =========================
# NAVIGATION
# =========================


@dp.callback_query_handler(lambda c: c.data.startswith("backws:"))
async def back_to_ws(cb: types.CallbackQuery):
    await cb.answer()
    wid = cb.data.split(":", 1)[1]
    data = await load_data()
    await navigate(data, wid, {"view": "ws"})


@dp.callback_query_handler(lambda c: c.data.startswith("cmpnew:"))
async def open_create_company(cb: types.CallbackQuery):
    await cb.answer()
    wid = cb.data.split(":", 1)[1]
    data = await load_data()
    await navigate(data, wid, {"view": "create_company"})


@dp.callback_query_handler(lambda c: c.data.startswith("cmp:"))
async def open_company(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx = cb.data.split(":")
    data = await load_data()
    await navigate(data, wid, {"view": "company", "company_idx": int(company_idx)})


@dp.callback_query_handler(lambda c: c.data.startswith("cmpset:"))
async def open_company_settings(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx = cb.data.split(":")
    data = await load_data()
    await navigate(data, wid, {"view": "company_settings", "company_idx": int(company_idx)})


@dp.callback_query_handler(lambda c: c.data.startswith("cats:"))
async def open_categories_settings(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx = cb.data.split(":")
    data = await load_data()
    await navigate(data, wid, {"view": "categories", "company_idx": int(company_idx)})


@dp.callback_query_handler(lambda c: c.data.startswith("cat:"))
async def open_category(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx, category_idx = cb.data.split(":")
    data = await load_data()
    await navigate(data, wid, {"view": "category", "company_idx": int(company_idx), "category_idx": int(category_idx)})


@dp.callback_query_handler(lambda c: c.data.startswith("catset:"))
async def open_category_settings(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx, category_idx = cb.data.split(":")
    data = await load_data()
    await navigate(data, wid, {"view": "category_settings", "company_idx": int(company_idx), "category_idx": int(category_idx)})


@dp.callback_query_handler(lambda c: c.data.startswith("task:"))
async def open_task(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx, task_idx = cb.data.split(":")
    data = await load_data()
    await navigate(data, wid, {"view": "task", "company_idx": int(company_idx), "task_idx": int(task_idx)})


@dp.callback_query_handler(lambda c: c.data.startswith("taskmovepick:"))
async def pick_task_category(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx, task_idx = cb.data.split(":")
    data = await load_data()
    await navigate(data, wid, {"view": "task_move", "company_idx": int(company_idx), "task_idx": int(task_idx)})


@dp.callback_query_handler(lambda c: c.data.startswith("tplcats:"))
async def open_template_categories_settings(cb: types.CallbackQuery):
    await cb.answer()
    wid = cb.data.split(":", 1)[1]
    data = await load_data()
    await navigate(data, wid, {"view": "template"})


@dp.callback_query_handler(lambda c: c.data.startswith("tplcatset:"))
async def open_template_category_settings(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, cat_idx = cb.data.split(":")
    data = await load_data()
    await navigate(data, wid, {"view": "template_category_settings", "category_idx": int(cat_idx)})


@dp.callback_query_handler(lambda c: c.data.startswith("tplcat:"))
async def open_template_category(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, cat_idx = cb.data.split(":")
    data = await load_data()
    await navigate(data, wid, {"view": "template_category", "category_idx": int(cat_idx)})


@dp.callback_query_handler(lambda c: c.data.startswith("tpltaskmovepick:"))
async def pick_template_task_category(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, task_idx = cb.data.split(":")
    data = await load_data()
    await navigate(data, wid, {"view": "template_task_move", "task_idx": int(task_idx)})


@dp.callback_query_handler(lambda c: c.data.startswith("tpltask:"))
async def open_template_task(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, task_idx = cb.data.split(":")
    data = await load_data()
    await navigate(data, wid, {"view": "template_task", "task_idx": int(task_idx)})


@dp.callback_query_handler(lambda c: c.data.startswith("tpl:"))
async def open_template(cb: types.CallbackQuery):
    await cb.answer()
    wid = cb.data.split(":", 1)[1]
    data = await load_data()
    await navigate(data, wid, {"view": "template"})


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
    awaiting = ws.get("awaiting") or {}
    if awaiting.get("prompt_msg_id"):
        await safe_delete_message(ws["chat_id"], awaiting["prompt_msg_id"])
    back_to = awaiting.get("back_to", {"view": "ws"})
    ws["awaiting"] = None
    await save_data(data)
    if ws.get("is_connected"):
        await navigate(data, wid, back_to)


# =========================
# COMPANY ACTIONS
# =========================


@dp.callback_query_handler(lambda c: c.data.startswith("cmpnewmode:"))
async def create_company_mode_prompt(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, mode = cb.data.split(":")
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    await set_prompt(ws, "✏️ Напишите название компании:", {"type": "new_company", "with_template": mode == "template", "back_to": {"view": "create_company"}})
    await save_data(data)


@dp.callback_query_handler(lambda c: c.data.startswith("cmpren:"))
async def rename_company_prompt(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx = cb.data.split(":")
    company_idx = int(company_idx)
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    await set_prompt(ws, "✏️ Введите новое название компании:", {"type": "rename_company", "company_idx": company_idx, "back_to": {"view": "company_settings", "company_idx": company_idx}})
    await save_data(data)


@dp.callback_query_handler(lambda c: c.data.startswith("cmpemoji:"))
async def company_emoji_prompt(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx = cb.data.split(":")
    company_idx = int(company_idx)
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    await set_prompt(ws, "😀 Пришлите один смайлик для компании:", {"type": "company_emoji", "company_idx": company_idx, "back_to": {"view": "company_settings", "company_idx": company_idx}})
    await save_data(data)


@dp.callback_query_handler(lambda c: c.data.startswith("cmpdel:"))
async def delete_company(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx = cb.data.split(":")
    company_idx = int(company_idx)
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected") or company_idx < 0 or company_idx >= len(ws["companies"]):
        return
    company = ws["companies"].pop(company_idx)
    await safe_delete_message(ws["chat_id"], company.get("card_msg_id"))
    mirror = company.get("mirror") or {}
    if mirror.get("message_id"):
        await safe_delete_message(mirror["chat_id"], mirror["message_id"])
    if company.get("pending_mirror_msg_id"):
        await safe_delete_message(ws["chat_id"], company["pending_mirror_msg_id"])
    clear_pending_mirror_tokens_for_company(data, wid, company["id"])
    await save_data(data)
    await navigate(data, wid, {"view": "ws"})


# =========================
# COMPANY CATEGORY ACTIONS
# =========================


@dp.callback_query_handler(lambda c: c.data.startswith("catnew:"))
async def create_category_prompt(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx = cb.data.split(":")
    company_idx = int(company_idx)
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    await set_prompt(ws, "✏️ Введите название категории:", {"type": "new_category", "company_idx": company_idx, "back_to": {"view": "company", "company_idx": company_idx}})
    await save_data(data)


@dp.callback_query_handler(lambda c: c.data.startswith("catren:"))
async def rename_category_prompt(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx, category_idx = cb.data.split(":")
    company_idx = int(company_idx)
    category_idx = int(category_idx)
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    await set_prompt(ws, "✏️ Введите новое название категории:", {"type": "rename_category", "company_idx": company_idx, "category_idx": category_idx, "back_to": {"view": "category_settings", "company_idx": company_idx, "category_idx": category_idx}})
    await save_data(data)


@dp.callback_query_handler(lambda c: c.data.startswith("catemoji:"))
async def category_emoji_prompt(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx, category_idx = cb.data.split(":")
    company_idx = int(company_idx)
    category_idx = int(category_idx)
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    await set_prompt(ws, "😀 Пришлите один смайлик для категории:", {"type": "category_emoji", "company_idx": company_idx, "category_idx": category_idx, "back_to": {"view": "category_settings", "company_idx": company_idx, "category_idx": category_idx}})
    await save_data(data)


@dp.callback_query_handler(lambda c: c.data.startswith("catdelall:"))
async def delete_category_with_tasks(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx, category_idx = cb.data.split(":")
    company_idx = int(company_idx)
    category_idx = int(category_idx)
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    if company_idx < 0 or company_idx >= len(ws["companies"]):
        return
    company = ws["companies"][company_idx]
    if category_idx < 0 or category_idx >= len(company.get("categories", [])):
        return
    category_id = company["categories"][category_idx]["id"]
    company["tasks"] = [task for task in company["tasks"] if task.get("category_id") != category_id]
    company["categories"].pop(category_idx)
    await save_data(data)
    await sync_company_everywhere(ws, company_idx)
    await navigate(data, wid, {"view": "company_settings", "company_idx": company_idx})


@dp.callback_query_handler(lambda c: c.data.startswith("catdel:"))
async def delete_category(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx, category_idx = cb.data.split(":")
    company_idx = int(company_idx)
    category_idx = int(category_idx)
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    if company_idx < 0 or company_idx >= len(ws["companies"]):
        return
    company = ws["companies"][company_idx]
    if category_idx < 0 or category_idx >= len(company.get("categories", [])):
        return
    category_id = company["categories"][category_idx]["id"]
    for task in company["tasks"]:
        if task.get("category_id") == category_id:
            task["category_id"] = None
    company["categories"].pop(category_idx)
    await save_data(data)
    await sync_company_everywhere(ws, company_idx)
    await navigate(data, wid, {"view": "company_settings", "company_idx": company_idx})


# =========================
# TASK ACTIONS
# =========================


@dp.callback_query_handler(lambda c: c.data.startswith("tasknew:"))
async def add_task_prompt(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx, target = cb.data.split(":")
    company_idx = int(company_idx)
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    back_to = {"view": "company", "company_idx": company_idx}
    category_idx = None
    if target != "root":
        category_idx = int(target)
        back_to = {"view": "category", "company_idx": company_idx, "category_idx": category_idx}
    await set_prompt(ws, "✏️ Введите текст новой задачи:", {"type": "new_task", "company_idx": company_idx, "category_idx": category_idx, "back_to": back_to})
    await save_data(data)


@dp.callback_query_handler(lambda c: c.data.startswith("taskren:"))
async def rename_task_prompt(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx, task_idx = cb.data.split(":")
    company_idx = int(company_idx)
    task_idx = int(task_idx)
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    await set_prompt(ws, "✏️ Введите новое название задачи:", {"type": "rename_task", "company_idx": company_idx, "task_idx": task_idx, "back_to": {"view": "task", "company_idx": company_idx, "task_idx": task_idx}})
    await save_data(data)


@dp.callback_query_handler(lambda c: c.data.startswith("taskmoveto:"))
async def move_task_to_category(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx, task_idx, cat_idx = cb.data.split(":")
    company_idx = int(company_idx)
    task_idx = int(task_idx)
    cat_idx = int(cat_idx)
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    company = ws["companies"][company_idx]
    old_category_id = company["tasks"][task_idx].get("category_id")
    company["tasks"][task_idx]["category_id"] = company["categories"][cat_idx]["id"]
    await save_data(data)
    await sync_company_everywhere(ws, company_idx)
    old_category_idx = find_category_index(company.get("categories", []), old_category_id) if old_category_id else None
    if old_category_idx is not None:
        await navigate(data, wid, {"view": "category", "company_idx": company_idx, "category_idx": old_category_idx})
    else:
        await navigate(data, wid, {"view": "company", "company_idx": company_idx})


@dp.callback_query_handler(lambda c: c.data.startswith("taskuncat:"))
async def move_task_out_of_category(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx, task_idx = cb.data.split(":")
    company_idx = int(company_idx)
    task_idx = int(task_idx)
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    company = ws["companies"][company_idx]
    old_category_id = company["tasks"][task_idx].get("category_id")
    company["tasks"][task_idx]["category_id"] = None
    await save_data(data)
    await sync_company_everywhere(ws, company_idx)
    old_category_idx = find_category_index(company.get("categories", []), old_category_id) if old_category_id else None
    if old_category_idx is not None:
        await navigate(data, wid, {"view": "category", "company_idx": company_idx, "category_idx": old_category_idx})
    else:
        await navigate(data, wid, {"view": "company", "company_idx": company_idx})


@dp.callback_query_handler(lambda c: c.data.startswith("taskdel:"))
async def delete_task(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx, task_idx = cb.data.split(":")
    company_idx = int(company_idx)
    task_idx = int(task_idx)
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    company = ws["companies"][company_idx]
    company["tasks"].pop(task_idx)
    await save_data(data)
    await sync_company_everywhere(ws, company_idx)
    await navigate(data, wid, {"view": "company", "company_idx": company_idx})


@dp.callback_query_handler(lambda c: c.data.startswith("taskdone:"))
async def toggle_task_done(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx, task_idx = cb.data.split(":")
    company_idx = int(company_idx)
    task_idx = int(task_idx)
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    company = ws["companies"][company_idx]
    company["tasks"][task_idx]["done"] = not company["tasks"][task_idx]["done"]
    await save_data(data)
    await sync_company_everywhere(ws, company_idx)
    category_id = company["tasks"][task_idx].get("category_id")
    category_idx = find_category_index(company.get("categories", []), category_id) if category_id else None
    if category_idx is not None:
        await navigate(data, wid, {"view": "category", "company_idx": company_idx, "category_idx": category_idx})
    else:
        await navigate(data, wid, {"view": "company", "company_idx": company_idx})


# =========================
# TEMPLATE ACTIONS
# =========================


@dp.callback_query_handler(lambda c: c.data.startswith("tplnew:"))
async def add_template_task_prompt(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, target = cb.data.split(":")
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    back_to = {"view": "template"}
    category_idx = None
    if target != "root":
        category_idx = int(target)
        back_to = {"view": "template_category", "category_idx": category_idx}
    await set_prompt(ws, "✏️ Введите название новой задачи шаблона:", {"type": "new_template_task", "category_idx": category_idx, "back_to": back_to})
    await save_data(data)


@dp.callback_query_handler(lambda c: c.data.startswith("tplren:"))
async def rename_template_task_prompt(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, task_idx = cb.data.split(":")
    task_idx = int(task_idx)
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    await set_prompt(ws, "✏️ Введите новое название задачи шаблона:", {"type": "rename_template_task", "task_idx": task_idx, "back_to": {"view": "template_task", "task_idx": task_idx}})
    await save_data(data)


@dp.callback_query_handler(lambda c: c.data.startswith("tpldel:"))
async def delete_template_task(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, task_idx = cb.data.split(":")
    task_idx = int(task_idx)
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    ws["template"]["tasks"].pop(task_idx)
    await save_data(data)
    await navigate(data, wid, {"view": "template"})


@dp.callback_query_handler(lambda c: c.data.startswith("tplcatnew:"))
async def create_template_category_prompt(cb: types.CallbackQuery):
    await cb.answer()
    wid = cb.data.split(":", 1)[1]
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    await set_prompt(ws, "✏️ Введите название категории шаблона:", {"type": "new_template_category", "back_to": {"view": "template"}})
    await save_data(data)


@dp.callback_query_handler(lambda c: c.data.startswith("tplcatren:"))
async def rename_template_category_prompt(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, cat_idx = cb.data.split(":")
    cat_idx = int(cat_idx)
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    await set_prompt(ws, "✏️ Введите новое название категории шаблона:", {"type": "rename_template_category", "category_idx": cat_idx, "back_to": {"view": "template_category_settings", "category_idx": cat_idx}})
    await save_data(data)


@dp.callback_query_handler(lambda c: c.data.startswith("tplcatemoji:"))
async def template_category_emoji_prompt(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, cat_idx = cb.data.split(":")
    cat_idx = int(cat_idx)
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    await set_prompt(ws, "😀 Пришлите один смайлик для категории шаблона:", {"type": "template_category_emoji", "category_idx": cat_idx, "back_to": {"view": "template_category_settings", "category_idx": cat_idx}})
    await save_data(data)


@dp.callback_query_handler(lambda c: c.data.startswith("tplcatdelall:"))
async def delete_template_category_with_tasks(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, cat_idx = cb.data.split(":")
    cat_idx = int(cat_idx)
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    category_id = ws["template"]["categories"][cat_idx]["id"]
    ws["template"]["tasks"] = [task for task in ws["template"]["tasks"] if task.get("category_id") != category_id]
    ws["template"]["categories"].pop(cat_idx)
    await save_data(data)
    await navigate(data, wid, {"view": "template"})


@dp.callback_query_handler(lambda c: c.data.startswith("tplcatdel:"))
async def delete_template_category(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, cat_idx = cb.data.split(":")
    cat_idx = int(cat_idx)
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    category_id = ws["template"]["categories"][cat_idx]["id"]
    for task in ws["template"]["tasks"]:
        if task.get("category_id") == category_id:
            task["category_id"] = None
    ws["template"]["categories"].pop(cat_idx)
    await save_data(data)
    await navigate(data, wid, {"view": "template"})


@dp.callback_query_handler(lambda c: c.data.startswith("tpltaskmoveto:"))
async def move_template_task_to_category(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, task_idx, cat_idx = cb.data.split(":")
    task_idx = int(task_idx)
    cat_idx = int(cat_idx)
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    ws["template"]["tasks"][task_idx]["category_id"] = ws["template"]["categories"][cat_idx]["id"]
    await save_data(data)
    await navigate(data, wid, {"view": "template"})


@dp.callback_query_handler(lambda c: c.data.startswith("tpltaskuncat:"))
async def move_template_task_out_of_category(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, task_idx = cb.data.split(":")
    task_idx = int(task_idx)
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    old_category_id = ws["template"]["tasks"][task_idx].get("category_id")
    ws["template"]["tasks"][task_idx]["category_id"] = None
    await save_data(data)
    old_category_idx = find_category_index(ws["template"].get("categories", []), old_category_id) if old_category_id else None
    if old_category_idx is not None:
        await navigate(data, wid, {"view": "template_category", "category_idx": old_category_idx})
    else:
        await navigate(data, wid, {"view": "template"})


# =========================
# GROUP TEXT INPUT
# =========================


@dp.message_handler(content_types=types.ContentTypes.TEXT)
async def handle_group_text(message: types.Message):
    if message.chat.type == "private":
        return
    if is_known_command(message.text):
        return

    data = await load_data()
    wid = make_ws_id(message.chat.id, message.message_thread_id or 0)
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    awaiting = ws.get("awaiting") or {}
    if not awaiting:
        return

    mode = awaiting.get("type")
    text = clean_text(message.text)
    if not text:
        return
    prompt_msg_id = awaiting.get("prompt_msg_id")

    if mode == "new_company":
        if company_exists(ws, text):
            await send_temp_message(ws["chat_id"], "Такая компания уже существует.", ws["thread_id"], delay=6)
            return
        company = normalize_company({"name": text})
        if awaiting.get("with_template"):
            category_map = {}
            company["categories"] = []
            for category in ws["template"].get("categories", []):
                new_cat = make_category(category["name"])
                company["categories"].append(new_cat)
                category_map[category["id"]] = new_cat["id"]
            company["tasks"] = []
            for task in ws["template"].get("tasks", []):
                company["tasks"].append(make_task(task["text"], False, category_map.get(task.get("category_id"))))
        ws["companies"].append(company)
        ws["awaiting"] = None
        new_idx = len(ws["companies"]) - 1
        await save_data(data)
        await sync_company_everywhere(ws, new_idx)
        await safe_delete_message(ws["chat_id"], prompt_msg_id)
        await try_delete_user_message(message)
        if ws.get("menu_msg_id"):
            await safe_delete_message(ws["chat_id"], ws["menu_msg_id"])
            ws["menu_msg_id"] = None
            live_menu_ids.pop(wid, None)
        await save_data(data)
        await navigate(data, wid, {"view": "ws"})
        return

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
        await try_delete_user_message(message)
        await sync_company_everywhere(ws, company_idx)
        await navigate(data, wid, {"view": "company_settings", "company_idx": company_idx})
        return

    if mode == "company_emoji":
        company_idx = awaiting["company_idx"]
        emoji = extract_emoji_input(message.text)
        if emoji is None or company_idx < 0 or company_idx >= len(ws["companies"]):
            await send_temp_message(ws["chat_id"], "Пришлите один смайлик.", ws["thread_id"], delay=6)
            return
        ws["companies"][company_idx]["name"] = reassign_leading_emoji(ws["companies"][company_idx]["name"], emoji)
        ws["awaiting"] = None
        await save_data(data)
        await safe_delete_message(ws["chat_id"], prompt_msg_id)
        await try_delete_user_message(message)
        await sync_company_everywhere(ws, company_idx)
        await navigate(data, wid, {"view": "company_settings", "company_idx": company_idx})
        return

    if mode == "new_category":
        company_idx = awaiting["company_idx"]
        if company_idx < 0 or company_idx >= len(ws["companies"]):
            ws["awaiting"] = None
            await save_data(data)
            return
        company = ws["companies"][company_idx]
        if category_exists(company.get("categories", []), text):
            await send_temp_message(ws["chat_id"], "Такая категория уже существует.", ws["thread_id"], delay=6)
            return
        company.setdefault("categories", []).append(make_category(text))
        ws["awaiting"] = None
        await save_data(data)
        await safe_delete_message(ws["chat_id"], prompt_msg_id)
        await try_delete_user_message(message)
        await sync_company_everywhere(ws, company_idx)
        await navigate(data, wid, {"view": "company", "company_idx": company_idx})
        return

    if mode == "rename_category":
        company_idx = awaiting["company_idx"]
        category_idx = awaiting["category_idx"]
        if company_idx < 0 or company_idx >= len(ws["companies"]):
            ws["awaiting"] = None
            await save_data(data)
            return
        company = ws["companies"][company_idx]
        if category_idx < 0 or category_idx >= len(company.get("categories", [])):
            ws["awaiting"] = None
            await save_data(data)
            return
        if category_exists(company.get("categories", []), text, exclude_idx=category_idx):
            await send_temp_message(ws["chat_id"], "Такая категория уже существует.", ws["thread_id"], delay=6)
            return
        company["categories"][category_idx]["name"] = text
        ws["awaiting"] = None
        await save_data(data)
        await safe_delete_message(ws["chat_id"], prompt_msg_id)
        await try_delete_user_message(message)
        await sync_company_everywhere(ws, company_idx)
        await navigate(data, wid, {"view": "category_settings", "company_idx": company_idx, "category_idx": category_idx})
        return

    if mode == "category_emoji":
        company_idx = awaiting["company_idx"]
        category_idx = awaiting["category_idx"]
        emoji = extract_emoji_input(message.text)
        if emoji is None:
            await send_temp_message(ws["chat_id"], "Пришлите один смайлик.", ws["thread_id"], delay=6)
            return
        company = ws["companies"][company_idx]
        company["categories"][category_idx]["name"] = reassign_leading_emoji(company["categories"][category_idx]["name"], emoji)
        ws["awaiting"] = None
        await save_data(data)
        await safe_delete_message(ws["chat_id"], prompt_msg_id)
        await try_delete_user_message(message)
        await sync_company_everywhere(ws, company_idx)
        await navigate(data, wid, {"view": "category_settings", "company_idx": company_idx, "category_idx": category_idx})
        return

    if mode == "new_task":
        company_idx = awaiting["company_idx"]
        if company_idx < 0 or company_idx >= len(ws["companies"]):
            ws["awaiting"] = None
            await save_data(data)
            return
        company = ws["companies"][company_idx]
        category_id = None
        category_idx = awaiting.get("category_idx")
        if category_idx is not None and 0 <= category_idx < len(company.get("categories", [])):
            category_id = company["categories"][category_idx]["id"]
        company["tasks"].append(make_task(text, False, category_id))
        ws["awaiting"] = None
        await save_data(data)
        await safe_delete_message(ws["chat_id"], prompt_msg_id)
        await try_delete_user_message(message)
        await sync_company_everywhere(ws, company_idx)
        await navigate(data, wid, awaiting.get("back_to", {"view": "company", "company_idx": company_idx}))
        return

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
        await try_delete_user_message(message)
        await sync_company_everywhere(ws, company_idx)
        await navigate(data, wid, {"view": "task", "company_idx": company_idx, "task_idx": task_idx})
        return

    if mode == "new_template_task":
        category_id = None
        category_idx = awaiting.get("category_idx")
        if category_idx is not None and 0 <= category_idx < len(ws["template"].get("categories", [])):
            category_id = ws["template"]["categories"][category_idx]["id"]
        ws["template"]["tasks"].append(make_task(text, False, category_id))
        ws["awaiting"] = None
        await save_data(data)
        await safe_delete_message(ws["chat_id"], prompt_msg_id)
        await try_delete_user_message(message)
        await navigate(data, wid, awaiting.get("back_to", {"view": "template"}))
        return

    if mode == "rename_template_task":
        task_idx = awaiting["task_idx"]
        if task_idx < 0 or task_idx >= len(ws["template"]["tasks"]):
            ws["awaiting"] = None
            await save_data(data)
            return
        ws["template"]["tasks"][task_idx]["text"] = text
        ws["awaiting"] = None
        await save_data(data)
        await safe_delete_message(ws["chat_id"], prompt_msg_id)
        await try_delete_user_message(message)
        await navigate(data, wid, {"view": "template_task", "task_idx": task_idx})
        return

    if mode == "new_template_category":
        if category_exists(ws["template"].get("categories", []), text):
            await send_temp_message(ws["chat_id"], "Такая категория уже существует.", ws["thread_id"], delay=6)
            return
        ws["template"].setdefault("categories", []).append(make_category(text))
        ws["awaiting"] = None
        await save_data(data)
        await safe_delete_message(ws["chat_id"], prompt_msg_id)
        await try_delete_user_message(message)
        await navigate(data, wid, {"view": "template"})
        return

    if mode == "rename_template_category":
        category_idx = awaiting["category_idx"]
        if category_idx < 0 or category_idx >= len(ws["template"].get("categories", [])):
            ws["awaiting"] = None
            await save_data(data)
            return
        if category_exists(ws["template"].get("categories", []), text, exclude_idx=category_idx):
            await send_temp_message(ws["chat_id"], "Такая категория уже существует.", ws["thread_id"], delay=6)
            return
        ws["template"]["categories"][category_idx]["name"] = text
        ws["awaiting"] = None
        await save_data(data)
        await safe_delete_message(ws["chat_id"], prompt_msg_id)
        await try_delete_user_message(message)
        await navigate(data, wid, {"view": "template_category_settings", "category_idx": category_idx})
        return

    if mode == "template_category_emoji":
        category_idx = awaiting["category_idx"]
        emoji = extract_emoji_input(message.text)
        if emoji is None:
            await send_temp_message(ws["chat_id"], "Пришлите один смайлик.", ws["thread_id"], delay=6)
            return
        ws["template"]["categories"][category_idx]["name"] = reassign_leading_emoji(ws["template"]["categories"][category_idx]["name"], emoji)
        ws["awaiting"] = None
        await save_data(data)
        await safe_delete_message(ws["chat_id"], prompt_msg_id)
        await try_delete_user_message(message)
        await navigate(data, wid, {"view": "template_category_settings", "category_idx": category_idx})
        return


# =========================
# RUN
# =========================

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
