import os
import json
import math
import asyncio
import time
import uuid
import copy
import re
import html
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils import executor
from aiogram.utils.exceptions import MessageNotModified, RetryAfter

TOKEN = os.getenv("API_TOKEN")
if not TOKEN:
    raise RuntimeError("API_TOKEN is not set")

bot = Bot(token=TOKEN)
dp = Dispatcher(bot)

DATA_FILE = "data.json"
TIMEZONE = ZoneInfo("Europe/Riga")
FILE_LOCK = asyncio.Lock()
MENU_LOCKS: dict[str, asyncio.Lock] = {}
RUNTIME_MENU_IDS: dict[str, int] = {}
RECENT_CALLBACKS: dict[tuple[int, int, str], float] = {}
CALLBACK_DEBOUNCE_SECONDS = 0.9


# =========================
# LOW LEVEL HELPERS
# =========================

def now_ts() -> int:
    return int(time.time())


def now_dt() -> datetime:
    return datetime.now(TIMEZONE)


def today_local() -> datetime:
    n = now_dt()
    return datetime(n.year, n.month, n.day, tzinfo=TIMEZONE)


async def tg_call(factory, retries: int = 2):
    last_error = None
    for _ in range(retries + 1):
        try:
            return await factory()
        except RetryAfter as e:
            last_error = e
            retry_after = int(getattr(e, "timeout", getattr(e, "retry_after", 1)))
            await asyncio.sleep(max(retry_after, 1))
        except Exception as e:
            last_error = e
            break
    if last_error:
        raise last_error


async def safe_delete_message(chat_id: int, message_id: int | None):
    if not message_id:
        return
    try:
        await tg_call(lambda: bot.delete_message(chat_id, message_id), retries=1)
    except Exception:
        pass


async def try_edit_text(chat_id: int, message_id: int, text: str, reply_markup=None) -> bool:
    try:
        await tg_call(lambda: bot.edit_message_text(text, chat_id, message_id, reply_markup=reply_markup, parse_mode="HTML"), retries=1)
        return True
    except MessageNotModified:
        return True
    except Exception:
        return False


async def safe_edit_text(chat_id: int, message_id: int, text: str, reply_markup=None):
    await try_edit_text(chat_id, message_id, text, reply_markup=reply_markup)


async def send_message(chat_id: int, text: str, reply_markup=None, thread_id: int = 0):
    return await tg_call(
        lambda: bot.send_message(
            chat_id,
            text,
            reply_markup=reply_markup,
            parse_mode="HTML",
            **({"message_thread_id": thread_id} if thread_id else {}),
        ),
        retries=1,
    )


async def send_temp_message(chat_id: int, text: str, thread_id: int = 0, delay: int = 8):
    try:
        msg = await send_message(chat_id, text, thread_id=thread_id)
    except Exception:
        return

    async def remover():
        await asyncio.sleep(delay)
        await safe_delete_message(chat_id, msg.message_id)

    asyncio.create_task(remover())


async def send_week_notice_pm(user_id: str, text: str):
    msg = await send_message(int(user_id), text)

    async def remover():
        await asyncio.sleep(7 * 24 * 3600)
        await safe_delete_message(int(user_id), msg.message_id)

    asyncio.create_task(remover())


async def try_delete_user_message(message: types.Message):
    try:
        await tg_call(lambda: message.delete(), retries=1)
    except Exception:
        pass


def get_menu_lock(wid: str) -> asyncio.Lock:
    MENU_LOCKS.setdefault(wid, asyncio.Lock())
    return MENU_LOCKS[wid]


def should_ignore_callback(cb: types.CallbackQuery) -> bool:
    key = (cb.from_user.id, cb.message.message_id if cb.message else 0, cb.data or "")
    ts = time.monotonic()
    prev = RECENT_CALLBACKS.get(key)
    RECENT_CALLBACKS[key] = ts

    if len(RECENT_CALLBACKS) > 5000:
        cutoff = ts - 60
        for k, v in list(RECENT_CALLBACKS.items()):
            if v < cutoff:
                RECENT_CALLBACKS.pop(k, None)

    return prev is not None and ts - prev < CALLBACK_DEBOUNCE_SECONDS


# =========================
# DATA
# =========================

def default_data():
    return {
        "users": {},
        "workspaces": {},
        "mirror_tokens": {},
    }


EMOJI_VARIATION_CHARS = {"\ufe0f", "\u200d"}


def is_single_emoji(text: str) -> bool:
    text = (text or "").strip()
    if not text or " " in text or any(ch.isalnum() for ch in text):
        return False
    cleaned = "".join(ch for ch in text if ch not in EMOJI_VARIATION_CHARS)
    return 1 <= len(cleaned) <= 4


COMMON_PREFIXES = ["📁", "😀", "😄", "🔥", "💋", "✅", "✔", "🗂", "⚙️", "⚙", "📂"]


def split_legacy_name(name: str | None, default_emoji: str = "📁") -> tuple[str, str]:
    raw = (name or "").strip()
    if not raw:
        return default_emoji, ""
    for prefix in COMMON_PREFIXES:
        if raw.startswith(prefix):
            title = raw[len(prefix):].lstrip()
            if title:
                return prefix, title
    return default_emoji, raw



def ensure_task(task, is_template: bool = False):
    if not isinstance(task, dict):
        task = {"text": str(task)}

    task.setdefault("id", uuid.uuid4().hex)
    task.setdefault("text", "")
    task.setdefault("category_id", None)
    task.setdefault("created_at", now_ts())

    if is_template:
        if task.get("deadline_seconds") is None:
            days = task.get("deadline_days")
            if isinstance(days, int) and days > 0:
                task["deadline_seconds"] = days * 86400
            else:
                task["deadline_seconds"] = None
        task.pop("deadline_days", None)
    else:
        task.setdefault("done", False)
        task.setdefault("deadline_due_at", None)
        task.setdefault("deadline_started_at", None)
    return task



def ensure_category(cat):
    if not isinstance(cat, dict):
        emoji, title = split_legacy_name(str(cat), "📁")
        cat = {"title": title or str(cat), "emoji": emoji}

    emoji, title = split_legacy_name(cat.get("name") or cat.get("title"), cat.get("emoji") or "📁")
    cat.setdefault("id", uuid.uuid4().hex)
    cat["emoji"] = cat.get("emoji") or emoji or "📁"
    cat["title"] = cat.get("title") or title or "Подгруппа"
    cat.setdefault("deadline_format", None)
    cat.pop("name", None)
    return cat



def ensure_company(company):
    if not isinstance(company, dict):
        company = {}

    legacy_name = company.get("name") or company.get("title")
    emoji, title = split_legacy_name(legacy_name, company.get("emoji") or "📁")

    company.setdefault("id", uuid.uuid4().hex)
    company["emoji"] = company.get("emoji") or emoji or "📁"
    company["title"] = company.get("title") or title or "Список"
    company.setdefault("card_msg_id", None)
    company.setdefault("mirror", None)
    company.setdefault("mirrors", [])
    company.setdefault("mirror_history", [])
    company.setdefault("tasks", [])
    company.setdefault("categories", [])
    company.setdefault("deadline_format", "relative")

    if company.get("mirror") and not company.get("mirrors"):
        company["mirrors"] = [company["mirror"]]
    if company.get("mirrors") and not company.get("mirror"):
        company["mirror"] = company["mirrors"][0]

    if not isinstance(company["tasks"], list):
        company["tasks"] = []
    if not isinstance(company["categories"], list):
        company["categories"] = []
    if not isinstance(company["mirror_history"], list):
        company["mirror_history"] = []
    if not isinstance(company["mirrors"], list):
        company["mirrors"] = []

    company["tasks"] = [ensure_task(t, is_template=False) for t in company["tasks"]]
    company["categories"] = [ensure_category(c) for c in company["categories"]]

    for history in company["mirror_history"]:
        if not isinstance(history, dict):
            continue
        history.setdefault("chat_id", None)
        history.setdefault("thread_id", 0)
        history.setdefault("message_id", None)

    norm_mirrors=[]
    for mirror in company.get("mirrors", []):
        if not isinstance(mirror, dict):
            continue
        mirror.setdefault("chat_id", None)
        mirror.setdefault("thread_id", 0)
        mirror.setdefault("message_id", None)
        mirror.setdefault("label", None)
        norm_mirrors.append(mirror)
    company["mirrors"] = norm_mirrors
    company["mirror"] = company["mirrors"][0] if company["mirrors"] else None

    company.pop("name", None)
    return company



def normalize_template(ws: dict):
    legacy_template = ws.get("template")
    if "templates" not in ws:
        if "template_tasks" in ws or "template_categories" in ws or isinstance(legacy_template, list):
            tasks = ws.get("template_tasks")
            categories = ws.get("template_categories")
            if not isinstance(tasks, list):
                tasks = [ensure_task({"text": item}, is_template=True) for item in legacy_template] if isinstance(legacy_template, list) else []
            if not isinstance(categories, list):
                categories = []
            ws["templates"] = [{
                "id": uuid.uuid4().hex,
                "title": ws.get("template_title") or "Шаблон",
                "emoji": ws.get("template_emoji") or "📁",
                "deadline_format": ws.get("template_deadline_format") or "relative",
                "tasks": [ensure_task(t, is_template=True) for t in tasks],
                "categories": [ensure_category(c) for c in categories],
            }]
        else:
            ws["templates"] = [{"id": uuid.uuid4().hex, "title": "Шаблон", "emoji": "📁", "deadline_format": "relative", "tasks": [], "categories": []}]

    if not isinstance(ws["templates"], list) or not ws["templates"]:
        ws["templates"] = [{"id": uuid.uuid4().hex, "title": "Шаблон", "emoji": "📁", "deadline_format": "relative", "tasks": [], "categories": []}]

    for tpl in ws["templates"]:
        if not isinstance(tpl, dict):
            tpl = {}
        tpl.setdefault("id", uuid.uuid4().hex)
        tpl.setdefault("title", "Шаблон")
        tpl.setdefault("emoji", "📁")
        tpl.setdefault("deadline_format", "relative")
        if not isinstance(tpl.get("tasks"), list):
            tpl["tasks"] = []
        if not isinstance(tpl.get("categories"), list):
            tpl["categories"] = []
        tpl["tasks"] = [ensure_task(t, is_template=True) for t in tpl["tasks"]]
        tpl["categories"] = [ensure_category(c) for c in tpl["categories"]]

    if ws.get("active_template_id") not in {tpl["id"] for tpl in ws["templates"]}:
        ws["active_template_id"] = ws["templates"][0]["id"]

    active = get_active_template(ws)
    ws["template_tasks"] = active["tasks"]
    ws["template_categories"] = active["categories"]
    ws["template_title"] = active.get("title") or "Шаблон"
    ws["template_emoji"] = active.get("emoji") or "📁"
    ws["template_deadline_format"] = active.get("deadline_format") or "relative"
    ws["template"] = [t["text"] for t in ws["template_tasks"] if not t.get("category_id")]



def get_active_template(ws: dict) -> dict:
    templates = ws.get("templates") or []
    if not templates:
        ws["templates"] = [{"id": uuid.uuid4().hex, "title": "Шаблон", "emoji": "📁", "deadline_format": "relative", "tasks": [], "categories": []}]
        templates = ws["templates"]
    active_id = ws.get("active_template_id") or templates[0]["id"]
    for tpl in templates:
        if tpl.get("id") == active_id:
            return tpl
    ws["active_template_id"] = templates[0]["id"]
    return templates[0]


def get_template_by_id(ws: dict, template_id: str | None) -> dict:
    templates = ws.get("templates") or []
    for tpl in templates:
        if tpl.get("id") == template_id:
            return tpl
    return get_active_template(ws)


def set_active_template(ws: dict, template_id: str):
    for tpl in ws.get("templates", []):
        if tpl.get("id") == template_id:
            ws["active_template_id"] = template_id
            ws["template_tasks"] = tpl["tasks"]
            ws["template_categories"] = tpl["categories"]
            ws["template_title"] = tpl.get("title") or "Шаблон"
            ws["template_emoji"] = tpl.get("emoji") or "📁"
            ws["template_deadline_format"] = tpl.get("deadline_format") or "relative"
            ws["template"] = [t["text"] for t in ws["template_tasks"] if not t.get("category_id")]
            return tpl
    return get_active_template(ws)


def display_template_name(template: dict) -> str:
    return f"{template.get('emoji') or '📁'}{template.get('title') or 'Шаблон'}"


def esc(value) -> str:
    return html.escape(str(value or ""))


def rich_display_company_name(company: dict) -> str:
    return f"<u>{esc(company.get('emoji') or '📁')}{esc(company.get('title') or 'Список')}</u>"


def rich_display_category_name(category: dict) -> str:
    return f"<u>{esc(category.get('emoji') or '📁')}{esc(category.get('title') or 'Подгруппа')}</u>"


def rich_display_template_name(template: dict) -> str:
    return f"<u>{esc(template.get('emoji') or '📁')}{esc(template.get('title') or 'Шаблон')}</u>"


def rich_task_text(task_text: str, done: bool = False) -> str:
    inner = f"<b><i>{esc(task_text)}</i></b>"
    if done:
        return f"<s>{inner}</s>"
    return inner


def template_exists(templates: list[dict], title: str, exclude_id: str | None = None) -> bool:
    target = (title or '').casefold()
    for tpl in templates:
        if exclude_id is not None and tpl.get('id') == exclude_id:
            continue
        if (tpl.get('title') or '').casefold() == target:
            return True
    return False


def clone_deadline_for_copy(task: dict) -> tuple[int | None, int | None]:
    started_at = task.get('deadline_started_at')
    due_at = task.get('deadline_due_at')
    if not started_at or not due_at or due_at <= started_at:
        return None, None
    total = max(int(due_at - started_at), 60)
    now_value = now_ts()
    return now_value, now_value + total


def copy_company_payload(company: dict, new_title: str) -> dict:
    category_map = {}
    new_company = {
        'id': uuid.uuid4().hex,
        'title': new_title,
        'emoji': company.get('emoji') or '📁',
        'card_msg_id': None,
        'mirror': None,
        'mirrors': [],
        'mirror_history': [],
        'deadline_format': company.get('deadline_format') or 'relative',
        'categories': [],
        'tasks': [],
    }
    for category in company.get('categories', []):
        new_id = uuid.uuid4().hex
        category_map[category['id']] = new_id
        new_company['categories'].append({'id': new_id, 'title': category.get('title') or 'Подгруппа', 'emoji': category.get('emoji') or '📁', 'deadline_format': category.get('deadline_format')})
    for task in company.get('tasks', []):
        started_at, due_at = clone_deadline_for_copy(task)
        new_company['tasks'].append({
            'id': uuid.uuid4().hex,
            'text': task.get('text') or '',
            'done': False,
            'category_id': category_map.get(task.get('category_id')),
            'created_at': now_ts(),
            'deadline_started_at': started_at,
            'deadline_due_at': due_at,
        })
    return new_company


def copy_category_into_company(company: dict, category_idx: int, new_title: str):
    source = company['categories'][category_idx]
    new_cat_id = uuid.uuid4().hex
    company['categories'].append({
        'id': new_cat_id,
        'title': new_title,
        'emoji': source.get('emoji') or '📁',
        'deadline_format': source.get('deadline_format'),
    })
    for task in list(company.get('tasks', [])):
        if task.get('category_id') != source.get('id'):
            continue
        started_at, due_at = clone_deadline_for_copy(task)
        company['tasks'].append({
            'id': uuid.uuid4().hex,
            'text': task.get('text') or '',
            'done': False,
            'category_id': new_cat_id,
            'created_at': now_ts(),
            'deadline_started_at': started_at,
            'deadline_due_at': due_at,
        })


def copy_template_category(template: dict, category_idx: int, new_title: str):
    source = template['categories'][category_idx]
    new_cat_id = uuid.uuid4().hex
    template['categories'].append({
        'id': new_cat_id,
        'title': new_title,
        'emoji': source.get('emoji') or '📁',
        'deadline_format': source.get('deadline_format'),
    })
    for task in list(template.get('tasks', [])):
        if task.get('category_id') != source.get('id'):
            continue
        template['tasks'].append({
            'id': uuid.uuid4().hex,
            'text': task.get('text') or '',
            'category_id': new_cat_id,
            'created_at': now_ts(),
            'deadline_seconds': task.get('deadline_seconds'),
        })


def copy_template_payload(template: dict, new_title: str) -> dict:
    category_map = {}
    new_tpl = {
        'id': uuid.uuid4().hex,
        'title': new_title,
        'emoji': template.get('emoji') or '📁',
        'deadline_format': template.get('deadline_format') or 'relative',
        'categories': [],
        'tasks': [],
    }
    for category in template.get('categories', []):
        new_id = uuid.uuid4().hex
        category_map[category['id']] = new_id
        new_tpl['categories'].append({'id': new_id, 'title': category.get('title') or 'Подгруппа', 'emoji': category.get('emoji') or '📁', 'deadline_format': category.get('deadline_format')})
    for task in template.get('tasks', []):
        new_tpl['tasks'].append({
            'id': uuid.uuid4().hex,
            'text': task.get('text') or '',
            'category_id': category_map.get(task.get('category_id')),
            'created_at': now_ts(),
            'deadline_seconds': task.get('deadline_seconds'),
        })
    return new_tpl


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
        user.setdefault("ui_pages", {})

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
        ws.setdefault("companies", [])
        ws.setdefault("awaiting", None)
        ws.setdefault("is_connected", True)
        ws.setdefault("ui_pages", {})

        if not isinstance(ws["companies"], list):
            ws["companies"] = []
        ws["companies"] = [ensure_company(c) for c in ws["companies"]]
        normalize_template(ws)

    valid_tokens = {}
    for token, payload in list(data["mirror_tokens"].items()):
        if not isinstance(payload, dict):
            continue
        source_wid = payload.get("source_wid")
        if not source_wid:
            continue
        if payload.get("company_id"):
            valid_tokens[token] = payload
            continue
        company_idx = payload.get("company_idx")
        ws = data["workspaces"].get(source_wid)
        if ws is None or not isinstance(company_idx, int):
            continue
        if company_idx < 0 or company_idx >= len(ws.get("companies", [])):
            continue
        payload["company_id"] = ws["companies"][company_idx]["id"]
        payload.pop("company_idx", None)
        valid_tokens[token] = payload
    data["mirror_tokens"] = valid_tokens
    return data


async def load_data_unlocked():
    if not os.path.exists(DATA_FILE):
        return default_data()
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return normalize_data(json.load(f))
    except Exception:
        return default_data()


async def save_data_unlocked(data):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(normalize_data(data), f, ensure_ascii=False, indent=2)


async def load_data():
    async with FILE_LOCK:
        return await load_data_unlocked()


async def save_data(data):
    async with FILE_LOCK:
        await save_data_unlocked(data)



def ensure_user(data, user_id: str):
    data["users"].setdefault(
        user_id,
        {
            "workspaces": [],
            "pm_menu_msg_id": None,
            "help_msg_id": None,
            "ui_pages": {},
        },
    )
    return data["users"][user_id]



def make_ws_id(chat_id: int, thread_id: int | None):
    return f"{chat_id}_{thread_id or 0}"



def thread_kwargs(thread_id: int):
    return {"message_thread_id": thread_id} if thread_id else {}



def clean_text(text: str) -> str:
    return (text or "").strip().lstrip("/").strip()



def is_known_command(text: str) -> bool:
    if not text or not text.startswith("/"):
        return False
    return text.split()[0].lower() in {"/start", "/connect", "/mirror"}



def workspace_full_name(chat_title: str, topic_title: str | None, thread_id: int) -> str:
    if thread_id:
        return f"{chat_title} - {(topic_title or f'Тред {thread_id}').strip()}"
    return chat_title



def extract_topic_title(message: types.Message) -> str | None:
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



def display_company_name(company: dict) -> str:
    return f"{company.get('emoji') or '📁'}{company.get('title') or 'Список'}"



def display_category_name(category: dict) -> str:
    return f"{category.get('emoji') or '📁'}{category.get('title') or 'Подгруппа'}"


def workspace_path_title(ws: dict, *parts: str) -> str:
    lines = [f"📂 {esc(ws.get('name') or 'Workspace')}:"]
    indent = "    "
    for part in parts:
        if part:
            lines.append(f"{indent}{part}")
            indent += "    "
    return "\n".join(lines)


def company_menu_title(ws: dict, company: dict) -> str:
    return workspace_path_title(ws, rich_display_company_name(company))


def company_settings_title(ws: dict, company: dict) -> str:
    return workspace_path_title(ws, rich_display_company_name(company), "⚙️ Настройки списка")


def category_menu_title(ws: dict, company: dict, category: dict) -> str:
    return workspace_path_title(ws, rich_display_company_name(company), rich_display_category_name(category))


def category_settings_title(ws: dict, company: dict, category: dict) -> str:
    return workspace_path_title(ws, rich_display_company_name(company), rich_display_category_name(category), "⚙️ Подгруппа")


def task_menu_title(ws: dict, company: dict, task: dict, category: dict | None = None) -> str:
    parts = [rich_display_company_name(company)]
    if category:
        parts.append(rich_display_category_name(category))
    parts.append(f"📌 {rich_task_text(task.get('text') or 'Задача', bool(task.get('done')))}")
    return workspace_path_title(ws, *parts)


def templates_root_title(ws: dict) -> str:
    return workspace_path_title(ws, "⚙️ Шаблоны задач")


def template_menu_title(ws: dict, template: dict) -> str:
    return workspace_path_title(ws, "⚙️ Шаблоны задач", rich_display_template_name(template))


def template_settings_title(ws: dict, template: dict) -> str:
    return workspace_path_title(ws, "⚙️ Шаблоны задач", rich_display_template_name(template), "⚙️ Шаблон")


def template_category_title(ws: dict, template: dict, category: dict) -> str:
    return workspace_path_title(ws, "⚙️ Шаблоны задач", rich_display_template_name(template), rich_display_category_name(category))


def template_category_settings_title(ws: dict, template: dict, category: dict) -> str:
    return workspace_path_title(ws, "⚙️ Шаблоны задач", rich_display_template_name(template), rich_display_category_name(category), "⚙️ Подгруппа")


def template_task_title(ws: dict, template: dict, task: dict, category: dict | None = None) -> str:
    parts = ["⚙️ Шаблоны задач", rich_display_template_name(template)]
    if category:
        parts.append(rich_display_category_name(category))
    parts.append(f"📌 {rich_task_text(task.get('text') or 'Задача')}{esc(template_task_deadline_suffix(task, template.get('deadline_format') or 'relative'))}")
    return workspace_path_title(ws, *parts)


def mirrors_menu_title(ws: dict, company: dict) -> str:
    return workspace_path_title(ws, rich_display_company_name(company), "📤 Дублирование списка")



def ceil_minutes(seconds: int) -> int:
    return max(0, math.ceil(seconds / 60))


def format_duration_text(seconds: int | None) -> str:
    if seconds is None:
        return ""
    minutes = ceil_minutes(seconds)
    days, rem = divmod(minutes, 60 * 24)
    hours, mins = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days} д.")
    if hours or days:
        parts.append(f"{hours} ч.")
    parts.append(f"{mins} м.")
    return "; ".join(parts)


def format_due_date_text(ts: int | None) -> str:
    if not ts:
        return ""
    return datetime.fromtimestamp(ts, TIMEZONE).strftime("до %d.%m.%Y г. %H:%M")




def iter_company_root_tasks(company: dict):
    return [task for task in company.get("tasks", []) if not task.get("category_id")]


def iter_template_root_tasks(template: dict):
    return [task for task in template.get("tasks", []) if not task.get("category_id")]


def task_status_icon(task: dict) -> str:
    return task_deadline_icon(task)


def template_task_deadline_suffix(task: dict, deadline_format: str = "relative") -> str:
    value = task.get("deadline_value")
    if not value:
        return ""
    return f" ({format_duration_text(value)})"


def display_template_task_button(task: dict) -> str:
    text = str(task.get("text") or "Без названия")
    return f"{task_status_icon(task)} {text}{template_task_deadline_suffix(task)}"
def display_task_deadline_suffix(task: dict, deadline_format: str = "relative") -> str:
    due_at = task.get("deadline_due_at")
    if not due_at:
        return ""
    if deadline_format == "date":
        return f" ({format_due_date_text(due_at)})"
    return f" ({format_duration_text(due_at - now_ts())})"



def task_deadline_icon(task: dict) -> str:
    if task.get("done"):
        return "✅"
    due_at = task.get("deadline_due_at")
    started_at = task.get("deadline_started_at")
    if not due_at or not started_at:
        return "⏺️"
    total = max(due_at - started_at, 1)
    remaining = due_at - now_ts()
    if remaining <= 0:
        return "💔"
    remaining_part = remaining / total
    if remaining_part >= 0.85:
        return "🩵"
    if remaining_part >= 0.5:
        return "💛"
    if remaining_part >= 0.25:
        return "🧡"
    return "❤️"



def sort_company_tasks(tasks: list[dict]) -> list[dict]:
    def key(task: dict):
        done = 1 if task.get("done") else 0
        due_at = task.get("deadline_due_at")
        no_due = 1 if not due_at else 0
        return (done, no_due, due_at or 10**18, task.get("created_at") or 0)

    return sorted(tasks, key=key)



def sort_template_tasks(tasks: list[dict]) -> list[dict]:
    def key(task: dict):
        seconds = task.get("deadline_seconds")
        no_due = 1 if seconds is None else 0
        return (no_due, seconds if seconds is not None else 10**18, task.get("created_at") or 0)

    return sorted(tasks, key=key)



def build_progress_bar(done_count: int, total_count: int) -> str:
    if total_count <= 0:
        progress = 0.0
    else:
        progress = (done_count / total_count) * 10.0

    full = int(progress)
    rem = progress - full
    cells = ["🌕"] * full

    if len(cells) < 10:
        if rem <= 0:
            partial = "🌑"
        elif rem < 0.375:
            partial = "🌘"
        elif rem < 0.625:
            partial = "🌗"
        elif rem < 0.875:
            partial = "🌖"
        else:
            partial = "🌕"
        cells.append(partial)

    cells = cells[:10] + ["🌑"] * max(0, 10 - len(cells[:10]))
    percent = 0.0 if total_count <= 0 else (done_count / total_count) * 100
    return f"{''.join(cells)} <b>{percent:.1f} %</b>"



def task_progress_bar_line(tasks: list[dict], indent: str = "") -> str:
    total = len(tasks)
    done = sum(1 for task in tasks if task.get("done"))
    return f"{indent}{build_progress_bar(done, total)}"


def company_card_text(company: dict) -> str:
    lines = [f"{rich_display_company_name(company)}:"]
    company_deadline_format = company.get("deadline_format") or "relative"
    all_tasks = company.get("tasks", [])
    lines.append(task_progress_bar_line(all_tasks))
    lines.append("")

    uncategorized = [t for t in all_tasks if not t.get("category_id")]
    if uncategorized:
        for task in sort_company_tasks(uncategorized):
            icon = task_deadline_icon(task)
            suffix = display_task_deadline_suffix(task, company_deadline_format) if not task.get("done") and task.get("deadline_due_at") else ""
            lines.append(f"{icon} {rich_task_text(task.get('text') or 'Задача', bool(task.get('done')))}{esc(suffix)}")

    for category in company.get("categories", []):
        if lines and lines[-1] != "":
            lines.append("")
        cat_tasks = [t for t in all_tasks if t.get("category_id") == category["id"]]
        lines.append(f"    {rich_display_category_name(category)}:")
        lines.append(task_progress_bar_line(cat_tasks, indent="    "))
        lines.append("")
        if cat_tasks:
            for task in sort_company_tasks(cat_tasks):
                icon = task_deadline_icon(task)
                suffix = display_task_deadline_suffix(task, category.get("deadline_format") or company_deadline_format) if not task.get("done") and task.get("deadline_due_at") else ""
                lines.append(f"        {icon} {rich_task_text(task.get('text') or 'Задача', bool(task.get('done')))}{esc(suffix)}")

    while lines and lines[-1] == "":
        lines.pop()

    if len(lines) == 2 and not all_tasks and not company.get("categories"):
        lines.append("—")
    return "\n".join(lines)




def pm_main_text(user_id: str, data: dict) -> str:
    user = ensure_user(data, user_id)
    ws_ids = [wid for wid in user.get("workspaces", []) if data["workspaces"].get(wid, {}).get("is_connected")]
    if not ws_ids:
        return "📂 Ваши workspace: Нет workspace"
    return "📂 Ваши workspace:"


def generate_mirror_token() -> str:
    return uuid.uuid4().hex[:8].upper()



PAGE_SIZE_PM = 8
PAGE_SIZE_WS = 8
PAGE_SIZE_TEMPLATES = 8
PAGE_SIZE_COMPANY = 8
PAGE_SIZE_CATEGORY = 8
PAGE_SIZE_CREATE = 8


def ui_pages(owner: dict) -> dict:
    owner.setdefault("ui_pages", {})
    return owner["ui_pages"]


def get_ui_page(owner: dict, key: str) -> int:
    try:
        return max(0, int(ui_pages(owner).get(key, 0) or 0))
    except Exception:
        return 0


def set_ui_page(owner: dict, key: str, page: int):
    ui_pages(owner)[key] = max(0, int(page))


def paginate_items(items, page: int, page_size: int):
    total = len(items)
    if total <= 0:
        return [], False, False
    max_page = max(0, (total - 1) // page_size)
    page = max(0, min(page, max_page))
    start = page * page_size
    end = start + page_size
    return items[start:end], page > 0, page < max_page


def ws_home_page_key() -> str:
    return "ws_home"


def company_create_page_key() -> str:
    return "cmp_create"


def templates_root_page_key() -> str:
    return "tpl_root"


def company_menu_page_key(company_idx: int) -> str:
    return f"cmp_{company_idx}"


def category_menu_page_key(company_idx: int, category_idx: int) -> str:
    return f"cat_{company_idx}_{category_idx}"


def active_template_page_key(ws: dict) -> str:
    tpl = get_active_template(ws)
    tpl_id = tpl.get("id") if tpl else "none"
    return f"tpl_{tpl_id}"


def active_template_category_page_key(ws: dict, category_idx: int) -> str:
    tpl = get_active_template(ws)
    tpl_id = tpl.get("id") if tpl else "none"
    return f"tplcat_{tpl_id}_{category_idx}"



def find_company_index_by_id(ws: dict, company_id: str) -> int | None:
    for idx, company in enumerate(ws.get("companies", [])):
        if company.get("id") == company_id:
            return idx
    return None



def find_category_index(categories: list[dict], category_id: str) -> int | None:
    for idx, category in enumerate(categories):
        if category.get("id") == category_id:
            return idx
    return None



def company_exists(ws: dict, title: str, exclude_idx: int | None = None) -> bool:
    target = title.casefold()
    for idx, company in enumerate(ws.get("companies", [])):
        if exclude_idx is not None and idx == exclude_idx:
            continue
        if (company.get("title") or "").casefold() == target:
            return True
    return False



def category_exists(categories: list[dict], title: str, exclude_id: str | None = None) -> bool:
    target = title.casefold()
    for category in categories:
        if exclude_id is not None and category.get("id") == exclude_id:
            continue
        if (category.get("title") or "").casefold() == target:
            return True
    return False



def delete_category_keep_tasks(tasks: list[dict], categories: list[dict], category_id: str):
    for task in tasks:
        if task.get("category_id") == category_id:
            task["category_id"] = None
    categories[:] = [c for c in categories if c.get("id") != category_id]



def delete_category_with_tasks(tasks: list[dict], categories: list[dict], category_id: str):
    tasks[:] = [t for t in tasks if t.get("category_id") != category_id]
    categories[:] = [c for c in categories if c.get("id") != category_id]



def make_company(title: str, with_template: bool, ws: dict, template_id: str | None = None) -> dict:
    company = {
        "id": uuid.uuid4().hex,
        "title": title,
        "emoji": "📁",
        "card_msg_id": None,
        "mirror": None,
        "mirrors": [],
        "mirror_history": [],
        "deadline_format": "relative",
        "categories": [],
        "tasks": [],
    }
    if not with_template:
        return company

    template = get_template_by_id(ws, template_id)
    company["deadline_format"] = template.get("deadline_format") or "relative"
    category_map = {}
    for template_category in template.get("categories", []):
        new_cat = {
            "id": uuid.uuid4().hex,
            "title": template_category.get("title") or "Подгруппа",
            "emoji": template_category.get("emoji") or "📁",
            "deadline_format": template_category.get("deadline_format"),
        }
        category_map[template_category["id"]] = new_cat["id"]
        company["categories"].append(new_cat)

    now_value = now_ts()
    for template_task in template.get("tasks", []):
        deadline_seconds = template_task.get("deadline_seconds")
        due_at = now_value + deadline_seconds if isinstance(deadline_seconds, int) and deadline_seconds > 0 else None
        company["tasks"].append({
            "id": uuid.uuid4().hex,
            "text": template_task.get("text") or "",
            "done": False,
            "category_id": category_map.get(template_task.get("category_id")),
            "created_at": now_value,
            "deadline_due_at": due_at,
            "deadline_started_at": now_value if due_at else None,
        })
    return company



def legacy_task_menu_title(company: dict, task: dict, category: dict | None = None) -> str:
    if category:
        return f"{display_category_name(category)}/📌 {task['text']}"
    return f"{display_company_name(company)}/📌 {task['text']}"



def template_task_label(task: dict, deadline_format: str = "relative") -> str:
    seconds = task.get("deadline_seconds")
    suffix = f" ({format_duration_text(seconds)})" if isinstance(seconds, int) and seconds > 0 else ""
    return f"📌 {task['text']}{suffix}"



def parse_relative_duration_seconds(text: str) -> int | None:
    raw = clean_text(text).lower()
    if not raw:
        return None
    if raw.isdigit():
        value = int(raw)
        return value * 86400 if value > 0 else None
    s = raw.replace(',', ' ').replace(';', ' ')
    s = re.sub(r'(\d)([а-яa-z])', r'\1 \2', s)
    s = re.sub(r'([а-яa-z])(\d)', r'\1 \2', s)
    tokens = re.findall(r'(\d+)\s*([а-яa-z\.]+)', s)
    if not tokens:
        return None
    total = 0
    for value, unit in tokens:
        n = int(value)
        unit = unit.strip('. ').lower()
        if unit.startswith('д'):
            total += n * 86400
        elif unit.startswith('ч') or unit.startswith('h'):
            total += n * 3600
        elif unit.startswith('м') or unit.startswith('min'):
            total += n * 60
        else:
            return None
    return total if total > 0 else None


def parse_flexible_datetime(text: str) -> int | None:
    raw = clean_text(text)
    m = re.match(r'^\s*(\d{1,2})\D+(\d{1,2})\D+(\d{2,4})(?:\D+(\d{1,2})(?:\D+(\d{1,2}))?)?\s*$', raw)
    if not m:
        return None
    day, month, year, hh, mm = m.groups()
    year = int(year)
    if year < 100:
        year += 2000
    hour = int(hh) if hh is not None else 23
    minute = int(mm) if mm is not None else 59
    try:
        dt = datetime(year, int(month), int(day), hour, minute, tzinfo=TIMEZONE)
    except ValueError:
        return None
    return int(dt.timestamp())


def parse_deadline_input(text: str, keep_started_at: int | None = None) -> tuple[int | None, int | None, str | None]:
    raw = clean_text(text)
    if not raw:
        return None, None, "Дату или срок введи корректно, барсурка стахановская"
    due_at = parse_flexible_datetime(raw)
    if due_at is None:
        seconds = parse_relative_duration_seconds(raw)
        if seconds is None:
            return None, None, "Дату или срок введи корректно, барсурка стахановская"
        started_at = keep_started_at or now_ts()
        return started_at, started_at + seconds, None
    started_at = keep_started_at or now_ts()
    if due_at <= started_at:
        return None, None, "Дату или срок введи корректно, барсурка стахановская"
    return started_at, due_at, None


def parse_template_deadline_seconds(text: str) -> tuple[int | None, str | None]:
    raw = clean_text(text)
    seconds = parse_relative_duration_seconds(raw)
    if seconds is None:
        return None, "Пришли срок, например: 3 дня, 7ч20м, 45 минут."
    return seconds, None


# =========================
# KEYBOARDS
# =========================


def infer_button_style(text: str, callback_data: str | None = None) -> str | None:
    t = (text or '').strip().lower()
    cb = (callback_data or '').strip().lower()

    if t.startswith('⬅️') or t.startswith('⬆️') or t.startswith('⬇️'):
        return 'primary'

    entity_prefixes = (
        'pmws:',
        'pmpersonal:',
        'cmp:',
        'cat:',
        'task:',
        'tpl:',
        'tplcat:',
        'tpltask:',
        'tplselect:',
        'cmpmode:',
        'mirroritem:',
        'taskmoveto:',
        'tpltaskmoveto:',
    )

    settings_action_prefixes = (
        'wsclearask:',
        'pmwsclearask:',
        'cmpren:',
        'cmpemoji:',
        'cmpcopy:',
        'cmpdeadlinefmt:',
        'cmpdelask:',
        'catren:',
        'catemoji:',
        'catcopy:',
        'catdeadlinefmt:',
        'catdel:',
        'catdelall:',
        'tplrenameset:',
        'tplemojiset:',
        'tplcopy:',
        'tpldelset:',
        'tplcatren:',
        'tplcatemoji:',
        'tplcatcopy:',
        'tplcatdel:',
        'tplcatdelall:',
    )

    if cb.startswith(entity_prefixes) or cb.startswith(settings_action_prefixes):
        return None

    if (
        t.startswith('➕')
        or t.startswith('добавить')
        or t.startswith('создать')
        or t.startswith('подключить')
    ):
        return 'success'

    return 'primary'


def kb_btn(text: str, callback_data: str | None = None, style: str | None = None, **kwargs):
    btn = InlineKeyboardButton(text=text, callback_data=callback_data, **kwargs)
    btn_style = style or infer_button_style(text, callback_data)
    if btn_style:
        try:
            btn.values['style'] = btn_style
        except Exception:
            pass
    return btn


def pm_main_kb(user_id: str, data: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("👤 Личный workspace", callback_data="pmpersonal:root"))

    user = ensure_user(data, user_id)
    items = []
    for wid in user.get("workspaces", []):
        if str(wid).startswith("pm_"):
            continue
        ws = data["workspaces"].get(wid)
        if ws and ws.get("is_connected"):
            items.append((ws["name"], f"pmws:{wid}"))

    page = get_ui_page(user, "pm_root")
    visible, has_prev, has_next = paginate_items(items, page, PAGE_SIZE_PM)

    for title, callback_data in visible:
        kb.add(kb_btn(title, callback_data=callback_data))

    if has_prev or has_next:
        row1 = [kb_btn("➕ Workspace", callback_data="pmhelp:root")]
        if has_prev:
            row1.append(kb_btn("⬆️", callback_data="pgpm:prev"))
        kb.row(*row1)

        row2 = [kb_btn("🔄 Обновить", callback_data="pmrefresh:root")]
        if has_next:
            row2.append(kb_btn("⬇️", callback_data="pgpm:next"))
        kb.row(*row2)
    else:
        kb.row(
            kb_btn("➕ Workspace", callback_data="pmhelp:root"),
            kb_btn("🔄 Обновить", callback_data="pmrefresh:root"),
        )
    return kb


def pm_ws_manage_kb(wid: str):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("🧹 Очистить workspace", callback_data=f"pmwsclearask:{wid}"))
    kb.add(kb_btn("🗑 Удалить workspace", callback_data=f"pmwsdelask:{wid}"))
    kb.add(kb_btn("⬅️", callback_data="pmrefresh:root"))
    return kb


def ws_settings_kb(wid: str):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("🧹 Очистить workspace", callback_data=f"wsclearask:{wid}"))
    kb.add(kb_btn("⬅️", callback_data=f"backws:{wid}"))
    return kb


def ws_home_kb(wid: str, ws: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    items = [(display_company_name(company), f"cmp:{wid}:{idx}") for idx, company in enumerate(ws.get("companies", []))]
    page = get_ui_page(ws, ws_home_page_key())
    visible, has_prev, has_next = paginate_items(items, page, PAGE_SIZE_WS)

    for title, callback_data in visible:
        kb.add(kb_btn(title, callback_data=callback_data))

    nav_prev_in_upper = has_prev and has_next
    nav_last = has_next or (has_prev and not has_next)

    is_personal = str(wid).startswith("pm_")
    if is_personal:
        row1 = [
            kb_btn("➕ Список", callback_data=f"cmpnew:{wid}"),
            kb_btn("📇 Шаблоны", callback_data=f"tplroot:{wid}"),
        ]
        if nav_prev_in_upper:
            row1.append(kb_btn("⬆️", callback_data=f"pg:{wid}:wh:x:x:prev"))
        kb.row(*row1)

        row2 = [
            kb_btn("⬅️", callback_data="pmrefresh:root"),
            kb_btn("⚙️ Workspace", callback_data=f"wsset:{wid}"),
        ]
        if nav_last:
            arrow_cb = f"pg:{wid}:wh:x:x:next" if has_next else f"pg:{wid}:wh:x:x:prev"
            arrow_text = "⬇️" if has_next else "⬆️"
            row2.append(kb_btn(arrow_text, callback_data=arrow_cb))
        kb.row(*row2)
    else:
        row1 = [kb_btn("➕ Список", callback_data=f"cmpnew:{wid}")]
        if nav_prev_in_upper:
            row1.append(kb_btn("⬆️", callback_data=f"pg:{wid}:wh:x:x:prev"))
        kb.row(*row1)

        row2 = [kb_btn("📇 Шаблоны", callback_data=f"tplroot:{wid}")]
        if nav_last:
            arrow_cb = f"pg:{wid}:wh:x:x:next" if has_next else f"pg:{wid}:wh:x:x:prev"
            arrow_text = "⬇️" if has_next else "⬆️"
            row2.append(kb_btn(arrow_text, callback_data=arrow_cb))
        kb.row(*row2)
    return kb


def company_create_mode_kb(wid: str, ws: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    templates = ws.get("templates", [])
    items = [(f"По шаблону {display_template_name(tpl)}", f"cmpmode:{wid}:tpl:{tpl['id']}") for tpl in templates]
    page = get_ui_page(ws, company_create_page_key())
    visible, has_prev, has_next = paginate_items(items, page, PAGE_SIZE_CREATE)

    for title, callback_data in visible:
        kb.add(kb_btn(title, callback_data=callback_data))

    if has_prev and has_next:
        kb.row(
            kb_btn("⬅️", callback_data=f"backws:{wid}"),
            kb_btn("⬆️", callback_data=f"pg:{wid}:cc:x:x:prev"),
        )
        kb.row(
            kb_btn("🐚 Пустую", callback_data=f"cmpmode:{wid}:empty"),
            kb_btn("⬇️", callback_data=f"pg:{wid}:cc:x:x:next"),
        )
    elif has_prev:
        kb.row(kb_btn("⬅️", callback_data=f"backws:{wid}"))
        kb.row(
            kb_btn("🐚 Пустую", callback_data=f"cmpmode:{wid}:empty"),
            kb_btn("⬆️", callback_data=f"pg:{wid}:cc:x:x:prev"),
        )
    elif has_next:
        kb.row(kb_btn("⬅️", callback_data=f"backws:{wid}"))
        kb.row(
            kb_btn("🐚 Пустую", callback_data=f"cmpmode:{wid}:empty"),
            kb_btn("⬇️", callback_data=f"pg:{wid}:cc:x:x:next"),
        )
    else:
        kb.row(
            kb_btn("⬅️", callback_data=f"backws:{wid}"),
            kb_btn("🐚 Пустую", callback_data=f"cmpmode:{wid}:empty"),
        )
    return kb


def company_menu_kb(wid: str, company_idx: int, company: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    items = []
    for task_idx, task in enumerate(iter_company_root_tasks(company)):
        icon = task_status_icon(task)
        items.append((f"{icon} {task['text']}", f"task:{wid}:{company_idx}:{task_idx}"))
    for category_idx, category in enumerate(company.get("categories", [])):
        items.append((display_category_name(category), f"cat:{wid}:{company_idx}:{category_idx}"))

    page = get_ui_page(company, company_menu_page_key(company_idx))
    visible, has_prev, has_next = paginate_items(items, page, PAGE_SIZE_COMPANY)

    for title, callback_data in visible:
        kb.add(kb_btn(title, callback_data=callback_data))

    nav_prev_in_upper = has_prev and has_next
    nav_last = has_next or (has_prev and not has_next)

    row1 = [
        kb_btn("➕ Задача", callback_data=f"tasknew:{wid}:{company_idx}:root"),
        kb_btn("➕ Подгруппа", callback_data=f"catnew:{wid}:{company_idx}"),
    ]
    if nav_prev_in_upper:
        row1.append(kb_btn("⬆️", callback_data=f"pg:{wid}:cm:{company_idx}:x:prev"))
    kb.row(*row1)

    row2 = [
        kb_btn("⬅️", callback_data=f"backws:{wid}"),
        kb_btn("⚙️ Список", callback_data=f"cmpset:{wid}:{company_idx}"),
    ]
    if nav_last:
        arrow_cb = f"pg:{wid}:cm:{company_idx}:x:next" if has_next else f"pg:{wid}:cm:{company_idx}:x:prev"
        arrow_text = "⬇️" if has_next else "⬆️"
        row2.append(kb_btn(arrow_text, callback_data=arrow_cb))
    kb.row(*row2)
    return kb

def company_settings_kb(wid: str, company_idx: int):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("✍️ Переименовать список", callback_data=f"cmpren:{wid}:{company_idx}"))
    kb.add(kb_btn("😀 Переприсвоить смайлик", callback_data=f"cmpemoji:{wid}:{company_idx}"))
    kb.add(kb_btn("🧬 Копия Списка", callback_data=f"cmpcopy:{wid}:{company_idx}"))
    kb.add(kb_btn("📤 Дублирование списка", callback_data=f"mirrors:{wid}:{company_idx}"))
    kb.add(kb_btn("🕒 Формат дедлайнов", callback_data=f"cmpdeadlinefmt:{wid}:{company_idx}"))
    kb.add(kb_btn("🗑 Удалить список", callback_data=f"cmpdelask:{wid}:{company_idx}"))
    kb.add(kb_btn("⬅️", callback_data=f"cmp:{wid}:{company_idx}"))
    return kb


def category_menu_kb(wid: str, company_idx: int, category_idx: int, category: dict, company: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    task_buttons = []
    for task_idx, task in enumerate(company.get("tasks", [])):
        if task.get("category_id") == category.get("id"):
            icon = task_status_icon(task)
            task_buttons.append((f"{icon} {task['text']}", f"task:{wid}:{company_idx}:{task_idx}"))

    page = get_ui_page(company, category_menu_page_key(company_idx, category_idx))
    visible, has_prev, has_next = paginate_items(task_buttons, page, PAGE_SIZE_CATEGORY)

    for title, callback_data in visible:
        kb.add(kb_btn(title, callback_data=callback_data))

    kb.row(
        kb_btn("➕ Задача", callback_data=f"tasknew:{wid}:{company_idx}:{category_idx}"),
        kb_btn("⚙️ Подгруппа", callback_data=f"catset:{wid}:{company_idx}:{category_idx}"),
    )

    row2 = [kb_btn("⬅️", callback_data=f"cmp:{wid}:{company_idx}")]
    if has_next:
        row2.append(kb_btn("⬇️", callback_data=f"pg:{wid}:ct:{company_idx}:{category_idx}:next"))
    elif has_prev:
        row2.append(kb_btn("⬆️", callback_data=f"pg:{wid}:ct:{company_idx}:{category_idx}:prev"))
    if has_prev and has_next:
        row2 = [kb_btn("⬅️", callback_data=f"cmp:{wid}:{company_idx}"), kb_btn("⬆️", callback_data=f"pg:{wid}:ct:{company_idx}:{category_idx}:prev"), kb_btn("⬇️", callback_data=f"pg:{wid}:ct:{company_idx}:{category_idx}:next")]
    kb.row(*row2)
    return kb

def category_settings_kb(wid: str, company_idx: int, category_idx: int):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("✍️ Переименовать", callback_data=f"catren:{wid}:{company_idx}:{category_idx}"))
    kb.add(kb_btn("😀 Переприсвоить смайлик", callback_data=f"catemoji:{wid}:{company_idx}:{category_idx}"))
    kb.add(kb_btn("🧬 Копия Подгруппы", callback_data=f"catcopy:{wid}:{company_idx}:{category_idx}"))
    kb.add(kb_btn("🕒 Формат дедлайнов", callback_data=f"catdeadlinefmt:{wid}:{company_idx}:{category_idx}"))
    kb.add(kb_btn("🗑 Удалить", callback_data=f"catdel:{wid}:{company_idx}:{category_idx}"))
    kb.add(kb_btn("🗑 Удалить с задачами", callback_data=f"catdelall:{wid}:{company_idx}:{category_idx}"))
    kb.add(kb_btn("⬅️", callback_data=f"cat:{wid}:{company_idx}:{category_idx}"))
    return kb

def task_menu_kb(wid: str, company_idx: int, task_idx: int, task: dict, company: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    if task.get("done"):
        kb.add(kb_btn("⏺️ Отменить выполнение", callback_data=f"taskdone:{wid}:{company_idx}:{task_idx}"))
    else:
        kb.add(kb_btn("✅ Отметить выполненной", callback_data=f"taskdone:{wid}:{company_idx}:{task_idx}"))
    kb.add(kb_btn("✍️ Переименовать", callback_data=f"taskren:{wid}:{company_idx}:{task_idx}"))

    if not task.get("done"):
        if task.get("deadline_due_at"):
            kb.add(kb_btn("⏰ Поменять дедлайн", callback_data=f"taskdeadline:{wid}:{company_idx}:{task_idx}"))
            kb.add(kb_btn("🗑 Удалить дедлайн", callback_data=f"taskdeadel:{wid}:{company_idx}:{task_idx}"))
        else:
            kb.add(kb_btn("⏰ Установить дедлайн", callback_data=f"taskdeadline:{wid}:{company_idx}:{task_idx}"))

    if company.get("categories"):
        if task.get("category_id"):
            kb.add(kb_btn("📥 Перевсунуть", callback_data=f"taskmove:{wid}:{company_idx}:{task_idx}"))
        else:
            kb.add(kb_btn("📥 Всунуть в подгруппу", callback_data=f"taskmove:{wid}:{company_idx}:{task_idx}"))

    kb.add(kb_btn("🗑 Удалить задачу", callback_data=f"taskdel:{wid}:{company_idx}:{task_idx}"))
    back = f"cat:{wid}:{company_idx}:{find_category_index(company.get('categories', []), task.get('category_id'))}" if task.get("category_id") and find_category_index(company.get('categories', []), task.get('category_id')) is not None else f"cmp:{wid}:{company_idx}"
    kb.add(kb_btn("⬅️", callback_data=back))
    return kb



def task_move_kb(wid: str, company_idx: int, task_idx: int, company: dict, task: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    current_category_id = task.get("category_id")
    for category_idx, category in enumerate(company.get("categories", [])):
        if category.get("id") == current_category_id:
            continue
        kb.add(kb_btn(display_category_name(category), callback_data=f"taskmoveto:{wid}:{company_idx}:{task_idx}:{category_idx}"))
    if current_category_id:
        kb.add(kb_btn("📤 Высунуть", callback_data=f"taskmoveout:{wid}:{company_idx}:{task_idx}"))
    kb.add(kb_btn("⬅️", callback_data=f"task:{wid}:{company_idx}:{task_idx}"))
    return kb




def templates_root_kb(wid: str, ws: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    items = [(display_template_name(tpl), f"tplselect:{wid}:{tpl['id']}") for tpl in ws.get("templates", [])]
    page = get_ui_page(ws, templates_root_page_key())
    visible, has_prev, has_next = paginate_items(items, page, PAGE_SIZE_TEMPLATES)

    for title, callback_data in visible:
        kb.add(kb_btn(title, callback_data=callback_data))

    if has_prev and has_next:
        kb.row(
            kb_btn("➕ Шаблон", callback_data=f"tplnewset:{wid}"),
            kb_btn("⬆️", callback_data=f"pg:{wid}:tr:x:x:prev"),
        )
        kb.row(
            kb_btn("⬅️", callback_data=f"backws:{wid}"),
            kb_btn("⬇️", callback_data=f"pg:{wid}:tr:x:x:next"),
        )
    elif has_prev:
        kb.row(kb_btn("➕ Шаблон", callback_data=f"tplnewset:{wid}"))
        kb.row(
            kb_btn("⬅️", callback_data=f"backws:{wid}"),
            kb_btn("⬆️", callback_data=f"pg:{wid}:tr:x:x:prev"),
        )
    elif has_next:
        kb.row(kb_btn("➕ Шаблон", callback_data=f"tplnewset:{wid}"))
        kb.row(
            kb_btn("⬅️", callback_data=f"backws:{wid}"),
            kb_btn("⬇️", callback_data=f"pg:{wid}:tr:x:x:next"),
        )
    else:
        kb.row(
            kb_btn("⬅️", callback_data=f"backws:{wid}"),
            kb_btn("➕ Шаблон", callback_data=f"tplnewset:{wid}"),
        )
    return kb


def template_menu_kb(wid: str, ws: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    template = get_active_template(ws)
    if not template:
        return templates_root_kb(wid, ws)

    items = []
    for task_idx, task in enumerate(iter_template_root_tasks(template)):
        items.append((display_template_task_button(task), f"tpltask:{wid}:{task_idx}"))
    for category_idx, category in enumerate(template.get("categories", [])):
        items.append((display_category_name(category), f"tplcat:{wid}:{category_idx}"))

    page = get_ui_page(ws, active_template_page_key(ws))
    visible, has_prev, has_next = paginate_items(items, page, PAGE_SIZE_COMPANY)

    for title, callback_data in visible:
        kb.add(kb_btn(title, callback_data=callback_data))

    nav_prev_in_upper = has_prev and has_next
    nav_last = has_next or (has_prev and not has_next)

    row1 = [
        kb_btn("➕ Задача", callback_data=f"tpltasknew:{wid}:root"),
        kb_btn("➕ Подгруппа", callback_data=f"tplcatnew:{wid}"),
    ]
    if nav_prev_in_upper:
        row1.append(kb_btn("⬆️", callback_data=f"pg:{wid}:tm:x:x:prev"))
    kb.row(*row1)

    row2 = [
        kb_btn("⬅️", callback_data=f"tplroot:{wid}"),
        kb_btn("⚙️ Шаблон", callback_data=f"tplsettings:{wid}"),
    ]
    if nav_last:
        arrow_cb = f"pg:{wid}:tm:x:x:next" if has_next else f"pg:{wid}:tm:x:x:prev"
        arrow_text = "⬇️" if has_next else "⬆️"
        row2.append(kb_btn(arrow_text, callback_data=arrow_cb))
    kb.row(*row2)
    return kb


def template_settings_kb(wid: str):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("✍️ Переименовать шаблон", callback_data=f"tplrenameset:{wid}"))
    kb.add(kb_btn("😀 Переприсвоить смайлик", callback_data=f"tplemojiset:{wid}"))
    kb.add(kb_btn("🧬 Копия шаблона", callback_data=f"tplcopy:{wid}"))
    kb.add(kb_btn("🗑 Удалить шаблон", callback_data=f"tpldelset:{wid}"))
    kb.add(kb_btn("⬅️", callback_data=f"tpl:{wid}"))
    return kb


def template_category_menu_kb(wid: str, category_idx: int, category: dict, template: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    task_buttons = []
    for task_idx, task in enumerate(template.get("tasks", [])):
        if task.get("category_id") == category.get("id"):
            task_buttons.append((display_template_task_button(task), f"tpltask:{wid}:{task_idx}"))

    page_key = f"tplcat_{template.get('id') or 'none'}_{category_idx}"
    page = get_ui_page(template, page_key)
    visible, has_prev, has_next = paginate_items(task_buttons, page, PAGE_SIZE_CATEGORY)

    for title, callback_data in visible:
        kb.add(kb_btn(title, callback_data=callback_data))

    kb.row(
        kb_btn("➕ Задача", callback_data=f"tpltasknew:{wid}:{category_idx}"),
        kb_btn("⚙️ Подгруппа", callback_data=f"tplcatset:{wid}:{category_idx}"),
    )

    row2 = [kb_btn("⬅️", callback_data=f"tpl:{wid}")]
    if has_next:
        row2.append(kb_btn("⬇️", callback_data=f"pg:{wid}:tc:{category_idx}:x:next"))
    elif has_prev:
        row2.append(kb_btn("⬆️", callback_data=f"pg:{wid}:tc:{category_idx}:x:prev"))
    if has_prev and has_next:
        row2 = [kb_btn("⬅️", callback_data=f"tpl:{wid}"), kb_btn("⬆️", callback_data=f"pg:{wid}:tc:{category_idx}:x:prev"), kb_btn("⬇️", callback_data=f"pg:{wid}:tc:{category_idx}:x:next")]
    kb.row(*row2)
    return kb

def template_category_settings_kb(wid: str, category_idx: int):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("✍️ Переименовать", callback_data=f"tplcatren:{wid}:{category_idx}"))
    kb.add(kb_btn("😀 Переприсвоить смайлик", callback_data=f"tplcatemoji:{wid}:{category_idx}"))
    kb.add(kb_btn("🧬 Копия Подгруппы", callback_data=f"tplcatcopy:{wid}:{category_idx}"))
    kb.add(kb_btn("🗑 Удалить", callback_data=f"tplcatdel:{wid}:{category_idx}"))
    kb.add(kb_btn("🗑 Удалить с задачами", callback_data=f"tplcatdelall:{wid}:{category_idx}"))
    kb.add(kb_btn("⬅️", callback_data=f"tplcat:{wid}:{category_idx}"))
    return kb



def template_task_menu_kb(wid: str, task_idx: int, task: dict, ws: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("✍️ Переименовать", callback_data=f"tpltaskren:{wid}:{task_idx}"))
    if task.get("deadline_seconds"):
        kb.add(kb_btn("⏰ Поменять дедлайн", callback_data=f"tpltaskdeadline:{wid}:{task_idx}"))
        kb.add(kb_btn("🗑 Удалить дедлайн", callback_data=f"tpltaskdeadel:{wid}:{task_idx}"))
    else:
        kb.add(kb_btn("⏰ Установить дедлайн", callback_data=f"tpltaskdeadline:{wid}:{task_idx}"))
    if ws.get("template_categories"):
        if task.get("category_id"):
            kb.add(kb_btn("📥 Перевсунуть", callback_data=f"tpltaskmove:{wid}:{task_idx}"))
        else:
            kb.add(kb_btn("📥 Всунуть в подгруппу", callback_data=f"tpltaskmove:{wid}:{task_idx}"))
    kb.add(kb_btn("🗑 Удалить", callback_data=f"tpltaskdel:{wid}:{task_idx}"))
    back = f"tplcat:{wid}:{find_category_index(ws.get('template_categories', []), task.get('category_id'))}" if task.get("category_id") and find_category_index(ws.get('template_categories', []), task.get('category_id')) is not None else f"tpl:{wid}"
    kb.add(kb_btn("⬅️", callback_data=back))
    return kb



def template_task_move_kb(wid: str, task_idx: int, ws: dict, task: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    current_category_id = task.get("category_id")
    for category_idx, category in enumerate(ws.get("template_categories", [])):
        if category.get("id") == current_category_id:
            continue
        kb.add(kb_btn(display_category_name(category), callback_data=f"tpltaskmoveto:{wid}:{task_idx}:{category_idx}"))
    if current_category_id:
        kb.add(kb_btn("📤 Высунуть", callback_data=f"tpltaskmoveout:{wid}:{task_idx}"))
    kb.add(kb_btn("⬅️", callback_data=f"tpltask:{wid}:{task_idx}"))
    return kb



def mirrors_menu_kb(wid: str, company_idx: int, company: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    for idx, mirror in enumerate(company.get("mirrors", [])):
        label = mirror.get("label") or f"{mirror.get('chat_id')}/{mirror.get('thread_id') or 0}"
        kb.add(kb_btn(label, callback_data=f"mirroritem:{wid}:{company_idx}:{idx}"))
    kb.add(kb_btn("➕ Добавить связку", callback_data=f"mirroron:{wid}:{company_idx}"))
    kb.add(kb_btn("🔄 Обновить", callback_data=f"mirrorsrefresh:{wid}:{company_idx}"))
    kb.add(kb_btn("⬅️", callback_data=f"cmpset:{wid}:{company_idx}"))
    return kb


def mirror_item_kb(wid: str, company_idx: int, mirror_idx: int):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("🔌 Отвязать список", callback_data=f"mirroroff:{wid}:{company_idx}:{mirror_idx}"))
    kb.add(kb_btn("⬅️", callback_data=f"mirrors:{wid}:{company_idx}"))
    return kb


def confirm_kb(confirm_cb: str, back_cb: str):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("Да!", callback_data=confirm_cb))
    kb.add(kb_btn("⬅️", callback_data=back_cb))
    return kb


def prompt_kb(wid: str):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("⬅️", callback_data=f"cancel:{wid}"))
    return kb


# =========================
# VIEW HELPERS
# =========================

async def persist_ws_menu_id(wid: str, message_id: int):
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data.get("workspaces", {}).get(wid)
        if ws:
            ws["menu_msg_id"] = message_id
            await save_data_unlocked(data)


async def upsert_ws_menu(data: dict, wid: str, text: str, reply_markup):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return False

    async with get_menu_lock(wid):
        current_id = RUNTIME_MENU_IDS.get(wid) or ws.get("menu_msg_id")
        if current_id:
            ws["menu_msg_id"] = current_id
            ok = await try_edit_text(ws["chat_id"], current_id, text, reply_markup=reply_markup)
            if ok:
                RUNTIME_MENU_IDS[wid] = current_id
                return False

        async with FILE_LOCK:
            fresh = await load_data_unlocked()
        fresh_ws = fresh.get("workspaces", {}).get(wid)
        fresh_id = fresh_ws.get("menu_msg_id") if fresh_ws else None
        if fresh_id and fresh_id != current_id:
            ws["menu_msg_id"] = fresh_id
            ok = await try_edit_text(ws["chat_id"], fresh_id, text, reply_markup=reply_markup)
            if ok:
                RUNTIME_MENU_IDS[wid] = fresh_id
                return False

        msg = await send_message(ws["chat_id"], text, reply_markup=reply_markup, thread_id=ws["thread_id"])
        ws["menu_msg_id"] = msg.message_id
        RUNTIME_MENU_IDS[wid] = msg.message_id
        await persist_ws_menu_id(wid, msg.message_id)
        return True


async def update_pm_menu(user_id: str, data: dict):
    user = ensure_user(data, user_id)
    text = pm_main_text(user_id, data)
    kb = pm_main_kb(user_id, data)
    if user.get("pm_menu_msg_id"):
        try:
            await tg_call(lambda: bot.edit_message_text(text, int(user_id), user["pm_menu_msg_id"], reply_markup=kb, parse_mode="HTML"), retries=1)
            return
        except MessageNotModified:
            return
        except Exception:
            user["pm_menu_msg_id"] = None
    try:
        msg = await send_message(int(user_id), text, reply_markup=kb)
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
    msg = await send_message(ws["chat_id"], text, thread_id=ws["thread_id"])
    company["card_msg_id"] = msg.message_id


async def upsert_company_mirror(mirror: dict, company: dict):
    if not mirror:
        return False
    text = company_card_text(company)
    msg_id = mirror.get("message_id")
    if msg_id:
        ok = await try_edit_text(mirror["chat_id"], msg_id, text)
        if ok:
            return False
    msg = await send_message(mirror["chat_id"], text, thread_id=mirror.get("thread_id") or 0)
    mirror["message_id"] = msg.message_id
    return True


async def ensure_all_company_cards(ws: dict):
    for idx in range(len(ws.get("companies", []))):
        await upsert_company_card(ws, idx)


async def sync_company_everywhere(ws: dict, company_idx: int):
    await upsert_company_card(ws, company_idx)
    company = ws["companies"][company_idx]
    for mirror in company.get("mirrors", []):
        await upsert_company_mirror(mirror, company)
    company["mirror"] = company.get("mirrors", [None])[0] if company.get("mirrors") else None


async def delete_old_prompt_if_any(ws: dict):
    awaiting = ws.get("awaiting") or {}
    if awaiting.get("prompt_msg_id"):
        await safe_delete_message(ws["chat_id"], awaiting["prompt_msg_id"])


async def set_prompt(ws: dict, prompt_text: str, awaiting_payload: dict):
    await delete_old_prompt_if_any(ws)
    msg = await send_message(ws["chat_id"], prompt_text, reply_markup=prompt_kb(ws["id"]), thread_id=ws["thread_id"])
    awaiting_payload["prompt_msg_id"] = msg.message_id
    ws["awaiting"] = awaiting_payload


async def send_or_replace_ws_home_menu(data: dict, wid: str):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    await upsert_ws_menu(data, wid, "📂 Меню workspace", ws_home_kb(wid, ws))


async def recreate_ws_home_menu(data: dict, wid: str):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    old_id = ws.get("menu_msg_id")
    ws["menu_msg_id"] = None
    RUNTIME_MENU_IDS.pop(wid, None)
    await safe_delete_message(ws["chat_id"], old_id)
    await upsert_ws_menu(data, wid, "📂 Меню workspace", ws_home_kb(wid, ws))


async def edit_ws_home_menu(data: dict, wid: str):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    await upsert_ws_menu(data, wid, "📂 Меню workspace", ws_home_kb(wid, ws))


async def edit_ws_settings_menu(data: dict, wid: str):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    await upsert_ws_menu(data, wid, workspace_path_title(ws, "⚙️ Workspace"), ws_settings_kb(wid))


async def edit_company_create_menu(data: dict, wid: str):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    await upsert_ws_menu(data, wid, "➕ Список", company_create_mode_kb(wid, ws))


async def edit_company_menu(data: dict, wid: str, company_idx: int):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    if company_idx < 0 or company_idx >= len(ws["companies"]):
        await edit_ws_home_menu(data, wid)
        return
    company = ws["companies"][company_idx]
    await upsert_ws_menu(data, wid, company_menu_title(ws, company), company_menu_kb(wid, company_idx, company))


async def edit_company_settings_menu(data: dict, wid: str, company_idx: int):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    if company_idx < 0 or company_idx >= len(ws["companies"]):
        await edit_ws_home_menu(data, wid)
        return
    company = ws["companies"][company_idx]
    await upsert_ws_menu(data, wid, company_settings_title(ws, company), company_settings_kb(wid, company_idx))


async def edit_category_menu(data: dict, wid: str, company_idx: int, category_idx: int):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    if company_idx < 0 or company_idx >= len(ws["companies"]):
        await edit_ws_home_menu(data, wid)
        return
    company = ws["companies"][company_idx]
    if category_idx < 0 or category_idx >= len(company.get("categories", [])):
        await edit_company_menu(data, wid, company_idx)
        return
    category = company["categories"][category_idx]
    await upsert_ws_menu(data, wid, category_menu_title(ws, company, category), category_menu_kb(wid, company_idx, category_idx, category, company))


async def edit_category_settings_menu(data: dict, wid: str, company_idx: int, category_idx: int):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    if company_idx < 0 or company_idx >= len(ws["companies"]):
        await edit_ws_home_menu(data, wid)
        return
    company = ws["companies"][company_idx]
    if category_idx < 0 or category_idx >= len(company.get("categories", [])):
        await edit_company_menu(data, wid, company_idx)
        return
    category = company["categories"][category_idx]
    await upsert_ws_menu(data, wid, category_settings_title(ws, company, category), category_settings_kb(wid, company_idx, category_idx))


async def edit_task_menu(data: dict, wid: str, company_idx: int, task_idx: int):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    if company_idx < 0 or company_idx >= len(ws["companies"]):
        await edit_ws_home_menu(data, wid)
        return
    company = ws["companies"][company_idx]
    if task_idx < 0 or task_idx >= len(company.get("tasks", [])):
        await edit_company_menu(data, wid, company_idx)
        return
    task = company["tasks"][task_idx]
    category = None
    if task.get("category_id"):
        cat_idx = find_category_index(company.get("categories", []), task.get("category_id"))
        if cat_idx is not None:
            category = company["categories"][cat_idx]
    await upsert_ws_menu(data, wid, task_menu_title(ws, company, task, category), task_menu_kb(wid, company_idx, task_idx, task, company))


async def edit_task_move_menu(data: dict, wid: str, company_idx: int, task_idx: int):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    company = ws["companies"].get(company_idx) if isinstance(ws.get("companies"), dict) else None
    if company is None:
        if company_idx < 0 or company_idx >= len(ws.get("companies", [])):
            await edit_ws_home_menu(data, wid)
            return
        company = ws["companies"][company_idx]
    if task_idx < 0 or task_idx >= len(company.get("tasks", [])):
        await edit_company_menu(data, wid, company_idx)
        return
    task = company["tasks"][task_idx]
    await upsert_ws_menu(data, wid, f"📥 {task['text']}", task_move_kb(wid, company_idx, task_idx, company, task))


async def edit_templates_root_menu(data: dict, wid: str):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    await upsert_ws_menu(data, wid, templates_root_title(ws), templates_root_kb(wid, ws))


async def edit_template_menu(data: dict, wid: str):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    active = get_active_template(ws)
    await upsert_ws_menu(data, wid, template_menu_title(ws, active), template_menu_kb(wid, ws))


async def edit_template_category_menu(data: dict, wid: str, category_idx: int):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    if category_idx < 0 or category_idx >= len(ws.get("template_categories", [])):
        await edit_template_menu(data, wid)
        return
    category = ws["template_categories"][category_idx]
    active = get_active_template(ws)
    await upsert_ws_menu(data, wid, template_category_title(ws, active, category), template_category_menu_kb(wid, category_idx, category, active))


async def edit_template_category_settings_menu(data: dict, wid: str, category_idx: int):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    if category_idx < 0 or category_idx >= len(ws.get("template_categories", [])):
        await edit_template_menu(data, wid)
        return
    category = ws["template_categories"][category_idx]
    active = get_active_template(ws)
    await upsert_ws_menu(data, wid, template_category_settings_title(ws, active, category), template_category_settings_kb(wid, category_idx))


async def edit_template_settings_menu(data: dict, wid: str):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    active = get_active_template(ws)
    await upsert_ws_menu(data, wid, template_settings_title(ws, active), template_settings_kb(wid))


async def edit_template_task_menu(data: dict, wid: str, task_idx: int):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    if task_idx < 0 or task_idx >= len(ws.get("template_tasks", [])):
        await edit_template_menu(data, wid)
        return
    task = ws["template_tasks"][task_idx]
    active = get_active_template(ws)
    category = None
    if task.get("category_id"):
        cat_idx = find_category_index(ws.get("template_categories", []), task.get("category_id"))
        if cat_idx is not None:
            category = ws["template_categories"][cat_idx]
    await upsert_ws_menu(data, wid, template_task_title(ws, active, task, category), template_task_menu_kb(wid, task_idx, task, ws))


async def edit_template_task_move_menu(data: dict, wid: str, task_idx: int):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    if task_idx < 0 or task_idx >= len(ws.get("template_tasks", [])):
        await edit_template_menu(data, wid)
        return
    task = ws["template_tasks"][task_idx]
    await upsert_ws_menu(data, wid, f"📥 {task['text']}", template_task_move_kb(wid, task_idx, ws, task))



async def clear_workspace_contents(ws: dict):
    awaiting = ws.get("awaiting") or {}
    prompt_msg_id = awaiting.get("prompt_msg_id")
    if prompt_msg_id:
        await safe_delete_message(ws["chat_id"], prompt_msg_id)

    for company in ws.get("companies", []):
        await safe_delete_message(ws["chat_id"], company.get("card_msg_id"))
        for mirror in company.get("mirrors", []):
            await safe_delete_message(mirror.get("chat_id"), mirror.get("message_id"))

    ws["companies"] = []
    ws["awaiting"] = None


def clear_pending_mirror_tokens_for_company(data: dict, wid: str, company_id: str):
    for token, payload in list(data.get("mirror_tokens", {}).items()):
        if payload.get("source_wid") == wid and payload.get("company_id") == company_id:
            data["mirror_tokens"].pop(token, None)


def clear_pending_mirror_tokens_for_workspace(data: dict, wid: str):
    for token, payload in list(data.get("mirror_tokens", {}).items()):
        if payload.get("source_wid") == wid:
            data["mirror_tokens"].pop(token, None)


# =========================
# PM
# =========================

@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    if message.chat.type != "private":
        return
    async with FILE_LOCK:
        data = await load_data_unlocked()
        uid = str(message.from_user.id)
        user = ensure_user(data, uid)
        await save_data_unlocked(data)
    if user.get("pm_menu_msg_id"):
        ok = await try_edit_text(int(uid), user["pm_menu_msg_id"], pm_main_text(uid, data), reply_markup=pm_main_kb(uid, data))
        if ok:
            return
    try:
        msg = await send_message(message.chat.id, pm_main_text(uid, data), reply_markup=pm_main_kb(uid, data))
        async with FILE_LOCK:
            fresh = await load_data_unlocked()
            ensure_user(fresh, uid)["pm_menu_msg_id"] = msg.message_id
            await save_data_unlocked(fresh)
    except Exception:
        pass


@dp.callback_query_handler(lambda c: c.data == "pmrefresh:root")
async def pm_refresh(cb: types.CallbackQuery):
    await cb.answer()
    if cb.message.chat.type != "private" or should_ignore_callback(cb):
        return
    async with FILE_LOCK:
        data = await load_data_unlocked()
        uid = str(cb.from_user.id)
        ensure_user(data, uid)["pm_menu_msg_id"] = cb.message.message_id
        await save_data_unlocked(data)
    await safe_edit_text(int(uid), cb.message.message_id, pm_main_text(uid, data), reply_markup=pm_main_kb(uid, data))


@dp.callback_query_handler(lambda c: c.data == "pmhelp:root")
async def pm_help(cb: types.CallbackQuery):
    await cb.answer()
    if cb.message.chat.type != "private" or should_ignore_callback(cb):
        return
    async with FILE_LOCK:
        data = await load_data_unlocked()
        uid = str(cb.from_user.id)
        user = ensure_user(data, uid)
        old_help = user.get("help_msg_id")
        await save_data_unlocked(data)
    await safe_delete_message(int(uid), old_help)
    try:
        msg = await send_message(int(uid), "📌 Как подключить workspace:\n1) Добавь меня в нужную группу;\n2) Перейди в нужный тред;\n3) Отправь команду /connect;\n4) Дождись появления меню;\n5) Profit!")
    except Exception:
        return
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ensure_user(data, uid)["help_msg_id"] = msg.message_id
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data == "pmpersonal:root")
async def pm_personal_workspace(cb: types.CallbackQuery):
    await cb.answer()
    if cb.message.chat.type != "private" or should_ignore_callback(cb):
        return
    uid = str(cb.from_user.id)
    wid = f"pm_{uid}"
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ensure_user(data, uid)
        ws = data["workspaces"].get(wid)
        if not ws:
            data["workspaces"][wid] = {
                "id": wid,
                "name": "Личный workspace",
                "chat_title": "Личный workspace",
                "topic_title": None,
                "chat_id": int(uid),
                "thread_id": 0,
                "menu_msg_id": cb.message.message_id,
                "template_tasks": [ensure_task({"text": "Создать договор"}, is_template=True), ensure_task({"text": "Выставить счёт"}, is_template=True)],
                "template_categories": [],
                "templates": [{"id": uuid.uuid4().hex, "title": "Шаблон", "emoji": "📁", "deadline_format": "relative", "tasks": [ensure_task({"text": "Создать договор"}, is_template=True), ensure_task({"text": "Выставить счёт"}, is_template=True)], "categories": []}],
                "active_template_id": None,
                "companies": [],
                "awaiting": None,
                "is_connected": True,
            }
            normalize_template(data["workspaces"][wid])
        else:
            ws["menu_msg_id"] = cb.message.message_id
            ws["is_connected"] = True
        if wid not in data["users"][uid]["workspaces"]:
            data["users"][uid]["workspaces"].append(wid)
        await save_data_unlocked(data)
    fresh = await load_data()
    await edit_ws_home_menu(fresh, wid)


@dp.callback_query_handler(lambda c: c.data.startswith("pmws:"))
async def pm_open_workspace(cb: types.CallbackQuery):
    await cb.answer()
    if cb.message.chat.type != "private" or should_ignore_callback(cb):
        return
    data = await load_data()
    uid = str(cb.from_user.id)
    wid = cb.data.split(":", 1)[1]
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected") or wid not in data["users"].get(uid, {}).get("workspaces", []):
        await safe_edit_text(int(uid), cb.message.message_id, pm_main_text(uid, data), reply_markup=pm_main_kb(uid, data))
        return
    await safe_edit_text(int(uid), cb.message.message_id, f"📂 {esc(ws.get('name') or 'Workspace')}", reply_markup=pm_ws_manage_kb(wid))


@dp.callback_query_handler(lambda c: c.data.startswith("wsset:"))
async def open_ws_settings(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    wid = cb.data.split(":", 1)[1]
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    await edit_ws_settings_menu(data, wid)


@dp.callback_query_handler(lambda c: c.data.startswith("wsclearask:"))
async def ws_clear_ask(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    wid = cb.data.split(":", 1)[1]
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    await upsert_ws_menu(data, wid, workspace_path_title(ws, "⚙️ Workspace", "🧹 Очистить workspace?"), confirm_kb(f"wsclear:{wid}", f"wsset:{wid}"))


@dp.callback_query_handler(lambda c: c.data.startswith("wsclear:"))
async def ws_clear_confirm(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    wid = cb.data.split(":", 1)[1]
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        await clear_workspace_contents(ws)
        clear_pending_mirror_tokens_for_workspace(data, wid)
        await save_data_unlocked(data)
    fresh = await load_data()
    await edit_ws_home_menu(fresh, wid)


@dp.callback_query_handler(lambda c: c.data.startswith("pmwsclearask:"))
async def pm_clear_workspace_ask(cb: types.CallbackQuery):
    await cb.answer()
    if cb.message.chat.type != "private" or should_ignore_callback(cb):
        return
    wid = cb.data.split(":", 1)[1]
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws:
        return
    await safe_edit_text(int(cb.from_user.id), cb.message.message_id, f"📂 {esc(ws.get('name') or 'Workspace')}\n\nОчистить workspace?", reply_markup=confirm_kb(f"pmwsclear:{wid}", f"pmws:{wid}"))


@dp.callback_query_handler(lambda c: c.data.startswith("pmwsclear:"))
async def pm_clear_workspace_confirm(cb: types.CallbackQuery):
    await cb.answer()
    if cb.message.chat.type != "private" or should_ignore_callback(cb):
        return
    uid = str(cb.from_user.id)
    wid = cb.data.split(":", 1)[1]
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws:
            return
        ws_name = ws["name"]
        await clear_workspace_contents(ws)
        clear_pending_mirror_tokens_for_workspace(data, wid)
        ensure_user(data, uid)["pm_menu_msg_id"] = cb.message.message_id
        await save_data_unlocked(data)
    fresh = await load_data()
    ws = fresh["workspaces"].get(wid)
    if ws and ws.get("is_connected"):
        await edit_ws_home_menu(fresh, wid)
    await safe_edit_text(int(uid), cb.message.message_id, f"📂 {esc(ws_name)}", reply_markup=pm_ws_manage_kb(wid))


@dp.callback_query_handler(lambda c: c.data.startswith("pmwsdelask:"))
async def pm_delete_workspace_ask(cb: types.CallbackQuery):
    await cb.answer()
    if cb.message.chat.type != "private" or should_ignore_callback(cb):
        return
    wid = cb.data.split(":", 1)[1]
    data = await load_data()
    uid = str(cb.from_user.id)
    ws = data["workspaces"].get(wid)
    title = f"Удалить workspace «{esc(ws.get('name') or 'Workspace')}»?" if ws else "Удалить workspace?"
    await safe_edit_text(int(uid), cb.message.message_id, title, reply_markup=confirm_kb(f"pmwsdel:{wid}", f"pmws:{wid}"))


@dp.callback_query_handler(lambda c: c.data.startswith("pmwsdel:"))
async def pm_delete_workspace(cb: types.CallbackQuery):
    await cb.answer()
    if cb.message.chat.type != "private" or should_ignore_callback(cb):
        return
    current_uid = str(cb.from_user.id)
    wid = cb.data.split(":", 1)[1]

    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws:
            await save_data_unlocked(data)
            await safe_edit_text(int(current_uid), cb.message.message_id, pm_main_text(current_uid, data), reply_markup=pm_main_kb(current_uid, data))
            return

        ws_name = ws["name"]
        chat_id = ws["chat_id"]
        thread_id = ws["thread_id"]
        menu_msg_id = ws.get("menu_msg_id")
        prompt_msg_id = (ws.get("awaiting") or {}).get("prompt_msg_id")
        ws["menu_msg_id"] = None
        ws["awaiting"] = None
        ws["is_connected"] = False
        clear_pending_mirror_tokens_for_workspace(data, wid)

        affected_users = []
        for uid, user in data["users"].items():
            if wid in user.get("workspaces", []):
                user["workspaces"].remove(wid)
                affected_users.append(uid)
        ensure_user(data, current_uid)["pm_menu_msg_id"] = cb.message.message_id
        await save_data_unlocked(data)

    await safe_delete_message(chat_id, menu_msg_id)
    await safe_delete_message(chat_id, prompt_msg_id)
    await safe_edit_text(int(current_uid), cb.message.message_id, pm_main_text(current_uid, data), reply_markup=pm_main_kb(current_uid, data))
    for uid in affected_users:
        if uid != current_uid:
            await update_pm_menu(uid, data)
    for uid in affected_users:
        await send_temp_message(int(uid), f"Workspace «{ws_name}» отключен", delay=10)
    await send_temp_message(chat_id, f"Workspace «{ws_name}» отключен", thread_id, delay=10)


# =========================
# CONNECT / TOPIC TRACKING
# =========================

@dp.message_handler(commands=["connect"])
async def cmd_connect(message: types.Message):
    if message.chat.type == "private":
        return

    uid = str(message.from_user.id)
    thread_id = message.message_thread_id or 0
    wid = make_ws_id(message.chat.id, thread_id)
    topic_title = extract_topic_title(message)
    old_menu_id = None
    old_prompt_id = None
    help_msg_id = None

    async with FILE_LOCK:
        data = await load_data_unlocked()
        ensure_user(data, uid)
        existing_ws = data["workspaces"].get(wid)
        if existing_ws and existing_ws.get("is_connected"):
            await save_data_unlocked(data)
            asyncio.create_task(send_temp_message(message.chat.id, f"Workspace «{existing_ws.get('name') or 'Workspace'}» уже подключён", thread_id, delay=10))
            return

        if not topic_title and existing_ws:
            topic_title = existing_ws.get("topic_title")

        chat_title = message.chat.title or "Workspace"
        ws_name = workspace_full_name(chat_title, topic_title, thread_id)

        old_companies = existing_ws["companies"] if existing_ws else []
        old_template_tasks = existing_ws.get("template_tasks") if existing_ws else [
            ensure_task({"text": "Создать договор"}, is_template=True),
            ensure_task({"text": "Выставить счёт"}, is_template=True),
        ]
        old_template_categories = existing_ws.get("template_categories") if existing_ws else []

        if existing_ws:
            old_menu_id = existing_ws.get("menu_msg_id")
            old_prompt_id = (existing_ws.get("awaiting") or {}).get("prompt_msg_id")

        data["workspaces"][wid] = {
            "id": wid,
            "name": ws_name,
            "chat_title": chat_title,
            "topic_title": topic_title,
            "chat_id": message.chat.id,
            "thread_id": thread_id,
            "menu_msg_id": existing_ws.get("menu_msg_id") if existing_ws else None,
            "template_tasks": old_template_tasks,
            "template_categories": old_template_categories,
            "template": [t["text"] for t in old_template_tasks if not t.get("category_id")],
            "companies": old_companies,
            "awaiting": None,
            "is_connected": True,
        }
        ws = data["workspaces"][wid]
        if wid not in data["users"][uid]["workspaces"]:
            data["users"][uid]["workspaces"].append(wid)
        help_msg_id = data["users"][uid].get("help_msg_id")
        data["users"][uid]["help_msg_id"] = None
        await save_data_unlocked(data)

    await safe_delete_message(message.chat.id, old_menu_id)
    await safe_delete_message(message.chat.id, old_prompt_id)
    if help_msg_id:
        await safe_delete_message(int(uid), help_msg_id)

    fresh = await load_data()
    ws = fresh["workspaces"].get(wid)
    if not ws:
        return

    await ensure_all_company_cards(ws)
    for company in ws.get("companies", []):
        for mirror in company.get("mirrors", []):
            await upsert_company_mirror(mirror, company)
    await send_or_replace_ws_home_menu(fresh, wid)
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

    async with FILE_LOCK:
        data = await load_data_unlocked()
        wid = make_ws_id(message.chat.id, thread_id)
        ws = data["workspaces"].get(wid)
        if not ws:
            return
        ws["topic_title"] = topic_title
        ws["chat_title"] = message.chat.title or ws.get("chat_title") or "Workspace"
        ws["name"] = workspace_full_name(ws["chat_title"], ws["topic_title"], thread_id)
        await save_data_unlocked(data)

    for uid, user in data["users"].items():
        if wid in user.get("workspaces", []):
            await update_pm_menu(uid, data)


# =========================
# MIRROR
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("mirrors:"))
async def open_mirrors_menu(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx = cb.data.split(":")
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws:
        return
    company_idx = int(company_idx)
    if company_idx < 0 or company_idx >= len(ws.get("companies", [])):
        return
    company = ws["companies"][company_idx]
    await upsert_ws_menu(data, wid, mirrors_menu_title(ws, company), mirrors_menu_kb(wid, company_idx, company))


@dp.callback_query_handler(lambda c: c.data.startswith("mirroritem:"))
async def open_mirror_item(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, mirror_idx = cb.data.split(":")
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws:
        return
    company_idx = int(company_idx)
    mirror_idx = int(mirror_idx)
    if company_idx < 0 or company_idx >= len(ws.get("companies", [])):
        return
    company = ws["companies"][company_idx]
    if mirror_idx < 0 or mirror_idx >= len(company.get("mirrors", [])):
        return
    mirror = company["mirrors"][mirror_idx]
    label = mirror.get("label") or f"{mirror.get('chat_id')}/{mirror.get('thread_id') or 0}"
    await upsert_ws_menu(data, wid, workspace_path_title(ws, display_company_name(company), "📤 Дублирование списка", label), mirror_item_kb(wid, company_idx, mirror_idx))


@dp.callback_query_handler(lambda c: c.data.startswith("mirrorsrefresh:"))
async def refresh_mirrors_menu(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx = cb.data.split(":")
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws:
        return
    company_idx = int(company_idx)
    if company_idx < 0 or company_idx >= len(ws.get("companies", [])):
        return
    company = ws["companies"][company_idx]
    await upsert_ws_menu(data, wid, mirrors_menu_title(ws, company), mirrors_menu_kb(wid, company_idx, company))


@dp.callback_query_handler(lambda c: c.data.startswith("mirroron:"))
async def mirror_on(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx = cb.data.split(":")
    company_idx = int(company_idx)

    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected") or company_idx < 0 or company_idx >= len(ws.get("companies", [])):
            return
        company = ws["companies"][company_idx]
        company_id = company["id"]
        token = generate_mirror_token()
        data["mirror_tokens"][token] = {
            "source_wid": wid,
            "company_id": company_id,
            "created_by": cb.from_user.id,
            "source_chat_id": ws["chat_id"],
            "source_thread_id": ws["thread_id"],
            "instruction_msg_id": None,
        }
        await save_data_unlocked(data)

    msg = await send_message(ws["chat_id"], "📤 Чтобы добавить связку:\n1) Перейди в целевой чат/тред\n2) Отправь команду:\n/mirror " + token, thread_id=ws["thread_id"])
    async with FILE_LOCK:
        data = await load_data_unlocked()
        if token in data.get("mirror_tokens", {}):
            data["mirror_tokens"][token]["instruction_msg_id"] = msg.message_id
            await save_data_unlocked(data)
    fresh = await load_data()
    ws2 = fresh["workspaces"].get(wid)
    if ws2 and 0 <= company_idx < len(ws2.get("companies", [])):
        company2 = ws2["companies"][company_idx]
        await upsert_ws_menu(fresh, wid, mirrors_menu_title(ws2, company2), mirrors_menu_kb(wid, company_idx, company2))


@dp.callback_query_handler(lambda c: c.data.startswith("mirroroff:"))
async def mirror_off(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    parts = cb.data.split(":")
    if len(parts) != 4:
        return
    _, wid, company_idx, mirror_idx = parts
    company_idx = int(company_idx)
    mirror_idx = int(mirror_idx)

    target = None
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or company_idx < 0 or company_idx >= len(ws.get("companies", [])):
            return
        company = ws["companies"][company_idx]
        if mirror_idx < 0 or mirror_idx >= len(company.get("mirrors", [])):
            return
        target = company["mirrors"].pop(mirror_idx)
        company["mirror"] = company.get("mirrors", [None])[0] if company.get("mirrors") else None
        await save_data_unlocked(data)

    if target and target.get("message_id"):
        await safe_delete_message(target.get("chat_id"), target.get("message_id"))
    fresh = await load_data()
    ws2 = fresh["workspaces"].get(wid)
    if ws2 and 0 <= company_idx < len(ws2.get("companies", [])):
        company2 = ws2["companies"][company_idx]
        await upsert_ws_menu(fresh, wid, f"📤 Дублирование списка: {display_company_name(company2)}", mirrors_menu_kb(wid, company_idx, company2))


@dp.message_handler(commands=["mirror"])
async def cmd_mirror(message: types.Message):
    if message.chat.type == "private":
        return
    code = (message.get_args() or "").strip().upper()
    if not code:
        await send_temp_message(message.chat.id, "Укажи код: /mirror CODE", message.message_thread_id or 0, delay=10)
        await try_delete_user_message(message)
        return

    async with FILE_LOCK:
        data = await load_data_unlocked()
        payload = data.get("mirror_tokens", {}).get(code)
        if not payload:
            await save_data_unlocked(data)
            asyncio.create_task(send_temp_message(message.chat.id, "Код не найден или уже использован.", message.message_thread_id or 0, delay=10))
            asyncio.create_task(try_delete_user_message(message))
            return
        source_wid = payload["source_wid"]
        company_id = payload["company_id"]
        ws = data["workspaces"].get(source_wid)
        if not ws:
            data["mirror_tokens"].pop(code, None)
            await save_data_unlocked(data)
            asyncio.create_task(send_temp_message(message.chat.id, "Исходный workspace не найден.", message.message_thread_id or 0, delay=10))
            asyncio.create_task(try_delete_user_message(message))
            return
        company_idx = find_company_index_by_id(ws, company_id)
        if company_idx is None:
            data["mirror_tokens"].pop(code, None)
            await save_data_unlocked(data)
            asyncio.create_task(send_temp_message(message.chat.id, "Список не найден.", message.message_thread_id or 0, delay=10))
            asyncio.create_task(try_delete_user_message(message))
            return
        company = ws["companies"][company_idx]
        thread_id = message.message_thread_id or 0
        label = workspace_full_name(message.chat.title or "Чат", extract_topic_title(message), thread_id)
        existing = None
        for mirror in company.get("mirrors", []):
            if mirror.get("chat_id") == message.chat.id and (mirror.get("thread_id") or 0) == thread_id:
                existing = mirror
                break
        if not existing:
            existing = {"chat_id": message.chat.id, "thread_id": thread_id, "message_id": None, "label": label}
            company.setdefault("mirrors", []).append(existing)
        existing["label"] = label
        instruction_msg_id = payload.get("instruction_msg_id")
        source_chat_id = payload.get("source_chat_id")
        source_thread_id = payload.get("source_thread_id") or 0
        data["mirror_tokens"].pop(code, None)
        await save_data_unlocked(data)

    fresh = await load_data()
    ws = fresh["workspaces"][source_wid]
    company_idx = find_company_index_by_id(ws, company_id)
    company = ws["companies"][company_idx]
    await sync_company_everywhere(ws, company_idx)
    await save_data(fresh)
    await try_delete_user_message(message)
    if instruction_msg_id:
        await safe_delete_message(source_chat_id, instruction_msg_id)
    await send_temp_message(ws["chat_id"], f"📤 Список «{company['title']}» дублируется ещё в один тред/чат", source_thread_id, delay=10)


# =========================
# NAVIGATION
# =========================
# =========================
# NAVIGATION
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("backws:"))
async def back_to_ws(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    wid = cb.data.split(":", 1)[1]
    data = await load_data()
    await edit_ws_home_menu(data, wid)



async def refresh_paged_view(data: dict, user_id: str, wid: str, view: str, a: str = "x", b: str = "x"):
    if view == "pm":
        await update_pm_menu(user_id, data)
        return

    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return

    if view == "wh":
        await edit_ws_home_menu(data, wid)
    elif view == "cc":
        await edit_company_create_menu(data, wid)
    elif view == "tr":
        await edit_templates_root_menu(data, wid)
    elif view == "cm" and a != "x":
        await edit_company_menu(data, wid, int(a))
    elif view == "ct" and a != "x" and b != "x":
        await edit_category_menu(data, wid, int(a), int(b))
    elif view == "tm":
        await edit_template_menu(data, wid)
    elif view == "tc" and a != "x":
        await edit_template_category_menu(data, wid, int(a))


@dp.callback_query_handler(lambda c: c.data.startswith("pgpm:"))
async def page_pm(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    async with FILE_LOCK:
        data = await load_data_unlocked()
        uid = str(cb.from_user.id)
        user = ensure_user(data, uid)
        if cb.message:
            user["pm_menu_msg_id"] = cb.message.message_id
        delta = -1 if cb.data.endswith(":prev") else 1
        set_ui_page(user, "pm_root", get_ui_page(user, "pm_root") + delta)
        await save_data_unlocked(data)
    await update_pm_menu(uid, data)


@dp.callback_query_handler(lambda c: c.data.startswith("pg:"))
async def page_ws(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    parts = cb.data.split(":")
    if len(parts) != 6:
        return
    _, wid, view, a, b, direction = parts
    delta = -1 if direction == "prev" else 1

    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws:
            return

        if view == "wh":
            set_ui_page(ws, ws_home_page_key(), get_ui_page(ws, ws_home_page_key()) + delta)
        elif view == "cc":
            set_ui_page(ws, company_create_page_key(), get_ui_page(ws, company_create_page_key()) + delta)
        elif view == "tr":
            set_ui_page(ws, templates_root_page_key(), get_ui_page(ws, templates_root_page_key()) + delta)
        elif view == "cm" and a != "x":
            company_idx = int(a)
            if 0 <= company_idx < len(ws.get("companies", [])):
                company = ws["companies"][company_idx]
                key = company_menu_page_key(company_idx)
                set_ui_page(company, key, get_ui_page(company, key) + delta)
        elif view == "ct" and a != "x" and b != "x":
            company_idx = int(a)
            category_idx = int(b)
            if 0 <= company_idx < len(ws.get("companies", [])):
                company = ws["companies"][company_idx]
                key = category_menu_page_key(company_idx, category_idx)
                set_ui_page(company, key, get_ui_page(company, key) + delta)
        elif view == "tm":
            key = active_template_page_key(ws)
            set_ui_page(ws, key, get_ui_page(ws, key) + delta)
        elif view == "tc" and a != "x":
            template = get_active_template(ws)
            owner = template if template else ws
            key = active_template_category_page_key(ws, int(a))
            set_ui_page(owner, key, get_ui_page(owner, key) + delta)

        await save_data_unlocked(data)

    await refresh_paged_view(data, str(cb.from_user.id), wid, view, a, b)


@dp.callback_query_handler(lambda c: c.data.startswith("cmpnew:"))
async def create_company_menu(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    wid = cb.data.split(":", 1)[1]
    data = await load_data()
    await edit_company_create_menu(data, wid)


@dp.callback_query_handler(lambda c: c.data.startswith("cmpmode:"))
async def create_company_prompt(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    parts = cb.data.split(":")
    wid = parts[1]
    mode = parts[2] if len(parts) > 2 else "empty"
    template_id = parts[3] if len(parts) > 3 and mode == "tpl" else None
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        await set_prompt(ws, "✏️ Напиши название списка:", {"type": "new_company", "use_template": mode == "tpl", "template_id": template_id, "back_to": {"view": "ws"}})
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("cmp:"))
async def open_company(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx = cb.data.split(":")
    data = await load_data()
    await edit_company_menu(data, wid, int(company_idx))


@dp.callback_query_handler(lambda c: c.data.startswith("cmpset:"))
async def open_company_settings(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx = cb.data.split(":")
    data = await load_data()
    await edit_company_settings_menu(data, wid, int(company_idx))


@dp.callback_query_handler(lambda c: c.data.startswith("cat:"))
async def open_category(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, category_idx = cb.data.split(":")
    data = await load_data()
    await edit_category_menu(data, wid, int(company_idx), int(category_idx))


@dp.callback_query_handler(lambda c: c.data.startswith("catset:"))
async def open_category_settings(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, category_idx = cb.data.split(":")
    data = await load_data()
    await edit_category_settings_menu(data, wid, int(company_idx), int(category_idx))


@dp.callback_query_handler(lambda c: c.data.startswith("task:"))
async def open_task_menu(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, task_idx = cb.data.split(":")
    data = await load_data()
    await edit_task_menu(data, wid, int(company_idx), int(task_idx))


@dp.callback_query_handler(lambda c: c.data.startswith("taskmove:") )
async def open_task_move_menu(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, task_idx = cb.data.split(":")
    data = await load_data()
    await edit_task_move_menu(data, wid, int(company_idx), int(task_idx))


@dp.callback_query_handler(lambda c: c.data.startswith("tpl:"))
async def open_template_menu(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    wid = cb.data.split(":", 1)[1]
    data = await load_data()
    await edit_template_menu(data, wid)


@dp.callback_query_handler(lambda c: c.data.startswith("tplcat:"))
async def open_template_category(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, category_idx = cb.data.split(":")
    data = await load_data()
    await edit_template_category_menu(data, wid, int(category_idx))


@dp.callback_query_handler(lambda c: c.data.startswith("tplcatset:"))
async def open_template_category_settings(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, category_idx = cb.data.split(":")
    data = await load_data()
    await edit_template_category_settings_menu(data, wid, int(category_idx))


@dp.callback_query_handler(lambda c: c.data.startswith("tpltask:") and not c.data.startswith("tpltasknew:") and not c.data.startswith("tpltaskren:") and not c.data.startswith("tpltaskdeadline:") and not c.data.startswith("tpltaskdeadel:") and not c.data.startswith("tpltaskdel:"))
async def open_template_task(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, task_idx = cb.data.split(":")
    data = await load_data()
    await edit_template_task_menu(data, wid, int(task_idx))


@dp.callback_query_handler(lambda c: c.data.startswith("tpltaskmove:"))
async def open_template_task_move(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, task_idx = cb.data.split(":")
    data = await load_data()
    await edit_template_task_move_menu(data, wid, int(task_idx))


# =========================
# CANCEL
# =========================

async def show_back_view(data: dict, wid: str, back_to: dict):
    view = back_to.get("view", "ws")
    if view == "company":
        await edit_company_menu(data, wid, back_to["company_idx"])
    elif view == "company_settings":
        await edit_company_settings_menu(data, wid, back_to["company_idx"])
    elif view == "category":
        await edit_category_menu(data, wid, back_to["company_idx"], back_to["category_idx"])
    elif view == "category_settings":
        await edit_category_settings_menu(data, wid, back_to["company_idx"], back_to["category_idx"])
    elif view == "task":
        await edit_task_menu(data, wid, back_to["company_idx"], back_to["task_idx"])
    elif view == "ws_settings":
        await edit_ws_settings_menu(data, wid)
    elif view == "template":
        await edit_template_menu(data, wid)
    elif view == "template_root":
        await edit_templates_root_menu(data, wid)
    elif view == "template_settings":
        await edit_template_settings_menu(data, wid)
    elif view == "template_category":
        await edit_template_category_menu(data, wid, back_to["category_idx"])
    elif view == "template_category_settings":
        await edit_template_category_settings_menu(data, wid, back_to["category_idx"])
    elif view == "template_task":
        await edit_template_task_menu(data, wid, back_to["task_idx"])
    else:
        await edit_ws_home_menu(data, wid)


@dp.callback_query_handler(lambda c: c.data.startswith("cancel:"))
async def cancel_input(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    wid = cb.data.split(":", 1)[1]
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws:
            return
        awaiting = ws.get("awaiting") or {}
        prompt_msg_id = awaiting.get("prompt_msg_id")
        back_to = awaiting.get("back_to", {"view": "ws"})
        ws["awaiting"] = None
        await save_data_unlocked(data)
    await safe_delete_message(ws["chat_id"], prompt_msg_id)
    if ws.get("is_connected"):
        fresh = await load_data()
        await show_back_view(fresh, wid, back_to)


# =========================
# COMPANY / CATEGORY ACTIONS
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("cmpren:"))
async def rename_company_prompt(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx = cb.data.split(":")
    company_idx = int(company_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        await set_prompt(ws, "✏️ Введи новое название списка:", {"type": "rename_company", "company_idx": company_idx, "back_to": {"view": "company_settings", "company_idx": company_idx}})
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("cmpemoji:"))
async def company_emoji_prompt(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx = cb.data.split(":")
    company_idx = int(company_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        await set_prompt(ws, "😀 Пришли один смайлик для списка:", {"type": "company_emoji", "company_idx": company_idx, "back_to": {"view": "company_settings", "company_idx": company_idx}})
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("cmpdelask:"))
async def delete_company_ask(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx = cb.data.split(":")
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws:
        return
    company_idx = int(company_idx)
    if company_idx < 0 or company_idx >= len(ws.get("companies", [])):
        return
    company = ws["companies"][company_idx]
    await upsert_ws_menu(data, wid, workspace_path_title(ws, display_company_name(company), "🗑 Удаление списка?"), confirm_kb(f"cmpdel:{wid}:{company_idx}", f"cmpset:{wid}:{company_idx}"))


@dp.callback_query_handler(lambda c: c.data.startswith("cmpdel:"))
async def delete_company(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx = cb.data.split(":")
    company_idx = int(company_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or company_idx < 0 or company_idx >= len(ws.get("companies", [])):
            return
        company = ws["companies"].pop(company_idx)
        company_id = company["id"]
        card_msg_id = company.get("card_msg_id")
        mirrors = list(company.get("mirrors", []))
        clear_pending_mirror_tokens_for_company(data, wid, company_id)
        await save_data_unlocked(data)
    await safe_delete_message(ws["chat_id"], card_msg_id)
    for mirror in mirrors:
        if mirror.get("message_id"):
            await safe_delete_message(mirror.get("chat_id"), mirror.get("message_id"))
    fresh = await load_data()
    await send_or_replace_ws_home_menu(fresh, wid)


@dp.callback_query_handler(lambda c: c.data.startswith("catnew:"))
async def add_category_prompt(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx = cb.data.split(":")
    company_idx = int(company_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        await set_prompt(ws, "✏️ Введи название подгруппы:", {"type": "new_category", "company_idx": company_idx, "back_to": {"view": "company", "company_idx": company_idx}})
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("catren:"))
async def rename_category_prompt(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, category_idx = cb.data.split(":")
    company_idx, category_idx = int(company_idx), int(category_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        await set_prompt(ws, "✏️ Введи новое название подгруппы:", {"type": "rename_category", "company_idx": company_idx, "category_idx": category_idx, "back_to": {"view": "category_settings", "company_idx": company_idx, "category_idx": category_idx}})
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("catemoji:"))
async def category_emoji_prompt(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, category_idx = cb.data.split(":")
    company_idx, category_idx = int(company_idx), int(category_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        await set_prompt(ws, "😀 Пришли один смайлик для подгруппы:", {"type": "category_emoji", "company_idx": company_idx, "category_idx": category_idx, "back_to": {"view": "category_settings", "company_idx": company_idx, "category_idx": category_idx}})
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("catdelall:"))
async def delete_category_with_tasks_cb(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, category_idx = cb.data.split(":")
    company_idx, category_idx = int(company_idx), int(category_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or company_idx < 0 or company_idx >= len(ws.get("companies", [])):
            return
        company = ws["companies"][company_idx]
        if category_idx < 0 or category_idx >= len(company.get("categories", [])):
            return
        category_id = company["categories"][category_idx]["id"]
        delete_category_with_tasks(company["tasks"], company["categories"], category_id)
        await save_data_unlocked(data)
    fresh = await load_data()
    await sync_company_everywhere(fresh["workspaces"][wid], company_idx)
    await edit_company_menu(fresh, wid, company_idx)


@dp.callback_query_handler(lambda c: c.data.startswith("catdel:"))
async def delete_category_keep_cb(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, category_idx = cb.data.split(":")
    company_idx, category_idx = int(company_idx), int(category_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or company_idx < 0 or company_idx >= len(ws.get("companies", [])):
            return
        company = ws["companies"][company_idx]
        if category_idx < 0 or category_idx >= len(company.get("categories", [])):
            return
        category_id = company["categories"][category_idx]["id"]
        delete_category_keep_tasks(company["tasks"], company["categories"], category_id)
        await save_data_unlocked(data)
    fresh = await load_data()
    await sync_company_everywhere(fresh["workspaces"][wid], company_idx)
    await edit_company_menu(fresh, wid, company_idx)


# =========================
# TASK ACTIONS
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("tasknew:"))
async def add_task_prompt(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, place = cb.data.split(":")
    company_idx = int(company_idx)
    category_idx = None if place == "root" else int(place)
    back_to = {"view": "company", "company_idx": company_idx} if category_idx is None else {"view": "category", "company_idx": company_idx, "category_idx": category_idx}
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        await set_prompt(ws, "✏️ Введи текст новой задачи:", {"type": "new_task", "company_idx": company_idx, "category_idx": category_idx, "back_to": back_to})
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("taskren:"))
async def rename_task_prompt(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, task_idx = cb.data.split(":")
    company_idx, task_idx = int(company_idx), int(task_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        await set_prompt(ws, "✏️ Введи новое название задачи:", {"type": "rename_task", "company_idx": company_idx, "task_idx": task_idx, "back_to": {"view": "task", "company_idx": company_idx, "task_idx": task_idx}})
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("taskdeadline:"))
async def task_deadline_prompt(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, task_idx = cb.data.split(":")
    company_idx, task_idx = int(company_idx), int(task_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        await set_prompt(ws, "⏰Пришли мне дату или срок", {"type": "task_deadline", "company_idx": company_idx, "task_idx": task_idx, "back_to": {"view": "task", "company_idx": company_idx, "task_idx": task_idx}})
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("taskdeadel:"))
async def delete_task_deadline(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, task_idx = cb.data.split(":")
    company_idx, task_idx = int(company_idx), int(task_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or company_idx < 0 or company_idx >= len(ws.get("companies", [])):
            return
        company = ws["companies"][company_idx]
        if task_idx < 0 or task_idx >= len(company.get("tasks", [])):
            return
        company["tasks"][task_idx]["deadline_due_at"] = None
        company["tasks"][task_idx]["deadline_started_at"] = None
        await save_data_unlocked(data)
    fresh = await load_data()
    await sync_company_everywhere(fresh["workspaces"][wid], company_idx)
    await edit_task_menu(fresh, wid, company_idx, task_idx)


@dp.callback_query_handler(lambda c: c.data.startswith("taskdel:"))
async def delete_task(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, task_idx = cb.data.split(":")
    company_idx, task_idx = int(company_idx), int(task_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or company_idx < 0 or company_idx >= len(ws.get("companies", [])):
            return
        company = ws["companies"][company_idx]
        if task_idx < 0 or task_idx >= len(company.get("tasks", [])):
            return
        category_id = company["tasks"][task_idx].get("category_id")
        company["tasks"].pop(task_idx)
        await save_data_unlocked(data)
    fresh = await load_data()
    await sync_company_everywhere(fresh["workspaces"][wid], company_idx)
    if category_id:
        cat_idx = find_category_index(fresh["workspaces"][wid]["companies"][company_idx].get("categories", []), category_id)
        if cat_idx is not None:
            await edit_category_menu(fresh, wid, company_idx, cat_idx)
            return
    await edit_company_menu(fresh, wid, company_idx)


@dp.callback_query_handler(lambda c: c.data.startswith("taskdone:"))
async def toggle_task_done(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, task_idx = cb.data.split(":")
    company_idx, task_idx = int(company_idx), int(task_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or company_idx < 0 or company_idx >= len(ws.get("companies", [])):
            return
        company = ws["companies"][company_idx]
        if task_idx < 0 or task_idx >= len(company.get("tasks", [])):
            return
        task = company["tasks"][task_idx]
        task["done"] = not task.get("done")
        category_id = task.get("category_id")
        await save_data_unlocked(data)
    fresh = await load_data()
    await sync_company_everywhere(fresh["workspaces"][wid], company_idx)
    if category_id:
        cat_idx = find_category_index(fresh["workspaces"][wid]["companies"][company_idx].get("categories", []), category_id)
        if cat_idx is not None:
            await edit_category_menu(fresh, wid, company_idx, cat_idx)
            return
    await edit_company_menu(fresh, wid, company_idx)


@dp.callback_query_handler(lambda c: c.data.startswith("taskmoveto:"))
async def move_task_to_category(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, task_idx, category_idx = cb.data.split(":")
    company_idx, task_idx, category_idx = int(company_idx), int(task_idx), int(category_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or company_idx < 0 or company_idx >= len(ws.get("companies", [])):
            return
        company = ws["companies"][company_idx]
        if task_idx < 0 or task_idx >= len(company.get("tasks", [])):
            return
        if category_idx < 0 or category_idx >= len(company.get("categories", [])):
            return
        prev_category_id = company["tasks"][task_idx].get("category_id")
        company["tasks"][task_idx]["category_id"] = company["categories"][category_idx]["id"]
        await save_data_unlocked(data)
    fresh = await load_data()
    await sync_company_everywhere(fresh["workspaces"][wid], company_idx)
    if prev_category_id:
        prev_idx = find_category_index(fresh["workspaces"][wid]["companies"][company_idx].get("categories", []), prev_category_id)
        if prev_idx is not None:
            await edit_category_menu(fresh, wid, company_idx, category_idx)
            return
    await edit_company_menu(fresh, wid, company_idx)


@dp.callback_query_handler(lambda c: c.data.startswith("taskmoveout:"))
async def move_task_out_of_category(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, task_idx = cb.data.split(":")
    company_idx, task_idx = int(company_idx), int(task_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or company_idx < 0 or company_idx >= len(ws.get("companies", [])):
            return
        company = ws["companies"][company_idx]
        if task_idx < 0 or task_idx >= len(company.get("tasks", [])):
            return
        prev_category_id = company["tasks"][task_idx].get("category_id")
        company["tasks"][task_idx]["category_id"] = None
        await save_data_unlocked(data)
    fresh = await load_data()
    await sync_company_everywhere(fresh["workspaces"][wid], company_idx)
    if prev_category_id:
        prev_idx = find_category_index(fresh["workspaces"][wid]["companies"][company_idx].get("categories", []), prev_category_id)
        if prev_idx is not None:
            await edit_category_menu(fresh, wid, company_idx, prev_idx)
            return
    await edit_company_menu(fresh, wid, company_idx)


# =========================
# TEMPLATE ACTIONS
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("tplcatnew:"))
async def add_template_category_prompt(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    wid = cb.data.split(":", 1)[1]
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        await set_prompt(ws, "✏️ Введи название подгруппы шаблона:", {"type": "new_template_category", "back_to": {"view": "template"}})
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("tplcatren:"))
async def rename_template_category_prompt(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, category_idx = cb.data.split(":")
    category_idx = int(category_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        await set_prompt(ws, "✏️ Введи новое название подгруппы шаблона:", {"type": "rename_template_category", "category_idx": category_idx, "back_to": {"view": "template_category_settings", "category_idx": category_idx}})
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("tplcatemoji:"))
async def template_category_emoji_prompt(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, category_idx = cb.data.split(":")
    category_idx = int(category_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        await set_prompt(ws, "😀 Пришли один смайлик для подгруппы шаблона:", {"type": "template_category_emoji", "category_idx": category_idx, "back_to": {"view": "template_category_settings", "category_idx": category_idx}})
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("tplcatdelall:"))
async def delete_template_category_all(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, category_idx = cb.data.split(":")
    category_idx = int(category_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or category_idx < 0 or category_idx >= len(ws.get("template_categories", [])):
            return
        category_id = ws["template_categories"][category_idx]["id"]
        delete_category_with_tasks(ws["template_tasks"], ws["template_categories"], category_id)
        await save_data_unlocked(data)
    fresh = await load_data()
    await edit_template_menu(fresh, wid)


@dp.callback_query_handler(lambda c: c.data.startswith("tplcatdel:"))
async def delete_template_category_keep(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, category_idx = cb.data.split(":")
    category_idx = int(category_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or category_idx < 0 or category_idx >= len(ws.get("template_categories", [])):
            return
        category_id = ws["template_categories"][category_idx]["id"]
        delete_category_keep_tasks(ws["template_tasks"], ws["template_categories"], category_id)
        await save_data_unlocked(data)
    fresh = await load_data()
    await edit_template_menu(fresh, wid)


@dp.callback_query_handler(lambda c: c.data.startswith("tplcatcopy:"))
async def copy_template_category_prompt(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, category_idx = cb.data.split(":")
    category_idx = int(category_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        await set_prompt(ws, "✏️ Введи имя новой подгруппы-копии:", {"type": "copy_template_category", "category_idx": category_idx, "back_to": {"view": "template_category_settings", "category_idx": category_idx}})
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("tpltasknew:"))
async def add_template_task_prompt(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, place = cb.data.split(":")
    category_idx = None if place == "root" else int(place)
    back_to = {"view": "template"} if category_idx is None else {"view": "template_category", "category_idx": category_idx}
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        await set_prompt(ws, "✏️ Введи название новой задачи шаблона:", {"type": "new_template_task", "category_idx": category_idx, "back_to": back_to})
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("tpltaskren:"))
async def rename_template_task_prompt(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, task_idx = cb.data.split(":")
    task_idx = int(task_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        await set_prompt(ws, "✏️ Введи новое название задачи шаблона:", {"type": "rename_template_task", "task_idx": task_idx, "back_to": {"view": "template_task", "task_idx": task_idx}})
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("tpltaskdeadline:"))
async def template_task_deadline_prompt(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, task_idx = cb.data.split(":")
    task_idx = int(task_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        await set_prompt(ws, "⏰ Пришли срок, например: 3 дня, 7ч20м, 45 минут.", {"type": "template_task_deadline", "task_idx": task_idx, "back_to": {"view": "template_task", "task_idx": task_idx}})
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("tpltaskdeadel:"))
async def delete_template_task_deadline(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, task_idx = cb.data.split(":")
    task_idx = int(task_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or task_idx < 0 or task_idx >= len(ws.get("template_tasks", [])):
            return
        ws["template_tasks"][task_idx]["deadline_seconds"] = None
        await save_data_unlocked(data)
    fresh = await load_data()
    await edit_template_task_menu(fresh, wid, task_idx)


@dp.callback_query_handler(lambda c: c.data.startswith("tpltaskdel:"))
async def delete_template_task(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, task_idx = cb.data.split(":")
    task_idx = int(task_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or task_idx < 0 or task_idx >= len(ws.get("template_tasks", [])):
            return
        category_id = ws["template_tasks"][task_idx].get("category_id")
        ws["template_tasks"].pop(task_idx)
        await save_data_unlocked(data)
    fresh = await load_data()
    if category_id:
        cat_idx = find_category_index(fresh["workspaces"][wid].get("template_categories", []), category_id)
        if cat_idx is not None:
            await edit_template_category_menu(fresh, wid, cat_idx)
            return
    await edit_template_menu(fresh, wid)


@dp.callback_query_handler(lambda c: c.data.startswith("tpltaskmoveto:"))
async def move_template_task_to_category(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, task_idx, category_idx = cb.data.split(":")
    task_idx, category_idx = int(task_idx), int(category_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or task_idx < 0 or task_idx >= len(ws.get("template_tasks", [])):
            return
        if category_idx < 0 or category_idx >= len(ws.get("template_categories", [])):
            return
        prev_category_id = ws["template_tasks"][task_idx].get("category_id")
        ws["template_tasks"][task_idx]["category_id"] = ws["template_categories"][category_idx]["id"]
        await save_data_unlocked(data)
    fresh = await load_data()
    if prev_category_id:
        await edit_template_category_menu(fresh, wid, category_idx)
    else:
        await edit_template_menu(fresh, wid)


@dp.callback_query_handler(lambda c: c.data.startswith("tpltaskmoveout:"))
async def move_template_task_out_of_category(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, task_idx = cb.data.split(":")
    task_idx = int(task_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or task_idx < 0 or task_idx >= len(ws.get("template_tasks", [])):
            return
        prev_category_id = ws["template_tasks"][task_idx].get("category_id")
        ws["template_tasks"][task_idx]["category_id"] = None
        await save_data_unlocked(data)
    fresh = await load_data()
    if prev_category_id:
        cat_idx = find_category_index(fresh["workspaces"][wid].get("template_categories", []), prev_category_id)
        if cat_idx is not None:
            await edit_template_category_menu(fresh, wid, cat_idx)
            return
    await edit_template_menu(fresh, wid)


# =========================
# TEXT INPUT
# =========================

@dp.message_handler(content_types=types.ContentTypes.TEXT)
async def handle_group_text(message: types.Message):
    if is_known_command(message.text):
        return

    if message.chat.type == "private":
        wid = f"pm_{message.from_user.id}"
    else:
        wid = make_ws_id(message.chat.id, message.message_thread_id or 0)

    async with FILE_LOCK:
        data = await load_data_unlocked()
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
        back_to = awaiting.get("back_to", {"view": "ws"})

        def finish():
            ws["awaiting"] = None

        if mode == "new_company":
            if company_exists(ws, text):
                await save_data_unlocked(data)
                asyncio.create_task(send_temp_message(ws["chat_id"], "Такой список уже существует.", ws["thread_id"], delay=6))
                return
            company = make_company(text, awaiting.get("use_template", False), ws, awaiting.get("template_id"))
            ws["companies"].append(company)
            new_company_idx = len(ws["companies"]) - 1
            finish()
            await save_data_unlocked(data)
            created_company_idx = new_company_idx
            created_company = True
        elif mode == "rename_company":
            company_idx = awaiting["company_idx"]
            if company_idx < 0 or company_idx >= len(ws["companies"]):
                finish()
                await save_data_unlocked(data)
                return
            if company_exists(ws, text, exclude_idx=company_idx):
                await save_data_unlocked(data)
                asyncio.create_task(send_temp_message(ws["chat_id"], "Такой список уже существует.", ws["thread_id"], delay=6))
                return
            ws["companies"][company_idx]["title"] = text
            finish()
            await save_data_unlocked(data)
            created_company = False
        elif mode == "company_emoji":
            if not is_single_emoji(text):
                await save_data_unlocked(data)
                asyncio.create_task(send_temp_message(ws["chat_id"], "Пришли один смайлик, балдабёб!", ws["thread_id"], delay=6))
                return
            company_idx = awaiting["company_idx"]
            if company_idx < 0 or company_idx >= len(ws["companies"]):
                finish()
                await save_data_unlocked(data)
                return
            ws["companies"][company_idx]["emoji"] = text
            finish()
            await save_data_unlocked(data)
            created_company = False
        elif mode == "new_category":
            company_idx = awaiting["company_idx"]
            if company_idx < 0 or company_idx >= len(ws["companies"]):
                finish()
                await save_data_unlocked(data)
                return
            company = ws["companies"][company_idx]
            if category_exists(company.get("categories", []), text):
                await save_data_unlocked(data)
                asyncio.create_task(send_temp_message(ws["chat_id"], "Такая подгруппа уже существует.", ws["thread_id"], delay=6))
                return
            company.setdefault("categories", []).append({"id": uuid.uuid4().hex, "title": text, "emoji": "📁"})
            finish()
            await save_data_unlocked(data)
            created_company = False
        elif mode == "rename_category":
            company_idx = awaiting["company_idx"]
            category_idx = awaiting["category_idx"]
            if company_idx < 0 or company_idx >= len(ws["companies"]):
                finish(); await save_data_unlocked(data); return
            company = ws["companies"][company_idx]
            if category_idx < 0 or category_idx >= len(company.get("categories", [])):
                finish(); await save_data_unlocked(data); return
            category = company["categories"][category_idx]
            if category_exists(company.get("categories", []), text, exclude_id=category["id"]):
                await save_data_unlocked(data)
                asyncio.create_task(send_temp_message(ws["chat_id"], "Такая подгруппа уже существует.", ws["thread_id"], delay=6))
                return
            category["title"] = text
            finish(); await save_data_unlocked(data); created_company = False
        elif mode == "category_emoji":
            if not is_single_emoji(text):
                await save_data_unlocked(data)
                asyncio.create_task(send_temp_message(ws["chat_id"], "Пришли один смайлик, балдабёб!", ws["thread_id"], delay=6))
                return
            company_idx = awaiting["company_idx"]
            category_idx = awaiting["category_idx"]
            if company_idx < 0 or company_idx >= len(ws["companies"]):
                finish(); await save_data_unlocked(data); return
            company = ws["companies"][company_idx]
            if category_idx < 0 or category_idx >= len(company.get("categories", [])):
                finish(); await save_data_unlocked(data); return
            company["categories"][category_idx]["emoji"] = text
            finish(); await save_data_unlocked(data); created_company = False
        elif mode == "new_task":
            company_idx = awaiting["company_idx"]
            category_idx = awaiting.get("category_idx")
            if company_idx < 0 or company_idx >= len(ws["companies"]):
                finish(); await save_data_unlocked(data); return
            company = ws["companies"][company_idx]
            category_id = None
            if category_idx is not None:
                if category_idx < 0 or category_idx >= len(company.get("categories", [])):
                    finish(); await save_data_unlocked(data); return
                category_id = company["categories"][category_idx]["id"]
            company["tasks"].append({
                "id": uuid.uuid4().hex,
                "text": text,
                "done": False,
                "category_id": category_id,
                "created_at": now_ts(),
                "deadline_due_at": None,
                "deadline_started_at": None,
            })
            finish(); await save_data_unlocked(data); created_company = False
        elif mode == "rename_task":
            company_idx = awaiting["company_idx"]
            task_idx = awaiting["task_idx"]
            if company_idx < 0 or company_idx >= len(ws["companies"]):
                finish(); await save_data_unlocked(data); return
            company = ws["companies"][company_idx]
            if task_idx < 0 or task_idx >= len(company.get("tasks", [])):
                finish(); await save_data_unlocked(data); return
            company["tasks"][task_idx]["text"] = text
            finish(); await save_data_unlocked(data); created_company = False
        elif mode == "task_deadline":
            company_idx = awaiting["company_idx"]
            task_idx = awaiting["task_idx"]
            if company_idx < 0 or company_idx >= len(ws["companies"]):
                finish(); await save_data_unlocked(data); return
            company = ws["companies"][company_idx]
            if task_idx < 0 or task_idx >= len(company.get("tasks", [])):
                finish(); await save_data_unlocked(data); return
            task = company["tasks"][task_idx]
            started_at, due_at, err = parse_deadline_input(text, task.get("deadline_started_at"))
            if err:
                await save_data_unlocked(data)
                asyncio.create_task(send_temp_message(ws["chat_id"], err, ws["thread_id"], delay=6))
                return
            task["deadline_started_at"] = started_at
            task["deadline_due_at"] = due_at
            finish(); await save_data_unlocked(data); created_company = False
        elif mode == "new_template_category":
            if category_exists(ws.get("template_categories", []), text):
                await save_data_unlocked(data)
                asyncio.create_task(send_temp_message(ws["chat_id"], "Такая подгруппа уже существует.", ws["thread_id"], delay=6))
                return
            ws.setdefault("template_categories", []).append({"id": uuid.uuid4().hex, "title": text, "emoji": "📁"})
            finish(); await save_data_unlocked(data); created_company = False
        elif mode == "rename_template_category":
            category_idx = awaiting["category_idx"]
            if category_idx < 0 or category_idx >= len(ws.get("template_categories", [])):
                finish(); await save_data_unlocked(data); return
            category = ws["template_categories"][category_idx]
            if category_exists(ws.get("template_categories", []), text, exclude_id=category["id"]):
                await save_data_unlocked(data)
                asyncio.create_task(send_temp_message(ws["chat_id"], "Такая подгруппа уже существует.", ws["thread_id"], delay=6))
                return
            category["title"] = text
            finish(); await save_data_unlocked(data); created_company = False
        elif mode == "template_category_emoji":
            if not is_single_emoji(text):
                await save_data_unlocked(data)
                asyncio.create_task(send_temp_message(ws["chat_id"], "Пришли один смайлик, балдабёб!", ws["thread_id"], delay=6))
                return
            category_idx = awaiting["category_idx"]
            if category_idx < 0 or category_idx >= len(ws.get("template_categories", [])):
                finish(); await save_data_unlocked(data); return
            ws["template_categories"][category_idx]["emoji"] = text
            finish(); await save_data_unlocked(data); created_company = False
        elif mode == "new_template_task":
            category_idx = awaiting.get("category_idx")
            category_id = None
            if category_idx is not None:
                if category_idx < 0 or category_idx >= len(ws.get("template_categories", [])):
                    finish(); await save_data_unlocked(data); return
                category_id = ws["template_categories"][category_idx]["id"]
            ws.setdefault("template_tasks", []).append({
                "id": uuid.uuid4().hex,
                "text": text,
                "category_id": category_id,
                "created_at": now_ts(),
                "deadline_seconds": None,
            })
            finish(); await save_data_unlocked(data); created_company = False
        elif mode == "rename_template_task":
            task_idx = awaiting["task_idx"]
            if task_idx < 0 or task_idx >= len(ws.get("template_tasks", [])):
                finish(); await save_data_unlocked(data); return
            ws["template_tasks"][task_idx]["text"] = text
            finish(); await save_data_unlocked(data); created_company = False
        elif mode == "template_task_deadline":
            task_idx = awaiting["task_idx"]
            if task_idx < 0 or task_idx >= len(ws.get("template_tasks", [])):
                finish(); await save_data_unlocked(data); return
            seconds, err = parse_template_deadline_seconds(text)
            if err:
                await save_data_unlocked(data)
                asyncio.create_task(send_temp_message(ws["chat_id"], err, ws["thread_id"], delay=6))
                asyncio.create_task(try_delete_user_message(message))
                return
            ws["template_tasks"][task_idx]["deadline_seconds"] = seconds
            finish(); await save_data_unlocked(data); created_company = False
        elif mode == "copy_company":
            source_idx = awaiting["company_idx"]
            if source_idx < 0 or source_idx >= len(ws.get("companies", [])) or company_exists(ws, text):
                await save_data_unlocked(data)
                asyncio.create_task(send_temp_message(ws["chat_id"], "Такой список уже существует.", ws["thread_id"], delay=6))
                return
            ws["companies"].append(copy_company_payload(ws["companies"][source_idx], text))
            created_company_idx = len(ws["companies"]) - 1
            finish(); await save_data_unlocked(data); created_company = True
        elif mode == "copy_category":
            company_idx = awaiting["company_idx"]
            category_idx = awaiting["category_idx"]
            if company_idx < 0 or company_idx >= len(ws.get("companies", [])):
                finish(); await save_data_unlocked(data); return
            company = ws["companies"][company_idx]
            if category_idx < 0 or category_idx >= len(company.get("categories", [])) or category_exists(company.get("categories", []), text):
                await save_data_unlocked(data)
                asyncio.create_task(send_temp_message(ws["chat_id"], "Такая подгруппа уже существует.", ws["thread_id"], delay=6))
                return
            copy_category_into_company(company, category_idx, text)
            finish(); await save_data_unlocked(data); created_company = False
        elif mode == "new_template_set":
            if template_exists(ws.get("templates", []), text):
                await save_data_unlocked(data)
                asyncio.create_task(send_temp_message(ws["chat_id"], "Такой шаблон уже существует.", ws["thread_id"], delay=6))
                return
            tpl = {"id": uuid.uuid4().hex, "title": text, "emoji": "📁", "deadline_format": "relative", "tasks": [], "categories": []}
            ws.setdefault("templates", []).append(tpl)
            set_active_template(ws, tpl["id"])
            finish(); await save_data_unlocked(data); created_company = False
        elif mode == "rename_template_set":
            tpl = get_active_template(ws)
            if template_exists(ws.get("templates", []), text, exclude_id=tpl["id"]):
                await save_data_unlocked(data)
                asyncio.create_task(send_temp_message(ws["chat_id"], "Такой шаблон уже существует.", ws["thread_id"], delay=6))
                return
            tpl["title"] = text
            finish(); await save_data_unlocked(data); created_company = False
        elif mode == "template_set_emoji":
            if not is_single_emoji(text):
                await save_data_unlocked(data)
                asyncio.create_task(send_temp_message(ws["chat_id"], "Пришли один смайлик, балдабёб!", ws["thread_id"], delay=6))
                asyncio.create_task(try_delete_user_message(message))
                return
            tpl = get_active_template(ws)
            tpl["emoji"] = text
            set_active_template(ws, tpl["id"])
            finish(); await save_data_unlocked(data); created_company = False
        elif mode == "copy_template_set":
            tpl = get_active_template(ws)
            if template_exists(ws.get("templates", []), text):
                await save_data_unlocked(data)
                asyncio.create_task(send_temp_message(ws["chat_id"], "Такой шаблон уже существует.", ws["thread_id"], delay=6))
                return
            new_tpl = copy_template_payload(tpl, text)
            ws.setdefault("templates", []).append(new_tpl)
            set_active_template(ws, new_tpl["id"])
            finish(); await save_data_unlocked(data); created_company = False
        elif mode == "copy_template_category":
            category_idx = awaiting["category_idx"]
            if category_idx < 0 or category_idx >= len(ws.get("template_categories", [])) or category_exists(ws.get("template_categories", []), text):
                await save_data_unlocked(data)
                asyncio.create_task(send_temp_message(ws["chat_id"], "Такая подгруппа уже существует.", ws["thread_id"], delay=6))
                return
            tpl = get_active_template(ws)
            copy_template_category(tpl, category_idx, text)
            set_active_template(ws, tpl["id"])
            finish(); await save_data_unlocked(data); created_company = False
        else:
            await save_data_unlocked(data)
            return

    await safe_delete_message(message.chat.id, prompt_msg_id)
    await try_delete_user_message(message)
    fresh = await load_data()
    ws = fresh["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return

    if mode in {"new_company", "copy_company"}:
        await sync_company_everywhere(ws, created_company_idx)
        await recreate_ws_home_menu(fresh, wid)
        await save_data(fresh)
        return

    company_modes = {"rename_company", "company_emoji", "new_category", "rename_category", "category_emoji", "new_task", "rename_task", "task_deadline", "copy_category"}
    if mode in company_modes:
        company_idx = awaiting.get("company_idx")
        if company_idx is not None and 0 <= company_idx < len(ws.get("companies", [])):
            await sync_company_everywhere(ws, company_idx)
            await save_data(fresh)

    if mode in {"new_template_set", "rename_template_set", "template_set_emoji", "copy_template_set"}:
        await edit_templates_root_menu(fresh, wid)
        return
    if mode == "copy_category":
        company_idx = awaiting.get("company_idx")
        if company_idx is not None:
            await sync_company_everywhere(ws, company_idx)
            await save_data(fresh)
            company = ws["companies"][company_idx]
            await edit_category_menu(fresh, wid, company_idx, len(company.get("categories", [])) - 1)
            return
    if mode == "copy_template_category":
        await edit_template_category_menu(fresh, wid, len(ws.get("template_categories", [])) - 1)
        return
    await show_back_view(fresh, wid, back_to)


@dp.callback_query_handler(lambda c: c.data.startswith("cmpdeadlinefmt:"))
async def toggle_company_deadline_format(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx = cb.data.split(":")
    company_idx = int(company_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or company_idx < 0 or company_idx >= len(ws.get("companies", [])):
            return
        company = ws["companies"][company_idx]
        company["deadline_format"] = "date" if company.get("deadline_format") != "date" else "relative"
        await save_data_unlocked(data)
    fresh = await load_data()
    await sync_company_everywhere(fresh["workspaces"][wid], company_idx)
    await edit_company_settings_menu(fresh, wid, company_idx)


@dp.callback_query_handler(lambda c: c.data.startswith("cmpcopy:"))
async def copy_company_prompt(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx = cb.data.split(":")
    company_idx = int(company_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws:
            return
        await set_prompt(ws, "✏️ Введи имя новой списка-копии:", {"type": "copy_company", "company_idx": company_idx, "back_to": {"view": "company_settings", "company_idx": company_idx}})
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("catcopy:"))
async def copy_category_prompt(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, category_idx = cb.data.split(":")
    company_idx = int(company_idx)
    category_idx = int(category_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws:
            return
        await set_prompt(ws, "✏️ Введи имя новой подгруппы-копии:", {"type": "copy_category", "company_idx": company_idx, "category_idx": category_idx, "back_to": {"view": "category_settings", "company_idx": company_idx, "category_idx": category_idx}})
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("catdeadlinefmt:"))
async def toggle_category_deadline_format(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, category_idx = cb.data.split(":")
    company_idx = int(company_idx)
    category_idx = int(category_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws:
            return
        company = ws["companies"][company_idx]
        if category_idx < 0 or category_idx >= len(company.get("categories", [])):
            return
        category = company["categories"][category_idx]
        category["deadline_format"] = "date" if category.get("deadline_format") != "date" else "relative"
        await save_data_unlocked(data)
    fresh = await load_data()
    await sync_company_everywhere(fresh["workspaces"][wid], company_idx)
    await edit_category_settings_menu(fresh, wid, company_idx, category_idx)


@dp.callback_query_handler(lambda c: c.data.startswith("tplroot:"))
async def open_templates_root(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    wid = cb.data.split(":",1)[1]
    data = await load_data()
    await edit_templates_root_menu(data, wid)


@dp.callback_query_handler(lambda c: c.data.startswith("tplselect:"))
async def select_template(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, template_id = cb.data.split(":")
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws:
            return
        set_active_template(ws, template_id)
        await save_data_unlocked(data)
    fresh = await load_data()
    await edit_template_menu(fresh, wid)


@dp.callback_query_handler(lambda c: c.data.startswith("tplsettings:"))
async def open_template_settings(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    wid = cb.data.split(":",1)[1]
    data = await load_data()
    await edit_template_settings_menu(data, wid)


@dp.callback_query_handler(lambda c: c.data.startswith("tplnewset:"))
async def add_template_set_prompt(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    wid = cb.data.split(":",1)[1]
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws:
            return
        await set_prompt(ws, "✏️ Введи название нового шаблона:", {"type": "new_template_set", "back_to": {"view": "template_root"}})
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("tplrenameset:"))
async def rename_template_set_prompt(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    wid = cb.data.split(":",1)[1]
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws:
            return
        await set_prompt(ws, "✏️ Введи новое название шаблона:", {"type": "rename_template_set", "back_to": {"view": "template_settings"}})
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("tplemojiset:"))
async def template_set_emoji_prompt(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    wid = cb.data.split(":",1)[1]
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws:
            return
        await set_prompt(ws, "😀 Пришли один смайлик для шаблона:", {"type": "template_set_emoji", "back_to": {"view": "template_settings"}})
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("tpldeadlinefmt:"))
async def toggle_template_deadline_format(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    wid = cb.data.split(":",1)[1]
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws:
        return
    await edit_template_settings_menu(data, wid)


@dp.callback_query_handler(lambda c: c.data.startswith("tplcopy:"))
async def copy_template_set_prompt(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    wid = cb.data.split(":",1)[1]
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws:
            return
        await set_prompt(ws, "✏️ Введи название копии шаблона:", {"type": "copy_template_set", "back_to": {"view": "template_settings"}})
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("tpldelsetask:"))
async def delete_template_set_ask(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    wid = cb.data.split(":",1)[1]
    data = await load_data()
    ws = data["workspaces"].get(wid)
    if not ws:
        return
    active = get_active_template(ws)
    await upsert_ws_menu(data, wid, workspace_path_title(ws, "⚙️ Шаблоны задач", display_template_name(active), "🗑 Удаление шаблона?"), confirm_kb(f"tpldelset:{wid}", f"tplsettings:{wid}"))


@dp.callback_query_handler(lambda c: c.data.startswith("tpldelset:"))
async def delete_template_set(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    wid = cb.data.split(":",1)[1]
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws:
            return
        active_id = ws.get("active_template_id")
        ws["templates"] = [tpl for tpl in ws.get("templates", []) if tpl.get("id") != active_id]
        if ws["templates"]:
            set_active_template(ws, ws["templates"][0]["id"])
        else:
            ws["templates"] = [{"id": uuid.uuid4().hex, "title": "Шаблон", "emoji": "📁", "deadline_format": "relative", "tasks": [], "categories": []}]
            set_active_template(ws, ws["templates"][0]["id"])
        await save_data_unlocked(data)
    fresh = await load_data()
    await edit_templates_root_menu(fresh, wid)


async def deadline_refresh_worker():
    while True:
        now = now_dt()
        wait = ((10 - (now.minute % 10)) % 10) * 60 - now.second
        if wait <= 0:
            wait = 600
        await asyncio.sleep(wait)
        try:
            data = await load_data()
            changed = False
            for wid, ws in data.get("workspaces", {}).items():
                if not ws.get("is_connected"):
                    continue
                for idx in range(len(ws.get("companies", []))):
                    await sync_company_everywhere(ws, idx)
                    changed = True
            if changed:
                await save_data(data)
        except Exception:
            pass


# =========================
# RUN
# =========================

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(deadline_refresh_worker())
    executor.start_polling(dp, skip_updates=True)
