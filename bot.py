import os
import json
import math
import asyncio
import re
import time
import uuid
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
        await tg_call(lambda: bot.edit_message_text(text, chat_id, message_id, reply_markup=reply_markup), retries=1)
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
        task.setdefault("deadline_days", None)
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
    cat["title"] = cat.get("title") or title or "Категория"
    cat.pop("name", None)
    return cat



def ensure_company(company):
    if not isinstance(company, dict):
        company = {}

    legacy_name = company.get("name") or company.get("title")
    emoji, title = split_legacy_name(legacy_name, company.get("emoji") or "📁")

    company.setdefault("id", uuid.uuid4().hex)
    company["emoji"] = company.get("emoji") or emoji or "📁"
    company["title"] = company.get("title") or title or "Компания"
    company.setdefault("card_msg_id", None)
    company.setdefault("mirror", None)
    company.setdefault("mirror_history", [])
    company.setdefault("tasks", [])
    company.setdefault("categories", [])
    company.setdefault("deadline_display_mode", "days")

    if not isinstance(company["tasks"], list):
        company["tasks"] = []
    if not isinstance(company["categories"], list):
        company["categories"] = []
    if not isinstance(company["mirror_history"], list):
        company["mirror_history"] = []

    company["tasks"] = [ensure_task(t, is_template=False) for t in company["tasks"]]
    company["categories"] = [ensure_category(c) for c in company["categories"]]

    for history in company["mirror_history"]:
        if not isinstance(history, dict):
            continue
        history.setdefault("chat_id", None)
        history.setdefault("thread_id", 0)
        history.setdefault("message_id", None)

    company.pop("name", None)
    return company



def normalize_template(ws: dict):
    legacy_template = ws.get("template")
    if "template_tasks" not in ws:
        if isinstance(legacy_template, list):
            ws["template_tasks"] = [ensure_task({"text": item}, is_template=True) for item in legacy_template]
        else:
            ws["template_tasks"] = []
    if "template_categories" not in ws:
        ws["template_categories"] = []

    if not isinstance(ws["template_tasks"], list):
        ws["template_tasks"] = []
    if not isinstance(ws["template_categories"], list):
        ws["template_categories"] = []

    ws["template_tasks"] = [ensure_task(t, is_template=True) for t in ws["template_tasks"]]
    ws["template_categories"] = [ensure_category(c) for c in ws["template_categories"]]
    if isinstance(legacy_template, list):
        ws["template"] = [t["text"] for t in ws["template_tasks"] if not t.get("category_id")]
    else:
        ws.setdefault("template", [])



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
        ws.setdefault("companies", [])
        ws.setdefault("awaiting", None)
        ws.setdefault("is_connected", True)

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
    return f"{company.get('emoji') or '📁'}{company.get('title') or 'Компания'}"



def display_category_name(category: dict) -> str:
    return f"{category.get('emoji') or '📁'}{category.get('title') or 'Категория'}"



def format_deadline_absolute(ts: int) -> str:
    dt = datetime.fromtimestamp(ts, TIMEZONE)
    return dt.strftime("%d.%m.%Y г. %H:%M")


def format_deadline_remaining(ts: int) -> str:
    remaining = max(ts - now_ts(), 0)
    days, rem = divmod(remaining, 86400)
    hours, rem = divmod(rem, 3600)
    minutes = rem // 60
    parts = []
    if days:
        parts.append(f"{days} д.")
    if hours or days:
        parts.append(f"{hours} ч.")
    parts.append(f"{minutes} м.")
    return "; ".join(parts) if parts else "0 м."


def display_task_deadline_suffix(company: dict, task: dict) -> str:
    due_at = task.get("deadline_due_at")
    if not due_at or task.get("done"):
        return ""
    mode = company.get("deadline_display_mode") or "days"
    if mode == "date":
        return f" (до {format_deadline_absolute(due_at)})"
    return f" ({format_deadline_remaining(due_at)})"


def task_deadline_icon(task: dict) -> str:
    if task.get("done"):
        return "✔"
    due_at = task.get("deadline_due_at")
    started_at = task.get("deadline_started_at")
    if not due_at or not started_at:
        return "⬜"
    total = max(due_at - started_at, 1)
    elapsed = now_ts() - started_at
    if now_ts() >= due_at:
        return "🟥"
    part = elapsed / total
    if part < 0.25:
        return "🟩"
    if part < 0.5:
        return "🟨"
    if part < 0.75:
        return "🟧"
    return "🟫"



def sort_company_tasks(tasks: list[dict]) -> list[dict]:
    def key(task: dict):
        done = 1 if task.get("done") else 0
        due_at = task.get("deadline_due_at")
        no_due = 1 if not due_at else 0
        return (done, no_due, due_at or 10**18, task.get("created_at") or 0)

    return sorted(tasks, key=key)



def sort_template_tasks(tasks: list[dict]) -> list[dict]:
    def key(task: dict):
        days = task.get("deadline_days")
        no_due = 1 if days is None else 0
        return (no_due, days if days is not None else 10**9, task.get("created_at") or 0)

    return sorted(tasks, key=key)



def company_card_text(company: dict) -> str:
    lines = [f"{display_company_name(company)}:"]

    uncategorized = [t for t in company["tasks"] if not t.get("category_id")]
    if uncategorized:
        for task in sort_company_tasks(uncategorized):
            icon = task_deadline_icon(task)
            suffix = display_task_deadline_suffix(company, task)
            lines.append(f"{icon} {task['text']}{suffix}")

    for category in company.get("categories", []):
        lines.append(f"    {display_category_name(category)}:")
        cat_tasks = [t for t in company["tasks"] if t.get("category_id") == category["id"]]
        if cat_tasks:
            for task in sort_company_tasks(cat_tasks):
                icon = task_deadline_icon(task)
                suffix = display_task_deadline_suffix(company, task)
                lines.append(f"        {icon} {task['text']}{suffix}")

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



def generate_mirror_token() -> str:
    return uuid.uuid4().hex[:8].upper()



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



def make_company(title: str, with_template: bool, ws: dict) -> dict:
    company = {
        "id": uuid.uuid4().hex,
        "title": title,
        "emoji": "📁",
        "card_msg_id": None,
        "mirror": None,
        "mirror_history": [],
        "categories": [],
        "tasks": [],
    }
    if not with_template:
        return company

    category_map = {}
    for template_category in ws.get("template_categories", []):
        new_cat = {
            "id": uuid.uuid4().hex,
            "title": template_category.get("title") or "Категория",
            "emoji": template_category.get("emoji") or "📁",
        }
        category_map[template_category["id"]] = new_cat["id"]
        company["categories"].append(new_cat)

    now_value = now_ts()
    for template_task in ws.get("template_tasks", []):
        deadline_days = template_task.get("deadline_days")
        due_at = now_value + deadline_days * 86400 if isinstance(deadline_days, int) and deadline_days > 0 else None
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



def task_menu_title(company: dict, task: dict, category: dict | None = None) -> str:
    if category:
        return f"{display_category_name(category)}/📌 {task['text']}"
    return f"{display_company_name(company)}/📌 {task['text']}"



def template_task_label(task: dict) -> str:
    suffix = f" ({task['deadline_days']} д.)" if isinstance(task.get("deadline_days"), int) and task.get("deadline_days") > 0 else ""
    return f"📌 {task['text']}{suffix}"



def parse_deadline_input(text: str, keep_started_at: int | None = None) -> tuple[int | None, int | None, str | None]:
    raw = clean_text(text).lower()
    if not raw:
        return None, None, "Пришлите дату или срок, например: 06.04.2026 16:00, 3 дня, 7ч20м."

    date_match = re.match(r"^\s*(\d{1,2})\D+(\d{1,2})\D+(\d{2,4})(?:\D+(\d{1,2})(?:\D+(\d{1,2}))?)?\s*$", raw)
    if date_match:
        day, month, year, hour, minute = date_match.groups()
        year = int(year)
        if year < 100:
            year += 2000
        hour = int(hour) if hour is not None else 23
        minute = int(minute) if minute is not None else 59
        try:
            dt = datetime(year, int(month), int(day), hour, minute, tzinfo=TIMEZONE)
        except ValueError:
            return None, None, "Не удалось распознать дату."
        due_at = int(dt.timestamp())
        started_at = keep_started_at or now_ts()
        if due_at <= started_at:
            return None, None, "Дата уже прошла."
        return started_at, due_at, None

    compact = raw
    compact = compact.replace(",", " ").replace(";", " ")
    compact = re.sub(r"\s+и\s+", " ", compact)
    compact = re.sub(r"(?<=\d)(?=[^\d\s])", " ", compact)
    compact = re.sub(r"(?<=[^\d\s])(?=\d)", " ", compact)
    compact = re.sub(r"\s+", " ", compact).strip()

    if compact.isdigit():
        days = int(compact)
        if days <= 0:
            return None, None, "Количество дней должно быть больше нуля."
        started_at = keep_started_at or now_ts()
        return started_at, started_at + days * 86400, None

    unit_pattern = re.compile(r"(\d+)\s*(д(?:н(?:ей|я)?)?\.?|д\.?|day|days|ч(?:ас(?:а|ов)?)?\.?|ч\.?|h|час\.?|часа|часов|м(?:ин(?:ут(?:а|ы)?)?)?\.?|м\.?|мин\.?|minute|minutes)")
    total_seconds = 0
    matched = False
    for value, unit in unit_pattern.findall(compact):
        matched = True
        v = int(value)
        if unit.startswith(("д", "day")):
            total_seconds += v * 86400
        elif unit.startswith(("ч", "h", "час")):
            total_seconds += v * 3600
        else:
            total_seconds += v * 60

    if matched and total_seconds > 0:
        started_at = keep_started_at or now_ts()
        return started_at, started_at + total_seconds, None

    return None, None, "Пришлите дату или срок, например: 06.04.2026 16:00, 3 дня, 7ч20м."


def parse_template_deadline_days(text: str) -> tuple[int | None, str | None]:
    raw = clean_text(text).lower()
    if not raw:
        return None, "Пришлите число дней, например 3 или 3 д."
    m = re.match(r"^\s*(\d+)\s*(?:д(?:н(?:ей|я)?)?\.?)?\s*$", raw)
    if not m:
        return None, "Пришлите число дней, например 3 или 3 д."
    days = int(m.group(1))
    if days <= 0:
        return None, "Количество дней должно быть больше нуля."
    return days, None


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
    for idx, company in enumerate(ws.get("companies", [])):
        kb.add(InlineKeyboardButton(display_company_name(company), callback_data=f"cmp:{wid}:{idx}"))
    kb.add(InlineKeyboardButton("➕ Создать компанию", callback_data=f"cmpnew:{wid}"))
    kb.add(InlineKeyboardButton("⚙️ Шаблон задач", callback_data=f"tpl:{wid}"))
    return kb



def company_create_mode_kb(wid: str):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("По шаблону", callback_data=f"cmpmode:{wid}:template"))
    kb.add(InlineKeyboardButton("Пустую", callback_data=f"cmpmode:{wid}:empty"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"backws:{wid}"))
    return kb



def company_menu_kb(wid: str, company_idx: int, company: dict):
    kb = InlineKeyboardMarkup(row_width=1)

    uncategorized = [t for t in company.get("tasks", []) if not t.get("category_id")]
    for task in sort_company_tasks(uncategorized):
        real_idx = company["tasks"].index(task)
        kb.add(InlineKeyboardButton(f"{task_deadline_icon(task)} {task['text']}", callback_data=f"task:{wid}:{company_idx}:{real_idx}"))

    for category_idx, category in enumerate(company.get("categories", [])):
        kb.add(InlineKeyboardButton(display_category_name(category), callback_data=f"cat:{wid}:{company_idx}:{category_idx}"))

    kb.add(InlineKeyboardButton("➕ Добавить задачу", callback_data=f"tasknew:{wid}:{company_idx}:root"))
    kb.add(InlineKeyboardButton("➕ Добавить категорию", callback_data=f"catnew:{wid}:{company_idx}"))
    kb.add(InlineKeyboardButton("⚙️ Настройки компании", callback_data=f"cmpset:{wid}:{company_idx}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"backws:{wid}"))
    return kb



def company_settings_kb(wid: str, company_idx: int, company: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("✍️ Переименовать компанию", callback_data=f"cmpren:{wid}:{company_idx}"))
    kb.add(InlineKeyboardButton("😀 Переприсвоить смайлик", callback_data=f"cmpemoji:{wid}:{company_idx}"))
    mode_label = "📅 Формат дедлайнов: дата" if (company.get("deadline_display_mode") == "date") else "⏳ Формат дедлайнов: дни"
    kb.add(InlineKeyboardButton(mode_label, callback_data=f"deadlinefmt:{wid}:{company_idx}"))
    if company.get("mirror"):
        kb.add(InlineKeyboardButton("🔌 Отвязать список", callback_data=f"mirroroff:{wid}:{company_idx}"))
    else:
        kb.add(InlineKeyboardButton("📤 Дублировать список", callback_data=f"mirroron:{wid}:{company_idx}"))
    kb.add(InlineKeyboardButton("🗑 Удалить компанию", callback_data=f"cmpdel:{wid}:{company_idx}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"cmp:{wid}:{company_idx}"))
    return kb



def category_menu_kb(wid: str, company_idx: int, category_idx: int, company: dict, category: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    cat_tasks = [t for t in company.get("tasks", []) if t.get("category_id") == category.get("id")]
    for task in sort_company_tasks(cat_tasks):
        real_idx = company["tasks"].index(task)
        kb.add(InlineKeyboardButton(f"{task_deadline_icon(task)} {task['text']}", callback_data=f"task:{wid}:{company_idx}:{real_idx}"))
    kb.add(InlineKeyboardButton("➕ Добавить задачу", callback_data=f"tasknew:{wid}:{company_idx}:{category_idx}"))
    kb.add(InlineKeyboardButton("⚙️ Настройки категории", callback_data=f"catset:{wid}:{company_idx}:{category_idx}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"cmp:{wid}:{company_idx}"))
    return kb



def category_settings_kb(wid: str, company_idx: int, category_idx: int):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("✍️ Переименовать", callback_data=f"catren:{wid}:{company_idx}:{category_idx}"))
    kb.add(InlineKeyboardButton("😀 Переприсвоить смайлик", callback_data=f"catemoji:{wid}:{company_idx}:{category_idx}"))
    kb.add(InlineKeyboardButton("🗑 Удалить", callback_data=f"catdel:{wid}:{company_idx}:{category_idx}"))
    kb.add(InlineKeyboardButton("🗑 Удалить с задачами", callback_data=f"catdelall:{wid}:{company_idx}:{category_idx}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"cat:{wid}:{company_idx}:{category_idx}"))
    return kb



def task_menu_kb(wid: str, company_idx: int, task_idx: int, task: dict, company: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    if task.get("done"):
        kb.add(InlineKeyboardButton("❌ Отменить выполнение", callback_data=f"taskdone:{wid}:{company_idx}:{task_idx}"))
    else:
        kb.add(InlineKeyboardButton("✔ Отметить выполненной", callback_data=f"taskdone:{wid}:{company_idx}:{task_idx}"))
    kb.add(InlineKeyboardButton("✍️ Переименовать", callback_data=f"taskren:{wid}:{company_idx}:{task_idx}"))

    if not task.get("done"):
        if task.get("deadline_due_at"):
            kb.add(InlineKeyboardButton("⏰ Поменять дедлайн", callback_data=f"taskdeadline:{wid}:{company_idx}:{task_idx}"))
            kb.add(InlineKeyboardButton("🗑 Удалить дедлайн", callback_data=f"taskdeadel:{wid}:{company_idx}:{task_idx}"))
        else:
            kb.add(InlineKeyboardButton("⏰ Установить дедлайн", callback_data=f"taskdeadline:{wid}:{company_idx}:{task_idx}"))

    if company.get("categories"):
        if task.get("category_id"):
            kb.add(InlineKeyboardButton("📥 Перевсунуть", callback_data=f"taskmove:{wid}:{company_idx}:{task_idx}"))
        else:
            kb.add(InlineKeyboardButton("📥 Всунуть в категорию", callback_data=f"taskmove:{wid}:{company_idx}:{task_idx}"))

    kb.add(InlineKeyboardButton("🗑 Удалить задачу", callback_data=f"taskdel:{wid}:{company_idx}:{task_idx}"))
    back = f"cat:{wid}:{company_idx}:{find_category_index(company.get('categories', []), task.get('category_id'))}" if task.get("category_id") and find_category_index(company.get('categories', []), task.get('category_id')) is not None else f"cmp:{wid}:{company_idx}"
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=back))
    return kb



def task_move_kb(wid: str, company_idx: int, task_idx: int, company: dict, task: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    current_category_id = task.get("category_id")
    for category_idx, category in enumerate(company.get("categories", [])):
        if category.get("id") == current_category_id:
            continue
        kb.add(InlineKeyboardButton(display_category_name(category), callback_data=f"taskmoveto:{wid}:{company_idx}:{task_idx}:{category_idx}"))
    if current_category_id:
        kb.add(InlineKeyboardButton("📤 Высунуть", callback_data=f"taskmoveout:{wid}:{company_idx}:{task_idx}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"task:{wid}:{company_idx}:{task_idx}"))
    return kb



def template_menu_kb(wid: str, ws: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    uncat = [t for t in ws.get("template_tasks", []) if not t.get("category_id")]
    for task in sort_template_tasks(uncat):
        idx = ws["template_tasks"].index(task)
        kb.add(InlineKeyboardButton(template_task_label(task), callback_data=f"tpltask:{wid}:{idx}"))
    for category_idx, category in enumerate(ws.get("template_categories", [])):
        kb.add(InlineKeyboardButton(display_category_name(category), callback_data=f"tplcat:{wid}:{category_idx}"))
    kb.add(InlineKeyboardButton("➕ Добавить задачу", callback_data=f"tpltasknew:{wid}:root"))
    kb.add(InlineKeyboardButton("➕ Добавить категорию", callback_data=f"tplcatnew:{wid}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"backws:{wid}"))
    return kb



def template_category_menu_kb(wid: str, category_idx: int, ws: dict, category: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    tasks = [t for t in ws.get("template_tasks", []) if t.get("category_id") == category.get("id")]
    for task in sort_template_tasks(tasks):
        idx = ws["template_tasks"].index(task)
        kb.add(InlineKeyboardButton(template_task_label(task), callback_data=f"tpltask:{wid}:{idx}"))
    kb.add(InlineKeyboardButton("➕ Добавить задачу", callback_data=f"tpltasknew:{wid}:{category_idx}"))
    kb.add(InlineKeyboardButton("⚙️ Настройки категории", callback_data=f"tplcatset:{wid}:{category_idx}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"tpl:{wid}"))
    return kb



def template_category_settings_kb(wid: str, category_idx: int):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("✍️ Переименовать", callback_data=f"tplcatren:{wid}:{category_idx}"))
    kb.add(InlineKeyboardButton("😀 Переприсвоить смайлик", callback_data=f"tplcatemoji:{wid}:{category_idx}"))
    kb.add(InlineKeyboardButton("🗑 Удалить", callback_data=f"tplcatdel:{wid}:{category_idx}"))
    kb.add(InlineKeyboardButton("🗑 Удалить с задачами", callback_data=f"tplcatdelall:{wid}:{category_idx}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"tplcat:{wid}:{category_idx}"))
    return kb



def template_task_menu_kb(wid: str, task_idx: int, task: dict, ws: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("✍️ Переименовать", callback_data=f"tpltaskren:{wid}:{task_idx}"))
    if task.get("deadline_days"):
        kb.add(InlineKeyboardButton("⏰ Поменять дедлайн", callback_data=f"tpltaskdeadline:{wid}:{task_idx}"))
        kb.add(InlineKeyboardButton("🗑 Удалить дедлайн", callback_data=f"tpltaskdeadel:{wid}:{task_idx}"))
    else:
        kb.add(InlineKeyboardButton("⏰ Установить дедлайн", callback_data=f"tpltaskdeadline:{wid}:{task_idx}"))
    if ws.get("template_categories"):
        if task.get("category_id"):
            kb.add(InlineKeyboardButton("📥 Перевсунуть", callback_data=f"tpltaskmove:{wid}:{task_idx}"))
        else:
            kb.add(InlineKeyboardButton("📥 Всунуть в категорию", callback_data=f"tpltaskmove:{wid}:{task_idx}"))
    kb.add(InlineKeyboardButton("🗑 Удалить", callback_data=f"tpltaskdel:{wid}:{task_idx}"))
    back = f"tplcat:{wid}:{find_category_index(ws.get('template_categories', []), task.get('category_id'))}" if task.get("category_id") and find_category_index(ws.get('template_categories', []), task.get('category_id')) is not None else f"tpl:{wid}"
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=back))
    return kb



def template_task_move_kb(wid: str, task_idx: int, ws: dict, task: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    current_category_id = task.get("category_id")
    for category_idx, category in enumerate(ws.get("template_categories", [])):
        if category.get("id") == current_category_id:
            continue
        kb.add(InlineKeyboardButton(display_category_name(category), callback_data=f"tpltaskmoveto:{wid}:{task_idx}:{category_idx}"))
    if current_category_id:
        kb.add(InlineKeyboardButton("📤 Высунуть", callback_data=f"tpltaskmoveout:{wid}:{task_idx}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"tpltask:{wid}:{task_idx}"))
    return kb



def prompt_kb(wid: str):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"cancel:{wid}"))
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
            await tg_call(lambda: bot.edit_message_text(text, int(user_id), user["pm_menu_msg_id"], reply_markup=kb), retries=1)
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


async def upsert_company_mirror(company: dict):
    mirror = company.get("mirror")
    if not mirror:
        return
    text = company_card_text(company)
    msg_id = mirror.get("message_id")
    if msg_id:
        ok = await try_edit_text(mirror["chat_id"], msg_id, text)
        if ok:
            return
    msg = await send_message(mirror["chat_id"], text, thread_id=mirror.get("thread_id") or 0)
    mirror["message_id"] = msg.message_id
    found = False
    for item in company.get("mirror_history", []):
        if item.get("chat_id") == mirror.get("chat_id") and (item.get("thread_id") or 0) == (mirror.get("thread_id") or 0):
            item["message_id"] = msg.message_id
            found = True
            break
    if not found:
        company.setdefault("mirror_history", []).append({
            "chat_id": mirror.get("chat_id"),
            "thread_id": mirror.get("thread_id") or 0,
            "message_id": msg.message_id,
        })


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


async def edit_company_create_menu(data: dict, wid: str):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    await upsert_ws_menu(data, wid, "➕ Создать компанию", company_create_mode_kb(wid))


async def edit_company_menu(data: dict, wid: str, company_idx: int):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    if company_idx < 0 or company_idx >= len(ws["companies"]):
        await edit_ws_home_menu(data, wid)
        return
    company = ws["companies"][company_idx]
    await upsert_ws_menu(data, wid, display_company_name(company), company_menu_kb(wid, company_idx, company))


async def edit_company_settings_menu(data: dict, wid: str, company_idx: int):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    if company_idx < 0 or company_idx >= len(ws["companies"]):
        await edit_ws_home_menu(data, wid)
        return
    company = ws["companies"][company_idx]
    await upsert_ws_menu(data, wid, f"⚙️ {display_company_name(company)}", company_settings_kb(wid, company_idx, company))


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
    await upsert_ws_menu(data, wid, display_category_name(category), category_menu_kb(wid, company_idx, category_idx, company, category))


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
    await upsert_ws_menu(data, wid, f"⚙️ {display_category_name(category)}", category_settings_kb(wid, company_idx, category_idx))


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
    await upsert_ws_menu(data, wid, task_menu_title(company, task, category), task_menu_kb(wid, company_idx, task_idx, task, company))


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


async def edit_template_menu(data: dict, wid: str):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    await upsert_ws_menu(data, wid, "⚙️ Шаблон задач", template_menu_kb(wid, ws))


async def edit_template_category_menu(data: dict, wid: str, category_idx: int):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    if category_idx < 0 or category_idx >= len(ws.get("template_categories", [])):
        await edit_template_menu(data, wid)
        return
    category = ws["template_categories"][category_idx]
    await upsert_ws_menu(data, wid, display_category_name(category), template_category_menu_kb(wid, category_idx, ws, category))


async def edit_template_category_settings_menu(data: dict, wid: str, category_idx: int):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    if category_idx < 0 or category_idx >= len(ws.get("template_categories", [])):
        await edit_template_menu(data, wid)
        return
    category = ws["template_categories"][category_idx]
    await upsert_ws_menu(data, wid, f"⚙️ {display_category_name(category)}", template_category_settings_kb(wid, category_idx))


async def edit_template_task_menu(data: dict, wid: str, task_idx: int):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    if task_idx < 0 or task_idx >= len(ws.get("template_tasks", [])):
        await edit_template_menu(data, wid)
        return
    task = ws["template_tasks"][task_idx]
    await upsert_ws_menu(data, wid, template_task_label(task), template_task_menu_kb(wid, task_idx, task, ws))


async def edit_template_task_move_menu(data: dict, wid: str, task_idx: int):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    if task_idx < 0 or task_idx >= len(ws.get("template_tasks", [])):
        await edit_template_menu(data, wid)
        return
    task = ws["template_tasks"][task_idx]
    await upsert_ws_menu(data, wid, f"📥 {task['text']}", template_task_move_kb(wid, task_idx, ws, task))



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
        msg = await send_message(int(uid), "📌 Как подключить workspace:\n\n1) Перейдите в нужный тред группы\n2) Отправьте команду /connect")
    except Exception:
        return
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ensure_user(data, uid)["help_msg_id"] = msg.message_id
        await save_data_unlocked(data)


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
    await safe_edit_text(int(uid), cb.message.message_id, f"📂 {ws['name']}", reply_markup=pm_ws_manage_kb(wid))


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
        if company.get("mirror"):
            await upsert_company_mirror(company)
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
        if company.get("mirror"):
            await save_data_unlocked(data)
            asyncio.create_task(send_temp_message(ws["chat_id"], "Этот список уже дублируется.", ws["thread_id"], delay=8))
            return
        company_id = company["id"]
        clear_pending_mirror_tokens_for_company(data, wid, company_id)
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

    msg = await send_message(
        ws["chat_id"],
        "📤 Чтобы привязать дубликат:\n1) Перейдите в целевой чат/тред\n2) Отправьте команду:\n/mirror " + token,
        thread_id=ws["thread_id"],
    )
    async with FILE_LOCK:
        data = await load_data_unlocked()
        if token in data.get("mirror_tokens", {}):
            data["mirror_tokens"][token]["instruction_msg_id"] = msg.message_id
            await save_data_unlocked(data)
    fresh = await load_data()
    await edit_company_settings_menu(fresh, wid, company_idx)


@dp.callback_query_handler(lambda c: c.data.startswith("mirroroff:"))
async def mirror_off(cb: types.CallbackQuery):
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
        mirror = company.get("mirror") or {}
        company["mirror"] = None
        clear_pending_mirror_tokens_for_company(data, wid, company["id"])
        await save_data_unlocked(data)

    if mirror.get("message_id"):
        await safe_delete_message(mirror.get("chat_id"), mirror.get("message_id"))
    fresh = await load_data()
    await edit_company_settings_menu(fresh, wid, company_idx)
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

    async with FILE_LOCK:
        data = await load_data_unlocked()
        payload = data.get("mirror_tokens", {}).get(code)
        if not payload:
            await save_data_unlocked(data)
            asyncio.create_task(send_temp_message(message.chat.id, "Код не найден или уже использован.", message.message_thread_id or 0, delay=10))
            return
        source_wid = payload["source_wid"]
        company_id = payload["company_id"]
        ws = data["workspaces"].get(source_wid)
        if not ws:
            data["mirror_tokens"].pop(code, None)
            await save_data_unlocked(data)
            asyncio.create_task(send_temp_message(message.chat.id, "Исходный workspace не найден.", message.message_thread_id or 0, delay=10))
            return
        company_idx = find_company_index_by_id(ws, company_id)
        if company_idx is None:
            data["mirror_tokens"].pop(code, None)
            await save_data_unlocked(data)
            asyncio.create_task(send_temp_message(message.chat.id, "Компания не найдена.", message.message_thread_id or 0, delay=10))
            return
        company = ws["companies"][company_idx]

        reused_message_id = None
        for item in company.get("mirror_history", []):
            if item.get("chat_id") == message.chat.id and (item.get("thread_id") or 0) == (message.message_thread_id or 0):
                reused_message_id = item.get("message_id")
                break

        company["mirror"] = {
            "chat_id": message.chat.id,
            "thread_id": message.message_thread_id or 0,
            "message_id": reused_message_id,
        }
        instruction_msg_id = payload.get("instruction_msg_id")
        source_chat_id = payload.get("source_chat_id")
        source_thread_id = payload.get("source_thread_id") or 0
        data["mirror_tokens"].pop(code, None)
        await save_data_unlocked(data)

    fresh = await load_data()
    ws = fresh["workspaces"][source_wid]
    company_idx = find_company_index_by_id(ws, company_id)
    company = ws["companies"][company_idx]
    await upsert_company_mirror(company)
    await save_data(fresh)
    await try_delete_user_message(message)
    if instruction_msg_id:
        await safe_delete_message(source_chat_id, instruction_msg_id)
    await send_temp_message(ws["chat_id"], f"📤 Список «{company['title']}» дублируется в другой тред/чат", source_thread_id, delay=10)
    if ws.get("is_connected"):
        await edit_company_settings_menu(fresh, source_wid, company_idx)


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
    _, wid, mode = cb.data.split(":")
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        await set_prompt(ws, "✏️ Напишите название компании:", {"type": "new_company", "use_template": mode == "template", "back_to": {"view": "ws"}})
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
    elif view == "template":
        await edit_template_menu(data, wid)
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

@dp.callback_query_handler(lambda c: c.data.startswith("deadlinefmt:"))
async def toggle_deadline_format(cb: types.CallbackQuery):
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
        company["deadline_display_mode"] = "date" if company.get("deadline_display_mode") != "date" else "days"
        await save_data_unlocked(data)
    fresh = await load_data()
    await sync_company_everywhere(fresh["workspaces"][wid], company_idx)
    await edit_company_settings_menu(fresh, wid, company_idx)


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
        await set_prompt(ws, "✏️ Введите новое название компании:", {"type": "rename_company", "company_idx": company_idx, "back_to": {"view": "company_settings", "company_idx": company_idx}})
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
        await set_prompt(ws, "😀 Пришлите один смайлик для компании:", {"type": "company_emoji", "company_idx": company_idx, "back_to": {"view": "company_settings", "company_idx": company_idx}})
        await save_data_unlocked(data)


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
        mirror = company.get("mirror") or {}
        clear_pending_mirror_tokens_for_company(data, wid, company_id)
        await save_data_unlocked(data)
    await safe_delete_message(ws["chat_id"], card_msg_id)
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
        await set_prompt(ws, "✏️ Введите название категории:", {"type": "new_category", "company_idx": company_idx, "back_to": {"view": "company", "company_idx": company_idx}})
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
        await set_prompt(ws, "✏️ Введите новое название категории:", {"type": "rename_category", "company_idx": company_idx, "category_idx": category_idx, "back_to": {"view": "category_settings", "company_idx": company_idx, "category_idx": category_idx}})
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
        await set_prompt(ws, "😀 Пришлите один смайлик для категории:", {"type": "category_emoji", "company_idx": company_idx, "category_idx": category_idx, "back_to": {"view": "category_settings", "company_idx": company_idx, "category_idx": category_idx}})
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
        await set_prompt(ws, "✏️ Введите текст новой задачи:", {"type": "new_task", "company_idx": company_idx, "category_idx": category_idx, "back_to": back_to})
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
        await set_prompt(ws, "✏️ Введите новое название задачи:", {"type": "rename_task", "company_idx": company_idx, "task_idx": task_idx, "back_to": {"view": "task", "company_idx": company_idx, "task_idx": task_idx}})
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
        await set_prompt(ws, "⏰ Пришлите дату или срок. Примеры: 06.04.2026 16:00, 3 дня, 7ч20м.", {"type": "task_deadline", "company_idx": company_idx, "task_idx": task_idx, "back_to": {"view": "task", "company_idx": company_idx, "task_idx": task_idx}})
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
        await set_prompt(ws, "✏️ Введите название категории шаблона:", {"type": "new_template_category", "back_to": {"view": "template"}})
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
        await set_prompt(ws, "✏️ Введите новое название категории шаблона:", {"type": "rename_template_category", "category_idx": category_idx, "back_to": {"view": "template_category_settings", "category_idx": category_idx}})
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
        await set_prompt(ws, "😀 Пришлите один смайлик для категории шаблона:", {"type": "template_category_emoji", "category_idx": category_idx, "back_to": {"view": "template_category_settings", "category_idx": category_idx}})
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
        await set_prompt(ws, "✏️ Введите название новой задачи шаблона:", {"type": "new_template_task", "category_idx": category_idx, "back_to": back_to})
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
        await set_prompt(ws, "✏️ Введите новое название задачи шаблона:", {"type": "rename_template_task", "task_idx": task_idx, "back_to": {"view": "template_task", "task_idx": task_idx}})
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
        await set_prompt(ws, "⏰ Пришлите число дней, например 3.", {"type": "template_task_deadline", "task_idx": task_idx, "back_to": {"view": "template_task", "task_idx": task_idx}})
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
        ws["template_tasks"][task_idx]["deadline_days"] = None
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
    if message.chat.type == "private":
        return
    if is_known_command(message.text):
        return

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
                asyncio.create_task(send_temp_message(ws["chat_id"], "Такая компания уже существует.", ws["thread_id"], delay=6))
                return
            company = make_company(text, awaiting.get("use_template", False), ws)
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
                asyncio.create_task(send_temp_message(ws["chat_id"], "Такая компания уже существует.", ws["thread_id"], delay=6))
                return
            ws["companies"][company_idx]["title"] = text
            finish()
            await save_data_unlocked(data)
            created_company = False
        elif mode == "company_emoji":
            if not is_single_emoji(text):
                await save_data_unlocked(data)
                asyncio.create_task(send_temp_message(ws["chat_id"], "Пришлите один смайлик.", ws["thread_id"], delay=6))
                asyncio.create_task(try_delete_user_message(message))
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
                asyncio.create_task(send_temp_message(ws["chat_id"], "Такая категория уже существует.", ws["thread_id"], delay=6))
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
                asyncio.create_task(send_temp_message(ws["chat_id"], "Такая категория уже существует.", ws["thread_id"], delay=6))
                return
            category["title"] = text
            finish(); await save_data_unlocked(data); created_company = False
        elif mode == "category_emoji":
            if not is_single_emoji(text):
                await save_data_unlocked(data)
                asyncio.create_task(send_temp_message(ws["chat_id"], "Пришлите один смайлик.", ws["thread_id"], delay=6))
                asyncio.create_task(try_delete_user_message(message))
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
                asyncio.create_task(try_delete_user_message(message))
                return
            task["deadline_started_at"] = started_at
            task["deadline_due_at"] = due_at
            finish(); await save_data_unlocked(data); created_company = False
        elif mode == "new_template_category":
            if category_exists(ws.get("template_categories", []), text):
                await save_data_unlocked(data)
                asyncio.create_task(send_temp_message(ws["chat_id"], "Такая категория уже существует.", ws["thread_id"], delay=6))
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
                asyncio.create_task(send_temp_message(ws["chat_id"], "Такая категория уже существует.", ws["thread_id"], delay=6))
                return
            category["title"] = text
            finish(); await save_data_unlocked(data); created_company = False
        elif mode == "template_category_emoji":
            if not is_single_emoji(text):
                await save_data_unlocked(data)
                asyncio.create_task(send_temp_message(ws["chat_id"], "Пришлите один смайлик.", ws["thread_id"], delay=6))
                asyncio.create_task(try_delete_user_message(message))
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
                "deadline_days": None,
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
            days, err = parse_template_deadline_days(text)
            if err:
                await save_data_unlocked(data)
                asyncio.create_task(send_temp_message(ws["chat_id"], err, ws["thread_id"], delay=6))
                asyncio.create_task(try_delete_user_message(message))
                return
            ws["template_tasks"][task_idx]["deadline_days"] = days
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

    if mode == "new_company":
        await sync_company_everywhere(ws, created_company_idx)
        await recreate_ws_home_menu(fresh, wid)
        await save_data(fresh)
        return

    company_modes = {"rename_company", "company_emoji", "new_category", "rename_category", "category_emoji", "new_task", "rename_task", "task_deadline"}
    if mode in company_modes:
        company_idx = awaiting.get("company_idx")
        if company_idx is not None and 0 <= company_idx < len(ws.get("companies", [])):
            await sync_company_everywhere(ws, company_idx)
            await save_data(fresh)

    await show_back_view(fresh, wid, back_to)


async def seconds_until_next_10_minutes() -> int:
    now = now_dt()
    total_seconds = now.minute * 60 + now.second
    next_slot = ((total_seconds // 600) + 1) * 600
    delta = next_slot - total_seconds
    if delta <= 0:
        delta = 600
    return delta


async def deadline_refresh_loop(_dp: Dispatcher):
    while True:
        await asyncio.sleep(await seconds_until_next_10_minutes())
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


async def on_startup(dp: Dispatcher):
    asyncio.create_task(deadline_refresh_loop(dp))


# =========================
# RUN
# =========================

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
