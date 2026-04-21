import os
import json
import math
import asyncio
import time
import uuid
import copy
import re
import html
from calendar import monthrange
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
RUNTIME_REPORT_OCCURRENCES: dict[tuple[str, str, str], int] = {}
RECENT_CALLBACKS: dict[tuple[int, int, str], float] = {}
CALLBACK_DEBOUNCE_SECONDS = 0.9

# =========================
# LOW LEVEL HELPERS
# =========================

def now_ts() -> int:
    return int(time.time())

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

async def try_delete_user_message(message: types.Message):
    try:
        await tg_call(lambda: message.delete(), retries=1)
    except Exception:
        pass

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
        "known_topics": {},
    }

EMOJI_VARIATION_CHARS = {"\ufe0f", "\u200d"}

def is_single_emoji(text: str) -> bool:
    text = (text or "").strip()
    if not text or " " in text or any(ch.isalnum() for ch in text):
        return False
    cleaned = "".join(ch for ch in text if ch not in EMOJI_VARIATION_CHARS)
    return 1 <= len(cleaned) <= 4

WEEKDAY_NAMES = [
    "понедельник",
    "вторник",
    "среда",
    "четверг",
    "пятница",
    "суббота",
    "воскресение",
]

def default_reporting() -> dict:
    return {
        "intervals": [],
        "targets": None,
        "history": [],
    }

def ensure_report_target(target):
    if not isinstance(target, dict):
        return None
    chat_id = target.get("chat_id")
    if chat_id is None:
        return None
    return {
        "chat_id": chat_id,
        "thread_id": int(target.get("thread_id") or 0),
        "label": target.get("label"),
        "message_id": target.get("message_id"),
    }

def report_target_key(target: dict) -> str:
    return f"{target.get('chat_id')}:{target.get('thread_id') or 0}"

def default_accumulation_for_kind(kind: str) -> dict:
    if kind == "on_done":
        return {"mode": "instant"}
    if kind == "monthly":
        return {"mode": "month"}
    if kind in {"daily", "weekly"}:
        return {"mode": "week"}
    return {"mode": "last_report"}

def ensure_report_accumulation(accumulation, interval_kind: str):
    if not isinstance(accumulation, dict):
        return default_accumulation_for_kind(interval_kind)

    mode = accumulation.get("mode")
    if mode == "instant" and interval_kind == "on_done":
        return {"mode": "instant"}
    if mode == "last_report":
        return {"mode": "last_report"}
    if mode == "week" and interval_kind in {"daily", "weekly"}:
        return {"mode": "week"}
    if mode == "month" and interval_kind == "monthly":
        return {"mode": "month"}
    if mode == "specific":
        start_at = accumulation.get("start_at")
        if isinstance(start_at, int):
            return {"mode": "specific", "type": "datetime", "start_at": start_at}
        if interval_kind == "monthly":
            day = accumulation.get("day")
            hour = accumulation.get("hour")
            minute = accumulation.get("minute")
            if isinstance(day, int) and 1 <= day <= 31 and isinstance(hour, int) and 0 <= hour <= 23 and isinstance(minute, int) and 0 <= minute <= 59:
                return {"mode": "specific", "type": "month_day", "day": day, "hour": hour, "minute": minute}
        if interval_kind in {"daily", "weekly"}:
            weekday = accumulation.get("weekday")
            hour = accumulation.get("hour")
            minute = accumulation.get("minute")
            if isinstance(weekday, int) and 0 <= weekday <= 6 and isinstance(hour, int) and 0 <= hour <= 23 and isinstance(minute, int) and 0 <= minute <= 59:
                return {"mode": "specific", "type": "weekday_time", "weekday": weekday, "hour": hour, "minute": minute}
    return default_accumulation_for_kind(interval_kind)

def ensure_report_interval(interval):
    if not isinstance(interval, dict):
        return None

    kind = interval.get("kind")
    if kind not in {"weekly", "daily", "monthly", "once", "on_done"}:
        return None

    normalized = {
        "id": interval.get("id") or uuid.uuid4().hex,
        "kind": kind,
        "created_at": interval.get("created_at") if isinstance(interval.get("created_at"), int) else now_ts(),
        "last_report_at": interval.get("last_report_at") if isinstance(interval.get("last_report_at"), int) else None,
    }

    if kind == "weekly":
        weekday = interval.get("weekday")
        hour = interval.get("hour")
        minute = interval.get("minute")
        if not (isinstance(weekday, int) and 0 <= weekday <= 6 and isinstance(hour, int) and 0 <= hour <= 23 and isinstance(minute, int) and 0 <= minute <= 59):
            return None
        normalized["weekday"] = weekday
        normalized["hour"] = hour
        normalized["minute"] = minute
    elif kind == "daily":
        hour = interval.get("hour")
        minute = interval.get("minute")
        if not (isinstance(hour, int) and 0 <= hour <= 23 and isinstance(minute, int) and 0 <= minute <= 59):
            return None
        normalized["hour"] = hour
        normalized["minute"] = minute
    elif kind == "monthly":
        day = interval.get("day")
        hour = interval.get("hour")
        minute = interval.get("minute")
        if not (isinstance(day, int) and 1 <= day <= 31 and isinstance(hour, int) and 0 <= hour <= 23 and isinstance(minute, int) and 0 <= minute <= 59):
            return None
        normalized["day"] = day
        normalized["hour"] = hour
        normalized["minute"] = minute
    else:
        if kind == "on_done":
            pass
        else:
            scheduled_at = interval.get("scheduled_at")
            if not isinstance(scheduled_at, int):
                return None
            normalized["scheduled_at"] = scheduled_at

    normalized["accumulation"] = ensure_report_accumulation(interval.get("accumulation"), kind)
    target_key = interval.get("target_key")
    if isinstance(target_key, str) and target_key:
        normalized["target_key"] = target_key
    return normalized

def ensure_reporting(reporting):
    if not isinstance(reporting, dict):
        reporting = default_reporting()
    reporting.setdefault("intervals", [])
    reporting.setdefault("targets", None)
    reporting.setdefault("history", [])

    normalized_intervals = []
    for interval in reporting.get("intervals", []):
        normalized = ensure_report_interval(interval)
        if normalized:
            normalized_intervals.append(normalized)
    reporting["intervals"] = normalized_intervals

    if reporting.get("targets") is None:
        reporting["targets"] = None
    else:
        normalized_targets = []
        for target in reporting.get("targets", []):
            normalized = ensure_report_target(target)
            if normalized:
                normalized_targets.append(normalized)
        reporting["targets"] = normalized_targets

    normalized_history = []
    for entry in reporting.get("history", []):
        if not isinstance(entry, dict):
            continue
        completed_at = entry.get("completed_at")
        task_text = entry.get("task_text")
        if not isinstance(completed_at, int) or task_text is None:
            continue
        normalized_history.append({
            "id": entry.get("id") or uuid.uuid4().hex,
            "task_id": entry.get("task_id"),
            "task_text": str(task_text),
            "completed_at": completed_at,
            "canceled_at": entry.get("canceled_at") if isinstance(entry.get("canceled_at"), int) else None,
        })
    reporting["history"] = normalized_history
    return reporting

def ensure_task(task, is_template: bool = False):
    if not isinstance(task, dict):
        task = {"text": str(task)}

    task.setdefault("id", uuid.uuid4().hex)
    task.setdefault("text", "")
    task.setdefault("category_id", None)
    task.setdefault("created_at", now_ts())

    if is_template:
        task.setdefault("deadline_seconds", None)
    else:
        task.setdefault("done", False)
        task.setdefault("deadline_due_at", None)
        task.setdefault("deadline_started_at", None)
        task.setdefault("done_event_id", None)
    task.pop("done_at", None)
    return task

def default_template_tasks() -> list[dict]:
    return [
        ensure_task({"text": "Создать договор"}, is_template=True),
        ensure_task({"text": "Выставить счёт"}, is_template=True),
    ]

def make_template(title: str = "Шаблон", tasks: list[dict] | None = None, categories: list[dict] | None = None) -> dict:
    return {
        "id": uuid.uuid4().hex,
        "title": title,
        "emoji": "📁",
        "deadline_format": "relative",
        "reporting": default_reporting(),
        "tasks": tasks or [],
        "categories": categories or [],
    }

def ensure_category(cat):
    if not isinstance(cat, dict):
        cat = {"title": str(cat), "emoji": "📁"}
    cat.setdefault("id", uuid.uuid4().hex)
    cat["emoji"] = cat.get("emoji") or "📁"
    cat["title"] = cat.get("title") or "Подгруппа"
    cat.setdefault("deadline_format", None)
    return cat

def ensure_company(company):
    if not isinstance(company, dict):
        company = {}

    company.setdefault("id", uuid.uuid4().hex)
    company["emoji"] = company.get("emoji") or "📁"
    company["title"] = company.get("title") or "Список"
    company.setdefault("card_msg_id", None)
    company.setdefault("mirrors", [])
    company.setdefault("tasks", [])
    company.setdefault("categories", [])
    company.setdefault("deadline_format", "relative")
    company.setdefault("reporting", default_reporting())

    if not isinstance(company["tasks"], list):
        company["tasks"] = []
    if not isinstance(company["categories"], list):
        company["categories"] = []
    company["reporting"] = ensure_reporting(company.get("reporting"))
    if not isinstance(company["mirrors"], list):
        company["mirrors"] = []

    company["tasks"] = [ensure_task(t, is_template=False) for t in company["tasks"]]
    company["categories"] = [ensure_category(c) for c in company["categories"]]

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
    return company

def normalize_template(ws: dict):
    if not isinstance(ws.get("templates"), list) or not ws["templates"]:
        ws["templates"] = [make_template()]

    normalized_templates = []
    for tpl in ws["templates"]:
        if not isinstance(tpl, dict):
            tpl = {}
        tpl.setdefault("id", uuid.uuid4().hex)
        tpl.setdefault("title", "Шаблон")
        tpl.setdefault("emoji", "📁")
        tpl.setdefault("deadline_format", "relative")
        tpl["reporting"] = ensure_reporting(tpl.get("reporting"))
        if not isinstance(tpl.get("tasks"), list):
            tpl["tasks"] = []
        if not isinstance(tpl.get("categories"), list):
            tpl["categories"] = []
        tpl["tasks"] = [ensure_task(t, is_template=True) for t in tpl["tasks"]]
        tpl["categories"] = [ensure_category(c) for c in tpl["categories"]]
        normalized_templates.append(tpl)
    ws["templates"] = normalized_templates

    if ws.get("active_template_id") not in {tpl["id"] for tpl in ws["templates"]}:
        ws["active_template_id"] = ws["templates"][0]["id"]

    active = get_active_template(ws)
    ws["template_tasks"] = active["tasks"]
    ws["template_categories"] = active["categories"]

def get_active_template(ws: dict) -> dict:
    templates = ws.get("templates") or []
    if not templates:
        ws["templates"] = [make_template()]
        templates = ws["templates"]
    active_id = ws.get("active_template_id") or templates[0]["id"]
    for tpl in templates:
        if tpl.get("id") == active_id:
            return tpl
    ws["active_template_id"] = templates[0]["id"]
    return templates[0]

def set_active_template(ws: dict, template_id: str):
    for tpl in ws.get("templates", []):
        if tpl.get("id") == template_id:
            ws["active_template_id"] = template_id
            ws["template_tasks"] = tpl["tasks"]
            ws["template_categories"] = tpl["categories"]
            return tpl
    return get_active_template(ws)

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
        'mirrors': [],
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
        'reporting': ensure_reporting(copy.deepcopy(template.get('reporting'))),
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
    data.setdefault("known_topics", {})

    if not isinstance(data["known_topics"], dict):
        data["known_topics"] = {}

    normalized_topics = {}
    for key, value in list(data["known_topics"].items()):
        if not isinstance(value, dict):
            continue
        entry = {}
        if value.get("chat_title"):
            entry["chat_title"] = str(value["chat_title"])
        if value.get("topic_title"):
            entry["topic_title"] = str(value["topic_title"])
        if entry:
            normalized_topics[key] = entry
    data["known_topics"] = normalized_topics

    for uid, user in list(data["users"].items()):
        if not isinstance(user, dict):
            data["users"][uid] = {}
            user = data["users"][uid]
        user.setdefault("workspaces", [])
        user.setdefault("pm_menu_msg_id", None)
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
        if payload.get("source_wid") and payload.get("company_id"):
            normalized_payload = {
                "source_wid": payload["source_wid"],
                "company_id": payload["company_id"],
            }
            if payload.get("source_thread_id") is not None:
                normalized_payload["source_thread_id"] = payload.get("source_thread_id")
            if payload.get("kind") == "report_target":
                normalized_payload["kind"] = "report_target"
            valid_tokens[token] = normalized_payload
    data["mirror_tokens"] = valid_tokens

    places_to_refresh: set[tuple[int, int]] = set()
    for ws in data["workspaces"].values():
        chat_id = ws.get("chat_id")
        if chat_id is not None:
            places_to_refresh.add((chat_id, ws.get("thread_id") or 0))
        for company in ws.get("companies", []):
            for mirror in company.get("mirrors", []):
                if mirror.get("chat_id") is not None:
                    places_to_refresh.add((mirror.get("chat_id"), mirror.get("thread_id") or 0))
            reporting = company.get("reporting")
            if not isinstance(reporting, dict):
                continue
            targets = reporting.get("targets")
            if not isinstance(targets, list):
                continue
            for target in targets:
                if isinstance(target, dict) and target.get("chat_id") is not None:
                    places_to_refresh.add((target.get("chat_id"), target.get("thread_id") or 0))

    for chat_id, thread_id in places_to_refresh:
        refresh_binding_labels(data, chat_id, thread_id)
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
            "ui_pages": {},
        },
    )
    return data["users"][user_id]

def make_ws_id(chat_id: int, thread_id: int | None):
    return f"{chat_id}_{thread_id or 0}"

def clean_text(text: str) -> str:
    return (text or "").strip().lstrip("/").strip()

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

def get_known_topic_entry(data: dict, chat_id: int, thread_id: int) -> dict | None:
    entry = (data.get("known_topics") or {}).get(make_ws_id(chat_id, thread_id))
    return entry if isinstance(entry, dict) else None

def find_workspace_by_binding(data: dict, chat_id: int, thread_id: int) -> dict | None:
    for ws in data.get("workspaces", {}).values():
        if ws.get("chat_id") == chat_id and (ws.get("thread_id") or 0) == thread_id:
            return ws
    return None

def resolve_binding_titles(data: dict, chat_id: int, thread_id: int, chat_title: str | None = None, topic_title: str | None = None) -> tuple[str | None, str | None]:
    ws = find_workspace_by_binding(data, chat_id, thread_id) or {}
    entry = get_known_topic_entry(data, chat_id, thread_id) or {}
    return (
        chat_title or ws.get("chat_title") or entry.get("chat_title"),
        topic_title or ws.get("topic_title") or entry.get("topic_title"),
    )

def remember_binding_place(data: dict, chat_id: int, thread_id: int, chat_title: str | None = None, topic_title: str | None = None) -> tuple[str | None, str | None]:
    known_topics = data.setdefault("known_topics", {})
    if not isinstance(known_topics, dict):
        known_topics = {}
        data["known_topics"] = known_topics

    key = make_ws_id(chat_id, thread_id)
    entry = known_topics.setdefault(key, {})
    if not isinstance(entry, dict):
        entry = {}
        known_topics[key] = entry

    if chat_title:
        entry["chat_title"] = chat_title
    if topic_title:
        entry["topic_title"] = topic_title
    elif not thread_id:
        entry.pop("topic_title", None)

    return resolve_binding_titles(data, chat_id, thread_id, chat_title, topic_title)

def binding_place_label(
    data: dict,
    chat_id: int,
    thread_id: int,
    fallback_label: str | None = None,
    chat_title: str | None = None,
    topic_title: str | None = None,
) -> str:
    resolved_chat, resolved_topic = resolve_binding_titles(data, chat_id, thread_id, chat_title, topic_title)
    if resolved_chat:
        return workspace_full_name(resolved_chat, resolved_topic, thread_id)
    return fallback_label or f"{chat_id}/{thread_id or 0}"

def refresh_binding_labels(data: dict, chat_id: int, thread_id: int) -> str:
    resolved_chat, resolved_topic = resolve_binding_titles(data, chat_id, thread_id)
    label = binding_place_label(data, chat_id, thread_id, chat_title=resolved_chat, topic_title=resolved_topic)

    for ws in data.get("workspaces", {}).values():
        if ws.get("chat_id") == chat_id and (ws.get("thread_id") or 0) == thread_id:
            if resolved_chat:
                ws["chat_title"] = resolved_chat
            if thread_id:
                ws["topic_title"] = resolved_topic
            ws["name"] = label

        for company in ws.get("companies", []):
            for mirror in company.get("mirrors", []):
                if mirror.get("chat_id") == chat_id and (mirror.get("thread_id") or 0) == thread_id:
                    mirror["label"] = label

            reporting = company.get("reporting")
            if not isinstance(reporting, dict):
                continue
            targets = reporting.get("targets")
            if not isinstance(targets, list):
                continue
            for target in targets:
                if target.get("chat_id") == chat_id and (target.get("thread_id") or 0) == thread_id:
                    target["label"] = label

    return label

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

def task_menu_title(ws: dict, company: dict, task: dict, category: dict | None = None) -> str:
    parts = [rich_display_company_name(company)]
    if category:
        parts.append(rich_display_category_name(category))
    deadline_format = (category.get("deadline_format") if category and category.get("deadline_format") else company.get("deadline_format")) or "relative"
    suffix = display_task_deadline_suffix(task, deadline_format) if not task.get("done") and task.get("deadline_due_at") else ""
    parts.append(f"📌 {rich_task_text(task.get('text') or 'Задача', bool(task.get('done')))}{esc(suffix)}")
    return workspace_path_title(ws, *parts)

def template_task_title(ws: dict, template: dict, task: dict, category: dict | None = None) -> str:
    parts = ["⚙️ Шаблоны задач", rich_display_template_name(template)]
    if category:
        parts.append(rich_display_category_name(category))
    parts.append(f"📌 {rich_task_text(task.get('text') or 'Задача')}{esc(template_task_deadline_suffix(task))}")
    return workspace_path_title(ws, *parts)

def format_duration_text(seconds: int | None) -> str:
    if seconds is None:
        return ""
    minutes = max(0, math.ceil(seconds / 60))
    days, rem = divmod(minutes, 60 * 24)
    hours, mins = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days} д.")
    if hours or days:
        parts.append(f"{hours} ч.")
    parts.append(f"{mins} м.")
    return "; ".join(parts)

def template_task_deadline_suffix(task: dict) -> str:
    value = task.get("deadline_seconds")
    if not value:
        return ""
    return f" ({format_duration_text(value)})"

def display_task_deadline_suffix(task: dict, deadline_format: str = "relative") -> str:
    due_at = task.get("deadline_due_at")
    if not due_at:
        return ""
    if deadline_format == "date":
        return f" ({datetime.fromtimestamp(due_at, TIMEZONE).strftime('до %d.%m.%Y г. %H:%M')})"
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

def company_card_text(company: dict) -> str:
    lines = [f"{rich_display_company_name(company)}:"]
    company_deadline_format = company.get("deadline_format") or "relative"
    all_tasks = company.get("tasks", [])
    sort_key = lambda task: (
        1 if task.get("done") else 0,
        1 if not task.get("deadline_due_at") else 0,
        task.get("deadline_due_at") or 10**18,
        task.get("created_at") or 0,
    )
    lines.append(build_progress_bar(sum(1 for task in all_tasks if task.get("done")), len(all_tasks)))
    lines.append("")

    uncategorized = [t for t in all_tasks if not t.get("category_id")]
    if uncategorized:
        for task in sorted(uncategorized, key=sort_key):
            icon = task_deadline_icon(task)
            suffix = display_task_deadline_suffix(task, company_deadline_format) if not task.get("done") and task.get("deadline_due_at") else ""
            lines.append(f"{icon} {rich_task_text(task.get('text') or 'Задача', bool(task.get('done')))}{esc(suffix)}")

    for category in company.get("categories", []):
        if lines and lines[-1] != "":
            lines.append("")
        cat_tasks = [t for t in all_tasks if t.get("category_id") == category["id"]]
        lines.append(f"    {rich_display_category_name(category)}:")
        lines.append(f"    {build_progress_bar(sum(1 for task in cat_tasks if task.get('done')), len(cat_tasks))}")
        lines.append("")
        if cat_tasks:
            for task in sorted(cat_tasks, key=sort_key):
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

def get_reporting(company: dict) -> dict:
    company["reporting"] = ensure_reporting(company.get("reporting"))
    return company["reporting"]

def get_report_intervals(company: dict) -> list[dict]:
    reporting = get_reporting(company)
    reporting.setdefault("intervals", [])
    return reporting["intervals"]

def get_report_history(company: dict) -> list[dict]:
    reporting = get_reporting(company)
    reporting.setdefault("history", [])
    return reporting["history"]

def get_effective_report_targets(company: dict) -> list[dict]:
    reporting = get_reporting(company)
    targets = reporting.get("targets") or []
    return [target for target in (ensure_report_target(item) for item in targets) if target]

def ensure_explicit_report_targets(company: dict) -> list[dict]:
    reporting = get_reporting(company)
    if reporting.get("targets") is None:
        reporting["targets"] = []
    return reporting["targets"]

def normalize_company_report_target_keys(company: dict):
    targets = get_effective_report_targets(company)
    if not targets:
        return
    fallback_key = report_target_key(targets[0])
    for interval in get_report_intervals(company):
        if not interval.get("target_key"):
            interval["target_key"] = fallback_key

def get_report_target(company: dict, target_idx: int) -> dict | None:
    targets = get_effective_report_targets(company)
    if 0 <= target_idx < len(targets):
        return targets[target_idx]
    return None

def get_target_report_pairs(company: dict, target_idx: int) -> list[tuple[int, dict]]:
    target = get_report_target(company, target_idx)
    if not target:
        return []
    normalize_company_report_target_keys(company)
    target_key = report_target_key(target)
    return [
        (idx, interval)
        for idx, interval in enumerate(get_report_intervals(company))
        if interval.get("target_key") == target_key
    ]

def same_binding_place(left: dict, right: dict) -> bool:
    return (
        (left.get("chat_id") if isinstance(left, dict) else None) == (right.get("chat_id") if isinstance(right, dict) else None)
        and ((left.get("thread_id") or 0) if isinstance(left, dict) else 0) == ((right.get("thread_id") or 0) if isinstance(right, dict) else 0)
    )

def missing_report_targets_for_mirrors(company: dict) -> list[tuple[int, dict]]:
    mirrors = company.get("mirrors", [])
    result = []
    for idx, target in enumerate(get_effective_report_targets(company)):
        if any(same_binding_place(target, mirror) for mirror in mirrors):
            continue
        result.append((idx, target))
    return result

def missing_mirrors_for_report_targets(company: dict) -> list[tuple[int, dict]]:
    targets = get_effective_report_targets(company)
    result = []
    for idx, mirror in enumerate(company.get("mirrors", [])):
        if any(same_binding_place(mirror, target) for target in targets):
            continue
        result.append((idx, mirror))
    return result

def binding_instruction_text(title: str, token: str) -> str:
    return (
        f"{title}:\n"
        "1) Добавь меня в нужную конфу;\n"
        "2) Перейди в нужный тред;\n"
        "3) Отправь команду:\n"
        f"/mirror {token}\n"
        "4) Пердани."
    )

def find_completion_entry(history: list[dict], entry_id: str | None) -> dict | None:
    if not entry_id:
        return None
    for entry in history:
        if entry.get("id") == entry_id:
            return entry
    return None

def add_task_completion_event(company: dict, task: dict, completed_at: int | None = None):
    ts = completed_at or now_ts()
    entry = {
        "id": uuid.uuid4().hex,
        "task_id": task.get("id"),
        "task_text": task.get("text") or "",
        "completed_at": ts,
        "canceled_at": None,
    }
    get_report_history(company).append(entry)
    task["done_event_id"] = entry["id"]

def cancel_task_completion_event(company: dict, task: dict, canceled_at: int | None = None):
    entry = find_completion_entry(get_report_history(company), task.get("done_event_id"))
    if entry and entry.get("canceled_at") is None:
        entry["canceled_at"] = canceled_at or now_ts()
    task["done_event_id"] = None

def format_clock(hour: int, minute: int) -> str:
    return f"{hour:02d}:{minute:02d}"

def format_report_timestamp(ts: int) -> str:
    return datetime.fromtimestamp(ts, TIMEZONE).strftime("%d.%m.%Y, %H:%M")

def format_report_schedule_label(interval: dict) -> str:
    kind = interval.get("kind")
    if kind == "on_done":
        return "Сразу после выполнения"
    if kind == "weekly":
        return f"{WEEKDAY_NAMES[interval['weekday']]} {format_clock(interval['hour'], interval['minute'])}"
    if kind == "daily":
        return f"каждый день {format_clock(interval['hour'], interval['minute'])}"
    if kind == "monthly":
        return f"каждый месяц {interval['day']}-го, {format_clock(interval['hour'], interval['minute'])}"
    return format_report_timestamp(interval["scheduled_at"])

def format_report_period_preview(interval: dict, start_at: int, end_at: int) -> str:
    def point_label(ts: int) -> str:
        dt = datetime.fromtimestamp(ts, TIMEZONE)
        if interval.get("kind") == "weekly":
            return f"{WEEKDAY_NAMES[dt.weekday()]} {dt.strftime('%H:%M')}"
        if interval.get("kind") == "daily":
            return dt.strftime("%H:%M")
        return dt.strftime("%d.%m.%Y, %H:%M")

    accumulation = interval.get("accumulation") or {}
    end_label = point_label(end_at)
    if accumulation.get("mode") == "last_report":
        return f"от последнего отчета - {end_label}"
    start_label = point_label(start_at)
    return f"{start_label} - {end_label}"

def build_month_datetime(year: int, month: int, day: int, hour: int, minute: int) -> datetime:
    last_day = monthrange(year, month)[1]
    return datetime(year, month, min(day, last_day), hour, minute, tzinfo=TIMEZONE)

def shift_month(dt: datetime, months: int, preferred_day: int | None = None) -> datetime:
    absolute_month = dt.year * 12 + (dt.month - 1) + months
    year = absolute_month // 12
    month = absolute_month % 12 + 1
    day = preferred_day if preferred_day is not None else dt.day
    return build_month_datetime(year, month, day, dt.hour, dt.minute)

def next_report_occurrence_after(interval: dict, after_ts: int) -> int | None:
    kind = interval.get("kind")
    if kind == "once":
        scheduled_at = interval.get("scheduled_at")
        return scheduled_at if isinstance(scheduled_at, int) and scheduled_at > after_ts else None

    after_dt = datetime.fromtimestamp(after_ts, TIMEZONE)
    if kind == "daily":
        candidate = after_dt.replace(hour=interval["hour"], minute=interval["minute"], second=0, microsecond=0)
        if candidate.timestamp() <= after_ts:
            candidate += timedelta(days=1)
        return int(candidate.timestamp())

    if kind == "weekly":
        days_ahead = (interval["weekday"] - after_dt.weekday()) % 7
        candidate_date = after_dt.date() + timedelta(days=days_ahead)
        candidate = datetime(candidate_date.year, candidate_date.month, candidate_date.day, interval["hour"], interval["minute"], tzinfo=TIMEZONE)
        if candidate.timestamp() <= after_ts:
            candidate += timedelta(days=7)
        return int(candidate.timestamp())

    candidate = build_month_datetime(after_dt.year, after_dt.month, interval["day"], interval["hour"], interval["minute"])
    if candidate.timestamp() <= after_ts:
        candidate = shift_month(candidate, 1, interval["day"])
    return int(candidate.timestamp())

def get_last_company_report_at(company: dict | None, before_ts: int | None = None) -> int | None:
    if not company:
        return None
    latest = None
    for item in get_report_intervals(company):
        published_at = item.get("last_report_at")
        if not isinstance(published_at, int):
            continue
        if before_ts is not None and published_at >= before_ts:
            continue
        if latest is None or published_at > latest:
            latest = published_at
    return latest

def resolve_report_period(interval: dict, occurrence_ts: int, company: dict | None = None) -> tuple[int, int]:
    end_at = occurrence_ts
    end_dt = datetime.fromtimestamp(end_at, TIMEZONE)
    accumulation = interval.get("accumulation") or {}
    mode = accumulation.get("mode")

    if mode == "last_report":
        start_at = get_last_company_report_at(company, before_ts=end_at) or interval.get("created_at") or max(end_at - 1, 0)
    elif mode == "week":
        start_at = int((end_dt - timedelta(days=7)).timestamp())
    elif mode == "month":
        start_at = int(shift_month(end_dt, -1, interval.get("day")).timestamp())
    elif accumulation.get("type") == "datetime":
        start_at = accumulation.get("start_at") or interval.get("created_at") or max(end_at - 1, 0)
    elif interval.get("kind") == "once":
        start_at = accumulation.get("start_at") or interval.get("created_at") or max(end_at - 1, 0)
    elif interval.get("kind") == "monthly":
        candidate = build_month_datetime(end_dt.year, end_dt.month, accumulation.get("day"), accumulation.get("hour"), accumulation.get("minute"))
        if candidate >= end_dt:
            candidate = shift_month(candidate, -1, accumulation.get("day"))
        start_at = int(candidate.timestamp())
    else:
        days_back = (end_dt.weekday() - accumulation.get("weekday")) % 7
        candidate_date = end_dt.date() - timedelta(days=days_back)
        candidate = datetime(candidate_date.year, candidate_date.month, candidate_date.day, accumulation.get("hour"), accumulation.get("minute"), tzinfo=TIMEZONE)
        if candidate >= end_dt:
            candidate -= timedelta(days=7)
        start_at = int(candidate.timestamp())

    if start_at >= end_at:
        start_at = max(end_at - 1, 0)
    return start_at, end_at

def collect_report_entries(company: dict, start_at: int, end_at: int) -> list[dict]:
    entries = []
    for entry in get_report_history(company):
        completed_at = entry.get("completed_at")
        if not isinstance(completed_at, int):
            continue
        if not (start_at < completed_at <= end_at):
            continue
        if entry.get("canceled_at") is not None:
            continue
        entries.append(entry)
    return sorted(entries, key=lambda item: (item.get("completed_at") or 0, item.get("task_text") or ""))

def build_report_message(company: dict, start_at: int, end_at: int) -> str:
    title = company.get("title") or "Список"
    lines = [
        f'Отчёт по "{esc(title)}"',
        f"за {format_report_timestamp(start_at)} - {format_report_timestamp(end_at)}:",
        "",
    ]
    for entry in collect_report_entries(company, start_at, end_at):
        lines.append(f"✅ {esc(entry.get('task_text') or 'Задача')}")
    lines.append("")
    lines.append(build_progress_bar(sum(1 for task in company.get("tasks", []) if task.get("done")), len(company.get("tasks", []))))
    return "\n".join(lines)

def build_task_completion_report_message(company: dict, task: dict) -> str:
    title = company.get("title") or "Список"
    task_text = task.get("text") or "Задача"
    lines = [
        f'Отчёт по "{esc(title)}"',
        "сразу после выполнения:",
        "",
        f"✅ {esc(task_text)}",
        "",
        build_progress_bar(sum(1 for item in company.get("tasks", []) if item.get("done")), len(company.get("tasks", []))),
    ]
    return "\n".join(lines)

def find_report_interval(company: dict, interval_idx: int) -> dict | None:
    intervals = get_report_intervals(company)
    if 0 <= interval_idx < len(intervals):
        return intervals[interval_idx]
    return None

def clone_report_interval(interval: dict) -> dict:
    return ensure_report_interval(copy.deepcopy(interval)) or copy.deepcopy(interval)

def report_preview_occurrence(interval: dict) -> int:
    kind = interval.get("kind")
    if kind == "on_done":
        return now_ts()
    if kind == "once":
        return interval.get("scheduled_at") or now_ts()
    anchor = max(now_ts(), interval.get("created_at") or 0) - 1
    occurrence = next_report_occurrence_after(interval, anchor)
    if occurrence is None:
        occurrence = interval.get("last_report_at") or now_ts()
    return occurrence

def report_interval_sort_key(interval: dict, original_idx: int) -> tuple:
    kind = interval.get("kind")
    if kind == "weekly":
        secondary = (0, interval.get("weekday") or 0, interval.get("hour") or 0, interval.get("minute") or 0)
    elif kind == "monthly":
        secondary = (1, interval.get("day") or 0, interval.get("hour") or 0, interval.get("minute") or 0)
    elif kind == "daily":
        secondary = (2, interval.get("hour") or 0, interval.get("minute") or 0)
    else:
        secondary = (3, interval.get("scheduled_at") or 0)
    return (report_preview_occurrence(interval), secondary, original_idx)

def make_report_interval_base(kind: str, created_at: int | None = None, target_key: str | None = None) -> dict:
    base = {
        "id": uuid.uuid4().hex,
        "kind": kind,
        "created_at": created_at or now_ts(),
        "last_report_at": None,
        "accumulation": default_accumulation_for_kind(kind),
    }
    if target_key:
        base["target_key"] = target_key
    if kind == "weekly":
        base.update({"weekday": 0, "hour": 0, "minute": 0})
    elif kind == "daily":
        base.update({"hour": 0, "minute": 0})
    elif kind == "monthly":
        base.update({"day": 1, "hour": 0, "minute": 0})
    elif kind == "once":
        base.update({"scheduled_at": created_at or now_ts()})
    return base

def upsert_report_interval(intervals: list[dict], normalized: dict, flow: str, interval_idx: int | None):
    if flow == "edit_accumulation":
        flow = "edit"
    if flow == "edit" and interval_idx is not None and 0 <= interval_idx < len(intervals):
        intervals[interval_idx] = normalized
    else:
        intervals.append(normalized)

def parse_optional_index(value: str) -> int | None:
    if value in {"x", "", None}:
        return None
    return int(value)

def prepare_report_interval_draft(company: dict, interval_idx: int | None, kind: str, target_key: str | None = None) -> dict:
    existing = find_report_interval(company, interval_idx) if interval_idx is not None else None
    if existing is None:
        return make_report_interval_base(kind, target_key=target_key)

    if existing.get("kind") == kind:
        return clone_report_interval(existing)

    draft = make_report_interval_base(kind, existing.get("created_at"), existing.get("target_key") or target_key)
    draft["id"] = existing.get("id") or draft["id"]
    draft["last_report_at"] = existing.get("last_report_at")
    draft["accumulation"] = ensure_report_accumulation(existing.get("accumulation"), kind)
    return draft

def report_schedule_prompt_text(kind: str) -> str:
    if kind == "once":
        return "🧾 Пришли дату и время отчета"
    if kind == "monthly":
        return "🧾 Пришли число и время отчета, например: 30 20:44"
    return "🧾 Пришли время отчета, например: 21:30"

PAGE_SIZE_PM = 8
PAGE_SIZE_WS = 8
PAGE_SIZE_TEMPLATES = 8
PAGE_SIZE_COMPANY = 8
PAGE_SIZE_CATEGORY = 8
PAGE_SIZE_CREATE = 8
PAGE_SIZE_REPORTS = 8
PAGE_SIZE_REPORT_BINDINGS = 8

def get_ui_page(owner: dict, key: str) -> int:
    try:
        owner.setdefault("ui_pages", {})
        return max(0, int(owner["ui_pages"].get(key, 0) or 0))
    except Exception:
        return 0

def set_ui_page(owner: dict, key: str, page: int):
    owner.setdefault("ui_pages", {})
    owner["ui_pages"][key] = max(0, int(page))

def paginate_items(items, page: int, page_size: int):
    total = len(items)
    if total <= 0:
        return [], False, False
    max_page = max(0, (total - 1) // page_size)
    page = max(0, min(page, max_page))
    start = page * page_size
    end = start + page_size
    return items[start:end], page > 0, page < max_page

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
        "mirrors": [],
        "deadline_format": "relative",
        "reporting": default_reporting(),
        "categories": [],
        "tasks": [],
    }
    if not with_template:
        return company

    template = next((tpl for tpl in ws.get("templates") or [] if tpl.get("id") == template_id), get_active_template(ws))
    company["deadline_format"] = template.get("deadline_format") or "relative"
    tpl_reporting = ensure_reporting(copy.deepcopy(template.get("reporting")))
    company["reporting"]["intervals"] = []
    for interval in tpl_reporting.get("intervals", []):
        normalized = ensure_report_interval(copy.deepcopy(interval))
        if not normalized:
            continue
        normalized["created_at"] = now_ts()
        normalized["last_report_at"] = None
        company["reporting"]["intervals"].append(normalized)
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

def parse_flexible_time(text: str) -> tuple[int, int] | None:
    raw = clean_text(text)
    if not raw:
        return None
    m = re.match(r'^\s*(\d{1,2})(?:\D+(\d{1,2}))?\s*$', raw)
    if not m:
        return None
    hour = int(m.group(1))
    minute = int(m.group(2) or 0)
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    return hour, minute

def parse_month_day_time(text: str) -> tuple[int, int, int] | None:
    raw = clean_text(text).lower().replace("-го", " ").replace("го", " ")
    m = re.match(r'^\s*(\d{1,2})\D+(\d{1,2})(?:\D+(\d{1,2}))?\s*$', raw)
    if not m:
        return None
    day = int(m.group(1))
    hour = int(m.group(2))
    minute = int(m.group(3) or 0)
    if not (1 <= day <= 31 and 0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    return day, hour, minute

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

NEUTRAL_BUTTON_PREFIXES = tuple(
    f"{prefix}:"
    for prefix in (
        "pmws pmpersonal cmp cat task tpl tplcat tpltask tplselect cmpmode mirroritem "
        "taskmoveto tpltaskmoveto cmpren cmpemoji cmpcopy cmpdeadlinefmt "
        "catren catemoji catcopy catdeadlinefmt taskdone taskren taskmove "
        "tpltaskren tpltaskmove tplrenameset tplemojiset tplcopy "
        "tplcatren tplcatemoji tplcatcopy reportacc"
    ).split()
)

def infer_button_style(text: str, callback_data: str | None = None) -> str | None:
    t = (text or '').strip().lower()
    cb = (callback_data or '').strip().lower()

    if t.startswith('⬅️') or t.startswith('⬆️') or t.startswith('⬇️'):
        return 'primary'

    if t in {'да', 'да!', 'yes'}:
        return 'danger'
    if t == 'ok':
        return None

    if 'удал' in t or 'очист' in t or 'отвяз' in t:
        return 'danger'

    if cb.startswith(NEUTRAL_BUTTON_PREFIXES):
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
    btn_style = None if style is False else (style or infer_button_style(text, callback_data))
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

    if has_prev and has_next:
        kb.row(
            kb_btn("➕ Workspace", callback_data="pmhelp:root"),
            kb_btn("⬆️", callback_data="pgpm:prev"),
        )
        kb.row(
            kb_btn("🔄 Обновить", callback_data="pmrefresh:root"),
            kb_btn("⬇️", callback_data="pgpm:next"),
        )
    elif has_prev or has_next:
        kb.row(kb_btn("➕ Workspace", callback_data="pmhelp:root"))
        kb.row(
            kb_btn("🔄 Обновить", callback_data="pmrefresh:root"),
            kb_btn("⬆️" if has_prev else "⬇️", callback_data="pgpm:prev" if has_prev else "pgpm:next"),
        )
    else:
        kb.row(
            kb_btn("➕ Workspace", callback_data="pmhelp:root"),
            kb_btn("🔄 Обновить", callback_data="pmrefresh:root"),
        )
    return kb

def ws_settings_kb(wid: str):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("🧹 Очистить workspace", callback_data=f"wsclearask:{wid}"))
    kb.add(kb_btn("⬅️", callback_data=f"backws:{wid}"))
    return kb

def ws_home_kb(wid: str, ws: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    items = [(display_company_name(company), f"cmp:{wid}:{idx}") for idx, company in enumerate(ws.get("companies", []))]
    page = get_ui_page(ws, "ws_home")
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
        if has_next:
            row1 = [kb_btn("➕ Список", callback_data=f"cmpnew:{wid}")]
            if nav_prev_in_upper:
                row1.append(kb_btn("⬆️", callback_data=f"pg:{wid}:wh:x:x:prev"))
            kb.row(*row1)

            row2 = [kb_btn("📇 Шаблоны", callback_data=f"tplroot:{wid}")]
            row2.append(kb_btn("⬇️", callback_data=f"pg:{wid}:wh:x:x:next"))
            kb.row(*row2)
        elif has_prev:
            kb.row(kb_btn("➕ Список", callback_data=f"cmpnew:{wid}"))
            kb.row(
                kb_btn("📇 Шаблоны", callback_data=f"tplroot:{wid}"),
                kb_btn("⬆️", callback_data=f"pg:{wid}:wh:x:x:prev"),
            )
        else:
            kb.row(
                kb_btn("➕ Список", callback_data=f"cmpnew:{wid}"),
                kb_btn("📇 Шаблоны", callback_data=f"tplroot:{wid}"),
            )
    return kb

def company_create_mode_kb(wid: str, ws: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    templates = ws.get("templates", [])
    items = [(f"По шаблону {tpl.get('emoji') or '📁'}{tpl.get('title') or 'Шаблон'}", f"cmpmode:{wid}:tpl:{tpl['id']}") for tpl in templates]
    page = get_ui_page(ws, "cmp_create")
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
    for task_idx, task in enumerate(company.get("tasks", [])):
        if task.get("category_id"):
            continue
        icon = task_deadline_icon(task)
        items.append((f"{icon} {task['text']}", f"task:{wid}:{company_idx}:{task_idx}"))
    for category_idx, category in enumerate(company.get("categories", [])):
        items.append((display_category_name(category), f"cat:{wid}:{company_idx}:{category_idx}"))

    page = get_ui_page(company, f"cmp_{company_idx}")
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

def company_settings_kb(wid: str, company_idx: int, company: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("✍️ Переименовать список", callback_data=f"cmpren:{wid}:{company_idx}"))
    kb.add(kb_btn("😀 Переприсвоить смайлик", callback_data=f"cmpemoji:{wid}:{company_idx}"))
    format_label = "дата" if company.get("deadline_format") == "date" else "время"
    kb.add(kb_btn(f"🕒 Формат дедлайнов: {format_label}", callback_data=f"cmpdeadlinefmt:{wid}:{company_idx}"))
    kb.add(kb_btn("🧬 Копия списка", callback_data=f"cmpcopy:{wid}:{company_idx}"))
    kb.add(kb_btn("🧾 Отчетность", callback_data=f"reports:{wid}:{company_idx}", style="primary"))
    kb.add(kb_btn("📤 Дублирование списка", callback_data=f"mirrors:{wid}:{company_idx}", style="primary"))
    kb.add(kb_btn("🗑 Удалить список", callback_data=f"cmpdelask:{wid}:{company_idx}"))
    kb.add(kb_btn("⬅️", callback_data=f"cmp:{wid}:{company_idx}"))
    return kb

def category_menu_kb(wid: str, company_idx: int, category_idx: int, category: dict, company: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    task_buttons = []
    for task_idx, task in enumerate(company.get("tasks", [])):
        if task.get("category_id") == category.get("id"):
            icon = task_deadline_icon(task)
            task_buttons.append((f"{icon} {task['text']}", f"task:{wid}:{company_idx}:{task_idx}"))

    page = get_ui_page(company, f"cat_{company_idx}_{category_idx}")
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

def category_settings_kb(wid: str, company_idx: int, category_idx: int, category: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("✍️ Переименовать", callback_data=f"catren:{wid}:{company_idx}:{category_idx}"))
    kb.add(kb_btn("😀 Переприсвоить смайлик", callback_data=f"catemoji:{wid}:{company_idx}:{category_idx}"))
    kb.add(kb_btn("🧬 Копия подгруппы", callback_data=f"catcopy:{wid}:{company_idx}:{category_idx}"))
    format_label = "дата" if category.get("deadline_format") == "date" else "время"
    kb.add(kb_btn(f"🕒 Формат дедлайнов: {format_label}", callback_data=f"catdeadlinefmt:{wid}:{company_idx}:{category_idx}"))
    kb.add(kb_btn("🗑 Удалить", callback_data=f"catdelask:{wid}:{company_idx}:{category_idx}"))
    kb.add(kb_btn("🗑 Удалить с задачами", callback_data=f"catdelallask:{wid}:{company_idx}:{category_idx}"))
    kb.add(kb_btn("⬅️", callback_data=f"cat:{wid}:{company_idx}:{category_idx}"))
    return kb

def task_menu_kb(wid: str, company_idx: int, task_idx: int, task: dict, company: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    if task.get("done"):
        kb.add(kb_btn("⏺️ Отменить выполнение", callback_data=f"taskdone:{wid}:{company_idx}:{task_idx}"))
    else:
        kb.add(kb_btn("✅ Отметить выполненной", callback_data=f"taskdone:{wid}:{company_idx}:{task_idx}"))
    kb.add(kb_btn("✍️ Переименовать", callback_data=f"taskren:{wid}:{company_idx}:{task_idx}"))

    if company.get("categories"):
        if task.get("category_id"):
            kb.add(kb_btn("📥 Перевсунуть", callback_data=f"taskmove:{wid}:{company_idx}:{task_idx}"))
        else:
            kb.add(kb_btn("📥 Всунуть в подгруппу", callback_data=f"taskmove:{wid}:{company_idx}:{task_idx}"))

    if not task.get("done"):
        if task.get("deadline_due_at"):
            kb.add(kb_btn("⏰ Дедлайн", callback_data=f"taskdeadlinebox:{wid}:{company_idx}:{task_idx}", style="primary"))
        else:
            kb.add(kb_btn("⏰ Установить дедлайн", callback_data=f"taskdeadline:{wid}:{company_idx}:{task_idx}", style=False))

    kb.add(kb_btn("🗑 Удалить задачу", callback_data=f"taskdel:{wid}:{company_idx}:{task_idx}"))
    back = f"cat:{wid}:{company_idx}:{find_category_index(company.get('categories', []), task.get('category_id'))}" if task.get("category_id") and find_category_index(company.get('categories', []), task.get('category_id')) is not None else f"cmp:{wid}:{company_idx}"
    kb.add(kb_btn("⬅️", callback_data=back))
    return kb

def task_move_kb(wid: str, company_idx: int, task_idx: int, company: dict, task: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    current_category_id = task.get("category_id")
    items = []
    for category_idx, category in enumerate(company.get("categories", [])):
        if category.get("id") == current_category_id:
            continue
        items.append((display_category_name(category), f"taskmoveto:{wid}:{company_idx}:{task_idx}:{category_idx}"))
    page = get_ui_page(company, f"task_move_{company_idx}_{task_idx}")
    visible, has_prev, has_next = paginate_items(items, page, PAGE_SIZE_CATEGORY)
    for title, callback_data in visible:
        kb.add(kb_btn(title, callback_data=callback_data))
    if current_category_id:
        out_btn = kb_btn("📤 Высунуть", callback_data=f"taskmoveout:{wid}:{company_idx}:{task_idx}", style="primary")
        if has_prev and has_next:
            kb.row(out_btn, kb_btn("⬆️", callback_data=f"pg:{wid}:tmv:{company_idx}:{task_idx}:prev"))
            kb.row(kb_btn("⬅️", callback_data=f"task:{wid}:{company_idx}:{task_idx}", style="primary"), kb_btn("⬇️", callback_data=f"pg:{wid}:tmv:{company_idx}:{task_idx}:next"))
            return kb
        if has_prev:
            kb.row(out_btn, kb_btn("⬆️", callback_data=f"pg:{wid}:tmv:{company_idx}:{task_idx}:prev"))
            kb.row(kb_btn("⬅️", callback_data=f"task:{wid}:{company_idx}:{task_idx}", style="primary"))
            return kb
        kb.row(out_btn)
        row = [kb_btn("⬅️", callback_data=f"task:{wid}:{company_idx}:{task_idx}", style="primary")]
        if has_next:
            row.append(kb_btn("⬇️", callback_data=f"pg:{wid}:tmv:{company_idx}:{task_idx}:next"))
        kb.row(*row)
        return kb
    row = [kb_btn("⬅️", callback_data=f"task:{wid}:{company_idx}:{task_idx}", style="primary")]
    if has_next:
        row.append(kb_btn("⬇️", callback_data=f"pg:{wid}:tmv:{company_idx}:{task_idx}:next"))
    if has_prev:
        row.append(kb_btn("⬆️", callback_data=f"pg:{wid}:tmv:{company_idx}:{task_idx}:prev"))
    kb.row(*row)
    return kb

def templates_root_kb(wid: str, ws: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    items = [(f"{tpl.get('emoji') or '📁'}{tpl.get('title') or 'Шаблон'}", f"tplselect:{wid}:{tpl['id']}") for tpl in ws.get("templates", [])]
    page = get_ui_page(ws, "tpl_root")
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
    items = []
    for task_idx, task in enumerate(template.get("tasks", [])):
        if task.get("category_id"):
            continue
        items.append((f"{task_deadline_icon(task)} {str(task.get('text') or 'Без названия')}{template_task_deadline_suffix(task)}", f"tpltask:{wid}:{task_idx}"))
    for category_idx, category in enumerate(template.get("categories", [])):
        items.append((display_category_name(category), f"tplcat:{wid}:{category_idx}"))

    page = get_ui_page(ws, f"tpl_{template.get('id')}")
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
    kb.add(kb_btn("🧾 Отчетность", callback_data=f"tplreport:{wid}", style=False))
    kb.add(kb_btn("🗑 Удалить шаблон", callback_data=f"tpldelsetask:{wid}"))
    kb.add(kb_btn("⬅️", callback_data=f"tpl:{wid}"))
    return kb

def template_category_menu_kb(wid: str, category_idx: int, category: dict, template: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    task_buttons = []
    for task_idx, task in enumerate(template.get("tasks", [])):
        if task.get("category_id") == category.get("id"):
            task_buttons.append((f"{task_deadline_icon(task)} {str(task.get('text') or 'Без названия')}{template_task_deadline_suffix(task)}", f"tpltask:{wid}:{task_idx}"))

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
    kb.add(kb_btn("🗑 Удалить", callback_data=f"tplcatdelask:{wid}:{category_idx}"))
    kb.add(kb_btn("🗑 Удалить с задачами", callback_data=f"tplcatdelallask:{wid}:{category_idx}"))
    kb.add(kb_btn("⬅️", callback_data=f"tplcat:{wid}:{category_idx}"))
    return kb

def template_task_menu_kb(wid: str, task_idx: int, task: dict, ws: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("✍️ Переименовать", callback_data=f"tpltaskren:{wid}:{task_idx}"))
    if ws.get("template_categories"):
        if task.get("category_id"):
            kb.add(kb_btn("📥 Перевсунуть", callback_data=f"tpltaskmove:{wid}:{task_idx}"))
        else:
            kb.add(kb_btn("📥 Всунуть в подгруппу", callback_data=f"tpltaskmove:{wid}:{task_idx}"))
    if task.get("deadline_seconds"):
        kb.add(kb_btn("⏰ Дедлайн", callback_data=f"tpltaskdeadlinebox:{wid}:{task_idx}", style="primary"))
    else:
        kb.add(kb_btn("⏰ Установить дедлайн", callback_data=f"tpltaskdeadline:{wid}:{task_idx}", style=False))
    kb.add(kb_btn("🗑 Удалить", callback_data=f"tpltaskdel:{wid}:{task_idx}"))
    back = f"tplcat:{wid}:{find_category_index(ws.get('template_categories', []), task.get('category_id'))}" if task.get("category_id") and find_category_index(ws.get('template_categories', []), task.get('category_id')) is not None else f"tpl:{wid}"
    kb.add(kb_btn("⬅️", callback_data=back))
    return kb

def template_task_move_kb(wid: str, task_idx: int, ws: dict, task: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    current_category_id = task.get("category_id")
    items = []
    for category_idx, category in enumerate(ws.get("template_categories", [])):
        if category.get("id") == current_category_id:
            continue
        items.append((display_category_name(category), f"tpltaskmoveto:{wid}:{task_idx}:{category_idx}"))
    page = get_ui_page(ws, f"template_task_move_{task_idx}")
    visible, has_prev, has_next = paginate_items(items, page, PAGE_SIZE_CATEGORY)
    for title, callback_data in visible:
        kb.add(kb_btn(title, callback_data=callback_data))
    if current_category_id:
        out_btn = kb_btn("📤 Высунуть", callback_data=f"tpltaskmoveout:{wid}:{task_idx}", style="primary")
        if has_prev and has_next:
            kb.row(out_btn, kb_btn("⬆️", callback_data=f"pg:{wid}:ttmv:{task_idx}:x:prev"))
            kb.row(kb_btn("⬅️", callback_data=f"tpltask:{wid}:{task_idx}", style="primary"), kb_btn("⬇️", callback_data=f"pg:{wid}:ttmv:{task_idx}:x:next"))
            return kb
        if has_prev:
            kb.row(out_btn, kb_btn("⬆️", callback_data=f"pg:{wid}:ttmv:{task_idx}:x:prev"))
            kb.row(kb_btn("⬅️", callback_data=f"tpltask:{wid}:{task_idx}", style="primary"))
            return kb
        kb.row(out_btn)
        row = [kb_btn("⬅️", callback_data=f"tpltask:{wid}:{task_idx}", style="primary")]
        if has_next:
            row.append(kb_btn("⬇️", callback_data=f"pg:{wid}:ttmv:{task_idx}:x:next"))
        kb.row(*row)
        return kb
    row = [kb_btn("⬅️", callback_data=f"tpltask:{wid}:{task_idx}", style="primary")]
    if has_next:
        row.append(kb_btn("⬇️", callback_data=f"pg:{wid}:ttmv:{task_idx}:x:next"))
    if has_prev:
        row.append(kb_btn("⬆️", callback_data=f"pg:{wid}:ttmv:{task_idx}:x:prev"))
    kb.row(*row)
    return kb

def mirrors_menu_kb(wid: str, company_idx: int, company: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    items = []
    for idx, mirror in enumerate(company.get("mirrors", [])):
        label = mirror.get("label") or f"{mirror.get('chat_id')}/{mirror.get('thread_id') or 0}"
        items.append((label, f"mirroritem:{wid}:{company_idx}:{idx}"))

    page = get_ui_page(company, f"mirrors_{company_idx}")
    visible, has_prev, has_next = paginate_items(items, page, PAGE_SIZE_REPORT_BINDINGS)
    for title, callback_data in visible:
        kb.add(kb_btn(title, callback_data=callback_data, style=False))

    add_btn = kb_btn("➕ Связка", callback_data=f"mirroron:{wid}:{company_idx}", style="success")
    refresh_btn = kb_btn("🔄 Обновить", callback_data=f"mirrorsrefresh:{wid}:{company_idx}")
    back_btn = kb_btn("⬅️", callback_data=f"cmpset:{wid}:{company_idx}", style="primary")
    up_btn = kb_btn("⬆️", callback_data=f"pg:{wid}:mm:{company_idx}:x:prev")
    down_btn = kb_btn("⬇️", callback_data=f"pg:{wid}:mm:{company_idx}:x:next")

    if has_prev and has_next:
        kb.row(add_btn)
        kb.row(refresh_btn, up_btn)
        kb.row(back_btn, down_btn)
    elif has_prev:
        kb.row(add_btn, refresh_btn)
        kb.row(back_btn, up_btn)
    elif has_next:
        kb.row(add_btn, refresh_btn)
        kb.row(back_btn, down_btn)
    else:
        kb.row(add_btn, refresh_btn)
        kb.row(back_btn)
    return kb

def mirror_import_candidates_kb(wid: str, company_idx: int, company: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    items = []
    for source_idx, target in missing_report_targets_for_mirrors(company):
        label = target.get("label") or f"{target.get('chat_id')}/{target.get('thread_id') or 0}"
        items.append((label, f"mirrorcopy:{wid}:{company_idx}:{source_idx}"))
    page = get_ui_page(company, f"mirror_import_{company_idx}")
    visible, has_prev, has_next = paginate_items(items, page, PAGE_SIZE_REPORT_BINDINGS)
    for title, callback_data in visible:
        kb.add(kb_btn(title, callback_data=callback_data, style=False))
    kb.add(kb_btn("➕ Новая связка", callback_data=f"mirrornew:{wid}:{company_idx}", style="success"))
    row = [kb_btn("⬅️", callback_data=f"mirrors:{wid}:{company_idx}", style="primary")]
    if has_prev:
        row.append(kb_btn("⬆️", callback_data=f"pg:{wid}:mic:{company_idx}:x:prev"))
    if has_next:
        row.append(kb_btn("⬇️", callback_data=f"pg:{wid}:mic:{company_idx}:x:next"))
    kb.row(*row)
    return kb

def report_menu_kb(wid: str, company_idx: int, target_idx: int, company: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    pairs = get_target_report_pairs(company, target_idx)
    ordered = sorted(pairs, key=lambda pair: report_interval_sort_key(pair[1], pair[0]))
    items = [(format_report_schedule_label(interval), f"reportitem:{wid}:{company_idx}:{target_idx}:{idx}") for idx, interval in ordered]
    page = get_ui_page(company, f"report_{company_idx}_{target_idx}")
    visible, has_prev, has_next = paginate_items(items, page, PAGE_SIZE_REPORTS)

    for title, callback_data in visible:
        kb.add(kb_btn(title, callback_data=callback_data, style=False))

    kb.row(
        kb_btn("➕ Отчет", callback_data=f"reportadd:{wid}:{company_idx}:{target_idx}", style="success"),
        kb_btn("⚙️ Отчетность", callback_data=f"reportsettings:{wid}:{company_idx}:{target_idx}", style="primary"),
    )
    row = [kb_btn("⬅️", callback_data=f"reportbind:{wid}:{company_idx}", style="primary")]
    if has_prev:
        row.append(kb_btn("⬆️", callback_data=f"pg:{wid}:rp:{company_idx}:{target_idx}:prev"))
    if has_next:
        row.append(kb_btn("⬇️", callback_data=f"pg:{wid}:rp:{company_idx}:{target_idx}:next"))
    kb.row(*row)
    return kb

def report_interval_kb(wid: str, company_idx: int, target_idx: int, interval_idx: int, interval: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    if interval.get("kind") != "on_done":
        kb.add(kb_btn("Изменить время отчета", callback_data=f"reportedit:{wid}:{company_idx}:{target_idx}:{interval_idx}", style=False))
        kb.add(kb_btn("Изменить интервал накопления", callback_data=f"reportaccedit:{wid}:{company_idx}:{target_idx}:{interval_idx}", style=False))
    kb.add(kb_btn("🗑 Удалить", callback_data=f"reportdelask:{wid}:{company_idx}:{target_idx}:{interval_idx}", style="danger"))
    kb.add(kb_btn("⬅️", callback_data=f"reportmenu:{wid}:{company_idx}:{target_idx}", style="primary"))
    return kb

def report_interval_kind_kb(wid: str, company_idx: int, target_idx: int, flow: str, interval_idx: int | None):
    kb = InlineKeyboardMarkup(row_width=1)
    token = "x" if interval_idx is None else str(interval_idx)
    kb.row(
        kb_btn("Понедельник", callback_data=f"reportweek:{wid}:{company_idx}:{target_idx}:{token}:{flow}:0", style=False),
        kb_btn("Вторник", callback_data=f"reportweek:{wid}:{company_idx}:{target_idx}:{token}:{flow}:1", style=False),
    )
    kb.row(
        kb_btn("Среда", callback_data=f"reportweek:{wid}:{company_idx}:{target_idx}:{token}:{flow}:2", style=False),
        kb_btn("Четверг", callback_data=f"reportweek:{wid}:{company_idx}:{target_idx}:{token}:{flow}:3", style=False),
    )
    kb.row(
        kb_btn("Пятница", callback_data=f"reportweek:{wid}:{company_idx}:{target_idx}:{token}:{flow}:4", style=False),
        kb_btn("Суббота", callback_data=f"reportweek:{wid}:{company_idx}:{target_idx}:{token}:{flow}:5", style=False),
    )
    kb.add(kb_btn("Воскресение", callback_data=f"reportweek:{wid}:{company_idx}:{target_idx}:{token}:{flow}:6", style=False))
    kb.add(kb_btn("📆 Каждый день", callback_data=f"reportdaily:{wid}:{company_idx}:{target_idx}:{token}:{flow}", style=False))
    kb.add(kb_btn("🗓 Каждый месяц", callback_data=f"reportmonth:{wid}:{company_idx}:{target_idx}:{token}:{flow}", style=False))
    kb.add(kb_btn("📆 Один раз", callback_data=f"reportonce:{wid}:{company_idx}:{target_idx}:{token}:{flow}", style=False))
    kb.add(kb_btn("📆 Сразу после выполнения", callback_data=f"reportinstant:{wid}:{company_idx}:{target_idx}:{token}:{flow}", style=False))
    back_cb = f"reportitem:{wid}:{company_idx}:{target_idx}:{interval_idx}" if flow == "edit" and interval_idx is not None else f"reportmenu:{wid}:{company_idx}:{target_idx}"
    kb.add(kb_btn("⬅️", callback_data=back_cb, style="primary"))
    return kb

def report_accumulation_kb(wid: str, interval: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    if interval.get("kind") == "monthly":
        kb.add(kb_btn("Весь месяц", callback_data=f"reportacc:{wid}:month"))
        kb.add(kb_btn("От последнего отчета", callback_data=f"reportacc:{wid}:last"))
    elif interval.get("kind") in {"daily", "weekly"}:
        kb.add(kb_btn("От последнего отчета", callback_data=f"reportacc:{wid}:last"))
        kb.add(kb_btn("Всю неделю", callback_data=f"reportacc:{wid}:week"))
    else:
        kb.add(kb_btn("От последнего отчета", callback_data=f"reportacc:{wid}:last"))
    kb.add(kb_btn("От определенного дня", callback_data=f"reportacc:{wid}:specific"))
    kb.add(kb_btn("⬅️", callback_data=f"reportaccback:{wid}", style="primary"))
    return kb

def report_targets_kb(wid: str, company_idx: int, company: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    items = []
    for idx, target in enumerate(get_effective_report_targets(company)):
        label = target.get("label") or f"{target.get('chat_id')}/{target.get('thread_id') or 0}"
        items.append((label, f"reportmenu:{wid}:{company_idx}:{idx}"))

    page = get_ui_page(company, f"report_targets_{company_idx}")
    visible, has_prev, has_next = paginate_items(items, page, PAGE_SIZE_REPORT_BINDINGS)
    for title, callback_data in visible:
        kb.add(kb_btn(title, callback_data=callback_data, style=False))

    add_btn = kb_btn("➕ Связка", callback_data=f"reportbindon:{wid}:{company_idx}", style="success")
    refresh_btn = kb_btn("🔄 Обновить", callback_data=f"reportbindrefresh:{wid}:{company_idx}")
    back_btn = kb_btn("⬅️", callback_data=f"cmpset:{wid}:{company_idx}", style="primary")
    up_btn = kb_btn("⬆️", callback_data=f"pg:{wid}:rb:{company_idx}:x:prev")
    down_btn = kb_btn("⬇️", callback_data=f"pg:{wid}:rb:{company_idx}:x:next")

    if has_prev and has_next:
        kb.row(add_btn)
        kb.row(refresh_btn, up_btn)
        kb.row(back_btn, down_btn)
    elif has_prev:
        kb.row(add_btn, refresh_btn)
        kb.row(back_btn, up_btn)
    elif has_next:
        kb.row(add_btn, refresh_btn)
        kb.row(back_btn, down_btn)
    else:
        kb.row(add_btn, refresh_btn)
        kb.row(back_btn)
    return kb

def report_import_candidates_kb(wid: str, company_idx: int, company: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    items = []
    for source_idx, mirror in missing_mirrors_for_report_targets(company):
        label = mirror.get("label") or f"{mirror.get('chat_id')}/{mirror.get('thread_id') or 0}"
        items.append((label, f"reportbindcopy:{wid}:{company_idx}:{source_idx}"))
    page = get_ui_page(company, f"report_import_{company_idx}")
    visible, has_prev, has_next = paginate_items(items, page, PAGE_SIZE_REPORT_BINDINGS)
    for title, callback_data in visible:
        kb.add(kb_btn(title, callback_data=callback_data, style=False))
    kb.add(kb_btn("➕ Новая связка", callback_data=f"reportbindnew:{wid}:{company_idx}", style="success"))
    row = [kb_btn("⬅️", callback_data=f"reportbind:{wid}:{company_idx}", style="primary")]
    if has_prev:
        row.append(kb_btn("⬆️", callback_data=f"pg:{wid}:ric:{company_idx}:x:prev"))
    if has_next:
        row.append(kb_btn("⬇️", callback_data=f"pg:{wid}:ric:{company_idx}:x:next"))
    kb.row(*row)
    return kb

def template_report_menu_kb(wid: str, ws: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    template = get_active_template(ws)
    intervals = get_report_intervals(template)
    items = [
        (format_report_schedule_label(interval), f"tplreportitem:{wid}:{idx}")
        for idx, interval in sorted(enumerate(intervals), key=lambda pair: report_interval_sort_key(pair[1], pair[0]))
    ]
    page = get_ui_page(template, f"tpl_report_{template.get('id')}")
    visible, has_prev, has_next = paginate_items(items, page, PAGE_SIZE_REPORTS)

    for title, callback_data in visible:
        kb.add(kb_btn(title, callback_data=callback_data, style=False))

    kb.row(
        kb_btn("➕ Отчет", callback_data=f"tplreportadd:{wid}", style="success"),
        kb_btn("⚙️ Отчетность", callback_data=f"tplreportsettings:{wid}", style="primary"),
    )
    row = [kb_btn("⬅️", callback_data=f"tplsettings:{wid}", style="primary")]
    if has_prev:
        row.append(kb_btn("⬆️", callback_data=f"pg:{wid}:tpr:x:x:prev"))
    if has_next:
        row.append(kb_btn("⬇️", callback_data=f"pg:{wid}:tpr:x:x:next"))
    kb.row(*row)
    return kb

def template_report_interval_kb(wid: str, interval_idx: int, interval: dict | None = None):
    kb = InlineKeyboardMarkup(row_width=1)
    if interval and interval.get("kind") != "on_done":
        kb.add(kb_btn("Изменить время отчета", callback_data=f"tplreportedit:{wid}:{interval_idx}", style=False))
        kb.add(kb_btn("Изменить интервал накопления", callback_data=f"tplreportaccedit:{wid}:{interval_idx}", style=False))
    kb.add(kb_btn("🗑 Удалить", callback_data=f"tplreportdelask:{wid}:{interval_idx}", style="danger"))
    kb.add(kb_btn("⬅️", callback_data=f"tplreport:{wid}", style="primary"))
    return kb

def template_report_interval_kind_kb(wid: str, flow: str, interval_idx: int | None):
    kb = InlineKeyboardMarkup(row_width=1)
    token = "x" if interval_idx is None else str(interval_idx)
    kb.row(
        kb_btn("Понедельник", callback_data=f"tplreportweek:{wid}:{token}:{flow}:0", style=False),
        kb_btn("Вторник", callback_data=f"tplreportweek:{wid}:{token}:{flow}:1", style=False),
    )
    kb.row(
        kb_btn("Среда", callback_data=f"tplreportweek:{wid}:{token}:{flow}:2", style=False),
        kb_btn("Четверг", callback_data=f"tplreportweek:{wid}:{token}:{flow}:3", style=False),
    )
    kb.row(
        kb_btn("Пятница", callback_data=f"tplreportweek:{wid}:{token}:{flow}:4", style=False),
        kb_btn("Суббота", callback_data=f"tplreportweek:{wid}:{token}:{flow}:5", style=False),
    )
    kb.add(kb_btn("Воскресение", callback_data=f"tplreportweek:{wid}:{token}:{flow}:6", style=False))
    kb.add(kb_btn("📆 Каждый день", callback_data=f"tplreportdaily:{wid}:{token}:{flow}", style=False))
    kb.add(kb_btn("🗓 Каждый месяц", callback_data=f"tplreportmonth:{wid}:{token}:{flow}", style=False))
    kb.add(kb_btn("📆 Сразу после выполнения", callback_data=f"tplreportinstant:{wid}:{token}:{flow}", style=False))
    back_cb = f"tplreportitem:{wid}:{interval_idx}" if flow == "edit" and interval_idx is not None else f"tplreport:{wid}"
    kb.add(kb_btn("⬅️", callback_data=back_cb, style="primary"))
    return kb

def confirm_kb(confirm_cb: str, back_cb: str):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("Да!", callback_data=confirm_cb, style="danger"))
    kb.add(kb_btn("⬅️", callback_data=back_cb, style="primary"))
    return kb

# =========================
# VIEW HELPERS
# =========================

async def upsert_ws_menu(data: dict, wid: str, text: str, reply_markup):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return False

    MENU_LOCKS.setdefault(wid, asyncio.Lock())
    async with MENU_LOCKS[wid]:
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
        async with FILE_LOCK:
            fresh = await load_data_unlocked()
            fresh_ws = fresh.get("workspaces", {}).get(wid)
            if fresh_ws:
                fresh_ws["menu_msg_id"] = msg.message_id
                await save_data_unlocked(fresh)
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
        return False
    company = ws["companies"][company_idx]
    text = company_card_text(company)
    card_msg_id = company.get("card_msg_id")
    if card_msg_id:
        ok = await try_edit_text(ws["chat_id"], card_msg_id, text)
        if ok:
            return False
    msg = await send_message(ws["chat_id"], text, thread_id=ws["thread_id"])
    company["card_msg_id"] = msg.message_id
    return True

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

async def publish_initial_company_mirror(company: dict, chat_id: int, thread_id: int = 0) -> int | None:
    text = company_card_text(company)
    msg = await send_message(chat_id, text, thread_id=thread_id)
    return msg.message_id

async def sync_company_everywhere(ws: dict, company_idx: int):
    changed = False
    recreated_card = await upsert_company_card(ws, company_idx)
    changed = changed or recreated_card
    company = ws["companies"][company_idx]
    for mirror in company.get("mirrors", []):
        mirror_changed = await upsert_company_mirror(mirror, company)
        changed = changed or mirror_changed
    if recreated_card and ws.get("is_connected"):
        old_menu_id = ws.get("menu_msg_id")
        ws["menu_msg_id"] = None
        RUNTIME_MENU_IDS.pop(ws["id"], None)
        await safe_delete_message(ws["chat_id"], old_menu_id)
        msg = await send_message(ws["chat_id"], "📂 Меню workspace", reply_markup=ws_home_kb(ws["id"], ws), thread_id=ws["thread_id"])
        ws["menu_msg_id"] = msg.message_id
        RUNTIME_MENU_IDS[ws["id"]] = msg.message_id
        changed = True
    return changed

async def publish_company_reports(ws: dict, company_idx: int, now_value: int) -> bool:
    if company_idx < 0 or company_idx >= len(ws.get("companies", [])):
        return False

    company = ws["companies"][company_idx]
    normalize_company_report_target_keys(company)
    targets = get_effective_report_targets(company)
    changed = False

    for target in targets:
        target_key = report_target_key(target)
        intervals = [interval for interval in get_report_intervals(company) if interval.get("target_key") == target_key and interval.get("kind") != "on_done"]
        for interval in intervals:
            interval_key = (
                str(ws.get("id") or ""),
                str(company.get("id") or company_idx),
                str(interval.get("id") or ""),
            )
            runtime_occurrence = RUNTIME_REPORT_OCCURRENCES.get(interval_key)
            saved_occurrence = interval.get("last_report_at") if isinstance(interval.get("last_report_at"), int) else None
            if runtime_occurrence is not None and (saved_occurrence is None or runtime_occurrence > saved_occurrence):
                interval["last_report_at"] = runtime_occurrence
                saved_occurrence = runtime_occurrence
                changed = True

            if isinstance(interval.get("last_report_at"), int):
                anchor = interval["last_report_at"]
            else:
                anchor = ((interval.get("created_at") or now_value) - 1)
            occurrence = next_report_occurrence_after(interval, anchor)
            if occurrence is None or occurrence > now_value:
                continue
            if saved_occurrence is not None and occurrence <= saved_occurrence:
                continue
            if runtime_occurrence is not None and occurrence <= runtime_occurrence:
                continue

            start_at, end_at = resolve_report_period(interval, occurrence, company)
            text = build_report_message(company, start_at, end_at)

            try:
                await send_message(target["chat_id"], text, thread_id=target.get("thread_id") or 0)
            except Exception:
                continue

            interval["last_report_at"] = occurrence
            RUNTIME_REPORT_OCCURRENCES[interval_key] = occurrence
            changed = True

    return changed

async def publish_company_done_reports(ws: dict, company_idx: int, task_idx: int) -> bool:
    if company_idx < 0 or company_idx >= len(ws.get("companies", [])):
        return False
    company = ws["companies"][company_idx]
    if task_idx < 0 or task_idx >= len(company.get("tasks", [])):
        return False
    normalize_company_report_target_keys(company)
    task = company["tasks"][task_idx]
    if not task.get("done"):
        return False
    text = build_task_completion_report_message(company, task)
    now_value = now_ts()
    changed = False
    for target in get_effective_report_targets(company):
        target_key = report_target_key(target)
        target_intervals = [interval for interval in get_report_intervals(company) if interval.get("target_key") == target_key and interval.get("kind") == "on_done"]
        if not target_intervals:
            continue
        try:
            await send_message(target["chat_id"], text, thread_id=target.get("thread_id") or 0)
        except Exception:
            continue
        for interval in target_intervals:
            interval["last_report_at"] = now_value
            interval_key = (
                str(ws.get("id") or ""),
                str(company.get("id") or company_idx),
                str(interval.get("id") or ""),
            )
            RUNTIME_REPORT_OCCURRENCES[interval_key] = now_value
            changed = True
    return changed

async def upsert_ws_menu_inline(ws: dict, text: str, reply_markup):
    wid = ws["id"]
    current_id = RUNTIME_MENU_IDS.get(wid) or ws.get("menu_msg_id")
    if current_id:
        ws["menu_msg_id"] = current_id
        ok = await try_edit_text(ws["chat_id"], current_id, text, reply_markup=reply_markup)
        if ok:
            RUNTIME_MENU_IDS[wid] = current_id
            return False

    msg = await send_message(ws["chat_id"], text, reply_markup=reply_markup, thread_id=ws["thread_id"])
    ws["menu_msg_id"] = msg.message_id
    RUNTIME_MENU_IDS[wid] = msg.message_id
    return True

async def set_prompt(ws: dict, prompt_text: str, awaiting_payload: dict):
    awaiting = ws.get("awaiting") or {}
    prompt_msg_id = awaiting.get("prompt_msg_id")
    current_menu_id = RUNTIME_MENU_IDS.get(ws.get("id")) or ws.get("menu_msg_id")
    if prompt_msg_id and prompt_msg_id != current_menu_id:
        await safe_delete_message(ws["chat_id"], prompt_msg_id)
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("⬅️", callback_data=f"cancel:{ws['id']}"))
    await upsert_ws_menu_inline(ws, prompt_text, kb)
    awaiting_payload["prompt_msg_id"] = None
    ws["awaiting"] = awaiting_payload

async def show_instruction_menu(data: dict, wid: str, text: str, back_cb: str):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("⬅️", callback_data=back_cb, style="primary"))
    await upsert_ws_menu(data, wid, text, kb)

def get_connected_ws(data: dict, wid: str) -> dict | None:
    ws = data["workspaces"].get(wid)
    return ws if ws and ws.get("is_connected") else None

async def get_connected_company(data: dict, wid: str, company_idx: int) -> tuple[dict | None, dict | None]:
    ws = get_connected_ws(data, wid)
    if not ws:
        return None, None
    companies = ws.get("companies", [])
    if company_idx < 0 or company_idx >= len(companies):
        await edit_ws_home_menu(data, wid)
        return None, None
    return ws, companies[company_idx]

async def get_report_target_context(data: dict, wid: str, company_idx: int, target_idx: int) -> tuple[dict | None, dict | None, dict | None]:
    ws, company = await get_connected_company(data, wid, company_idx)
    if not company:
        return None, None, None
    target = get_report_target(company, target_idx)
    if not target:
        await edit_report_targets_menu(data, wid, company_idx)
        return None, None, None
    return ws, company, target

def get_connected_active_template(data: dict, wid: str) -> tuple[dict | None, dict | None]:
    ws = get_connected_ws(data, wid)
    return (ws, get_active_template(ws)) if ws else (None, None)

def resolve_task_category(categories: list[dict], task: dict) -> dict | None:
    category_id = task.get("category_id")
    if not category_id:
        return None
    category_idx = find_category_index(categories, category_id)
    return categories[category_idx] if category_idx is not None else None

async def get_company_category_context(data: dict, wid: str, company_idx: int, category_idx: int) -> tuple[dict | None, dict | None, dict | None]:
    ws, company = await get_connected_company(data, wid, company_idx)
    if not company:
        return None, None, None
    categories = company.get("categories", [])
    if category_idx < 0 or category_idx >= len(categories):
        await edit_company_menu(data, wid, company_idx)
        return None, None, None
    return ws, company, categories[category_idx]

async def get_company_task_context(data: dict, wid: str, company_idx: int, task_idx: int) -> tuple[dict | None, dict | None, dict | None, dict | None]:
    ws, company = await get_connected_company(data, wid, company_idx)
    if not company:
        return None, None, None, None
    tasks = company.get("tasks", [])
    if task_idx < 0 or task_idx >= len(tasks):
        await edit_company_menu(data, wid, company_idx)
        return None, None, None, None
    task = tasks[task_idx]
    return ws, company, task, resolve_task_category(company.get("categories", []), task)

async def get_template_category_context(data: dict, wid: str, category_idx: int) -> tuple[dict | None, dict | None, dict | None]:
    ws, active = get_connected_active_template(data, wid)
    if not ws:
        return None, None, None
    categories = ws.get("template_categories", [])
    if category_idx < 0 or category_idx >= len(categories):
        await edit_template_menu(data, wid)
        return None, None, None
    return ws, active, categories[category_idx]

async def get_template_task_context(data: dict, wid: str, task_idx: int) -> tuple[dict | None, dict | None, dict | None, dict | None]:
    ws, active = get_connected_active_template(data, wid)
    if not ws:
        return None, None, None, None
    tasks = ws.get("template_tasks", [])
    if task_idx < 0 or task_idx >= len(tasks):
        await edit_template_menu(data, wid)
        return None, None, None, None
    task = tasks[task_idx]
    return ws, active, task, resolve_task_category(ws.get("template_categories", []), task)

async def begin_callback(cb: types.CallbackQuery) -> bool:
    await cb.answer()
    return not should_ignore_callback(cb)

async def open_wid_menu_from_callback(cb: types.CallbackQuery, editor):
    if not await begin_callback(cb):
        return
    wid = cb.data.split(":", 1)[1]
    data = await load_data()
    await editor(data, wid)

async def open_company_menu_from_callback(cb: types.CallbackQuery, editor):
    if not await begin_callback(cb):
        return
    _, wid, company_idx = cb.data.split(":")
    data = await load_data()
    await editor(data, wid, int(company_idx))

async def open_company_target_menu_from_callback(cb: types.CallbackQuery, editor):
    if not await begin_callback(cb):
        return
    _, wid, company_idx, target_idx = cb.data.split(":")
    data = await load_data()
    await editor(data, wid, int(company_idx), int(target_idx))

async def open_company_target_index_menu_from_callback(cb: types.CallbackQuery, editor):
    if not await begin_callback(cb):
        return
    _, wid, company_idx, target_idx, item_idx = cb.data.split(":")
    data = await load_data()
    await editor(data, wid, int(company_idx), int(target_idx), int(item_idx))

async def open_company_category_menu_from_callback(cb: types.CallbackQuery, editor):
    if not await begin_callback(cb):
        return
    _, wid, company_idx, category_idx = cb.data.split(":")
    data = await load_data()
    await editor(data, wid, int(company_idx), int(category_idx))

async def open_company_task_menu_from_callback(cb: types.CallbackQuery, editor):
    if not await begin_callback(cb):
        return
    _, wid, company_idx, task_idx = cb.data.split(":")
    data = await load_data()
    await editor(data, wid, int(company_idx), int(task_idx))

async def open_template_index_menu_from_callback(cb: types.CallbackQuery, editor):
    if not await begin_callback(cb):
        return
    _, wid, item_idx = cb.data.split(":")
    data = await load_data()
    await editor(data, wid, int(item_idx))

async def open_wid_prompt_from_callback(cb: types.CallbackQuery, prompt_text: str, awaiting_payload: dict):
    if not await begin_callback(cb):
        return
    wid = cb.data.split(":", 1)[1]
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = get_connected_ws(data, wid)
        if not ws:
            return
        await set_prompt(ws, prompt_text, awaiting_payload)
        await save_data_unlocked(data)

async def open_company_prompt_from_callback(cb: types.CallbackQuery, prompt_text: str, payload_factory):
    if not await begin_callback(cb):
        return
    _, wid, company_idx = cb.data.split(":")
    company_idx = int(company_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = get_connected_ws(data, wid)
        if not ws:
            return
        await set_prompt(ws, prompt_text, payload_factory(company_idx))
        await save_data_unlocked(data)

async def open_company_category_prompt_from_callback(cb: types.CallbackQuery, prompt_text: str, payload_factory):
    if not await begin_callback(cb):
        return
    _, wid, company_idx, category_idx = cb.data.split(":")
    company_idx, category_idx = int(company_idx), int(category_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = get_connected_ws(data, wid)
        if not ws:
            return
        await set_prompt(ws, prompt_text, payload_factory(company_idx, category_idx))
        await save_data_unlocked(data)

async def open_company_task_prompt_from_callback(cb: types.CallbackQuery, prompt_text: str, payload_factory):
    if not await begin_callback(cb):
        return
    _, wid, company_idx, task_idx = cb.data.split(":")
    company_idx, task_idx = int(company_idx), int(task_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = get_connected_ws(data, wid)
        if not ws:
            return
        await set_prompt(ws, prompt_text, payload_factory(ws, company_idx, task_idx))
        await save_data_unlocked(data)

async def open_template_category_prompt_from_callback(cb: types.CallbackQuery, prompt_text: str, payload_factory):
    if not await begin_callback(cb):
        return
    _, wid, category_idx = cb.data.split(":")
    category_idx = int(category_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = get_connected_ws(data, wid)
        if not ws:
            return
        await set_prompt(ws, prompt_text, payload_factory(category_idx))
        await save_data_unlocked(data)

async def open_template_task_prompt_from_callback(cb: types.CallbackQuery, prompt_text: str, payload_factory):
    if not await begin_callback(cb):
        return
    _, wid, task_idx = cb.data.split(":")
    task_idx = int(task_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = get_connected_ws(data, wid)
        if not ws:
            return
        await set_prompt(ws, prompt_text, payload_factory(ws, task_idx))
        await save_data_unlocked(data)

async def open_report_schedule_prompt_from_callback(cb: types.CallbackQuery, kind: str):
    if not await begin_callback(cb):
        return
    _, wid, company_idx, target_idx, interval_idx, flow, *rest = cb.data.split(":")
    await open_report_schedule_prompt(
        wid,
        int(company_idx),
        int(target_idx),
        parse_optional_index(interval_idx),
        flow,
        kind,
        int(rest[0]) if rest else None,
    )

async def recreate_ws_home_menu(data: dict, wid: str):
    ws = get_connected_ws(data, wid)
    if not ws:
        return
    old_id = ws.get("menu_msg_id")
    ws["menu_msg_id"] = None
    RUNTIME_MENU_IDS.pop(wid, None)
    await safe_delete_message(ws["chat_id"], old_id)
    await upsert_ws_menu(data, wid, "📂 Меню workspace", ws_home_kb(wid, ws))

async def edit_ws_home_menu(data: dict, wid: str):
    ws = get_connected_ws(data, wid)
    if not ws:
        return
    await upsert_ws_menu(data, wid, "📂 Меню workspace", ws_home_kb(wid, ws))

async def edit_ws_settings_menu(data: dict, wid: str):
    ws = get_connected_ws(data, wid)
    if not ws:
        return
    await upsert_ws_menu(data, wid, workspace_path_title(ws, "⚙️ Настройки Workspace"), ws_settings_kb(wid))

async def edit_company_create_menu(data: dict, wid: str):
    ws = get_connected_ws(data, wid)
    if not ws:
        return
    await upsert_ws_menu(data, wid, "➕ Список", company_create_mode_kb(wid, ws))

async def edit_company_menu(data: dict, wid: str, company_idx: int):
    ws, company = await get_connected_company(data, wid, company_idx)
    if not ws:
        return
    await upsert_ws_menu(data, wid, workspace_path_title(ws, rich_display_company_name(company)), company_menu_kb(wid, company_idx, company))

async def edit_company_settings_menu(data: dict, wid: str, company_idx: int):
    ws, company = await get_connected_company(data, wid, company_idx)
    if not ws:
        return
    await upsert_ws_menu(data, wid, workspace_path_title(ws, rich_display_company_name(company), "⚙️ Настройки списка"), company_settings_kb(wid, company_idx, company))

async def edit_report_menu(data: dict, wid: str, company_idx: int, target_idx: int):
    ws, company, target = await get_report_target_context(data, wid, company_idx, target_idx)
    if not ws:
        return
    title = [rich_display_company_name(company), "🧾 Отчетность"]
    if target:
        title.append(target.get("label") or f"{target.get('chat_id')}/{target.get('thread_id') or 0}")
    await upsert_ws_menu(data, wid, workspace_path_title(ws, *title), report_menu_kb(wid, company_idx, target_idx, company))

async def edit_report_settings_menu(data: dict, wid: str, company_idx: int, target_idx: int):
    ws, company, target = await get_report_target_context(data, wid, company_idx, target_idx)
    if not ws:
        return
    title = [rich_display_company_name(company), "🧾 Отчетность"]
    if target:
        title.append(target.get("label") or f"{target.get('chat_id')}/{target.get('thread_id') or 0}")
    title.append("⚙️ Отчетность")
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("🔌 Отвязать", callback_data=f"reportbindoff:{wid}:{company_idx}:{target_idx}", style="danger"))
    kb.add(kb_btn("🧹 Очистить график", callback_data=f"reportclearask:{wid}:{company_idx}:{target_idx}", style="danger"))
    kb.add(kb_btn("⬅️", callback_data=f"reportmenu:{wid}:{company_idx}:{target_idx}", style="primary"))
    await upsert_ws_menu(data, wid, workspace_path_title(ws, *title), kb)

async def edit_report_interval_menu(data: dict, wid: str, company_idx: int, target_idx: int, interval_idx: int):
    ws, company, target = await get_report_target_context(data, wid, company_idx, target_idx)
    if not ws:
        return
    interval = find_report_interval(company, interval_idx)
    if not interval or interval.get("target_key") != report_target_key(target):
        await edit_report_menu(data, wid, company_idx, target_idx)
        return
    title = [rich_display_company_name(company), "🧾 Отчетность"]
    if target:
        title.append(target.get("label") or f"{target.get('chat_id')}/{target.get('thread_id') or 0}")
    title.append(format_report_schedule_label(interval))
    if interval.get("kind") != "on_done":
        start_at, end_at = resolve_report_period(interval, report_preview_occurrence(interval), company)
        title.append(format_report_period_preview(interval, start_at, end_at))
    await upsert_ws_menu(data, wid, workspace_path_title(ws, *title), report_interval_kb(wid, company_idx, target_idx, interval_idx, interval))

async def edit_report_interval_kind_menu(data: dict, wid: str, company_idx: int, target_idx: int, flow: str, interval_idx: int | None):
    ws, company, target = await get_report_target_context(data, wid, company_idx, target_idx)
    if not ws:
        return
    label = "Изменить время отчета" if flow == "edit" and interval_idx is not None else "Добавить время отчета"
    await upsert_ws_menu(
        data,
        wid,
        workspace_path_title(ws, rich_display_company_name(company), "🧾 Отчетность", target.get("label") or f"{target.get('chat_id')}/{target.get('thread_id') or 0}", label),
        report_interval_kind_kb(wid, company_idx, target_idx, flow, interval_idx),
    )

async def edit_report_accumulation_menu(data: dict, wid: str):
    ws = get_connected_ws(data, wid)
    if not ws:
        return
    awaiting = ws.get("awaiting") or {}
    draft_interval = awaiting.get("draft_interval")
    if not isinstance(draft_interval, dict):
        await edit_ws_home_menu(data, wid)
        return
    if awaiting.get("type", "").startswith("template_report"):
        active = get_active_template(ws)
        title = workspace_path_title(
            ws,
            "⚙️ Шаблоны задач",
            rich_display_template_name(active),
            "🧾 Отчетность",
            format_report_schedule_label(draft_interval),
            "Копить задачи:",
        )
    else:
        company_idx = awaiting.get("company_idx")
        if company_idx is None or company_idx < 0 or company_idx >= len(ws.get("companies", [])):
            await edit_ws_home_menu(data, wid)
            return
        company = ws["companies"][company_idx]
        title = workspace_path_title(
            ws,
            rich_display_company_name(company),
            "🧾 Отчетность",
            format_report_schedule_label(draft_interval),
            "Копить задачи:",
        )
    await upsert_ws_menu(data, wid, title, report_accumulation_kb(wid, draft_interval))

async def edit_report_targets_menu(data: dict, wid: str, company_idx: int):
    ws, company = await get_connected_company(data, wid, company_idx)
    if not ws:
        return
    await upsert_ws_menu(data, wid, workspace_path_title(ws, rich_display_company_name(company), "🧾 Отчетность", "📎 Привязка"), report_targets_kb(wid, company_idx, company))

async def edit_category_menu(data: dict, wid: str, company_idx: int, category_idx: int):
    ws, company, category = await get_company_category_context(data, wid, company_idx, category_idx)
    if not ws:
        return
    await upsert_ws_menu(data, wid, workspace_path_title(ws, rich_display_company_name(company), rich_display_category_name(category)), category_menu_kb(wid, company_idx, category_idx, category, company))

async def edit_category_settings_menu(data: dict, wid: str, company_idx: int, category_idx: int):
    ws, company, category = await get_company_category_context(data, wid, company_idx, category_idx)
    if not ws:
        return
    await upsert_ws_menu(data, wid, workspace_path_title(ws, rich_display_company_name(company), rich_display_category_name(category), "⚙️ Подгруппа"), category_settings_kb(wid, company_idx, category_idx, category))

async def edit_task_menu(data: dict, wid: str, company_idx: int, task_idx: int):
    ws, company, task, category = await get_company_task_context(data, wid, company_idx, task_idx)
    if not ws:
        return
    await upsert_ws_menu(data, wid, task_menu_title(ws, company, task, category), task_menu_kb(wid, company_idx, task_idx, task, company))

async def edit_task_deadline_menu(data: dict, wid: str, company_idx: int, task_idx: int):
    ws, company, task, category = await get_company_task_context(data, wid, company_idx, task_idx)
    if not ws:
        return
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("⏰ Поменять дедлайн", callback_data=f"taskdeadline:{wid}:{company_idx}:{task_idx}", style=False))
    kb.add(kb_btn("🗑 Удалить дедлайн", callback_data=f"taskdeadel:{wid}:{company_idx}:{task_idx}", style="danger"))
    kb.add(kb_btn("⬅️", callback_data=f"task:{wid}:{company_idx}:{task_idx}", style="primary"))
    await upsert_ws_menu(data, wid, task_menu_title(ws, company, task, category), kb)

async def edit_task_move_menu(data: dict, wid: str, company_idx: int, task_idx: int):
    ws, company, task, _ = await get_company_task_context(data, wid, company_idx, task_idx)
    if not ws:
        return
    await upsert_ws_menu(data, wid, f"📥 {task['text']}", task_move_kb(wid, company_idx, task_idx, company, task))

async def edit_templates_root_menu(data: dict, wid: str):
    ws = get_connected_ws(data, wid)
    if not ws:
        return
    await upsert_ws_menu(data, wid, workspace_path_title(ws, "⚙️ Шаблоны задач"), templates_root_kb(wid, ws))

async def edit_template_menu(data: dict, wid: str):
    ws, active = get_connected_active_template(data, wid)
    if not ws:
        return
    await upsert_ws_menu(data, wid, workspace_path_title(ws, "⚙️ Шаблоны задач", rich_display_template_name(active)), template_menu_kb(wid, ws))

async def edit_template_category_menu(data: dict, wid: str, category_idx: int):
    ws, active, category = await get_template_category_context(data, wid, category_idx)
    if not ws:
        return
    await upsert_ws_menu(data, wid, workspace_path_title(ws, "⚙️ Шаблоны задач", rich_display_template_name(active), rich_display_category_name(category)), template_category_menu_kb(wid, category_idx, category, active))

async def edit_template_category_settings_menu(data: dict, wid: str, category_idx: int):
    ws, active, category = await get_template_category_context(data, wid, category_idx)
    if not ws:
        return
    await upsert_ws_menu(data, wid, workspace_path_title(ws, "⚙️ Шаблоны задач", rich_display_template_name(active), rich_display_category_name(category), "⚙️ Подгруппа"), template_category_settings_kb(wid, category_idx))

async def edit_template_settings_menu(data: dict, wid: str):
    ws, active = get_connected_active_template(data, wid)
    if not ws:
        return
    await upsert_ws_menu(data, wid, workspace_path_title(ws, "⚙️ Шаблоны задач", rich_display_template_name(active), "⚙️ Шаблон"), template_settings_kb(wid))

async def edit_template_report_menu(data: dict, wid: str):
    ws, active = get_connected_active_template(data, wid)
    if not ws:
        return
    await upsert_ws_menu(data, wid, workspace_path_title(ws, "⚙️ Шаблоны задач", rich_display_template_name(active), "🧾 Отчетность"), template_report_menu_kb(wid, ws))

async def edit_template_report_settings_menu(data: dict, wid: str):
    ws, active = get_connected_active_template(data, wid)
    if not ws:
        return
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("🧹 Очистить график", callback_data=f"tplreportclearask:{wid}", style="danger"))
    kb.add(kb_btn("⬅️", callback_data=f"tplreport:{wid}", style="primary"))
    await upsert_ws_menu(data, wid, workspace_path_title(ws, "⚙️ Шаблоны задач", rich_display_template_name(active), "🧾 Отчетность", "⚙️ Отчетность"), kb)

async def edit_template_report_interval_menu(data: dict, wid: str, interval_idx: int):
    ws, active = get_connected_active_template(data, wid)
    if not ws:
        return
    interval = find_report_interval(active, interval_idx)
    if not interval:
        await edit_template_report_menu(data, wid)
        return
    title = [
        "⚙️ Шаблоны задач",
        rich_display_template_name(active),
        "🧾 Отчетность",
        format_report_schedule_label(interval),
    ]
    if interval.get("kind") != "on_done":
        start_at, end_at = resolve_report_period(interval, report_preview_occurrence(interval))
        title.append(format_report_period_preview(interval, start_at, end_at))
    await upsert_ws_menu(data, wid, workspace_path_title(ws, *title), template_report_interval_kb(wid, interval_idx, interval))

async def edit_template_report_interval_kind_menu(data: dict, wid: str, flow: str, interval_idx: int | None):
    ws, active = get_connected_active_template(data, wid)
    if not ws:
        return
    label = "Изменить время отчета" if flow == "edit" and interval_idx is not None else "Добавить время отчета"
    await upsert_ws_menu(
        data,
        wid,
        workspace_path_title(ws, "⚙️ Шаблоны задач", rich_display_template_name(active), "🧾 Отчетность", label),
        template_report_interval_kind_kb(wid, flow, interval_idx),
    )

async def edit_template_task_menu(data: dict, wid: str, task_idx: int):
    ws, active, task, category = await get_template_task_context(data, wid, task_idx)
    if not ws:
        return
    await upsert_ws_menu(data, wid, template_task_title(ws, active, task, category), template_task_menu_kb(wid, task_idx, task, ws))

async def edit_template_task_deadline_menu(data: dict, wid: str, task_idx: int):
    ws, active, task, category = await get_template_task_context(data, wid, task_idx)
    if not ws:
        return
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("⏰ Поменять дедлайн", callback_data=f"tpltaskdeadline:{wid}:{task_idx}", style=False))
    kb.add(kb_btn("🗑 Удалить дедлайн", callback_data=f"tpltaskdeadel:{wid}:{task_idx}", style="danger"))
    kb.add(kb_btn("⬅️", callback_data=f"tpltask:{wid}:{task_idx}", style="primary"))
    await upsert_ws_menu(data, wid, template_task_title(ws, active, task, category), kb)

async def edit_template_task_move_menu(data: dict, wid: str, task_idx: int):
    ws, _, task, _ = await get_template_task_context(data, wid, task_idx)
    if not ws:
        return
    await upsert_ws_menu(data, wid, f"📥 {task['text']}", template_task_move_kb(wid, task_idx, ws, task))

async def clear_workspace_contents(ws: dict):
    awaiting = ws.get("awaiting") or {}
    prompt_msg_id = awaiting.get("prompt_msg_id")
    if prompt_msg_id and prompt_msg_id != ws.get("menu_msg_id"):
        await safe_delete_message(ws["chat_id"], prompt_msg_id)

    for company in ws.get("companies", []):
        await safe_delete_message(ws["chat_id"], company.get("card_msg_id"))
        for mirror in company.get("mirrors", []):
            await safe_delete_message(mirror.get("chat_id"), mirror.get("message_id"))

    ws["companies"] = []
    ws["awaiting"] = None

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
    data = await load_data()
    uid = str(cb.from_user.id)
    await safe_edit_text(
        int(uid),
        cb.message.message_id,
        "📌 Как подключить workspace:\n1) Добавь меня в нужную группу;\n2) Перейди в нужный тред;\n3) Отправь команду /connect;\n4) Дождись появления меню;\n5) Profit!",
        reply_markup=InlineKeyboardMarkup(row_width=1).add(kb_btn("⬅️", callback_data="pmrefresh:root", style="primary")),
    )

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
                "templates": [make_template(tasks=default_template_tasks())],
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
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("🧹 Очистить workspace", callback_data=f"pmwsclearask:{wid}"))
    kb.add(kb_btn("🗑 Удалить workspace", callback_data=f"pmwsdelask:{wid}"))
    kb.add(kb_btn("⬅️", callback_data="pmrefresh:root"))
    await safe_edit_text(int(uid), cb.message.message_id, f"📂 {esc(ws.get('name') or 'Workspace')}", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith("wsset:"))
async def open_ws_settings(cb: types.CallbackQuery):
    await open_wid_menu_from_callback(cb, edit_ws_settings_menu)

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
    await upsert_ws_menu(data, wid, workspace_path_title(ws, "⚙️ Настройки Workspace", "🧹 Очистить workspace?"), confirm_kb(f"wsclear:{wid}", f"wsset:{wid}"))

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
            asyncio.create_task(try_delete_user_message(message))
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
        await clear_workspace_contents(ws)
        clear_pending_mirror_tokens_for_workspace(data, wid)
        ensure_user(data, uid)["pm_menu_msg_id"] = cb.message.message_id
        await save_data_unlocked(data)
    fresh = await load_data()
    ws = fresh["workspaces"].get(wid)
    if ws and ws.get("is_connected"):
        await edit_ws_home_menu(fresh, wid)
    await safe_edit_text(int(uid), cb.message.message_id, pm_main_text(uid, fresh), reply_markup=pm_main_kb(uid, fresh))

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
    if prompt_msg_id and prompt_msg_id != menu_msg_id:
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
    chat_title = message.chat.title or "Workspace"
    topic_title = extract_topic_title(message)
    old_menu_id = None
    old_prompt_id = None
    old_company_card_ids = []

    async with FILE_LOCK:
        data = await load_data_unlocked()
        ensure_user(data, uid)
        existing_ws = data["workspaces"].get(wid)
        if not topic_title and existing_ws:
            topic_title = existing_ws.get("topic_title")
        chat_title, topic_title = remember_binding_place(data, message.chat.id, thread_id, chat_title, topic_title)
        ws_name = refresh_binding_labels(data, message.chat.id, thread_id)
        if existing_ws and existing_ws.get("is_connected"):
            existing_ws["chat_title"] = chat_title
            existing_ws["topic_title"] = topic_title
            existing_ws["name"] = ws_name
            await save_data_unlocked(data)
            asyncio.create_task(send_temp_message(message.chat.id, f"Workspace «{existing_ws.get('name') or 'Workspace'}» уже подключён", thread_id, delay=10))
            asyncio.create_task(try_delete_user_message(message))
            return

        old_companies = existing_ws["companies"] if existing_ws else []
        old_templates = existing_ws.get("templates") if existing_ws else [make_template(tasks=default_template_tasks())]
        old_active_template_id = existing_ws.get("active_template_id") if existing_ws else None

        if existing_ws:
            old_menu_id = existing_ws.get("menu_msg_id")
            old_prompt_id = (existing_ws.get("awaiting") or {}).get("prompt_msg_id")
            old_company_card_ids = [company.get("card_msg_id") for company in old_companies if company.get("card_msg_id")]
            for company in old_companies:
                company["card_msg_id"] = None

        data["workspaces"][wid] = {
            "id": wid,
            "name": ws_name,
            "chat_title": chat_title,
            "topic_title": topic_title,
            "chat_id": message.chat.id,
            "thread_id": thread_id,
            "menu_msg_id": existing_ws.get("menu_msg_id") if existing_ws else None,
            "templates": old_templates,
            "active_template_id": old_active_template_id,
            "companies": old_companies,
            "awaiting": None,
            "is_connected": True,
        }
        ws = data["workspaces"][wid]
        normalize_template(ws)
        if wid not in data["users"][uid]["workspaces"]:
            data["users"][uid]["workspaces"].append(wid)
        await save_data_unlocked(data)

    await safe_delete_message(message.chat.id, old_menu_id)
    await safe_delete_message(message.chat.id, old_prompt_id)
    for card_msg_id in old_company_card_ids:
        await safe_delete_message(message.chat.id, card_msg_id)

    fresh = await load_data()
    ws = fresh["workspaces"].get(wid)
    if not ws:
        return

    for idx in range(len(ws.get("companies", []))):
        await upsert_company_card(ws, idx)
    for company in ws.get("companies", []):
        for mirror in company.get("mirrors", []):
            await upsert_company_mirror(mirror, company)
    await edit_ws_home_menu(fresh, wid)
    await update_pm_menu(uid, fresh)
    await save_data(fresh)
    await try_delete_user_message(message)
    try:
        await send_temp_message(int(uid), f"Workspace «{ws['name']}» подключён", delay=10)
    except Exception:
        pass

@dp.message_handler(lambda message: bool(getattr(message, "forum_topic_created", None) or getattr(message, "forum_topic_edited", None)), content_types=types.ContentTypes.ANY)
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
        remember_binding_place(data, message.chat.id, thread_id, message.chat.title or "Workspace", topic_title)
        refresh_binding_labels(data, message.chat.id, thread_id)
        await save_data_unlocked(data)

    wid = make_ws_id(message.chat.id, thread_id)
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
    await upsert_ws_menu(data, wid, workspace_path_title(ws, rich_display_company_name(company), "📤 Дублирование списка"), mirrors_menu_kb(wid, company_idx, company))

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
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("🔌 Отвязать список", callback_data=f"mirroroff:{wid}:{company_idx}:{mirror_idx}"))
    kb.add(kb_btn("⬅️", callback_data=f"mirrors:{wid}:{company_idx}", style="primary"))
    await upsert_ws_menu(data, wid, workspace_path_title(ws, display_company_name(company), "📤 Дублирование списка", label), kb)

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
    await upsert_ws_menu(data, wid, workspace_path_title(ws, rich_display_company_name(company), "📤 Дублирование списка"), mirrors_menu_kb(wid, company_idx, company))

@dp.callback_query_handler(lambda c: c.data.startswith("mirroron:"))
async def mirror_on(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx = cb.data.split(":")
    company_idx = int(company_idx)
    show_import_menu = False

    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected") or company_idx < 0 or company_idx >= len(ws.get("companies", [])):
            return
        company = ws["companies"][company_idx]
        if missing_report_targets_for_mirrors(company):
            show_import_menu = True
            await save_data_unlocked(data)
        else:
            company_id = company["id"]
            token = generate_mirror_token()
            data["mirror_tokens"][token] = {
                "source_wid": wid,
                "company_id": company_id,
                "source_thread_id": ws["thread_id"],
            }
            await save_data_unlocked(data)

    fresh = await load_data()
    ws2 = fresh["workspaces"].get(wid)
    if not ws2 or not (0 <= company_idx < len(ws2.get("companies", []))):
        return
    company2 = ws2["companies"][company_idx]
    if show_import_menu:
        await upsert_ws_menu(
            fresh,
            wid,
            workspace_path_title(ws2, rich_display_company_name(company2), "📤 Дублирование списка", "➕ Добавить связку"),
            mirror_import_candidates_kb(wid, company_idx, company2),
        )
        return
    await show_instruction_menu(
        fresh,
        wid,
        binding_instruction_text("📤 Чтобы добавить связку", token),
        f"mirrors:{wid}:{company_idx}",
    )

@dp.callback_query_handler(lambda c: c.data.startswith("mirrornew:"))
async def mirror_new(cb: types.CallbackQuery):
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
        token = generate_mirror_token()
        data["mirror_tokens"][token] = {
            "source_wid": wid,
            "company_id": company["id"],
            "source_thread_id": ws["thread_id"],
        }
        await save_data_unlocked(data)

    fresh = await load_data()
    ws2 = fresh["workspaces"].get(wid)
    if ws2 and 0 <= company_idx < len(ws2.get("companies", [])):
        await show_instruction_menu(
            fresh,
            wid,
            binding_instruction_text("📤 Чтобы добавить связку", token),
            f"mirrors:{wid}:{company_idx}",
        )

@dp.callback_query_handler(lambda c: c.data.startswith("mirrorcopy:"))
async def mirror_copy_existing(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, source_idx = cb.data.split(":")
    company_idx = int(company_idx)
    source_idx = int(source_idx)

    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or company_idx < 0 or company_idx >= len(ws.get("companies", [])):
            return
        company = ws["companies"][company_idx]
        candidates = missing_report_targets_for_mirrors(company)
        picked = next((target for idx, target in candidates if idx == source_idx), None)
        if not picked:
            return
        chat_id = picked.get("chat_id")
        thread_id = picked.get("thread_id") or 0
        label = binding_place_label(data, chat_id, thread_id, fallback_label=picked.get("label"))
        company.setdefault("mirrors", []).append({
            "chat_id": chat_id,
            "thread_id": thread_id,
            "message_id": None,
            "label": label,
        })
        await save_data_unlocked(data)

    fresh = await load_data()
    ws2 = fresh["workspaces"].get(wid)
    if ws2 and 0 <= company_idx < len(ws2.get("companies", [])):
        company2 = ws2["companies"][company_idx]
        published_message_id = await publish_initial_company_mirror(company2, chat_id, thread_id)
        if published_message_id:
            for mirror in company2.get("mirrors", []):
                if mirror.get("chat_id") == chat_id and (mirror.get("thread_id") or 0) == thread_id:
                    mirror["message_id"] = published_message_id
                    break
            async with FILE_LOCK:
                data = await load_data_unlocked()
                source_ws = data.get("workspaces", {}).get(wid)
                if source_ws and 0 <= company_idx < len(source_ws.get("companies", [])):
                    live_company = source_ws["companies"][company_idx]
                    for mirror in live_company.get("mirrors", []):
                        if mirror.get("chat_id") == chat_id and (mirror.get("thread_id") or 0) == thread_id:
                            mirror["message_id"] = published_message_id
                            break
                    await save_data_unlocked(data)
        await upsert_ws_menu(fresh, wid, workspace_path_title(ws2, rich_display_company_name(company2), "📤 Дублирование списка"), mirrors_menu_kb(wid, company_idx, company2))

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
        token_kind = payload.get("kind") or "mirror"
        thread_id = message.message_thread_id or 0
        remember_binding_place(data, message.chat.id, thread_id, message.chat.title or "Чат", extract_topic_title(message))
        label = refresh_binding_labels(data, message.chat.id, thread_id)
        existing = None
        created_new_binding = False
        if token_kind == "report_target":
            targets = ensure_explicit_report_targets(company)
            for target in targets:
                if target.get("chat_id") == message.chat.id and (target.get("thread_id") or 0) == thread_id:
                    existing = target
                    break
            if not existing:
                existing = {"chat_id": message.chat.id, "thread_id": thread_id, "message_id": None, "label": label}
                targets.append(existing)
                created_new_binding = True
        else:
            for mirror in company.get("mirrors", []):
                if mirror.get("chat_id") == message.chat.id and (mirror.get("thread_id") or 0) == thread_id:
                    existing = mirror
                    break
            if not existing:
                existing = {"chat_id": message.chat.id, "thread_id": thread_id, "message_id": None, "label": label}
                company.setdefault("mirrors", []).append(existing)
                created_new_binding = True
        existing["label"] = label
        source_thread_id = payload.get("source_thread_id") or 0
        data["mirror_tokens"].pop(code, None)
        await save_data_unlocked(data)

    fresh = await load_data()
    ws = fresh["workspaces"][source_wid]
    company_idx = find_company_index_by_id(ws, company_id)
    company = ws["companies"][company_idx]
    if token_kind == "mirror":
        if created_new_binding:
            published_message_id = await publish_initial_company_mirror(company, message.chat.id, thread_id)
            if published_message_id:
                for mirror in company.get("mirrors", []):
                    if mirror.get("chat_id") == message.chat.id and (mirror.get("thread_id") or 0) == thread_id:
                        mirror["message_id"] = published_message_id
                        break
                async with FILE_LOCK:
                    data = await load_data_unlocked()
                    source_ws = data.get("workspaces", {}).get(source_wid)
                    if source_ws:
                        live_company_idx = find_company_index_by_id(source_ws, company_id)
                        if live_company_idx is not None:
                            live_company = source_ws["companies"][live_company_idx]
                            for mirror in live_company.get("mirrors", []):
                                if mirror.get("chat_id") == message.chat.id and (mirror.get("thread_id") or 0) == thread_id:
                                    mirror["message_id"] = published_message_id
                                    break
                            await save_data_unlocked(data)
        else:
            await sync_company_everywhere(ws, company_idx)
        await save_data(fresh)
    await try_delete_user_message(message)
    fresh = await load_data()
    if source_wid in fresh.get("workspaces", {}):
        if token_kind == "report_target":
            await edit_report_targets_menu(fresh, source_wid, company_idx)
        else:
            ws2 = fresh["workspaces"].get(source_wid)
            if ws2 and 0 <= company_idx < len(ws2.get("companies", [])):
                company2 = ws2["companies"][company_idx]
                await upsert_ws_menu(fresh, source_wid, workspace_path_title(ws2, rich_display_company_name(company2), "📤 Дублирование списка"), mirrors_menu_kb(source_wid, company_idx, company2))
    if token_kind == "report_target":
        await send_temp_message(ws["chat_id"], f"🧾 Отчеты по списку «{company['title']}» теперь будут выгружаться еще в один тред/чат", source_thread_id, delay=10)
    else:
        await send_temp_message(ws["chat_id"], f"📤 Список «{company['title']}» дублируется ещё в один тред/чат", source_thread_id, delay=10)

# =========================
# REPORTS
# =========================

async def open_report_schedule_prompt(wid: str, company_idx: int, target_idx: int, interval_idx: int | None, flow: str, kind: str, weekday: int | None = None):
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected") or company_idx < 0 or company_idx >= len(ws.get("companies", [])):
            return
        company = ws["companies"][company_idx]
        target = get_report_target(company, target_idx)
        if not target:
            return
        draft = prepare_report_interval_draft(company, interval_idx, kind, report_target_key(target))
        if kind == "weekly" and weekday is not None:
            draft["weekday"] = weekday

        await set_prompt(
            ws,
            report_schedule_prompt_text(kind),
            {
                "type": "report_schedule_time",
                "company_idx": company_idx,
                "target_idx": target_idx,
                "interval_idx": interval_idx,
                "flow": flow,
                "draft_interval": draft,
                "back_to": {"view": "report_interval_kind", "company_idx": company_idx, "target_idx": target_idx, "interval_idx": interval_idx, "flow": flow},
            },
        )
        await save_data_unlocked(data)

async def finalize_report_interval(wid: str, company_idx: int, draft_interval: dict, flow: str, interval_idx: int | None):
    normalized = ensure_report_interval(draft_interval)
    if not normalized:
        return

    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or company_idx < 0 or company_idx >= len(ws.get("companies", [])):
            return
        company = ws["companies"][company_idx]
        upsert_report_interval(get_report_intervals(company), normalized, flow, interval_idx)
        ws["awaiting"] = None
        await save_data_unlocked(data)

    fresh = await load_data()
    company = fresh["workspaces"][wid]["companies"][company_idx]
    target_key = normalized.get("target_key")
    target_idx = 0
    if target_key:
        for idx, target in enumerate(get_effective_report_targets(company)):
            if report_target_key(target) == target_key:
                target_idx = idx
                break
    await edit_report_menu(fresh, wid, company_idx, target_idx)

async def finalize_template_report_interval(wid: str, draft_interval: dict, flow: str, interval_idx: int | None):
    normalized = ensure_report_interval(draft_interval)
    if not normalized:
        return

    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws:
            return
        template = get_active_template(ws)
        upsert_report_interval(get_report_intervals(template), normalized, flow, interval_idx)
        ws["awaiting"] = None
        await save_data_unlocked(data)

    fresh = await load_data()
    await edit_template_report_menu(fresh, wid)

async def open_template_report_schedule_prompt(
    wid: str,
    interval_idx: int | None,
    flow: str,
    kind: str,
    weekday: int | None = None,
):
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        template = get_active_template(ws)
        draft = prepare_report_interval_draft(template, interval_idx, kind)
        if kind == "weekly" and weekday is not None:
            draft["weekday"] = weekday
        await set_prompt(
            ws,
            report_schedule_prompt_text(kind),
            {
                "type": "template_report_schedule_time",
                "interval_idx": interval_idx,
                "flow": flow,
                "draft_interval": draft,
                "back_to": {"view": "template_report_interval_kind", "interval_idx": interval_idx, "flow": flow},
            },
        )
        await save_data_unlocked(data)

async def open_template_report_schedule_prompt_from_callback(cb: types.CallbackQuery, kind: str):
    if not await begin_callback(cb):
        return
    _, wid, interval_idx, flow, *rest = cb.data.split(":")
    await open_template_report_schedule_prompt(
        wid,
        parse_optional_index(interval_idx),
        flow,
        kind,
        int(rest[0]) if rest else None,
    )

@dp.callback_query_handler(lambda c: c.data.startswith("reports:"))
async def open_reports_menu(cb: types.CallbackQuery):
    await open_company_menu_from_callback(cb, edit_report_targets_menu)

@dp.callback_query_handler(lambda c: c.data.startswith("reportsettings:"))
async def open_report_settings_menu(cb: types.CallbackQuery):
    await open_company_target_menu_from_callback(cb, edit_report_settings_menu)

@dp.callback_query_handler(lambda c: c.data.startswith("reportmenu:"))
async def open_report_target_menu(cb: types.CallbackQuery):
    await open_company_target_menu_from_callback(cb, edit_report_menu)

@dp.callback_query_handler(lambda c: c.data.startswith("reportitem:"))
async def open_report_item(cb: types.CallbackQuery):
    await open_company_target_index_menu_from_callback(cb, edit_report_interval_menu)

@dp.callback_query_handler(lambda c: c.data.startswith("reportadd:"))
async def open_report_add_menu(cb: types.CallbackQuery):
    await open_company_target_menu_from_callback(cb, lambda data, wid, company_idx, target_idx: edit_report_interval_kind_menu(data, wid, company_idx, target_idx, "new", None))

@dp.callback_query_handler(lambda c: c.data.startswith("reportedit:"))
async def open_report_edit_schedule_menu(cb: types.CallbackQuery):
    await open_company_target_index_menu_from_callback(cb, lambda data, wid, company_idx, target_idx, interval_idx: edit_report_interval_kind_menu(data, wid, company_idx, target_idx, "edit", interval_idx))

@dp.callback_query_handler(lambda c: c.data.startswith("reportdaily:"))
async def open_report_daily_prompt(cb: types.CallbackQuery):
    await open_report_schedule_prompt_from_callback(cb, "daily")

@dp.callback_query_handler(lambda c: c.data.startswith("reportmonth:"))
async def open_report_monthly_prompt(cb: types.CallbackQuery):
    await open_report_schedule_prompt_from_callback(cb, "monthly")

@dp.callback_query_handler(lambda c: c.data.startswith("reportonce:"))
async def open_report_once_prompt(cb: types.CallbackQuery):
    await open_report_schedule_prompt_from_callback(cb, "once")

@dp.callback_query_handler(lambda c: c.data.startswith("reportweek:"))
async def open_report_weekly_prompt(cb: types.CallbackQuery):
    await open_report_schedule_prompt_from_callback(cb, "weekly")

@dp.callback_query_handler(lambda c: c.data.startswith("reportinstant:"))
async def open_report_instant(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, target_idx, interval_idx, flow = cb.data.split(":")
    company_idx_value = int(company_idx)
    target_idx_value = int(target_idx)
    interval_idx_value = parse_optional_index(interval_idx)

    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or company_idx_value < 0 or company_idx_value >= len(ws.get("companies", [])):
            return
        company = ws["companies"][company_idx_value]
        target = get_report_target(company, target_idx_value)
        if not target:
            return
        normalized = prepare_report_interval_draft(company, interval_idx_value, "on_done", report_target_key(target))
        upsert_report_interval(get_report_intervals(company), normalized, flow, interval_idx_value)
        ws["awaiting"] = None
        await save_data_unlocked(data)

    fresh = await load_data()
    await edit_report_menu(fresh, wid, company_idx_value, target_idx_value)

@dp.callback_query_handler(lambda c: c.data.startswith("reportaccedit:"))
async def open_report_edit_accumulation_menu(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, target_idx, interval_idx = cb.data.split(":")
    company_idx = int(company_idx)
    target_idx_value = int(target_idx)
    interval_idx_value = int(interval_idx)

    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or company_idx < 0 or company_idx >= len(ws.get("companies", [])):
            return
        company = ws["companies"][company_idx]
        target = get_report_target(company, target_idx_value)
        if not target:
            return
        interval = find_report_interval(company, interval_idx_value)
        if not interval or interval.get("target_key") != report_target_key(target):
            return
        ws["awaiting"] = {
            "type": "report_accumulation_choice",
            "company_idx": company_idx,
            "target_idx": target_idx_value,
            "interval_idx": interval_idx_value,
            "flow": "edit_accumulation",
            "draft_interval": clone_report_interval(interval),
        }
        await save_data_unlocked(data)

    fresh = await load_data()
    await edit_report_accumulation_menu(fresh, wid)

@dp.callback_query_handler(lambda c: c.data.startswith("reportaccback:"))
async def report_accumulation_back(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    wid = cb.data.split(":", 1)[1]
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        awaiting = (ws or {}).get("awaiting") or {}
        company_idx = awaiting.get("company_idx")
        target_idx = awaiting.get("target_idx")
        is_template = awaiting.get("type", "").startswith("template_report")
        if company_idx is None and not is_template:
            return
        flow = awaiting.get("flow")
        interval_idx = awaiting.get("interval_idx")
        if ws is not None:
            ws["awaiting"] = None
        await save_data_unlocked(data)

    data = await load_data()
    if awaiting.get("type", "").startswith("template_report"):
        if flow == "edit_accumulation" and interval_idx is not None:
            await edit_template_report_interval_menu(data, wid, interval_idx)
            return
        await edit_template_report_interval_kind_menu(data, wid, "edit" if flow == "edit" else "new", interval_idx)
        return
    if flow == "edit_accumulation" and interval_idx is not None:
        await edit_report_interval_menu(data, wid, company_idx, target_idx, interval_idx)
        return
    await edit_report_interval_kind_menu(data, wid, company_idx, target_idx, "edit" if flow == "edit" else "new", interval_idx)

@dp.callback_query_handler(lambda c: c.data.startswith("reportacc:"))
async def report_accumulation_choice(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, mode_token = cb.data.split(":")
    mode = {"last": "last_report", "week": "week", "month": "month", "specific": "specific"}.get(mode_token)
    if not mode:
        return

    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        awaiting = ws.get("awaiting") or {}
        if awaiting.get("type") not in {"report_accumulation_choice", "template_report_accumulation_choice"}:
            return
        draft_interval = clone_report_interval(awaiting.get("draft_interval") or {})
        company_idx = awaiting.get("company_idx")
        target_idx = awaiting.get("target_idx")
        interval_idx = awaiting.get("interval_idx")
        flow = awaiting.get("flow")
        is_template = awaiting.get("type", "").startswith("template_report")
        if mode != "specific":
            draft_interval["accumulation"] = {"mode": mode}
            await save_data_unlocked(data)
        else:
            if draft_interval.get("kind") == "once":
                prompt_text = "🧾 Пришли точную дату и время начала накопления"
            elif draft_interval.get("kind") == "monthly":
                prompt_text = "🧾 Пришли число и время начала накопления, например: 15 08:30"
            else:
                prompt_text = "🧾 Пришли точную дату и время начала накопления"
            await set_prompt(
                ws,
                prompt_text,
                {
                    "type": "template_report_accumulation_value" if is_template else "report_accumulation_value",
                    "company_idx": company_idx,
                    "target_idx": target_idx,
                    "interval_idx": interval_idx,
                    "flow": flow,
                    "draft_interval": draft_interval,
                    "back_to": {
                        "view": "report_accumulation",
                        "company_idx": company_idx,
                        "target_idx": target_idx,
                        "interval_idx": interval_idx,
                        "flow": flow,
                        "restore_awaiting": {
                            "type": "template_report_accumulation_choice" if is_template else "report_accumulation_choice",
                            "company_idx": company_idx,
                            "target_idx": target_idx,
                            "interval_idx": interval_idx,
                            "flow": flow,
                            "draft_interval": draft_interval,
                        },
                    },
                },
            )
            await save_data_unlocked(data)
            return

    if mode != "specific":
        data = await load_data()
        ws = data["workspaces"].get(wid)
        awaiting = (ws or {}).get("awaiting") or {}
        if awaiting.get("type", "").startswith("template_report"):
            await finalize_template_report_interval(wid, draft_interval, flow, interval_idx)
        else:
            await finalize_report_interval(wid, awaiting.get("company_idx"), draft_interval, flow, interval_idx)

@dp.callback_query_handler(lambda c: c.data.startswith("reportdelask:"))
async def report_delete_ask(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, target_idx, interval_idx = cb.data.split(":")
    data = await load_data()
    await upsert_ws_menu(
        data,
        wid,
        "Удалить интервал отчета?",
        confirm_kb(f"reportdel:{wid}:{company_idx}:{target_idx}:{interval_idx}", f"reportitem:{wid}:{company_idx}:{target_idx}:{interval_idx}"),
    )

@dp.callback_query_handler(lambda c: c.data.startswith("reportdel:"))
async def report_delete(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, target_idx, interval_idx = cb.data.split(":")
    company_idx = int(company_idx)
    target_idx = int(target_idx)
    interval_idx = int(interval_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or company_idx < 0 or company_idx >= len(ws.get("companies", [])):
            return
        company = ws["companies"][company_idx]
        target = get_report_target(company, target_idx)
        if not target:
            return
        intervals = get_report_intervals(company)
        if interval_idx < 0 or interval_idx >= len(intervals) or intervals[interval_idx].get("target_key") != report_target_key(target):
            return
        intervals.pop(interval_idx)
        ws["awaiting"] = None
        await save_data_unlocked(data)
    fresh = await load_data()
    await edit_report_menu(fresh, wid, company_idx, target_idx)

@dp.callback_query_handler(lambda c: c.data.startswith("reportclearask:"))
async def report_clear_ask(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, target_idx = cb.data.split(":")
    data = await load_data()
    await upsert_ws_menu(
        data,
        wid,
        "Очистить весь график отчетности?",
        confirm_kb(f"reportclear:{wid}:{company_idx}:{target_idx}", f"reportsettings:{wid}:{company_idx}:{target_idx}"),
    )

@dp.callback_query_handler(lambda c: c.data.startswith("reportclear:"))
async def report_clear(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, target_idx = cb.data.split(":")
    company_idx = int(company_idx)
    target_idx = int(target_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or company_idx < 0 or company_idx >= len(ws.get("companies", [])):
            return
        company = ws["companies"][company_idx]
        target = get_report_target(company, target_idx)
        if not target:
            return
        target_key = report_target_key(target)
        intervals = get_report_intervals(company)
        intervals[:] = [interval for interval in intervals if interval.get("target_key") != target_key]
        ws["awaiting"] = None
        await save_data_unlocked(data)
    fresh = await load_data()
    await edit_report_menu(fresh, wid, company_idx, target_idx)

@dp.callback_query_handler(lambda c: c.data.startswith("reportbind:"))
async def open_report_bindings_menu(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx = cb.data.split(":")
    data = await load_data()
    await edit_report_targets_menu(data, wid, int(company_idx))

@dp.callback_query_handler(lambda c: c.data.startswith("reportbindrefresh:"))
async def refresh_report_bindings_menu(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx = cb.data.split(":")
    data = await load_data()
    await edit_report_targets_menu(data, wid, int(company_idx))

@dp.callback_query_handler(lambda c: c.data.startswith("reportbindon:"))
async def report_bind_on(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx = cb.data.split(":")
    company_idx = int(company_idx)
    show_import_menu = False

    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected") or company_idx < 0 or company_idx >= len(ws.get("companies", [])):
            return
        company = ws["companies"][company_idx]
        if missing_mirrors_for_report_targets(company):
            show_import_menu = True
        else:
            token = generate_mirror_token()
            data["mirror_tokens"][token] = {
                "source_wid": wid,
                "company_id": company["id"],
                "source_thread_id": ws["thread_id"],
                "kind": "report_target",
            }
        await save_data_unlocked(data)

    fresh = await load_data()
    ws2 = fresh["workspaces"].get(wid)
    if not ws2 or not (0 <= company_idx < len(ws2.get("companies", []))):
        return
    company2 = ws2["companies"][company_idx]
    if show_import_menu:
        await upsert_ws_menu(
            fresh,
            wid,
            workspace_path_title(ws2, rich_display_company_name(company2), "🧾 Отчетность", "📎 Привязка", "➕ Добавить связку"),
            report_import_candidates_kb(wid, company_idx, company2),
        )
        return
    await show_instruction_menu(
        fresh,
        wid,
        binding_instruction_text("🧾 Чтобы добавить привязку для отчетности", token),
        f"reportbind:{wid}:{company_idx}",
    )

@dp.callback_query_handler(lambda c: c.data.startswith("reportbindnew:"))
async def report_bind_new(cb: types.CallbackQuery):
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
        token = generate_mirror_token()
        data["mirror_tokens"][token] = {
            "source_wid": wid,
            "company_id": company["id"],
            "source_thread_id": ws["thread_id"],
            "kind": "report_target",
        }
        await save_data_unlocked(data)

    fresh = await load_data()
    await show_instruction_menu(
        fresh,
        wid,
        binding_instruction_text("🧾 Чтобы добавить привязку для отчетности", token),
        f"reportbind:{wid}:{company_idx}",
    )

@dp.callback_query_handler(lambda c: c.data.startswith("reportbindcopy:"))
async def report_bind_copy_existing(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, source_idx = cb.data.split(":")
    company_idx = int(company_idx)
    source_idx = int(source_idx)

    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or company_idx < 0 or company_idx >= len(ws.get("companies", [])):
            return
        company = ws["companies"][company_idx]
        candidates = missing_mirrors_for_report_targets(company)
        picked = next((mirror for idx, mirror in candidates if idx == source_idx), None)
        if not picked:
            return
        targets = ensure_explicit_report_targets(company)
        label = binding_place_label(data, picked.get("chat_id"), picked.get("thread_id") or 0, fallback_label=picked.get("label"))
        targets.append({
            "chat_id": picked.get("chat_id"),
            "thread_id": picked.get("thread_id") or 0,
            "message_id": None,
            "label": label,
        })
        await save_data_unlocked(data)

    fresh = await load_data()
    await edit_report_targets_menu(fresh, wid, company_idx)

@dp.callback_query_handler(lambda c: c.data.startswith("reportbindoff:"))
async def report_bind_off(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, target_idx = cb.data.split(":")
    company_idx = int(company_idx)
    target_idx = int(target_idx)

    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or company_idx < 0 or company_idx >= len(ws.get("companies", [])):
            return
        company = ws["companies"][company_idx]
        targets = ensure_explicit_report_targets(company)
        if target_idx < 0 or target_idx >= len(targets):
            return
        target = ensure_report_target(targets[target_idx])
        targets.pop(target_idx)
        if target:
            target_key = report_target_key(target)
            intervals = get_report_intervals(company)
            intervals[:] = [interval for interval in intervals if interval.get("target_key") != target_key]
        await save_data_unlocked(data)

    fresh = await load_data()
    await edit_report_targets_menu(fresh, wid, company_idx)

@dp.callback_query_handler(lambda c: c.data.startswith("tplreport:"))
async def open_template_reports_menu(cb: types.CallbackQuery):
    await open_wid_menu_from_callback(cb, edit_template_report_menu)

@dp.callback_query_handler(lambda c: c.data.startswith("tplreportsettings:"))
async def open_template_report_settings_menu(cb: types.CallbackQuery):
    await open_wid_menu_from_callback(cb, edit_template_report_settings_menu)

@dp.callback_query_handler(lambda c: c.data.startswith("tplreportitem:"))
async def open_template_report_item(cb: types.CallbackQuery):
    await open_template_index_menu_from_callback(cb, edit_template_report_interval_menu)

@dp.callback_query_handler(lambda c: c.data.startswith("tplreportadd:"))
async def open_template_report_add_menu(cb: types.CallbackQuery):
    await open_wid_menu_from_callback(cb, lambda data, wid: edit_template_report_interval_kind_menu(data, wid, "new", None))

@dp.callback_query_handler(lambda c: c.data.startswith("tplreportedit:"))
async def open_template_report_edit_menu(cb: types.CallbackQuery):
    await open_template_index_menu_from_callback(cb, lambda data, wid, interval_idx: edit_template_report_interval_kind_menu(data, wid, "edit", interval_idx))

@dp.callback_query_handler(lambda c: c.data.startswith("tplreportdaily:"))
async def open_template_report_daily_prompt(cb: types.CallbackQuery):
    await open_template_report_schedule_prompt_from_callback(cb, "daily")

@dp.callback_query_handler(lambda c: c.data.startswith("tplreportmonth:"))
async def open_template_report_monthly_prompt(cb: types.CallbackQuery):
    await open_template_report_schedule_prompt_from_callback(cb, "monthly")

@dp.callback_query_handler(lambda c: c.data.startswith("tplreportweek:"))
async def open_template_report_weekly_prompt(cb: types.CallbackQuery):
    await open_template_report_schedule_prompt_from_callback(cb, "weekly")

@dp.callback_query_handler(lambda c: c.data.startswith("tplreportinstant:"))
async def open_template_report_instant(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, interval_idx, flow = cb.data.split(":")
    interval_idx_value = parse_optional_index(interval_idx)

    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        template = get_active_template(ws)
        normalized = prepare_report_interval_draft(template, interval_idx_value, "on_done")
        upsert_report_interval(get_report_intervals(template), normalized, flow, interval_idx_value)
        ws["awaiting"] = None
        await save_data_unlocked(data)

    fresh = await load_data()
    await edit_template_report_menu(fresh, wid)

@dp.callback_query_handler(lambda c: c.data.startswith("tplreportaccedit:"))
async def open_template_report_accumulation_menu(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, interval_idx = cb.data.split(":")
    interval_idx = int(interval_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        template = get_active_template(ws)
        interval = find_report_interval(template, interval_idx)
        if not interval:
            return
        ws["awaiting"] = {"type": "template_report_accumulation_choice", "interval_idx": interval_idx, "flow": "edit_accumulation", "draft_interval": clone_report_interval(interval)}
        await save_data_unlocked(data)
    fresh = await load_data()
    await edit_report_accumulation_menu(fresh, wid)

@dp.callback_query_handler(lambda c: c.data.startswith("tplreportdelask:"))
async def template_report_delete_ask(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, interval_idx = cb.data.split(":")
    data = await load_data()
    await upsert_ws_menu(data, wid, "Удалить Интервал Отчета?", confirm_kb(f"tplreportdel:{wid}:{interval_idx}", f"tplreportitem:{wid}:{interval_idx}"))

@dp.callback_query_handler(lambda c: c.data.startswith("tplreportdel:"))
async def template_report_delete(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, interval_idx = cb.data.split(":")
    interval_idx = int(interval_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws:
            return
        template = get_active_template(ws)
        intervals = get_report_intervals(template)
        if 0 <= interval_idx < len(intervals):
            intervals.pop(interval_idx)
        await save_data_unlocked(data)
    fresh = await load_data()
    await edit_template_report_menu(fresh, wid)

@dp.callback_query_handler(lambda c: c.data.startswith("tplreportclearask:"))
async def template_report_clear_ask(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    wid = cb.data.split(":", 1)[1]
    data = await load_data()
    await upsert_ws_menu(data, wid, "Очистить весь график отчетности?", confirm_kb(f"tplreportclear:{wid}", f"tplreportsettings:{wid}"))

@dp.callback_query_handler(lambda c: c.data.startswith("tplreportclear:"))
async def template_report_clear(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    wid = cb.data.split(":", 1)[1]
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws:
            return
        template = get_active_template(ws)
        get_report_intervals(template).clear()
        await save_data_unlocked(data)
    fresh = await load_data()
    await edit_template_report_menu(fresh, wid)

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
    elif view == "rp" and a != "x" and b != "x":
        await edit_report_menu(data, wid, int(a), int(b))
    elif view == "rb" and a != "x":
        await edit_report_targets_menu(data, wid, int(a))
    elif view == "mm" and a != "x":
        company_idx = int(a)
        company = ws["companies"][company_idx]
        await upsert_ws_menu(
            data,
            wid,
            workspace_path_title(ws, rich_display_company_name(company), "📤 Дублирование списка"),
            mirrors_menu_kb(wid, company_idx, company),
        )
    elif view == "mic" and a != "x":
        company_idx = int(a)
        company = ws["companies"][company_idx]
        await upsert_ws_menu(
            data,
            wid,
            workspace_path_title(ws, rich_display_company_name(company), "📤 Дублирование списка", "➕ Добавить связку"),
            mirror_import_candidates_kb(wid, company_idx, company),
        )
    elif view == "ric" and a != "x":
        company_idx = int(a)
        company = ws["companies"][company_idx]
        await upsert_ws_menu(
            data,
            wid,
            workspace_path_title(ws, rich_display_company_name(company), "🧾 Отчетность", "📎 Привязка", "➕ Добавить связку"),
            report_import_candidates_kb(wid, company_idx, company),
        )
    elif view == "tmv" and a != "x" and b != "x":
        await edit_task_move_menu(data, wid, int(a), int(b))
    elif view == "ttmv" and a != "x":
        await edit_template_task_move_menu(data, wid, int(a))
    elif view == "tpr":
        await edit_template_report_menu(data, wid)
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
            set_ui_page(ws, "ws_home", get_ui_page(ws, "ws_home") + delta)
        elif view == "cc":
            set_ui_page(ws, "cmp_create", get_ui_page(ws, "cmp_create") + delta)
        elif view == "tr":
            set_ui_page(ws, "tpl_root", get_ui_page(ws, "tpl_root") + delta)
        elif view == "cm" and a != "x":
            company_idx = int(a)
            if 0 <= company_idx < len(ws.get("companies", [])):
                company = ws["companies"][company_idx]
                key = f"cmp_{company_idx}"
                set_ui_page(company, key, get_ui_page(company, key) + delta)
        elif view == "ct" and a != "x" and b != "x":
            company_idx = int(a)
            category_idx = int(b)
            if 0 <= company_idx < len(ws.get("companies", [])):
                company = ws["companies"][company_idx]
                key = f"cat_{company_idx}_{category_idx}"
                set_ui_page(company, key, get_ui_page(company, key) + delta)
        elif view == "rp" and a != "x" and b != "x":
            company_idx = int(a)
            target_idx = int(b)
            if 0 <= company_idx < len(ws.get("companies", [])):
                company = ws["companies"][company_idx]
                key = f"report_{company_idx}_{target_idx}"
                set_ui_page(company, key, get_ui_page(company, key) + delta)
        elif view == "rb" and a != "x":
            company_idx = int(a)
            if 0 <= company_idx < len(ws.get("companies", [])):
                company = ws["companies"][company_idx]
                key = f"report_targets_{company_idx}"
                set_ui_page(company, key, get_ui_page(company, key) + delta)
        elif view == "mm" and a != "x":
            company_idx = int(a)
            if 0 <= company_idx < len(ws.get("companies", [])):
                company = ws["companies"][company_idx]
                key = f"mirrors_{company_idx}"
                set_ui_page(company, key, get_ui_page(company, key) + delta)
        elif view == "mic" and a != "x":
            company_idx = int(a)
            if 0 <= company_idx < len(ws.get("companies", [])):
                company = ws["companies"][company_idx]
                key = f"mirror_import_{company_idx}"
                set_ui_page(company, key, get_ui_page(company, key) + delta)
        elif view == "ric" and a != "x":
            company_idx = int(a)
            if 0 <= company_idx < len(ws.get("companies", [])):
                company = ws["companies"][company_idx]
                key = f"report_import_{company_idx}"
                set_ui_page(company, key, get_ui_page(company, key) + delta)
        elif view == "tmv" and a != "x" and b != "x":
            company_idx = int(a)
            task_idx = int(b)
            if 0 <= company_idx < len(ws.get("companies", [])):
                company = ws["companies"][company_idx]
                key = f"task_move_{company_idx}_{task_idx}"
                set_ui_page(company, key, get_ui_page(company, key) + delta)
        elif view == "ttmv" and a != "x":
            task_idx = int(a)
            key = f"template_task_move_{task_idx}"
            set_ui_page(ws, key, get_ui_page(ws, key) + delta)
        elif view == "tpr":
            active = get_active_template(ws)
            key = f"tpl_report_{active.get('id')}"
            set_ui_page(active, key, get_ui_page(active, key) + delta)
        elif view == "tm":
            key = f"tpl_{get_active_template(ws).get('id')}"
            set_ui_page(ws, key, get_ui_page(ws, key) + delta)
        elif view == "tc" and a != "x":
            template = get_active_template(ws)
            key = f"tplcat_{template.get('id')}_{int(a)}"
            set_ui_page(template, key, get_ui_page(template, key) + delta)

        await save_data_unlocked(data)

    await refresh_paged_view(data, str(cb.from_user.id), wid, view, a, b)

@dp.callback_query_handler(lambda c: c.data.startswith("cmpnew:"))
async def create_company_menu(cb: types.CallbackQuery):
    await open_wid_menu_from_callback(cb, edit_company_create_menu)

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
    await open_company_menu_from_callback(cb, edit_company_menu)

@dp.callback_query_handler(lambda c: c.data.startswith("cmpset:"))
async def open_company_settings(cb: types.CallbackQuery):
    await open_company_menu_from_callback(cb, edit_company_settings_menu)

@dp.callback_query_handler(lambda c: c.data.startswith("cat:"))
async def open_category(cb: types.CallbackQuery):
    await open_company_category_menu_from_callback(cb, edit_category_menu)

@dp.callback_query_handler(lambda c: c.data.startswith("catset:"))
async def open_category_settings(cb: types.CallbackQuery):
    await open_company_category_menu_from_callback(cb, edit_category_settings_menu)

@dp.callback_query_handler(lambda c: c.data.startswith("task:"))
async def open_task_menu(cb: types.CallbackQuery):
    await open_company_task_menu_from_callback(cb, edit_task_menu)

@dp.callback_query_handler(lambda c: c.data.startswith("taskmove:") )
async def open_task_move_menu(cb: types.CallbackQuery):
    await open_company_task_menu_from_callback(cb, edit_task_move_menu)

@dp.callback_query_handler(lambda c: c.data.startswith("tpl:"))
async def open_template_menu(cb: types.CallbackQuery):
    await open_wid_menu_from_callback(cb, edit_template_menu)

@dp.callback_query_handler(lambda c: c.data.startswith("tplcat:"))
async def open_template_category(cb: types.CallbackQuery):
    await open_template_index_menu_from_callback(cb, edit_template_category_menu)

@dp.callback_query_handler(lambda c: c.data.startswith("tplcatset:"))
async def open_template_category_settings(cb: types.CallbackQuery):
    await open_template_index_menu_from_callback(cb, edit_template_category_settings_menu)

@dp.callback_query_handler(lambda c: c.data.startswith("tpltask:"))
async def open_template_task(cb: types.CallbackQuery):
    await open_template_index_menu_from_callback(cb, edit_template_task_menu)

@dp.callback_query_handler(lambda c: c.data.startswith("tpltaskmove:"))
async def open_template_task_move(cb: types.CallbackQuery):
    await open_template_index_menu_from_callback(cb, edit_template_task_move_menu)

# =========================
# CANCEL
# =========================

async def show_back_view(data: dict, wid: str, back_to: dict):
    view = back_to.get("view", "ws")
    if view == "company":
        await edit_company_menu(data, wid, back_to["company_idx"])
    elif view == "company_settings":
        await edit_company_settings_menu(data, wid, back_to["company_idx"])
    elif view == "report":
        await edit_report_menu(data, wid, back_to["company_idx"], back_to["target_idx"])
    elif view == "report_item":
        await edit_report_interval_menu(data, wid, back_to["company_idx"], back_to["target_idx"], back_to["interval_idx"])
    elif view == "report_interval_kind":
        await edit_report_interval_kind_menu(data, wid, back_to["company_idx"], back_to["target_idx"], back_to.get("flow", "new"), back_to.get("interval_idx"))
    elif view == "report_accumulation":
        await edit_report_accumulation_menu(data, wid)
    elif view == "report_targets":
        await edit_report_targets_menu(data, wid, back_to["company_idx"])
    elif view == "category":
        await edit_category_menu(data, wid, back_to["company_idx"], back_to["category_idx"])
    elif view == "category_settings":
        await edit_category_settings_menu(data, wid, back_to["company_idx"], back_to["category_idx"])
    elif view == "task":
        await edit_task_menu(data, wid, back_to["company_idx"], back_to["task_idx"])
    elif view == "task_deadline":
        await edit_task_deadline_menu(data, wid, back_to["company_idx"], back_to["task_idx"])
    elif view == "ws_settings":
        await edit_ws_settings_menu(data, wid)
    elif view == "template":
        await edit_template_menu(data, wid)
    elif view == "template_root":
        await edit_templates_root_menu(data, wid)
    elif view == "template_settings":
        await edit_template_settings_menu(data, wid)
    elif view == "template_report":
        await edit_template_report_menu(data, wid)
    elif view == "template_report_item":
        await edit_template_report_interval_menu(data, wid, back_to["interval_idx"])
    elif view == "template_report_interval_kind":
        await edit_template_report_interval_kind_menu(data, wid, back_to.get("flow", "new"), back_to.get("interval_idx"))
    elif view == "template_category":
        await edit_template_category_menu(data, wid, back_to["category_idx"])
    elif view == "template_category_settings":
        await edit_template_category_settings_menu(data, wid, back_to["category_idx"])
    elif view == "template_task":
        await edit_template_task_menu(data, wid, back_to["task_idx"])
    elif view == "template_task_deadline":
        await edit_template_task_deadline_menu(data, wid, back_to["task_idx"])
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
        menu_msg_id = ws.get("menu_msg_id")
        back_to = awaiting.get("back_to", {"view": "ws"})
        ws["awaiting"] = back_to.get("restore_awaiting")
        await save_data_unlocked(data)
    if prompt_msg_id and prompt_msg_id != menu_msg_id:
        await safe_delete_message(ws["chat_id"], prompt_msg_id)
    if ws.get("is_connected"):
        fresh = await load_data()
        await show_back_view(fresh, wid, back_to)

# =========================
# COMPANY / CATEGORY ACTIONS
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("cmpren:"))
async def rename_company_prompt(cb: types.CallbackQuery):
    await open_company_prompt_from_callback(
        cb,
        "✏️ Введи новое название списка:",
        lambda company_idx: {"type": "rename_company", "company_idx": company_idx, "back_to": {"view": "company_settings", "company_idx": company_idx}},
    )

@dp.callback_query_handler(lambda c: c.data.startswith("cmpemoji:"))
async def company_emoji_prompt(cb: types.CallbackQuery):
    await open_company_prompt_from_callback(
        cb,
        "😀 Пришли один смайлик для списка:",
        lambda company_idx: {"type": "company_emoji", "company_idx": company_idx, "back_to": {"view": "company_settings", "company_idx": company_idx}},
    )

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
        for token, payload in list(data.get("mirror_tokens", {}).items()):
            if payload.get("source_wid") == wid and payload.get("company_id") == company_id:
                data["mirror_tokens"].pop(token, None)
        await save_data_unlocked(data)
    await safe_delete_message(ws["chat_id"], card_msg_id)
    for mirror in mirrors:
        if mirror.get("message_id"):
            await safe_delete_message(mirror.get("chat_id"), mirror.get("message_id"))
    fresh = await load_data()
    await edit_ws_home_menu(fresh, wid)

@dp.callback_query_handler(lambda c: c.data.startswith("catnew:"))
async def add_category_prompt(cb: types.CallbackQuery):
    await open_company_prompt_from_callback(
        cb,
        "✏️ Введи название подгруппы:",
        lambda company_idx: {"type": "new_category", "company_idx": company_idx, "back_to": {"view": "company", "company_idx": company_idx}},
    )

@dp.callback_query_handler(lambda c: c.data.startswith("catren:"))
async def rename_category_prompt(cb: types.CallbackQuery):
    await open_company_category_prompt_from_callback(
        cb,
        "✏️ Введи новое название подгруппы:",
        lambda company_idx, category_idx: {
            "type": "rename_category",
            "company_idx": company_idx,
            "category_idx": category_idx,
            "back_to": {"view": "category_settings", "company_idx": company_idx, "category_idx": category_idx},
        },
    )

@dp.callback_query_handler(lambda c: c.data.startswith("catemoji:"))
async def category_emoji_prompt(cb: types.CallbackQuery):
    await open_company_category_prompt_from_callback(
        cb,
        "😀 Пришли один смайлик для подгруппы:",
        lambda company_idx, category_idx: {
            "type": "category_emoji",
            "company_idx": company_idx,
            "category_idx": category_idx,
            "back_to": {"view": "category_settings", "company_idx": company_idx, "category_idx": category_idx},
        },
    )

@dp.callback_query_handler(lambda c: c.data.startswith("catdelallask:"))
async def delete_category_with_tasks_ask(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, category_idx = cb.data.split(":")
    data = await load_data()
    ws, company, category = await get_company_category_context(data, wid, int(company_idx), int(category_idx))
    if not ws:
        return
    await upsert_ws_menu(
        data,
        wid,
        workspace_path_title(ws, rich_display_company_name(company), rich_display_category_name(category), "🗑 Удалить подгруппу с задачами?"),
        confirm_kb(f"catdelall:{wid}:{company_idx}:{category_idx}", f"catset:{wid}:{company_idx}:{category_idx}"),
    )

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

@dp.callback_query_handler(lambda c: c.data.startswith("catdelask:"))
async def delete_category_keep_ask(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, category_idx = cb.data.split(":")
    data = await load_data()
    ws, company, category = await get_company_category_context(data, wid, int(company_idx), int(category_idx))
    if not ws:
        return
    await upsert_ws_menu(
        data,
        wid,
        workspace_path_title(ws, rich_display_company_name(company), rich_display_category_name(category), "🗑 Удалить подгруппу?"),
        confirm_kb(f"catdel:{wid}:{company_idx}:{category_idx}", f"catset:{wid}:{company_idx}:{category_idx}"),
    )

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
    await open_company_task_prompt_from_callback(
        cb,
        "✏️ Введи новое название задачи:",
        lambda ws, company_idx, task_idx: {
            "type": "rename_task",
            "company_idx": company_idx,
            "task_idx": task_idx,
            "back_to": {"view": "task", "company_idx": company_idx, "task_idx": task_idx},
        },
    )

@dp.callback_query_handler(lambda c: c.data.startswith("taskdeadline:"))
async def task_deadline_prompt(cb: types.CallbackQuery):
    await open_company_task_prompt_from_callback(
        cb,
        "⏰ Пришли мне дату или срок для дедлайна",
        lambda ws, company_idx, task_idx: {
            "type": "task_deadline",
            "company_idx": company_idx,
            "task_idx": task_idx,
            "back_to": {
                "view": "task_deadline" if ws["companies"][company_idx]["tasks"][task_idx].get("deadline_due_at") else "task",
                "company_idx": company_idx,
                "task_idx": task_idx,
            },
        },
    )

@dp.callback_query_handler(lambda c: c.data.startswith("taskdeadlinebox:"))
async def open_task_deadline_box(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, task_idx = cb.data.split(":")
    data = await load_data()
    await edit_task_deadline_menu(data, wid, int(company_idx), int(task_idx))

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
        if task.get("done"):
            task["done"] = False
            cancel_task_completion_event(company, task)
        else:
            task["done"] = True
            add_task_completion_event(company, task)
        category_id = task.get("category_id")
        await save_data_unlocked(data)
    fresh = await load_data()
    ws_fresh = fresh["workspaces"][wid]
    instant_changed = await publish_company_done_reports(ws_fresh, company_idx, task_idx)
    await sync_company_everywhere(ws_fresh, company_idx)
    if instant_changed:
        await save_data(fresh)
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
    await open_wid_prompt_from_callback(cb, "✏️ Введи название подгруппы шаблона:", {"type": "new_template_category", "back_to": {"view": "template"}})

@dp.callback_query_handler(lambda c: c.data.startswith("tplcatren:"))
async def rename_template_category_prompt(cb: types.CallbackQuery):
    await open_template_category_prompt_from_callback(
        cb,
        "✏️ Введи новое название подгруппы шаблона:",
        lambda category_idx: {
            "type": "rename_template_category",
            "category_idx": category_idx,
            "back_to": {"view": "template_category_settings", "category_idx": category_idx},
        },
    )

@dp.callback_query_handler(lambda c: c.data.startswith("tplcatemoji:"))
async def template_category_emoji_prompt(cb: types.CallbackQuery):
    await open_template_category_prompt_from_callback(
        cb,
        "😀 Пришли один смайлик для подгруппы шаблона:",
        lambda category_idx: {
            "type": "template_category_emoji",
            "category_idx": category_idx,
            "back_to": {"view": "template_category_settings", "category_idx": category_idx},
        },
    )

@dp.callback_query_handler(lambda c: c.data.startswith("tplcatdelallask:"))
async def delete_template_category_all_ask(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, category_idx = cb.data.split(":")
    data = await load_data()
    ws, active, category = await get_template_category_context(data, wid, int(category_idx))
    if not ws:
        return
    await upsert_ws_menu(
        data,
        wid,
        workspace_path_title(ws, "⚙️ Шаблоны задач", rich_display_template_name(active), rich_display_category_name(category), "🗑 Удалить подгруппу с задачами?"),
        confirm_kb(f"tplcatdelall:{wid}:{category_idx}", f"tplcatset:{wid}:{category_idx}"),
    )

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

@dp.callback_query_handler(lambda c: c.data.startswith("tplcatdelask:"))
async def delete_template_category_keep_ask(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, category_idx = cb.data.split(":")
    data = await load_data()
    ws, active, category = await get_template_category_context(data, wid, int(category_idx))
    if not ws:
        return
    await upsert_ws_menu(
        data,
        wid,
        workspace_path_title(ws, "⚙️ Шаблоны задач", rich_display_template_name(active), rich_display_category_name(category), "🗑 Удалить подгруппу?"),
        confirm_kb(f"tplcatdel:{wid}:{category_idx}", f"tplcatset:{wid}:{category_idx}"),
    )

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
    await open_template_category_prompt_from_callback(
        cb,
        "✏️ Введи имя новой подгруппы-копии:",
        lambda category_idx: {
            "type": "copy_template_category",
            "category_idx": category_idx,
            "back_to": {"view": "template_category_settings", "category_idx": category_idx},
        },
    )

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
    await open_template_task_prompt_from_callback(
        cb,
        "✏️ Введи новое название задачи шаблона:",
        lambda ws, task_idx: {"type": "rename_template_task", "task_idx": task_idx, "back_to": {"view": "template_task", "task_idx": task_idx}},
    )

@dp.callback_query_handler(lambda c: c.data.startswith("tpltaskdeadline:"))
async def template_task_deadline_prompt(cb: types.CallbackQuery):
    await open_template_task_prompt_from_callback(
        cb,
        "⏰ Пришли срок для дедлайна, например: 3 дня, 7ч20м, 45 минут.",
        lambda ws, task_idx: {
            "type": "template_task_deadline",
            "task_idx": task_idx,
            "back_to": {"view": "template_task_deadline" if ws["template_tasks"][task_idx].get("deadline_seconds") else "template_task", "task_idx": task_idx},
        },
    )

@dp.callback_query_handler(lambda c: c.data.startswith("tpltaskdeadlinebox:"))
async def open_template_task_deadline_box(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, task_idx = cb.data.split(":")
    data = await load_data()
    await edit_template_task_deadline_menu(data, wid, int(task_idx))

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
    if message.text and message.text.startswith("/") and message.text.split()[0].lower() in {"/start", "/connect", "/mirror"}:
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
            asyncio.create_task(try_delete_user_message(message))
            return
        mode = awaiting.get("type")
        text = clean_text(message.text)
        if not text:
            asyncio.create_task(try_delete_user_message(message))
            return

        prompt_msg_id = awaiting.get("prompt_msg_id")
        back_to = awaiting.get("back_to", {"view": "ws"})
        report_followup = None
        report_followup_payload = {}

        def finish():
            ws["awaiting"] = None

        async def reject_input(error_text: str):
            await save_data_unlocked(data)
            asyncio.create_task(send_temp_message(ws["chat_id"], error_text, ws["thread_id"], delay=6))
            asyncio.create_task(try_delete_user_message(message))

        if mode == "new_company":
            if company_exists(ws, text):
                await reject_input("Такой список уже существует.")
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
                await reject_input("Такой список уже существует.")
                return
            ws["companies"][company_idx]["title"] = text
            finish()
            await save_data_unlocked(data)
            created_company = False
        elif mode == "company_emoji":
            if not is_single_emoji(text):
                await reject_input("Пришли один смайлик, балдабёб!")
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
                await reject_input("Такая подгруппа уже существует.")
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
                await reject_input("Такая подгруппа уже существует.")
                return
            category["title"] = text
            finish(); await save_data_unlocked(data); created_company = False
        elif mode == "category_emoji":
            if not is_single_emoji(text):
                await reject_input("Пришли один смайлик, балдабёб!")
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
            entry = find_completion_entry(get_report_history(company), company["tasks"][task_idx].get("done_event_id"))
            if entry and entry.get("canceled_at") is None:
                entry["task_text"] = company["tasks"][task_idx].get("text") or ""
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
                await reject_input(err)
                return
            task["deadline_started_at"] = started_at
            task["deadline_due_at"] = due_at
            finish(); await save_data_unlocked(data); created_company = False
        elif mode == "report_schedule_time":
            company_idx = awaiting["company_idx"]
            target_idx = awaiting.get("target_idx")
            draft_interval = clone_report_interval(awaiting.get("draft_interval") or {})
            kind = draft_interval.get("kind")
            flow = awaiting.get("flow")
            interval_idx = awaiting.get("interval_idx")
            if company_idx < 0 or company_idx >= len(ws["companies"]) or kind not in {"weekly", "daily", "monthly", "once"}:
                finish(); await save_data_unlocked(data); return
            if kind == "once":
                scheduled_at = parse_flexible_datetime(text)
                if scheduled_at is None or scheduled_at <= now_ts():
                    await reject_input("Дату введи корректно, барсурка стахановская")
                    return
                draft_interval["scheduled_at"] = scheduled_at
            elif kind == "monthly":
                parsed = parse_month_day_time(text)
                if parsed is None:
                    await reject_input("Пришли число и время, например: 30 20:44")
                    return
                day, hour, minute = parsed
                draft_interval["day"] = day
                draft_interval["hour"] = hour
                draft_interval["minute"] = minute
            else:
                parsed = parse_flexible_time(text)
                if parsed is None:
                    await reject_input("Пришли время, например: 21:30")
                    return
                hour, minute = parsed
                draft_interval["hour"] = hour
                draft_interval["minute"] = minute

            normalized_draft = ensure_report_interval(draft_interval) or draft_interval
            if flow == "edit" and interval_idx is not None:
                finish()
                await save_data_unlocked(data)
                created_company = False
                report_followup = "report_finalize"
                report_followup_payload = {
                    "company_idx": company_idx,
                    "interval_idx": interval_idx,
                    "flow": flow,
                    "draft_interval": normalized_draft,
                }
            else:
                ws["awaiting"] = {
                    "type": "report_accumulation_choice",
                    "company_idx": company_idx,
                    "target_idx": target_idx,
                    "interval_idx": interval_idx,
                    "flow": flow,
                    "draft_interval": normalized_draft,
                }
                await save_data_unlocked(data)
                created_company = False
                report_followup = "report_accumulation"
        elif mode == "report_accumulation_value":
            company_idx = awaiting["company_idx"]
            draft_interval = clone_report_interval(awaiting.get("draft_interval") or {})
            kind = draft_interval.get("kind")
            if company_idx < 0 or company_idx >= len(ws["companies"]) or kind not in {"weekly", "daily", "monthly", "once"}:
                finish(); await save_data_unlocked(data); return
            if kind == "once":
                start_at = parse_flexible_datetime(text)
                if start_at is None or start_at >= (draft_interval.get("scheduled_at") or 0):
                    await reject_input("Пришли точную дату и время раньше даты отчета.")
                    return
                draft_interval["accumulation"] = {"mode": "specific", "type": "datetime", "start_at": start_at}
            elif kind == "monthly":
                parsed = parse_month_day_time(text)
                if parsed is None:
                    await reject_input("Пришли число и время, например: 15 08:30")
                    return
                day, hour, minute = parsed
                draft_interval["accumulation"] = {"mode": "specific", "type": "month_day", "day": day, "hour": hour, "minute": minute}
            else:
                start_at = parse_flexible_datetime(text)
                if start_at is None or start_at >= report_preview_occurrence(draft_interval):
                    await reject_input("Пришли точную дату и время раньше даты отчета.")
                    return
                draft_interval["accumulation"] = {"mode": "specific", "type": "datetime", "start_at": start_at}

            finish()
            await save_data_unlocked(data)
            created_company = False
            report_followup = "report_finalize"
            report_followup_payload = {
                "company_idx": company_idx,
                "interval_idx": awaiting.get("interval_idx"),
                "flow": awaiting.get("flow"),
                "draft_interval": draft_interval,
            }
        elif mode == "template_report_schedule_time":
            draft_interval = clone_report_interval(awaiting.get("draft_interval") or {})
            kind = draft_interval.get("kind")
            flow = awaiting.get("flow")
            interval_idx = awaiting.get("interval_idx")
            if kind not in {"weekly", "daily", "monthly"}:
                finish(); await save_data_unlocked(data); return
            if kind == "monthly":
                parsed = parse_month_day_time(text)
                if parsed is None:
                    await reject_input("Пришли число и время, например: 30 20:44")
                    return
                day, hour, minute = parsed
                draft_interval["day"] = day
                draft_interval["hour"] = hour
                draft_interval["minute"] = minute
            else:
                parsed = parse_flexible_time(text)
                if parsed is None:
                    await reject_input("Пришли время, например: 21:30")
                    return
                hour, minute = parsed
                draft_interval["hour"] = hour
                draft_interval["minute"] = minute

            normalized_draft = ensure_report_interval(draft_interval) or draft_interval
            if flow == "edit" and interval_idx is not None:
                finish()
                await save_data_unlocked(data)
                created_company = False
                report_followup = "template_report_finalize"
                report_followup_payload = {
                    "interval_idx": interval_idx,
                    "flow": flow,
                    "draft_interval": normalized_draft,
                }
            else:
                ws["awaiting"] = {
                    "type": "template_report_accumulation_choice",
                    "interval_idx": interval_idx,
                    "flow": flow,
                    "draft_interval": normalized_draft,
                }
                await save_data_unlocked(data)
                created_company = False
                report_followup = "template_report_accumulation"
        elif mode == "template_report_accumulation_value":
            draft_interval = clone_report_interval(awaiting.get("draft_interval") or {})
            kind = draft_interval.get("kind")
            if kind == "monthly":
                parsed = parse_month_day_time(text)
                if parsed is None:
                    await reject_input("Пришли число и время, например: 15 08:30")
                    return
                day, hour, minute = parsed
                draft_interval["accumulation"] = {"mode": "specific", "type": "month_day", "day": day, "hour": hour, "minute": minute}
            else:
                start_at = parse_flexible_datetime(text)
                if start_at is None or start_at >= report_preview_occurrence(draft_interval):
                    await reject_input("Пришли точную дату и время раньше даты отчета.")
                    return
                draft_interval["accumulation"] = {"mode": "specific", "type": "datetime", "start_at": start_at}

            finish()
            await save_data_unlocked(data)
            created_company = False
            report_followup = "template_report_finalize"
            report_followup_payload = {
                "interval_idx": awaiting.get("interval_idx"),
                "flow": awaiting.get("flow"),
                "draft_interval": draft_interval,
            }
        elif mode == "new_template_category":
            if category_exists(ws.get("template_categories", []), text):
                await reject_input("Такая подгруппа уже существует.")
                return
            ws.setdefault("template_categories", []).append({"id": uuid.uuid4().hex, "title": text, "emoji": "📁"})
            finish(); await save_data_unlocked(data); created_company = False
        elif mode == "rename_template_category":
            category_idx = awaiting["category_idx"]
            if category_idx < 0 or category_idx >= len(ws.get("template_categories", [])):
                finish(); await save_data_unlocked(data); return
            category = ws["template_categories"][category_idx]
            if category_exists(ws.get("template_categories", []), text, exclude_id=category["id"]):
                await reject_input("Такая подгруппа уже существует.")
                return
            category["title"] = text
            finish(); await save_data_unlocked(data); created_company = False
        elif mode == "template_category_emoji":
            if not is_single_emoji(text):
                await reject_input("Пришли один смайлик, балдабёб!")
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
                await reject_input(err)
                return
            ws["template_tasks"][task_idx]["deadline_seconds"] = seconds
            finish(); await save_data_unlocked(data); created_company = False
        elif mode == "copy_company":
            source_idx = awaiting["company_idx"]
            if source_idx < 0 or source_idx >= len(ws.get("companies", [])) or company_exists(ws, text):
                await reject_input("Такой список уже существует.")
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
                await reject_input("Такая подгруппа уже существует.")
                return
            copy_category_into_company(company, category_idx, text)
            finish(); await save_data_unlocked(data); created_company = False
        elif mode == "new_template_set":
            if template_exists(ws.get("templates", []), text):
                await reject_input("Такой шаблон уже существует.")
                return
            tpl = make_template(text)
            ws.setdefault("templates", []).append(tpl)
            set_active_template(ws, tpl["id"])
            finish(); await save_data_unlocked(data); created_company = False
        elif mode == "rename_template_set":
            tpl = get_active_template(ws)
            if template_exists(ws.get("templates", []), text, exclude_id=tpl["id"]):
                await reject_input("Такой шаблон уже существует.")
                return
            tpl["title"] = text
            finish(); await save_data_unlocked(data); created_company = False
        elif mode == "template_set_emoji":
            if not is_single_emoji(text):
                await reject_input("Пришли один смайлик, балдабёб!")
                return
            tpl = get_active_template(ws)
            tpl["emoji"] = text
            set_active_template(ws, tpl["id"])
            finish(); await save_data_unlocked(data); created_company = False
        elif mode == "copy_template_set":
            tpl = get_active_template(ws)
            if template_exists(ws.get("templates", []), text):
                await reject_input("Такой шаблон уже существует.")
                return
            new_tpl = copy_template_payload(tpl, text)
            ws.setdefault("templates", []).append(new_tpl)
            set_active_template(ws, new_tpl["id"])
            finish(); await save_data_unlocked(data); created_company = False
        elif mode == "copy_template_category":
            category_idx = awaiting["category_idx"]
            if category_idx < 0 or category_idx >= len(ws.get("template_categories", [])) or category_exists(ws.get("template_categories", []), text):
                await reject_input("Такая подгруппа уже существует.")
                return
            tpl = get_active_template(ws)
            copy_template_category(tpl, category_idx, text)
            set_active_template(ws, tpl["id"])
            finish(); await save_data_unlocked(data); created_company = False
        else:
            await save_data_unlocked(data)
            asyncio.create_task(try_delete_user_message(message))
            return

    if prompt_msg_id and prompt_msg_id != ws.get("menu_msg_id"):
        await safe_delete_message(message.chat.id, prompt_msg_id)
    await try_delete_user_message(message)
    fresh = await load_data()
    ws = fresh["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    if report_followup == "report_accumulation":
        await edit_report_accumulation_menu(fresh, wid)
        return
    if report_followup == "template_report_accumulation":
        await edit_report_accumulation_menu(fresh, wid)
        return
    if report_followup == "report_finalize":
        await finalize_report_interval(
            wid,
            report_followup_payload.get("company_idx"),
            report_followup_payload.get("draft_interval") or {},
            report_followup_payload.get("flow"),
            report_followup_payload.get("interval_idx"),
        )
        return
    if report_followup == "template_report_finalize":
        await finalize_template_report_interval(
            wid,
            report_followup_payload.get("draft_interval") or {},
            report_followup_payload.get("flow"),
            report_followup_payload.get("interval_idx"),
        )
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
    await open_wid_menu_from_callback(cb, edit_templates_root_menu)

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
    await open_wid_menu_from_callback(cb, edit_template_settings_menu)

@dp.callback_query_handler(lambda c: c.data.startswith("tplnewset:"))
async def add_template_set_prompt(cb: types.CallbackQuery):
    await open_wid_prompt_from_callback(cb, "✏️ Введи название нового шаблона:", {"type": "new_template_set", "back_to": {"view": "template_root"}})

@dp.callback_query_handler(lambda c: c.data.startswith("tplrenameset:"))
async def rename_template_set_prompt(cb: types.CallbackQuery):
    await open_wid_prompt_from_callback(cb, "✏️ Введи новое название шаблона:", {"type": "rename_template_set", "back_to": {"view": "template_settings"}})

@dp.callback_query_handler(lambda c: c.data.startswith("tplemojiset:"))
async def template_set_emoji_prompt(cb: types.CallbackQuery):
    await open_wid_prompt_from_callback(cb, "😀 Пришли один смайлик для шаблона:", {"type": "template_set_emoji", "back_to": {"view": "template_settings"}})

@dp.callback_query_handler(lambda c: c.data.startswith("tplcopy:"))
async def copy_template_set_prompt(cb: types.CallbackQuery):
    await open_wid_prompt_from_callback(cb, "✏️ Введи название копии шаблона:", {"type": "copy_template_set", "back_to": {"view": "template_settings"}})

@dp.callback_query_handler(lambda c: c.data.startswith("tpldelsetask:"))
async def delete_template_set_ask(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    wid = cb.data.split(":", 1)[1]
    data = await load_data()
    ws, active = get_connected_active_template(data, wid)
    if not ws:
        return
    await upsert_ws_menu(
        data,
        wid,
        workspace_path_title(ws, "⚙️ Шаблоны задач", rich_display_template_name(active), "🗑 Удалить шаблон?"),
        confirm_kb(f"tpldelset:{wid}", f"tplsettings:{wid}"),
    )

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
            ws["templates"] = [make_template()]
            set_active_template(ws, ws["templates"][0]["id"])
        await save_data_unlocked(data)
    fresh = await load_data()
    await edit_templates_root_menu(fresh, wid)

async def deadline_refresh_worker():
    last_report_tick = None
    last_deadline_tick = None
    while True:
        now = datetime.now(TIMEZONE)
        report_tick = now.replace(second=0, microsecond=0)
        deadline_tick = (now.year, now.month, now.day, now.hour, now.minute // 10)

        if report_tick != last_report_tick:
            last_report_tick = report_tick
            try:
                data = await load_data()
                changed = False
                now_value = int(report_tick.timestamp())
                for ws in data.get("workspaces", {}).values():
                    if not ws.get("is_connected"):
                        continue
                    for idx in range(len(ws.get("companies", []))):
                        if await publish_company_reports(ws, idx, now_value):
                            changed = True
                if changed:
                    await save_data(data)
            except Exception:
                pass

        if now.minute % 10 == 0 and deadline_tick != last_deadline_tick:
            last_deadline_tick = deadline_tick
            try:
                data = await load_data()
                changed = False
                for ws in data.get("workspaces", {}).values():
                    if not ws.get("is_connected"):
                        continue
                    for idx in range(len(ws.get("companies", []))):
                        company = ws["companies"][idx]
                        if not any(
                            isinstance(task.get("deadline_due_at"), int) and not task.get("done")
                            for task in company.get("tasks", [])
                        ):
                            continue
                        if await sync_company_everywhere(ws, idx):
                            changed = True
                if changed:
                    await save_data(data)
            except Exception:
                pass

        await asyncio.sleep(5 if now.second >= 55 else max(1, 60 - now.second))

# =========================
# RUN
# =========================

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(deadline_refresh_worker())
    executor.start_polling(dp, skip_updates=True)
