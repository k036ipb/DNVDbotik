import os
import json
import math
import asyncio
import time
import random
import uuid
import copy
import re
import html
from datetime import datetime
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils import executor
from aiogram.utils.exceptions import MessageNotModified, RetryAfter
from reporting_core import (
    add_task_completion_event,
    cancel_task_completion_event,
    clear_reporting_runtime_cache,
    clone_report_interval,
    collect_report_entries,
    default_reporting,
    ensure_explicit_report_targets,
    ensure_report_interval,
    ensure_report_target,
    ensure_reporting,
    find_completion_entry,
    find_report_interval,
    format_report_period_preview,
    format_report_schedule_label,
    format_report_timestamp,
    get_effective_report_targets,
    get_report_history,
    get_report_intervals,
    get_report_target,
    get_target_report_pairs,
    missing_mirrors_for_report_targets,
    missing_report_targets_for_mirrors,
    next_report_occurrence_after,
    normalize_company_report_target_keys,
    parse_optional_index,
    prepare_report_interval_draft,
    report_interval_sort_key,
    report_preview_occurrence,
    report_schedule_prompt_text,
    report_target_key,
    resolve_report_period,
    upsert_report_interval,
)

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
RUNTIME_VIEW_CACHE: dict[tuple[int, int], tuple[str, float]] = {}
RECENT_CALLBACKS: dict[tuple[int, int, str], float] = {}
CALLBACK_DEBOUNCE_SECONDS = 0.9
RUNTIME_VIEW_CACHE_TTL_SECONDS = 2.0
RUNTIME_VIEW_CACHE_LIMIT = 10000
DATA_CACHE: dict | None = None

# =========================
# LOW LEVEL HELPERS
# =========================

def now_ts() -> int:
    return int(time.time())

def _runtime_view_payload(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): _runtime_view_payload(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (list, tuple)):
        return [_runtime_view_payload(item) for item in value]
    values = getattr(value, "values", None)
    if isinstance(values, dict):
        return _runtime_view_payload(values)
    return str(value)

def build_runtime_view_signature(text: str, reply_markup=None, disable_web_page_preview: bool = False) -> str:
    markup_payload = None
    inline_keyboard = getattr(reply_markup, "inline_keyboard", None) if reply_markup is not None else None
    if inline_keyboard is not None:
        markup_payload = [
            [_runtime_view_payload(getattr(button, "values", None) or button) for button in row]
            for row in inline_keyboard
        ]
    elif reply_markup is not None:
        markup_payload = _runtime_view_payload(getattr(reply_markup, "values", None) or reply_markup)
    return json.dumps(
        {
            "text": text,
            "disable_web_page_preview": disable_web_page_preview,
            "reply_markup": markup_payload,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )

def forget_runtime_view(chat_id: int, message_id: int | None):
    if message_id:
        RUNTIME_VIEW_CACHE.pop((chat_id, message_id), None)

def get_recent_runtime_view_signature(chat_id: int, message_id: int) -> str | None:
    payload = RUNTIME_VIEW_CACHE.get((chat_id, message_id))
    if not payload:
        return None
    signature, expires_at = payload
    if expires_at < time.monotonic():
        RUNTIME_VIEW_CACHE.pop((chat_id, message_id), None)
        return None
    return signature

def remember_runtime_view(chat_id: int, message_id: int, text: str, reply_markup=None, disable_web_page_preview: bool = False, signature: str | None = None):
    now_value = time.monotonic()
    RUNTIME_VIEW_CACHE[(chat_id, message_id)] = (
        signature or build_runtime_view_signature(text, reply_markup=reply_markup, disable_web_page_preview=disable_web_page_preview),
        now_value + RUNTIME_VIEW_CACHE_TTL_SECONDS,
    )
    if len(RUNTIME_VIEW_CACHE) <= RUNTIME_VIEW_CACHE_LIMIT:
        return
    for key, (_, expires_at) in list(RUNTIME_VIEW_CACHE.items()):
        if expires_at < now_value:
            RUNTIME_VIEW_CACHE.pop(key, None)
    while len(RUNTIME_VIEW_CACHE) > RUNTIME_VIEW_CACHE_LIMIT:
        RUNTIME_VIEW_CACHE.pop(next(iter(RUNTIME_VIEW_CACHE)), None)

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
    forget_runtime_view(chat_id, message_id)
    try:
        await tg_call(lambda: bot.delete_message(chat_id, message_id), retries=1)
    except Exception:
        pass

async def try_edit_text(chat_id: int, message_id: int, text: str, reply_markup=None, disable_web_page_preview: bool = False) -> bool:
    signature = build_runtime_view_signature(text, reply_markup=reply_markup, disable_web_page_preview=disable_web_page_preview)
    if get_recent_runtime_view_signature(chat_id, message_id) == signature:
        return True
    try:
        await tg_call(
            lambda: bot.edit_message_text(
                text,
                chat_id,
                message_id,
                reply_markup=reply_markup,
                parse_mode="HTML",
                disable_web_page_preview=disable_web_page_preview,
            ),
            retries=1,
        )
        remember_runtime_view(chat_id, message_id, text, reply_markup=reply_markup, disable_web_page_preview=disable_web_page_preview, signature=signature)
        return True
    except MessageNotModified:
        remember_runtime_view(chat_id, message_id, text, reply_markup=reply_markup, disable_web_page_preview=disable_web_page_preview, signature=signature)
        return True
    except Exception:
        forget_runtime_view(chat_id, message_id)
        return False

async def safe_edit_text(chat_id: int, message_id: int, text: str, reply_markup=None, disable_web_page_preview: bool = False):
    await try_edit_text(chat_id, message_id, text, reply_markup=reply_markup, disable_web_page_preview=disable_web_page_preview)

async def send_message(chat_id: int, text: str, reply_markup=None, thread_id: int = 0, disable_web_page_preview: bool = False):
    message = await tg_call(
        lambda: bot.send_message(
            chat_id,
            text,
            reply_markup=reply_markup,
            parse_mode="HTML",
            disable_web_page_preview=disable_web_page_preview,
            **({"message_thread_id": thread_id} if thread_id else {}),
        ),
        retries=1,
    )
    remember_runtime_view(chat_id, message.message_id, text, reply_markup=reply_markup, disable_web_page_preview=disable_web_page_preview)
    return message

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


def _read_data_file() -> dict:
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_data_file(data: dict):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))

EMOJI_VARIATION_CHARS = {"\ufe0f", "\u200d"}
DEFAULT_BINDING_EMOJI = "рџ“¬"
BINDING_RITUAL_LINES = (
    "Р¤С‹РІРґР°РЅРё.",
    "РџРћРљР›РћРќР•РќРР•! РџРћРљРђРЇРќРР•!",
    "РЈС‚РѕРїРё РјРµРЅСЏ! РЈРўРћРџРР РњР•РќРЇРЇРЇ! РљР°Рє РЅРµРЅСѓР¶РЅРѕРµ, Р±РµСЃРїРѕР»РµР·РЅРѕРµ...",
    "РџСѓСЃС‚СЊ РјРѕР»С‡Р°С‚ Р»СЋРґРё, РїСѓСЃС‚СЊ РіРѕРІРѕСЂСЏС‚ СЃС‚РІРѕР»С‹",
    "РџРѕР№РґРµРј-РєР° РїРѕРєСѓСЂРёРј-РєР°.",
    "Рђ РќРЈ-РљРђ Р’РЎРўРђРўР¬!!!",
    "РЎР»Р°РґРєРѕ РґСѓРЅСѓС‚СЊ.",
    "РџРѕР·РІРѕРЅРёС‚СЊ РјР°РјРµ.",
    "РџРѕРєРѕСЂРјРёС‚СЊ РєРѕС‚Р°.",
    "РџРѕР»РёС‚СЊ С†РІРµС‚С‹.",
    "РћРіСѓСЂС†С‹ 5 С€С‚.",
    "Рћ СЃР»РµРїРёС‚Рµ РјРЅРµ РјР°СЃРєСѓ РѕС‚ РґРѕРЅРѕСЃС‡РёРІС‹С… РіР»Р°Р·.",
    "Р’Р»РµР·С‚СЊ РІ РєСЂРµРґРёС‚С‹ Рё СЃРїРёС‚СЃСЏ.",
    "Darude - Sandstorm.",
    "РџРћРљР›РћРќР•РќРР•! РџРћРљРђРЇРќРР•!",
    "Р’РЅРёРјР°РЅРёРµ! РћР±СЉСЏРІР»РµРЅР° РѕРїР°СЃРЅРѕСЃС‚СЊ Р‘РџР›Рђ РІРѕР·РґСѓС€РЅРѕРј РїСЂРѕСЃС‚СЂР°РЅСЃС‚РІРµ РЁР°СЂР°СЂР°РјР°.",
    "РќР°С‡Р°Р»СЊРЅРёРєСѓ РІС‚РѕСЂРѕРіРѕ РѕС‚РґРµР»РµРЅРёСЏ РїСЂРёР±С‹С‚СЊ РІ 314-Р№ РєР°Р±РёРЅРµС‚.",
    "РЇРІРёС‚СЊСЃСЏ РІ РІРѕРµРЅРєРѕРјР°С‚.",
    "РР·Р±РёС‚СЊ Р‘РћРњР–Р°.",
    "РџРѕР»СѓС‡РёС‚СЊ РіСЂР°Р¶РґР°РЅСЃС‚РІРѕ РР·СЂР°РёР»СЏ.",
    "Р’СЃС‚СѓРїРёС‚СЊ РІ СЃРѕСЃС‚Р°РІ Р РѕСЃСЃРёР№СЃРєРѕР№ Р¤РµРґРµСЂР°С†РёРё.",
    "РџРѕРјРѕС‡СЊ РјР°РјРµ Рё РїРѕРіСѓР»СЏС‚СЊ СЃ СЃРѕР±Р°РєРѕР№.",
    "Р“РѕР»РѕСЃСѓР№, РёР»Рё РїСЂРѕРёРіСЂР°РµС€СЊ.",
    "Maria, you've gotta see her.",
    "Р’.РЎ.РЃ!",
    "Р—Р°С‰РёС‰Р°Р№СЃСЏ! Р—Р°С‰РёС‰Р°Р№СЃСЏ!",
    "РћР±РѕСЃСЃР°С‚СЊ РіРѕСЏ.",
    "Р—Р°РїР»Р°С‚РёС‚СЊ РЅР°Р»РѕРіРё.",
    "РџСЂРёРЅСЏС‚СЊ РёСЃР»Р°Рј.",
    "РћСЃС‚Р°РІСЊ РјРЅРµ РґРѕРєСѓСЂРёС‚СЊ Р±С‹С‡РѕРє.",
    "РђРЅР¶СѓРјР°РЅСЏ.",
    "РќР°Р№РґРё СЂРµС€РµРЅРёРµ РіРёРїРѕС‚РµР·С‹ Р РёРјР°РЅР°.",
    "РЎР»РѕР¶Рё РѕСЂСѓР¶РёРµ Рё РІС‹С…РѕРґРё СЃ РїРѕРґРЅСЏС‚С‹РјРё СЂСѓРєР°РјРё.",
    "Р›РёС€РёСЃСЊ РґРµРІСЃС‚РІРµРЅРЅРѕСЃС‚Рё.",
    "РЎРєРёРЅСЊ РґРѕРјР°С€РєСѓ.",
    "РџРѕРєР°Р¶Рё СЃРёСЃСЊРєРё.",
    "Wake up, Neo.",
    "РЈРґР°Р»РёС‚СЊ РїР°РїРєСѓ system32.",
    "Profit!",
    ("Р“Р”Р— РѕС‚ РџСѓС‚РёРЅР°", "https://www.youtube.com/watch?v=dQw4w9WgXcQ"),
)

def is_single_emoji(text: str) -> bool:
    text = (text or "").strip()
    if not text or " " in text or any(ch.isalnum() for ch in text):
        return False
    cleaned = "".join(ch for ch in text if ch not in EMOJI_VARIATION_CHARS)
    return 1 <= len(cleaned) <= 4

def clean_binding_label_text(text: str | None) -> str:
    return re.sub(r"\s+", " ", clean_text(text or "")).strip()

def fit_button_text(text: str, limit: int = 60) -> str:
    normalized = clean_binding_label_text(text) or "..."
    if len(normalized) <= limit:
        return normalized
    return f"{normalized[:limit - 1].rstrip()}вЂ¦"

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
        ensure_task({"text": "РЎРѕР·РґР°С‚СЊ РґРѕРіРѕРІРѕСЂ"}, is_template=True),
        ensure_task({"text": "Р’С‹СЃС‚Р°РІРёС‚СЊ СЃС‡С‘С‚"}, is_template=True),
    ]

def make_template(title: str = "РЁР°Р±Р»РѕРЅ", tasks: list[dict] | None = None, categories: list[dict] | None = None) -> dict:
    return {
        "id": uuid.uuid4().hex,
        "title": title,
        "emoji": "рџ“Ѓ",
        "deadline_format": "relative",
        "reporting": default_reporting(),
        "tasks": tasks or [],
        "categories": categories or [],
    }

def ensure_category(cat):
    if not isinstance(cat, dict):
        cat = {"title": str(cat), "emoji": "рџ“Ѓ"}
    cat.setdefault("id", uuid.uuid4().hex)
    cat["emoji"] = cat.get("emoji") or "рџ“Ѓ"
    cat["title"] = cat.get("title") or "РџРѕРґРіСЂСѓРїРїР°"
    cat.setdefault("deadline_format", None)
    return cat

def ensure_company(company):
    if not isinstance(company, dict):
        company = {}

    company.setdefault("id", uuid.uuid4().hex)
    company["emoji"] = company.get("emoji") or "рџ“Ѓ"
    company["title"] = company.get("title") or "РЎРїРёСЃРѕРє"
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
        tpl.setdefault("title", "РЁР°Р±Р»РѕРЅ")
        tpl.setdefault("emoji", "рџ“Ѓ")
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
    return f"{esc(company.get('emoji') or 'рџ“Ѓ')}{esc(company.get('title') or 'РЎРїРёСЃРѕРє')}"

def rich_display_category_name(category: dict) -> str:
    return f"{esc(category.get('emoji') or 'рџ“Ѓ')}{esc(category.get('title') or 'РџРѕРґРіСЂСѓРїРїР°')}"

def rich_display_template_name(template: dict) -> str:
    return f"<u>{esc(template.get('emoji') or 'рџ“Ѓ')}{esc(template.get('title') or 'РЁР°Р±Р»РѕРЅ')}</u>"

def rich_task_text(task_text: str, done: bool = False) -> str:
    if done:
        return f"<s><i>{esc(task_text)}</i></s>"
    return f"<b><i>{esc(task_text)}</i></b>"

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
        'emoji': company.get('emoji') or 'рџ“Ѓ',
        'card_msg_id': None,
        'mirrors': [],
        'deadline_format': company.get('deadline_format') or 'relative',
        'categories': [],
        'tasks': [],
    }
    for category in company.get('categories', []):
        new_id = uuid.uuid4().hex
        category_map[category['id']] = new_id
        new_company['categories'].append({'id': new_id, 'title': category.get('title') or 'РџРѕРґРіСЂСѓРїРїР°', 'emoji': category.get('emoji') or 'рџ“Ѓ', 'deadline_format': category.get('deadline_format')})
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
        'emoji': source.get('emoji') or 'рџ“Ѓ',
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
        'emoji': source.get('emoji') or 'рџ“Ѓ',
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
        'emoji': template.get('emoji') or 'рџ“Ѓ',
        'deadline_format': template.get('deadline_format') or 'relative',
        'reporting': ensure_reporting(copy.deepcopy(template.get('reporting'))),
        'categories': [],
        'tasks': [],
    }
    for category in template.get('categories', []):
        new_id = uuid.uuid4().hex
        category_map[category['id']] = new_id
        new_tpl['categories'].append({'id': new_id, 'title': category.get('title') or 'РџРѕРґРіСЂСѓРїРїР°', 'emoji': category.get('emoji') or 'рџ“Ѓ', 'deadline_format': category.get('deadline_format')})
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
        try:
            _, raw_thread_id = str(key).rsplit("_", 1)
            topic_thread_id = int(raw_thread_id)
        except Exception:
            topic_thread_id = 0
        entry = {}
        if value.get("chat_title"):
            entry["chat_title"] = sanitize_binding_chat_title(str(value["chat_title"]), str(value.get("topic_title") or ""), topic_thread_id)
        topic_title = str(value.get("topic_title") or "").strip()
        topic_title_source = value.get("topic_title_source")
        if topic_title and topic_title_source in {"created", "edited"}:
            entry["topic_title"] = topic_title
            entry["topic_title_source"] = topic_title_source
        custom_label = clean_binding_label_text(str(value.get("custom_label") or ""))
        if custom_label:
            entry["custom_label"] = custom_label
        custom_emoji = clean_text(str(value.get("custom_emoji") or ""))
        if is_single_emoji(custom_emoji):
            entry["custom_emoji"] = custom_emoji
        if entry:
            normalized_topics[key] = entry
    data["known_topics"] = normalized_topics

    for uid, user in list(data["users"].items()):
        if not isinstance(user, dict):
            data["users"][uid] = {}
            user = data["users"][uid]
        user.setdefault("workspaces", [])
        user.setdefault("pm_menu_msg_id", None)
        user.setdefault("pm_awaiting", None)
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
        ws["chat_title"] = sanitize_binding_chat_title(ws.get("chat_title"), ws.get("topic_title"), ws.get("thread_id") or 0)

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
    global DATA_CACHE
    if DATA_CACHE is not None:
        return DATA_CACHE
    if not os.path.exists(DATA_FILE):
        clear_reporting_runtime_cache()
        DATA_CACHE = normalize_data(default_data())
        return DATA_CACHE
    try:
        clear_reporting_runtime_cache()
        DATA_CACHE = normalize_data(await asyncio.to_thread(_read_data_file))
    except Exception:
        clear_reporting_runtime_cache()
        DATA_CACHE = normalize_data(default_data())
    return DATA_CACHE

async def save_data_unlocked(data):
    global DATA_CACHE
    if not isinstance(data, dict):
        data = default_data()
    DATA_CACHE = data
    await asyncio.to_thread(_write_data_file, data)

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
            "pm_awaiting": None,
            "ui_pages": {},
        },
    )
    return data["users"][user_id]

def make_ws_id(chat_id: int, thread_id: int | None):
    return f"{chat_id}_{thread_id or 0}"

def clean_text(text: str) -> str:
    return (text or "").strip().lstrip("/").strip()

def sanitize_binding_chat_title(chat_title: str | None, topic_title: str | None, thread_id: int) -> str | None:
    title = (chat_title or "").strip()
    if not title:
        return None
    if not thread_id:
        return title
    if topic_title:
        suffix = f" - {topic_title.strip()}"
        if title.endswith(suffix):
            title = title[:-len(suffix)].rstrip()
    title = re.sub(r"\s*-\s*РўСЂРµРґ\s+\d+\s*$", "", title, flags=re.IGNORECASE).strip()
    return title or None

def workspace_full_name(chat_title: str, topic_title: str | None, thread_id: int) -> str:
    if thread_id:
        return f"{(chat_title or 'Р§Р°С‚').strip()} - {(topic_title or f'РўСЂРµРґ {thread_id}').strip()}"
    return chat_title

def is_personal_workspace(ws: dict | None) -> bool:
    if not isinstance(ws, dict):
        return False
    return str(ws.get("id") or "").startswith("pm_")

def strip_leading_label_emoji(label: str | None) -> str:
    raw = clean_binding_label_text(label or "")
    if not raw:
        return ""
    head, sep, tail = raw.partition(" ")
    if sep and is_single_emoji(head):
        return tail.strip()
    return raw

def workspace_title_label(ws: dict) -> str:
    if is_personal_workspace(ws):
        base = strip_leading_label_emoji(str(ws.get("name") or "")) or "Р›РёС‡РЅС‹Р№ workspace"
        return f"рџ‘¤ {esc(base)}"
    return esc(ws.get("name") or "Workspace")

def workspace_home_title(ws: dict) -> str:
    return f"{workspace_title_label(ws)}:"

def extract_message_topic_title(message: types.Message) -> str | None:
    if getattr(message, "forum_topic_created", None):
        created_name = getattr(message.forum_topic_created, "name", None)
        if created_name:
            return created_name
    if getattr(message, "forum_topic_edited", None):
        new_name = getattr(message.forum_topic_edited, "name", None)
        if new_name:
            return new_name
    return None

def extract_reply_topic_title(message: types.Message) -> tuple[str | None, str | None]:
    reply = getattr(message, "reply_to_message", None)
    if not reply:
        return None, None
    if getattr(reply, "forum_topic_edited", None):
        new_name = getattr(reply.forum_topic_edited, "name", None)
        if new_name:
            return new_name, "edited"
    if getattr(reply, "forum_topic_created", None):
        created_name = getattr(reply.forum_topic_created, "name", None)
        if created_name:
            return created_name, "created"
    return None, None

def resolve_message_topic_title(data: dict, message: types.Message) -> tuple[str | None, str | None]:
    thread_id = message.message_thread_id or 0
    if not thread_id:
        return None, None
    direct_title = extract_message_topic_title(message)
    if direct_title:
        source = "edited" if getattr(message, "forum_topic_edited", None) else "created"
        return direct_title, source
    entry = get_known_topic_entry(data, message.chat.id, thread_id) or {}
    if entry.get("topic_title") and entry.get("topic_title_source") in {"created", "edited"}:
        return entry.get("topic_title"), entry.get("topic_title_source")
    reply_title, reply_source = extract_reply_topic_title(message)
    if reply_title:
        return reply_title, reply_source
    return None, None

def get_known_topic_entry(data: dict, chat_id: int, thread_id: int) -> dict | None:
    entry = (data.get("known_topics") or {}).get(make_ws_id(chat_id, thread_id))
    return entry if isinstance(entry, dict) else None

def ensure_known_topic_entry(data: dict, chat_id: int, thread_id: int) -> dict:
    known_topics = data.setdefault("known_topics", {})
    if not isinstance(known_topics, dict):
        known_topics = {}
        data["known_topics"] = known_topics
    key = make_ws_id(chat_id, thread_id)
    entry = known_topics.get(key)
    if not isinstance(entry, dict):
        entry = {}
        known_topics[key] = entry
    return entry

def get_binding_custom_label(data: dict, chat_id: int, thread_id: int) -> str | None:
    entry = get_known_topic_entry(data, chat_id, thread_id) or {}
    label = clean_binding_label_text(str(entry.get("custom_label") or ""))
    return label or None

def set_binding_custom_label(data: dict, chat_id: int, thread_id: int, label: str | None) -> str | None:
    entry = ensure_known_topic_entry(data, chat_id, thread_id)
    custom_label = clean_binding_label_text(label)
    if custom_label:
        entry["custom_label"] = custom_label
        return custom_label
    entry.pop("custom_label", None)
    return None

def get_binding_emoji(data: dict, chat_id: int, thread_id: int) -> str:
    entry = get_known_topic_entry(data, chat_id, thread_id) or {}
    emoji = clean_text(str(entry.get("custom_emoji") or ""))
    return emoji if is_single_emoji(emoji) else DEFAULT_BINDING_EMOJI

def set_binding_emoji(data: dict, chat_id: int, thread_id: int, emoji: str | None) -> str | None:
    entry = ensure_known_topic_entry(data, chat_id, thread_id)
    custom_emoji = clean_text(emoji or "")
    if is_single_emoji(custom_emoji):
        entry["custom_emoji"] = custom_emoji
        return custom_emoji
    entry.pop("custom_emoji", None)
    return None

def decorate_binding_label(data: dict, chat_id: int, thread_id: int, label: str) -> str:
    base = clean_binding_label_text(label) or f"{chat_id}/{thread_id or 0}"
    return f"{get_binding_emoji(data, chat_id, thread_id)} {base}"

def find_workspace_by_binding(data: dict, chat_id: int, thread_id: int) -> dict | None:
    for ws in data.get("workspaces", {}).values():
        if ws.get("chat_id") == chat_id and (ws.get("thread_id") or 0) == thread_id:
            return ws
    return None

def resolve_binding_titles(data: dict, chat_id: int, thread_id: int, chat_title: str | None = None, topic_title: str | None = None) -> tuple[str | None, str | None]:
    ws = find_workspace_by_binding(data, chat_id, thread_id) or {}
    entry = get_known_topic_entry(data, chat_id, thread_id) or {}
    resolved_topic = topic_title if topic_title is not None else entry.get("topic_title")
    resolved_chat = chat_title or entry.get("chat_title") or ws.get("chat_title")
    return (
        sanitize_binding_chat_title(resolved_chat, resolved_topic, thread_id),
        resolved_topic,
    )

def remember_binding_place(data: dict, chat_id: int, thread_id: int, chat_title: str | None = None, topic_title: str | None = None, topic_title_source: str | None = None) -> tuple[str | None, str | None]:
    entry = ensure_known_topic_entry(data, chat_id, thread_id)

    if chat_title:
        entry["chat_title"] = sanitize_binding_chat_title(chat_title, topic_title, thread_id)
    if topic_title:
        entry["topic_title"] = topic_title
        if topic_title_source:
            entry["topic_title_source"] = topic_title_source
    elif not thread_id:
        entry.pop("topic_title", None)
        entry.pop("topic_title_source", None)

    return resolve_binding_titles(data, chat_id, thread_id, chat_title, topic_title)

def binding_place_label(
    data: dict,
    chat_id: int,
    thread_id: int,
    fallback_label: str | None = None,
    chat_title: str | None = None,
    topic_title: str | None = None,
) -> str:
    custom_label = get_binding_custom_label(data, chat_id, thread_id)
    if custom_label:
        return decorate_binding_label(data, chat_id, thread_id, custom_label)
    resolved_chat, resolved_topic = resolve_binding_titles(data, chat_id, thread_id, chat_title, topic_title)
    base_label = None
    if resolved_chat or thread_id:
        base_label = workspace_full_name(resolved_chat, resolved_topic, thread_id)
    else:
        base_label = fallback_label or f"{chat_id}/{thread_id or 0}"
    return decorate_binding_label(data, chat_id, thread_id, base_label)

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
    return f"{company.get('emoji') or 'рџ“Ѓ'}{company.get('title') or 'РЎРїРёСЃРѕРє'}"

def display_category_name(category: dict) -> str:
    return f"{category.get('emoji') or 'рџ“Ѓ'}{category.get('title') or 'РџРѕРґРіСЂСѓРїРїР°'}"

def workspace_path_title(ws: dict, *parts: str) -> str:
    lines = [f"{workspace_title_label(ws)}:"]
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
    parts.append(f"рџ“Њ {rich_task_text(task.get('text') or 'Р—Р°РґР°С‡Р°', bool(task.get('done')))}{esc(suffix)}")
    return workspace_path_title(ws, *parts)

def template_task_title(ws: dict, template: dict, task: dict, category: dict | None = None) -> str:
    parts = ["вљ™пёЏ РЁР°Р±Р»РѕРЅС‹ Р·Р°РґР°С‡", rich_display_template_name(template)]
    if category:
        parts.append(rich_display_category_name(category))
    parts.append(f"рџ“Њ {rich_task_text(task.get('text') or 'Р—Р°РґР°С‡Р°')}{esc(template_task_deadline_suffix(task))}")
    return workspace_path_title(ws, *parts)

def format_duration_text(seconds: int | None) -> str:
    if seconds is None:
        return ""
    minutes = max(0, math.ceil(seconds / 60))
    days, rem = divmod(minutes, 60 * 24)
    hours, mins = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days} Рґ.")
    if hours or days:
        parts.append(f"{hours} С‡.")
    parts.append(f"{mins} Рј.")
    return "; ".join(parts)

def template_task_deadline_suffix(task: dict) -> str:
    value = task.get("deadline_seconds")
    if not value:
        return ""
    return f" ({format_duration_text(value)})"

def display_task_deadline_suffix(task: dict, deadline_format: str = "relative", now_value: int | None = None) -> str:
    due_at = task.get("deadline_due_at")
    if not due_at:
        return ""
    if deadline_format == "date":
        return f" ({datetime.fromtimestamp(due_at, TIMEZONE).strftime('РґРѕ %d.%m.%Y Рі. %H:%M')})"
    return f" ({format_duration_text(due_at - (now_value if now_value is not None else now_ts()))})"

def task_deadline_icon(task: dict, now_value: int | None = None) -> str:
    if task.get("done"):
        return "рџ¦ѕ"
    due_at = task.get("deadline_due_at")
    started_at = task.get("deadline_started_at")
    if not due_at or not started_at:
        return "рџ¤ћ"
    now_value = now_value if now_value is not None else now_ts()
    if now_value >= due_at:
        return "рџ–•рџЏї"
    total = max(due_at - started_at, 1)
    elapsed = min(max(now_value - started_at, 0), total)
    elapsed_part = elapsed / total
    if elapsed_part <= 0.2:
        return "рџ¤ћрџЏ»"
    if elapsed_part <= 0.4:
        return "рџ¤ћрџЏј"
    if elapsed_part <= 0.6:
        return "рџ¤ћрџЏЅ"
    if elapsed_part <= 0.8:
        return "рџ¤ћрџЏѕ"
    return "рџ¤ћрџЏї"

def build_progress_bar(done_count: int, total_count: int) -> str:
    if total_count <= 0:
        progress = 0.0
    else:
        progress = (done_count / total_count) * 10.0

    full = int(progress)
    rem = progress - full
    cells = ["рџЊ•"] * full

    if len(cells) < 10:
        if rem <= 0:
            partial = "рџЊ‘"
        elif rem < 0.375:
            partial = "рџЊ"
        elif rem < 0.625:
            partial = "рџЊ—"
        elif rem < 0.875:
            partial = "рџЊ–"
        else:
            partial = "рџЊ•"
        cells.append(partial)

    cells = cells[:10] + ["рџЊ‘"] * max(0, 10 - len(cells[:10]))
    percent = 0.0 if total_count <= 0 else (done_count / total_count) * 100
    return f"<b>[ </b>{''.join(cells)} <b>{percent:.1f} % ]</b>"

def group_task_entries(tasks: list[dict]) -> tuple[list[tuple[int, dict]], dict[str, list[tuple[int, dict]]], int, dict[str, int]]:
    root_entries: list[tuple[int, dict]] = []
    entries_by_category: dict[str, list[tuple[int, dict]]] = {}
    done_total = 0
    done_by_category: dict[str, int] = {}
    for task_idx, task in enumerate(tasks):
        if task.get("done"):
            done_total += 1
        category_id = task.get("category_id")
        entry = (task_idx, task)
        if category_id:
            entries_by_category.setdefault(category_id, []).append(entry)
            if task.get("done"):
                done_by_category[category_id] = done_by_category.get(category_id, 0) + 1
        else:
            root_entries.append(entry)
    return root_entries, entries_by_category, done_total, done_by_category

def pm_main_text(user_id: str, data: dict) -> str:
    user = ensure_user(data, user_id)
    ws_ids = [wid for wid in user.get("workspaces", []) if data["workspaces"].get(wid, {}).get("is_connected")]
    if not ws_ids:
        return "рџ“‚ Р’Р°С€Рё workspace: РќРµС‚ workspace"
    return "рџ“‚ Р’Р°С€Рё workspace:"

def generate_mirror_token() -> str:
    return uuid.uuid4().hex[:8].upper()

def binding_instruction_text(title: str, token: str) -> str:
    return (
        f"{title}:\n"
        f"{instruction_step_html(1, 'Р”РѕР±Р°РІРёС‚СЊ РјРµРЅСЏ РІ РЅСѓР¶РЅСѓСЋ РєРѕРЅС„Сѓ;')}\n"
        f"{instruction_step_html(2, 'РџРµСЂРµР№С‚Рё РІ РЅСѓР¶РЅС‹Р№ С‚СЂРµРґ;')}\n"
        f"{instruction_step_html(3, 'РћС‚РїСЂР°РІРёС‚СЊ РєРѕРјР°РЅРґСѓ:')}\n"
        f"<code>/mirror {esc(token)}</code>\n"
        f"{instruction_step_html(4, random_instruction_variant_html(), is_html=True)}"
    )

def random_instruction_variant_html() -> str:
    variant = random.choice(BINDING_RITUAL_LINES)
    if isinstance(variant, tuple):
        label, url = variant
        return f'<a href="{esc(url)}">{esc(label)}</a>.'
    return esc(variant)

def instruction_step_html(number: int, content: str, is_html: bool = False) -> str:
    body = content if is_html else esc(content)
    return f"<b><i>{number})</i></b> <i>{body}</i>"

def workspace_connect_instruction_text() -> str:
    return (
        "рџ“Њ РљР°Рє РїРѕРґРєР»СЋС‡РёС‚СЊ workspace:\n"
        f"{instruction_step_html(1, 'Р”РѕР±Р°РІРёС‚СЊ РјРµРЅСЏ РІ РЅСѓР¶РЅСѓСЋ РіСЂСѓРїРїСѓ;')}\n"
        f"{instruction_step_html(2, 'РџРµСЂРµР№С‚Рё РІ РЅСѓР¶РЅС‹Р№ С‚СЂРµРґ;')}\n"
        f"{instruction_step_html(3, 'РћС‚РїСЂР°РІРёС‚СЊ РєРѕРјР°РЅРґСѓ:')}\n"
        "<code>/connect</code>\n"
        f"{instruction_step_html(4, 'Р”РѕР¶РґР°С‚СЊСЃСЏ РїРѕСЏРІР»РµРЅРёСЏ РјРµРЅСЋ;')}\n"
        f"{instruction_step_html(5, random_instruction_variant_html(), is_html=True)}"
    )

def build_report_message(company: dict, start_at: int, end_at: int) -> str:
    title = company.get("title") or "РЎРїРёСЃРѕРє"
    lines = [
        f'РћС‚С‡С‘С‚ РїРѕ "{esc(title)}"',
        f"Р·Р° {format_report_timestamp(start_at)} - {format_report_timestamp(end_at)}:",
        "",
    ]
    for entry in collect_report_entries(company, start_at, end_at):
        lines.append(f"рџ¦ѕ {esc(entry.get('task_text') or 'Р—Р°РґР°С‡Р°')}")
    lines.append("")
    lines.append(build_progress_bar(sum(1 for task in company.get("tasks", []) if task.get("done")), len(company.get("tasks", []))))
    return "\n".join(lines)

def build_task_completion_report_message(company: dict, task: dict) -> str:
    title = company.get("title") or "РЎРїРёСЃРѕРє"
    task_text = task.get("text") or "Р—Р°РґР°С‡Р°"
    lines = [
        f'РћС‚С‡С‘С‚ РїРѕ "{esc(title)}"',
        "СЃСЂР°Р·Сѓ РїРѕСЃР»Рµ РІС‹РїРѕР»РЅРµРЅРёСЏ:",
        "",
        f"рџ¦ѕ {esc(task_text)}",
        "",
        build_progress_bar(sum(1 for item in company.get("tasks", []) if item.get("done")), len(company.get("tasks", []))),
    ]
    return "\n".join(lines)

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

def paginate_window(total: int, page: int, page_size: int):
    if total <= 0:
        return 0, 0, False, False
    max_page = max(0, (total - 1) // page_size)
    page = max(0, min(page, max_page))
    start = page * page_size
    end = min(total, start + page_size)
    return start, end, page > 0, page < max_page

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
        "emoji": "рџ“Ѓ",
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
            "title": template_category.get("title") or "РџРѕРґРіСЂСѓРїРїР°",
            "emoji": template_category.get("emoji") or "рџ“Ѓ",
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
    s = re.sub(r'(\d)([Р°-СЏa-z])', r'\1 \2', s)
    s = re.sub(r'([Р°-СЏa-z])(\d)', r'\1 \2', s)
    tokens = re.findall(r'(\d+)\s*([Р°-СЏa-z\.]+)', s)
    if not tokens:
        return None
    total = 0
    for value, unit in tokens:
        n = int(value)
        unit = unit.strip('. ').lower()
        if unit.startswith('Рґ'):
            total += n * 86400
        elif unit.startswith('С‡') or unit.startswith('h'):
            total += n * 3600
        elif unit.startswith('Рј') or unit.startswith('min'):
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
    raw = clean_text(text).lower().replace("-РіРѕ", " ").replace("РіРѕ", " ")
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
        return None, None, "Р”Р°С‚Сѓ РёР»Рё СЃСЂРѕРє РІРІРµРґРё РєРѕСЂСЂРµРєС‚РЅРѕ, Р±Р°СЂСЃСѓСЂРєР° СЃС‚Р°С…Р°РЅРѕРІСЃРєР°СЏ"
    due_at = parse_flexible_datetime(raw)
    if due_at is None:
        seconds = parse_relative_duration_seconds(raw)
        if seconds is None:
            return None, None, "Р”Р°С‚Сѓ РёР»Рё СЃСЂРѕРє РІРІРµРґРё РєРѕСЂСЂРµРєС‚РЅРѕ, Р±Р°СЂСЃСѓСЂРєР° СЃС‚Р°С…Р°РЅРѕРІСЃРєР°СЏ"
        started_at = keep_started_at or now_ts()
        return started_at, started_at + seconds, None
    started_at = keep_started_at or now_ts()
    if due_at <= started_at:
        return None, None, "Р”Р°С‚Сѓ РёР»Рё СЃСЂРѕРє РІРІРµРґРё РєРѕСЂСЂРµРєС‚РЅРѕ, Р±Р°СЂСЃСѓСЂРєР° СЃС‚Р°С…Р°РЅРѕРІСЃРєР°СЏ"
    return started_at, due_at, None

def parse_template_deadline_seconds(text: str) -> tuple[int | None, str | None]:
    raw = clean_text(text)
    seconds = parse_relative_duration_seconds(raw)
    if seconds is None:
        return None, "РџСЂРёС€Р»Рё СЃСЂРѕРє, РЅР°РїСЂРёРјРµСЂ: 3 РґРЅСЏ, 7С‡20Рј, 45 РјРёРЅСѓС‚."
    return seconds, None

def apply_report_schedule_input(draft_interval: dict, text: str) -> str | None:
    kind = draft_interval.get("kind")
    if kind == "once":
        scheduled_at = parse_flexible_datetime(text)
        if scheduled_at is None or scheduled_at <= now_ts():
            return "Р”Р°С‚Сѓ РІРІРµРґРё РєРѕСЂСЂРµРєС‚РЅРѕ, Р±Р°СЂСЃСѓСЂРєР° СЃС‚Р°С…Р°РЅРѕРІСЃРєР°СЏ"
        draft_interval["scheduled_at"] = scheduled_at
        return None
    if kind == "monthly":
        parsed = parse_month_day_time(text)
        if parsed is None:
            return "РџСЂРёС€Р»Рё С‡РёСЃР»Рѕ Рё РІСЂРµРјСЏ, РЅР°РїСЂРёРјРµСЂ: 30 20:44"
        day, hour, minute = parsed
        draft_interval["day"] = day
        draft_interval["hour"] = hour
        draft_interval["minute"] = minute
        return None
    parsed = parse_flexible_time(text)
    if parsed is None:
        return "РџСЂРёС€Р»Рё РІСЂРµРјСЏ, РЅР°РїСЂРёРјРµСЂ: 21:30"
    hour, minute = parsed
    draft_interval["hour"] = hour
    draft_interval["minute"] = minute
    return None

def apply_report_accumulation_input(draft_interval: dict, text: str) -> str | None:
    kind = draft_interval.get("kind")
    if kind == "once":
        start_at = parse_flexible_datetime(text)
        if start_at is None or start_at >= (draft_interval.get("scheduled_at") or 0):
            return "РџСЂРёС€Р»Рё С‚РѕС‡РЅСѓСЋ РґР°С‚Сѓ Рё РІСЂРµРјСЏ СЂР°РЅСЊС€Рµ РґР°С‚С‹ РѕС‚С‡РµС‚Р°."
        draft_interval["accumulation"] = {"mode": "specific", "type": "datetime", "start_at": start_at}
        return None
    if kind == "monthly":
        parsed = parse_month_day_time(text)
        if parsed is None:
            return "РџСЂРёС€Р»Рё С‡РёСЃР»Рѕ Рё РІСЂРµРјСЏ, РЅР°РїСЂРёРјРµСЂ: 15 08:30"
        day, hour, minute = parsed
        draft_interval["accumulation"] = {"mode": "specific", "type": "month_day", "day": day, "hour": hour, "minute": minute}
        return None
    start_at = parse_flexible_datetime(text)
    if start_at is None or start_at >= report_preview_occurrence(draft_interval):
        return "РџСЂРёС€Р»Рё С‚РѕС‡РЅСѓСЋ РґР°С‚Сѓ Рё РІСЂРµРјСЏ СЂР°РЅСЊС€Рµ РґР°С‚С‹ РѕС‚С‡РµС‚Р°."
    draft_interval["accumulation"] = {"mode": "specific", "type": "datetime", "start_at": start_at}
    return None

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

    if t.startswith('в¬…пёЏ') or t.startswith('в¬†пёЏ') or t.startswith('в¬‡пёЏ'):
        return 'primary'

    if t in {'РґР°', 'РґР°!', 'yes'}:
        return 'danger'
    if t == 'ok':
        return None

    if 'СѓРґР°Р»' in t or 'РѕС‡РёСЃС‚' in t or 'РѕС‚РІСЏР·' in t:
        return 'danger'

    if cb.startswith(NEUTRAL_BUTTON_PREFIXES):
        return None

    if (
        t.startswith('вћ•')
        or t.startswith('РґРѕР±Р°РІРёС‚СЊ')
        or t.startswith('СЃРѕР·РґР°С‚СЊ')
        or t.startswith('РїРѕРґРєР»СЋС‡РёС‚СЊ')
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
    kb.add(kb_btn("рџ‘¤ Р›РёС‡РЅС‹Р№ workspace", callback_data="pmpersonal:root"))

    user = ensure_user(data, user_id)
    page = get_ui_page(user, "pm_root")
    workspace_ids = user.get("workspaces", [])
    total = 0
    for wid in workspace_ids:
        if str(wid).startswith("pm_"):
            continue
        ws = data["workspaces"].get(wid)
        if ws and ws.get("is_connected"):
            total += 1
    start, end, has_prev, has_next = paginate_window(total, page, PAGE_SIZE_PM)

    visible_pos = 0
    for wid in workspace_ids:
        if str(wid).startswith("pm_"):
            continue
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            continue
        if visible_pos >= end:
            break
        if visible_pos >= start:
            title = ws.get("name") or binding_place_label(
                data,
                ws.get("chat_id"),
                ws.get("thread_id") or 0,
                fallback_label="Workspace",
            )
            kb.add(kb_btn(fit_button_text(title), callback_data=f"pmws:{wid}"))
        visible_pos += 1

    if has_prev and has_next:
        kb.row(
            kb_btn("вћ• Workspace", callback_data="pmhelp:root"),
            kb_btn("в¬†пёЏ", callback_data="pgpm:prev"),
        )
        kb.row(
            kb_btn("рџ”„ РћР±РЅРѕРІРёС‚СЊ", callback_data="pmrefresh:root"),
            kb_btn("в¬‡пёЏ", callback_data="pgpm:next"),
        )
    elif has_prev or has_next:
        kb.row(kb_btn("вћ• Workspace", callback_data="pmhelp:root"))
        kb.row(
            kb_btn("рџ”„ РћР±РЅРѕРІРёС‚СЊ", callback_data="pmrefresh:root"),
            kb_btn("в¬†пёЏ" if has_prev else "в¬‡пёЏ", callback_data="pgpm:prev" if has_prev else "pgpm:next"),
        )
    else:
        kb.row(
            kb_btn("вћ• Workspace", callback_data="pmhelp:root"),
            kb_btn("рџ”„ РћР±РЅРѕРІРёС‚СЊ", callback_data="pmrefresh:root"),
        )
    return kb

def ws_settings_kb(wid: str):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("рџ§№ РћС‡РёСЃС‚РёС‚СЊ workspace", callback_data=f"wsclearask:{wid}"))
    kb.add(kb_btn("в¬…пёЏ", callback_data=f"backws:{wid}"))
    return kb

def pm_workspace_kb(wid: str):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("вњЌрџЏ» РџРµСЂРµРёРјРµРЅРѕРІР°С‚СЊ Workspace", callback_data=f"pmwsren:{wid}", style=False))
    kb.add(kb_btn("рџ’…рџЏ» РџРµСЂРµРїСЂРёСЃРІРѕРёС‚СЊ СЃРјР°Р№Р»РёРє", callback_data=f"pmwsemoji:{wid}", style=False))
    kb.add(kb_btn("рџ§№ РћС‡РёСЃС‚РёС‚СЊ workspace", callback_data=f"pmwsclearask:{wid}"))
    kb.add(kb_btn("рџ—‘ РЈРґР°Р»РёС‚СЊ workspace", callback_data=f"pmwsdelask:{wid}"))
    kb.add(kb_btn("в¬…пёЏ", callback_data="pmrefresh:root", style="primary"))
    return kb

def ws_home_kb(wid: str, ws: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    companies = ws.get("companies", [])
    page = get_ui_page(ws, "ws_home")
    start, end, has_prev, has_next = paginate_window(len(companies), page, PAGE_SIZE_WS)
    for idx in range(start, end):
        kb.add(kb_btn(display_company_name(companies[idx]), callback_data=f"cmp:{wid}:{idx}"))

    nav_prev_in_upper = has_prev and has_next
    nav_last = has_next or (has_prev and not has_next)

    is_personal = str(wid).startswith("pm_")
    if is_personal:
        row1 = [
            kb_btn("вћ• РЎРїРёСЃРѕРє", callback_data=f"cmpnew:{wid}"),
            kb_btn("рџ“‡ РЁР°Р±Р»РѕРЅС‹", callback_data=f"tplroot:{wid}"),
        ]
        if nav_prev_in_upper:
            row1.append(kb_btn("в¬†пёЏ", callback_data=f"pg:{wid}:wh:x:x:prev"))
        kb.row(*row1)

        row2 = [
            kb_btn("в¬…пёЏ", callback_data="pmrefresh:root"),
            kb_btn("вљ™пёЏ Workspace", callback_data=f"wsset:{wid}"),
        ]
        if nav_last:
            arrow_cb = f"pg:{wid}:wh:x:x:next" if has_next else f"pg:{wid}:wh:x:x:prev"
            arrow_text = "в¬‡пёЏ" if has_next else "в¬†пёЏ"
            row2.append(kb_btn(arrow_text, callback_data=arrow_cb))
        kb.row(*row2)
    else:
        if has_next:
            row1 = [kb_btn("вћ• РЎРїРёСЃРѕРє", callback_data=f"cmpnew:{wid}")]
            if nav_prev_in_upper:
                row1.append(kb_btn("в¬†пёЏ", callback_data=f"pg:{wid}:wh:x:x:prev"))
            kb.row(*row1)

            row2 = [kb_btn("рџ“‡ РЁР°Р±Р»РѕРЅС‹", callback_data=f"tplroot:{wid}")]
            row2.append(kb_btn("в¬‡пёЏ", callback_data=f"pg:{wid}:wh:x:x:next"))
            kb.row(*row2)
        elif has_prev:
            kb.row(kb_btn("вћ• РЎРїРёСЃРѕРє", callback_data=f"cmpnew:{wid}"))
            kb.row(
                kb_btn("рџ“‡ РЁР°Р±Р»РѕРЅС‹", callback_data=f"tplroot:{wid}"),
                kb_btn("в¬†пёЏ", callback_data=f"pg:{wid}:wh:x:x:prev"),
            )
        else:
            kb.row(
                kb_btn("вћ• РЎРїРёСЃРѕРє", callback_data=f"cmpnew:{wid}"),
                kb_btn("рџ“‡ РЁР°Р±Р»РѕРЅС‹", callback_data=f"tplroot:{wid}"),
            )
    return kb

def company_create_mode_kb(wid: str, ws: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    templates = ws.get("templates", [])
    page = get_ui_page(ws, "cmp_create")
    start, end, has_prev, has_next = paginate_window(len(templates), page, PAGE_SIZE_CREATE)
    for tpl in templates[start:end]:
        title = f"РџРѕ С€Р°Р±Р»РѕРЅСѓ {tpl.get('emoji') or 'рџ“Ѓ'}{tpl.get('title') or 'РЁР°Р±Р»РѕРЅ'}"
        kb.add(kb_btn(title, callback_data=f"cmpmode:{wid}:tpl:{tpl['id']}"))

    if has_prev and has_next:
        kb.row(
            kb_btn("в¬…пёЏ", callback_data=f"backws:{wid}"),
            kb_btn("в¬†пёЏ", callback_data=f"pg:{wid}:cc:x:x:prev"),
        )
        kb.row(
            kb_btn("рџђљ РџСѓСЃС‚СѓСЋ", callback_data=f"cmpmode:{wid}:empty"),
            kb_btn("в¬‡пёЏ", callback_data=f"pg:{wid}:cc:x:x:next"),
        )
    elif has_prev:
        kb.row(kb_btn("в¬…пёЏ", callback_data=f"backws:{wid}"))
        kb.row(
            kb_btn("рџђљ РџСѓСЃС‚СѓСЋ", callback_data=f"cmpmode:{wid}:empty"),
            kb_btn("в¬†пёЏ", callback_data=f"pg:{wid}:cc:x:x:prev"),
        )
    elif has_next:
        kb.row(kb_btn("в¬…пёЏ", callback_data=f"backws:{wid}"))
        kb.row(
            kb_btn("рџђљ РџСѓСЃС‚СѓСЋ", callback_data=f"cmpmode:{wid}:empty"),
            kb_btn("в¬‡пёЏ", callback_data=f"pg:{wid}:cc:x:x:next"),
        )
    else:
        kb.row(
            kb_btn("в¬…пёЏ", callback_data=f"backws:{wid}"),
            kb_btn("рџђљ РџСѓСЃС‚СѓСЋ", callback_data=f"cmpmode:{wid}:empty"),
        )
    return kb


def company_settings_kb(wid: str, company_idx: int, company: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("вњЌрџЏ» РџРµСЂРµРёРјРµРЅРѕРІР°С‚СЊ СЃРїРёСЃРѕРє", callback_data=f"cmpren:{wid}:{company_idx}"))
    kb.add(kb_btn("рџ’…рџЏ» РџРµСЂРµРїСЂРёСЃРІРѕРёС‚СЊ СЃРјР°Р№Р»РёРє", callback_data=f"cmpemoji:{wid}:{company_idx}"))
    format_label = "РґР°С‚Р°" if company.get("deadline_format") == "date" else "РІСЂРµРјСЏ"
    kb.add(kb_btn(f"рџ•’ Р¤РѕСЂРјР°С‚ РґРµРґР»Р°Р№РЅРѕРІ: {format_label}", callback_data=f"cmpdeadlinefmt:{wid}:{company_idx}"))
    kb.add(kb_btn("рџ§¬ РљРѕРїРёСЏ СЃРїРёСЃРєР°", callback_data=f"cmpcopy:{wid}:{company_idx}"))
    kb.add(kb_btn("рџ§ѕ РћС‚С‡РµС‚РЅРѕСЃС‚СЊ", callback_data=f"reports:{wid}:{company_idx}", style="primary"))
    kb.add(kb_btn("рџ“¤ Р”СѓР±Р»РёСЂРѕРІР°РЅРёРµ СЃРїРёСЃРєР°", callback_data=f"mirrors:{wid}:{company_idx}", style="primary"))
    kb.add(kb_btn("рџ—‘ РЈРґР°Р»РёС‚СЊ СЃРїРёСЃРѕРє", callback_data=f"cmpdelask:{wid}:{company_idx}"))
    kb.add(kb_btn("в¬…пёЏ", callback_data=f"cmp:{wid}:{company_idx}"))
    return kb


def category_settings_kb(wid: str, company_idx: int, category_idx: int, category: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("вњЌрџЏ» РџРµСЂРµРёРјРµРЅРѕРІР°С‚СЊ", callback_data=f"catren:{wid}:{company_idx}:{category_idx}"))
    kb.add(kb_btn("рџ’…рџЏ» РџРµСЂРµРїСЂРёСЃРІРѕРёС‚СЊ СЃРјР°Р№Р»РёРє", callback_data=f"catemoji:{wid}:{company_idx}:{category_idx}"))
    kb.add(kb_btn("рџ§¬ РљРѕРїРёСЏ РїРѕРґРіСЂСѓРїРїС‹", callback_data=f"catcopy:{wid}:{company_idx}:{category_idx}"))
    format_label = "РґР°С‚Р°" if category.get("deadline_format") == "date" else "РІСЂРµРјСЏ"
    kb.add(kb_btn(f"рџ•’ Р¤РѕСЂРјР°С‚ РґРµРґР»Р°Р№РЅРѕРІ: {format_label}", callback_data=f"catdeadlinefmt:{wid}:{company_idx}:{category_idx}"))
    kb.add(kb_btn("рџ—‘ РЈРґР°Р»РёС‚СЊ", callback_data=f"catdelask:{wid}:{company_idx}:{category_idx}"))
    kb.add(kb_btn("рџ—‘ РЈРґР°Р»РёС‚СЊ СЃ Р·Р°РґР°С‡Р°РјРё", callback_data=f"catdelallask:{wid}:{company_idx}:{category_idx}"))
    kb.add(kb_btn("в¬…пёЏ", callback_data=f"cat:{wid}:{company_idx}:{category_idx}"))
    return kb

def task_menu_kb(wid: str, company_idx: int, task_idx: int, task: dict, company: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    if task.get("done"):
        kb.add(kb_btn("рџ¤ћ РћС‚РјРµРЅРёС‚СЊ РІС‹РїРѕР»РЅРµРЅРёРµ", callback_data=f"taskdone:{wid}:{company_idx}:{task_idx}"))
    else:
        kb.add(kb_btn("рџ¦ѕ РћС‚РјРµС‚РёС‚СЊ РІС‹РїРѕР»РЅРµРЅРЅРѕР№", callback_data=f"taskdone:{wid}:{company_idx}:{task_idx}"))
    kb.add(kb_btn("вњЌрџЏ» РџРµСЂРµРёРјРµРЅРѕРІР°С‚СЊ", callback_data=f"taskren:{wid}:{company_idx}:{task_idx}"))

    if company.get("categories"):
        if task.get("category_id"):
            kb.add(kb_btn("рџ“Ґ РџРµСЂРµРІСЃСѓРЅСѓС‚СЊ", callback_data=f"taskmove:{wid}:{company_idx}:{task_idx}"))
        else:
            kb.add(kb_btn("рџ“Ґ Р’СЃСѓРЅСѓС‚СЊ РІ РїРѕРґРіСЂСѓРїРїСѓ", callback_data=f"taskmove:{wid}:{company_idx}:{task_idx}"))

    if not task.get("done"):
        if task.get("deadline_due_at"):
            kb.add(kb_btn("вЏ° Р”РµРґР»Р°Р№РЅ", callback_data=f"taskdeadlinebox:{wid}:{company_idx}:{task_idx}", style="primary"))
        else:
            kb.add(kb_btn("вЏ° РЈСЃС‚Р°РЅРѕРІРёС‚СЊ РґРµРґР»Р°Р№РЅ", callback_data=f"taskdeadline:{wid}:{company_idx}:{task_idx}", style=False))

    kb.add(kb_btn("рџ—‘ РЈРґР°Р»РёС‚СЊ Р·Р°РґР°С‡Сѓ", callback_data=f"taskdel:{wid}:{company_idx}:{task_idx}"))
    category_back_idx = find_category_index(company.get("categories", []), task.get("category_id")) if task.get("category_id") else None
    back = f"cat:{wid}:{company_idx}:{category_back_idx}" if category_back_idx is not None else f"cmp:{wid}:{company_idx}"
    kb.add(kb_btn("в¬…пёЏ", callback_data=back))
    return kb

def task_move_kb(wid: str, company_idx: int, task_idx: int, company: dict, task: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    current_category_id = task.get("category_id")
    categories = company.get("categories", [])
    total = sum(1 for category in categories if category.get("id") != current_category_id)
    page = get_ui_page(company, f"task_move_{company_idx}_{task_idx}")
    start, end, has_prev, has_next = paginate_window(total, page, PAGE_SIZE_CATEGORY)
    visible_pos = 0
    for category_idx, category in enumerate(categories):
        if category.get("id") == current_category_id:
            continue
        if visible_pos >= end:
            break
        if visible_pos >= start:
            kb.add(kb_btn(display_category_name(category), callback_data=f"taskmoveto:{wid}:{company_idx}:{task_idx}:{category_idx}"))
        visible_pos += 1
    if current_category_id:
        out_btn = kb_btn("рџ“¤ Р’С‹СЃСѓРЅСѓС‚СЊ", callback_data=f"taskmoveout:{wid}:{company_idx}:{task_idx}", style="primary")
        if has_prev and has_next:
            kb.row(out_btn, kb_btn("в¬†пёЏ", callback_data=f"pg:{wid}:tmv:{company_idx}:{task_idx}:prev"))
            kb.row(kb_btn("в¬…пёЏ", callback_data=f"task:{wid}:{company_idx}:{task_idx}", style="primary"), kb_btn("в¬‡пёЏ", callback_data=f"pg:{wid}:tmv:{company_idx}:{task_idx}:next"))
            return kb
        if has_prev:
            kb.row(out_btn, kb_btn("в¬†пёЏ", callback_data=f"pg:{wid}:tmv:{company_idx}:{task_idx}:prev"))
            kb.row(kb_btn("в¬…пёЏ", callback_data=f"task:{wid}:{company_idx}:{task_idx}", style="primary"))
            return kb
        kb.row(out_btn)
        row = [kb_btn("в¬…пёЏ", callback_data=f"task:{wid}:{company_idx}:{task_idx}", style="primary")]
        if has_next:
            row.append(kb_btn("в¬‡пёЏ", callback_data=f"pg:{wid}:tmv:{company_idx}:{task_idx}:next"))
        kb.row(*row)
        return kb
    row = [kb_btn("в¬…пёЏ", callback_data=f"task:{wid}:{company_idx}:{task_idx}", style="primary")]
    if has_next:
        row.append(kb_btn("в¬‡пёЏ", callback_data=f"pg:{wid}:tmv:{company_idx}:{task_idx}:next"))
    if has_prev:
        row.append(kb_btn("в¬†пёЏ", callback_data=f"pg:{wid}:tmv:{company_idx}:{task_idx}:prev"))
    kb.row(*row)
    return kb

def templates_root_kb(wid: str, ws: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    templates = ws.get("templates", [])
    page = get_ui_page(ws, "tpl_root")
    start, end, has_prev, has_next = paginate_window(len(templates), page, PAGE_SIZE_TEMPLATES)
    for idx in range(start, end):
        tpl = templates[idx]
        kb.add(kb_btn(f"{tpl.get('emoji') or 'рџ“Ѓ'}{tpl.get('title') or 'РЁР°Р±Р»РѕРЅ'}", callback_data=f"tplselect:{wid}:{tpl['id']}"))

    if has_prev and has_next:
        kb.row(
            kb_btn("вћ• РЁР°Р±Р»РѕРЅ", callback_data=f"tplnewset:{wid}"),
            kb_btn("в¬†пёЏ", callback_data=f"pg:{wid}:tr:x:x:prev"),
        )
        kb.row(
            kb_btn("в¬…пёЏ", callback_data=f"backws:{wid}"),
            kb_btn("в¬‡пёЏ", callback_data=f"pg:{wid}:tr:x:x:next"),
        )
    elif has_prev:
        kb.row(kb_btn("вћ• РЁР°Р±Р»РѕРЅ", callback_data=f"tplnewset:{wid}"))
        kb.row(
            kb_btn("в¬…пёЏ", callback_data=f"backws:{wid}"),
            kb_btn("в¬†пёЏ", callback_data=f"pg:{wid}:tr:x:x:prev"),
        )
    elif has_next:
        kb.row(kb_btn("вћ• РЁР°Р±Р»РѕРЅ", callback_data=f"tplnewset:{wid}"))
        kb.row(
            kb_btn("в¬…пёЏ", callback_data=f"backws:{wid}"),
            kb_btn("в¬‡пёЏ", callback_data=f"pg:{wid}:tr:x:x:next"),
        )
    else:
        kb.row(
            kb_btn("в¬…пёЏ", callback_data=f"backws:{wid}"),
            kb_btn("вћ• РЁР°Р±Р»РѕРЅ", callback_data=f"tplnewset:{wid}"),
        )
    return kb


def template_settings_kb(wid: str):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("вњЌрџЏ» РџРµСЂРµРёРјРµРЅРѕРІР°С‚СЊ С€Р°Р±Р»РѕРЅ", callback_data=f"tplrenameset:{wid}"))
    kb.add(kb_btn("рџ’…рџЏ» РџРµСЂРµРїСЂРёСЃРІРѕРёС‚СЊ СЃРјР°Р№Р»РёРє", callback_data=f"tplemojiset:{wid}"))
    kb.add(kb_btn("рџ§¬ РљРѕРїРёСЏ С€Р°Р±Р»РѕРЅР°", callback_data=f"tplcopy:{wid}"))
    kb.add(kb_btn("рџ§ѕ РћС‚С‡РµС‚РЅРѕСЃС‚СЊ", callback_data=f"tplreport:{wid}", style=False))
    kb.add(kb_btn("рџ—‘ РЈРґР°Р»РёС‚СЊ С€Р°Р±Р»РѕРЅ", callback_data=f"tpldelsetask:{wid}"))
    kb.add(kb_btn("в¬…пёЏ", callback_data=f"tpl:{wid}"))
    return kb


def template_category_settings_kb(wid: str, category_idx: int):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("вњЌрџЏ» РџРµСЂРµРёРјРµРЅРѕРІР°С‚СЊ", callback_data=f"tplcatren:{wid}:{category_idx}"))
    kb.add(kb_btn("рџ’…рџЏ» РџРµСЂРµРїСЂРёСЃРІРѕРёС‚СЊ СЃРјР°Р№Р»РёРє", callback_data=f"tplcatemoji:{wid}:{category_idx}"))
    kb.add(kb_btn("рџ§¬ РљРѕРїРёСЏ РџРѕРґРіСЂСѓРїРїС‹", callback_data=f"tplcatcopy:{wid}:{category_idx}"))
    kb.add(kb_btn("рџ—‘ РЈРґР°Р»РёС‚СЊ", callback_data=f"tplcatdelask:{wid}:{category_idx}"))
    kb.add(kb_btn("рџ—‘ РЈРґР°Р»РёС‚СЊ СЃ Р·Р°РґР°С‡Р°РјРё", callback_data=f"tplcatdelallask:{wid}:{category_idx}"))
    kb.add(kb_btn("в¬…пёЏ", callback_data=f"tplcat:{wid}:{category_idx}"))
    return kb


def company_card_text(company: dict) -> str:
    lines = [f"{rich_display_company_name(company)}:"]
    company_deadline_format = company.get("deadline_format") or "relative"
    all_tasks = company.get("tasks", [])
    categories = company.get("categories", [])
    now_value = now_ts()
    sort_key = lambda task: (
        1 if task.get("done") else 0,
        1 if not task.get("deadline_due_at") else 0,
        task.get("deadline_due_at") or 10**18,
        task.get("created_at") or 0,
    )
    uncategorized, tasks_by_category, done_total, done_by_category = group_task_entries(all_tasks)

    lines.append(build_progress_bar(done_total, len(all_tasks)))

    if uncategorized:
        for _, task in sorted(uncategorized, key=lambda entry: sort_key(entry[1])):
            icon = task_deadline_icon(task, now_value)
            suffix = display_task_deadline_suffix(task, company_deadline_format, now_value) if not task.get("done") and task.get("deadline_due_at") else ""
            lines.append(f"{icon} {rich_task_text(task.get('text') or 'Задача', bool(task.get('done')))}{esc(suffix)}")

    for category in categories:
        if lines and lines[-1] != "":
            lines.append("")
        category_id = category.get("id")
        cat_entries = tasks_by_category.get(category_id, [])
        done_count = done_by_category.get(category_id, 0)
        lines.append(f"    {rich_display_category_name(category)}:")
        lines.append(f"    {build_progress_bar(done_count, len(cat_entries))}")
        if cat_entries:
            for _, task in sorted(cat_entries, key=lambda entry: sort_key(entry[1])):
                icon = task_deadline_icon(task, now_value)
                suffix = display_task_deadline_suffix(task, category.get("deadline_format") or company_deadline_format, now_value) if not task.get("done") and task.get("deadline_due_at") else ""
                lines.append(f"        {icon} {rich_task_text(task.get('text') or 'Задача', bool(task.get('done')))}{esc(suffix)}")

    while lines and lines[-1] == "":
        lines.pop()

    if len(lines) == 2 and not all_tasks and not company.get("categories"):
        lines.append("—")
    return "\n".join(lines)

def company_menu_kb(wid: str, company_idx: int, company: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    tasks = company.get("tasks", [])
    categories = company.get("categories", [])
    root_entries = [(task_idx, task) for task_idx, task in enumerate(tasks) if not task.get("category_id")]
    root_task_total = len(root_entries)
    page = get_ui_page(company, f"cmp_{company_idx}")
    total = root_task_total + len(categories)
    start, end, has_prev, has_next = paginate_window(total, page, PAGE_SIZE_COMPANY)
    now_value = now_ts()

    if start < root_task_total:
        visible_root_end = min(end, root_task_total)
        for task_idx, task in root_entries[start:visible_root_end]:
            icon = task_deadline_icon(task, now_value)
            kb.add(kb_btn(f"{icon} {task.get('text') or 'Задача'}", callback_data=f"task:{wid}:{company_idx}:{task_idx}"))

    if end > root_task_total:
        category_start = max(0, start - root_task_total)
        category_end = min(len(categories), end - root_task_total)
        for category_idx in range(category_start, category_end):
            kb.add(kb_btn(display_category_name(categories[category_idx]), callback_data=f"cat:{wid}:{company_idx}:{category_idx}"))

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

def category_menu_kb(wid: str, company_idx: int, category_idx: int, category: dict, company: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    tasks = company.get("tasks", [])
    category_id = category.get("id")
    category_entries = [(task_idx, task) for task_idx, task in enumerate(tasks) if task.get("category_id") == category_id]
    total = len(category_entries)
    now_value = now_ts()
    page = get_ui_page(company, f"cat_{company_idx}_{category_idx}")
    start, end, has_prev, has_next = paginate_window(total, page, PAGE_SIZE_CATEGORY)
    for task_idx, task in category_entries[start:end]:
        icon = task_deadline_icon(task, now_value)
        kb.add(kb_btn(f"{icon} {task.get('text') or 'Задача'}", callback_data=f"task:{wid}:{company_idx}:{task_idx}"))

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

def template_menu_kb(wid: str, ws: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    template = get_active_template(ws)
    tasks = template.get("tasks", [])
    categories = template.get("categories", [])
    root_entries = [(task_idx, task) for task_idx, task in enumerate(tasks) if not task.get("category_id")]
    root_task_total = len(root_entries)
    page = get_ui_page(ws, f"tpl_{template.get('id')}")
    total = root_task_total + len(categories)
    start, end, has_prev, has_next = paginate_window(total, page, PAGE_SIZE_COMPANY)
    now_value = now_ts()

    if start < root_task_total:
        visible_root_end = min(end, root_task_total)
        for task_idx, task in root_entries[start:visible_root_end]:
            title = f"{task_deadline_icon(task, now_value)} {str(task.get('text') or 'Без названия')}{template_task_deadline_suffix(task)}"
            kb.add(kb_btn(title, callback_data=f"tpltask:{wid}:{task_idx}"))

    if end > root_task_total:
        category_start = max(0, start - root_task_total)
        category_end = min(len(categories), end - root_task_total)
        for category_idx in range(category_start, category_end):
            kb.add(kb_btn(display_category_name(categories[category_idx]), callback_data=f"tplcat:{wid}:{category_idx}"))

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

def template_category_menu_kb(wid: str, category_idx: int, category: dict, template: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    tasks = template.get("tasks", [])
    category_id = category.get("id")
    category_entries = [(task_idx, task) for task_idx, task in enumerate(tasks) if task.get("category_id") == category_id]
    total = len(category_entries)
    page_key = f"tplcat_{template.get('id') or 'none'}_{category_idx}"
    page = get_ui_page(template, page_key)
    start, end, has_prev, has_next = paginate_window(total, page, PAGE_SIZE_CATEGORY)
    now_value = now_ts()
    for task_idx, task in category_entries[start:end]:
        title = f"{task_deadline_icon(task, now_value)} {str(task.get('text') or 'Без названия')}{template_task_deadline_suffix(task)}"
        kb.add(kb_btn(title, callback_data=f"tpltask:{wid}:{task_idx}"))

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

def template_task_menu_kb(wid: str, task_idx: int, task: dict, ws: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("✍🏻 Переименовать", callback_data=f"tpltaskren:{wid}:{task_idx}"))
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
    category_back_idx = find_category_index(ws.get("template_categories", []), task.get("category_id")) if task.get("category_id") else None
    back = f"tplcat:{wid}:{category_back_idx}" if category_back_idx is not None else f"tpl:{wid}"
    kb.add(kb_btn("⬅️", callback_data=back))
    return kb

def template_task_move_kb(wid: str, task_idx: int, ws: dict, task: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    current_category_id = task.get("category_id")
    categories = ws.get("template_categories", [])
    total = sum(1 for category in categories if category.get("id") != current_category_id)
    page = get_ui_page(ws, f"template_task_move_{task_idx}")
    start, end, has_prev, has_next = paginate_window(total, page, PAGE_SIZE_CATEGORY)
    visible_pos = 0
    for category_idx, category in enumerate(categories):
        if category.get("id") == current_category_id:
            continue
        if visible_pos >= end:
            break
        if visible_pos >= start:
            kb.add(kb_btn(display_category_name(category), callback_data=f"tpltaskmoveto:{wid}:{task_idx}:{category_idx}"))
        visible_pos += 1
    if current_category_id:
        out_btn = kb_btn("рџ“¤ Р’С‹СЃСѓРЅСѓС‚СЊ", callback_data=f"tpltaskmoveout:{wid}:{task_idx}", style="primary")
        if has_prev and has_next:
            kb.row(out_btn, kb_btn("в¬†пёЏ", callback_data=f"pg:{wid}:ttmv:{task_idx}:x:prev"))
            kb.row(kb_btn("в¬…пёЏ", callback_data=f"tpltask:{wid}:{task_idx}", style="primary"), kb_btn("в¬‡пёЏ", callback_data=f"pg:{wid}:ttmv:{task_idx}:x:next"))
            return kb
        if has_prev:
            kb.row(out_btn, kb_btn("в¬†пёЏ", callback_data=f"pg:{wid}:ttmv:{task_idx}:x:prev"))
            kb.row(kb_btn("в¬…пёЏ", callback_data=f"tpltask:{wid}:{task_idx}", style="primary"))
            return kb
        kb.row(out_btn)
        row = [kb_btn("в¬…пёЏ", callback_data=f"tpltask:{wid}:{task_idx}", style="primary")]
        if has_next:
            row.append(kb_btn("в¬‡пёЏ", callback_data=f"pg:{wid}:ttmv:{task_idx}:x:next"))
        kb.row(*row)
        return kb
    row = [kb_btn("в¬…пёЏ", callback_data=f"tpltask:{wid}:{task_idx}", style="primary")]
    if has_next:
        row.append(kb_btn("в¬‡пёЏ", callback_data=f"pg:{wid}:ttmv:{task_idx}:x:next"))
    if has_prev:
        row.append(kb_btn("в¬†пёЏ", callback_data=f"pg:{wid}:ttmv:{task_idx}:x:prev"))
    kb.row(*row)
    return kb

def mirrors_menu_kb(wid: str, company_idx: int, company: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    mirrors = company.get("mirrors", [])
    page = get_ui_page(company, f"mirrors_{company_idx}")
    start, end, has_prev, has_next = paginate_window(len(mirrors), page, PAGE_SIZE_REPORT_BINDINGS)
    for idx in range(start, end):
        mirror = mirrors[idx]
        label = mirror.get("label") or f"{mirror.get('chat_id')}/{mirror.get('thread_id') or 0}"
        kb.add(kb_btn(fit_button_text(label), callback_data=f"mirroritem:{wid}:{company_idx}:{idx}", style=False))

    add_btn = kb_btn("вћ• РЎРІСЏР·РєР°", callback_data=f"mirroron:{wid}:{company_idx}", style="success")
    refresh_btn = kb_btn("рџ”„ РћР±РЅРѕРІРёС‚СЊ", callback_data=f"mirrorsrefresh:{wid}:{company_idx}")
    back_btn = kb_btn("в¬…пёЏ", callback_data=f"cmpset:{wid}:{company_idx}", style="primary")
    up_btn = kb_btn("в¬†пёЏ", callback_data=f"pg:{wid}:mm:{company_idx}:x:prev")
    down_btn = kb_btn("в¬‡пёЏ", callback_data=f"pg:{wid}:mm:{company_idx}:x:next")

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
    candidates = missing_report_targets_for_mirrors(company)
    page = get_ui_page(company, f"mirror_import_{company_idx}")
    start, end, has_prev, has_next = paginate_window(len(candidates), page, PAGE_SIZE_REPORT_BINDINGS)
    for source_idx, target in candidates[start:end]:
        label = target.get("label") or f"{target.get('chat_id')}/{target.get('thread_id') or 0}"
        kb.add(kb_btn(fit_button_text(label), callback_data=f"mirrorcopy:{wid}:{company_idx}:{source_idx}", style=False))
    kb.add(kb_btn("вћ• РќРѕРІР°СЏ СЃРІСЏР·РєР°", callback_data=f"mirrornew:{wid}:{company_idx}", style="success"))
    row = [kb_btn("в¬…пёЏ", callback_data=f"mirrors:{wid}:{company_idx}", style="primary")]
    if has_prev:
        row.append(kb_btn("в¬†пёЏ", callback_data=f"pg:{wid}:mic:{company_idx}:x:prev"))
    if has_next:
        row.append(kb_btn("в¬‡пёЏ", callback_data=f"pg:{wid}:mic:{company_idx}:x:next"))
    kb.row(*row)
    return kb

def report_menu_kb(wid: str, company_idx: int, target_idx: int, company: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    pairs = get_target_report_pairs(company, target_idx)
    ordered = sorted(pairs, key=lambda pair: report_interval_sort_key(pair[1], pair[0]))
    page = get_ui_page(company, f"report_{company_idx}_{target_idx}")
    start, end, has_prev, has_next = paginate_window(len(ordered), page, PAGE_SIZE_REPORTS)
    for idx, interval in ordered[start:end]:
        kb.add(kb_btn(format_report_schedule_label(interval), callback_data=f"reportitem:{wid}:{company_idx}:{target_idx}:{idx}", style=False))

    kb.row(
        kb_btn("вћ• РћС‚С‡РµС‚", callback_data=f"reportadd:{wid}:{company_idx}:{target_idx}", style="success"),
        kb_btn("вљ™пёЏ РћС‚С‡РµС‚РЅРѕСЃС‚СЊ", callback_data=f"reportsettings:{wid}:{company_idx}:{target_idx}", style="primary"),
    )
    row = [kb_btn("в¬…пёЏ", callback_data=f"reportbind:{wid}:{company_idx}", style="primary")]
    if has_prev:
        row.append(kb_btn("в¬†пёЏ", callback_data=f"pg:{wid}:rp:{company_idx}:{target_idx}:prev"))
    if has_next:
        row.append(kb_btn("в¬‡пёЏ", callback_data=f"pg:{wid}:rp:{company_idx}:{target_idx}:next"))
    kb.row(*row)
    return kb

def report_interval_kb(wid: str, company_idx: int, target_idx: int, interval_idx: int, interval: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    if interval.get("kind") != "on_done":
        kb.add(kb_btn("РР·РјРµРЅРёС‚СЊ РІСЂРµРјСЏ РѕС‚С‡РµС‚Р°", callback_data=f"reportedit:{wid}:{company_idx}:{target_idx}:{interval_idx}", style=False))
        kb.add(kb_btn("РР·РјРµРЅРёС‚СЊ РёРЅС‚РµСЂРІР°Р» РЅР°РєРѕРїР»РµРЅРёСЏ", callback_data=f"reportaccedit:{wid}:{company_idx}:{target_idx}:{interval_idx}", style=False))
    kb.add(kb_btn("рџ—‘ РЈРґР°Р»РёС‚СЊ", callback_data=f"reportdelask:{wid}:{company_idx}:{target_idx}:{interval_idx}", style="danger"))
    kb.add(kb_btn("в¬…пёЏ", callback_data=f"reportmenu:{wid}:{company_idx}:{target_idx}", style="primary"))
    return kb

def report_interval_kind_kb(wid: str, company_idx: int, target_idx: int, flow: str, interval_idx: int | None):
    kb = InlineKeyboardMarkup(row_width=1)
    token = "x" if interval_idx is None else str(interval_idx)
    kb.row(
        kb_btn("РџРѕРЅРµРґРµР»СЊРЅРёРє", callback_data=f"reportweek:{wid}:{company_idx}:{target_idx}:{token}:{flow}:0", style=False),
        kb_btn("Р’С‚РѕСЂРЅРёРє", callback_data=f"reportweek:{wid}:{company_idx}:{target_idx}:{token}:{flow}:1", style=False),
    )
    kb.row(
        kb_btn("РЎСЂРµРґР°", callback_data=f"reportweek:{wid}:{company_idx}:{target_idx}:{token}:{flow}:2", style=False),
        kb_btn("Р§РµС‚РІРµСЂРі", callback_data=f"reportweek:{wid}:{company_idx}:{target_idx}:{token}:{flow}:3", style=False),
    )
    kb.row(
        kb_btn("РџСЏС‚РЅРёС†Р°", callback_data=f"reportweek:{wid}:{company_idx}:{target_idx}:{token}:{flow}:4", style=False),
        kb_btn("РЎСѓР±Р±РѕС‚Р°", callback_data=f"reportweek:{wid}:{company_idx}:{target_idx}:{token}:{flow}:5", style=False),
    )
    kb.add(kb_btn("Р’РѕСЃРєСЂРµСЃРµРЅРёРµ", callback_data=f"reportweek:{wid}:{company_idx}:{target_idx}:{token}:{flow}:6", style=False))
    kb.add(kb_btn("рџ“† РљР°Р¶РґС‹Р№ РґРµРЅСЊ", callback_data=f"reportdaily:{wid}:{company_idx}:{target_idx}:{token}:{flow}", style=False))
    kb.add(kb_btn("рџ—“ РљР°Р¶РґС‹Р№ РјРµСЃСЏС†", callback_data=f"reportmonth:{wid}:{company_idx}:{target_idx}:{token}:{flow}", style=False))
    kb.add(kb_btn("рџ“† РћРґРёРЅ СЂР°Р·", callback_data=f"reportonce:{wid}:{company_idx}:{target_idx}:{token}:{flow}", style=False))
    kb.add(kb_btn("рџ“† РЎСЂР°Р·Сѓ РїРѕСЃР»Рµ РІС‹РїРѕР»РЅРµРЅРёСЏ", callback_data=f"reportinstant:{wid}:{company_idx}:{target_idx}:{token}:{flow}", style=False))
    back_cb = f"reportitem:{wid}:{company_idx}:{target_idx}:{interval_idx}" if flow == "edit" and interval_idx is not None else f"reportmenu:{wid}:{company_idx}:{target_idx}"
    kb.add(kb_btn("в¬…пёЏ", callback_data=back_cb, style="primary"))
    return kb

def report_accumulation_kb(wid: str, interval: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    if interval.get("kind") == "monthly":
        kb.add(kb_btn("Р’РµСЃСЊ РјРµСЃСЏС†", callback_data=f"reportacc:{wid}:month"))
        kb.add(kb_btn("РћС‚ РїРѕСЃР»РµРґРЅРµРіРѕ РѕС‚С‡РµС‚Р°", callback_data=f"reportacc:{wid}:last"))
    elif interval.get("kind") in {"daily", "weekly"}:
        kb.add(kb_btn("РћС‚ РїРѕСЃР»РµРґРЅРµРіРѕ РѕС‚С‡РµС‚Р°", callback_data=f"reportacc:{wid}:last"))
        kb.add(kb_btn("Р’СЃСЋ РЅРµРґРµР»СЋ", callback_data=f"reportacc:{wid}:week"))
    else:
        kb.add(kb_btn("РћС‚ РїРѕСЃР»РµРґРЅРµРіРѕ РѕС‚С‡РµС‚Р°", callback_data=f"reportacc:{wid}:last"))
    kb.add(kb_btn("РћС‚ РѕРїСЂРµРґРµР»РµРЅРЅРѕРіРѕ РґРЅСЏ", callback_data=f"reportacc:{wid}:specific"))
    kb.add(kb_btn("в¬…пёЏ", callback_data=f"reportaccback:{wid}", style="primary"))
    return kb

def report_targets_kb(wid: str, company_idx: int, company: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    targets = get_effective_report_targets(company)
    page = get_ui_page(company, f"report_targets_{company_idx}")
    start, end, has_prev, has_next = paginate_window(len(targets), page, PAGE_SIZE_REPORT_BINDINGS)
    for idx in range(start, end):
        target = targets[idx]
        label = target.get("label") or f"{target.get('chat_id')}/{target.get('thread_id') or 0}"
        kb.add(kb_btn(fit_button_text(label), callback_data=f"reportmenu:{wid}:{company_idx}:{idx}", style=False))

    add_btn = kb_btn("вћ• РЎРІСЏР·РєР°", callback_data=f"reportbindon:{wid}:{company_idx}", style="success")
    refresh_btn = kb_btn("рџ”„ РћР±РЅРѕРІРёС‚СЊ", callback_data=f"reportbindrefresh:{wid}:{company_idx}")
    back_btn = kb_btn("в¬…пёЏ", callback_data=f"cmpset:{wid}:{company_idx}", style="primary")
    up_btn = kb_btn("в¬†пёЏ", callback_data=f"pg:{wid}:rb:{company_idx}:x:prev")
    down_btn = kb_btn("в¬‡пёЏ", callback_data=f"pg:{wid}:rb:{company_idx}:x:next")

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
    candidates = missing_mirrors_for_report_targets(company)
    page = get_ui_page(company, f"report_import_{company_idx}")
    start, end, has_prev, has_next = paginate_window(len(candidates), page, PAGE_SIZE_REPORT_BINDINGS)
    for source_idx, mirror in candidates[start:end]:
        label = mirror.get("label") or f"{mirror.get('chat_id')}/{mirror.get('thread_id') or 0}"
        kb.add(kb_btn(fit_button_text(label), callback_data=f"reportbindcopy:{wid}:{company_idx}:{source_idx}", style=False))
    kb.add(kb_btn("вћ• РќРѕРІР°СЏ СЃРІСЏР·РєР°", callback_data=f"reportbindnew:{wid}:{company_idx}", style="success"))
    row = [kb_btn("в¬…пёЏ", callback_data=f"reportbind:{wid}:{company_idx}", style="primary")]
    if has_prev:
        row.append(kb_btn("в¬†пёЏ", callback_data=f"pg:{wid}:ric:{company_idx}:x:prev"))
    if has_next:
        row.append(kb_btn("в¬‡пёЏ", callback_data=f"pg:{wid}:ric:{company_idx}:x:next"))
    kb.row(*row)
    return kb

def template_report_menu_kb(wid: str, ws: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    template = get_active_template(ws)
    intervals = get_report_intervals(template)
    ordered = sorted(enumerate(intervals), key=lambda pair: report_interval_sort_key(pair[1], pair[0]))
    page = get_ui_page(template, f"tpl_report_{template.get('id')}")
    start, end, has_prev, has_next = paginate_window(len(ordered), page, PAGE_SIZE_REPORTS)
    for idx, interval in ordered[start:end]:
        kb.add(kb_btn(format_report_schedule_label(interval), callback_data=f"tplreportitem:{wid}:{idx}", style=False))

    kb.row(
        kb_btn("вћ• РћС‚С‡РµС‚", callback_data=f"tplreportadd:{wid}", style="success"),
        kb_btn("вљ™пёЏ РћС‚С‡РµС‚РЅРѕСЃС‚СЊ", callback_data=f"tplreportsettings:{wid}", style="primary"),
    )
    row = [kb_btn("в¬…пёЏ", callback_data=f"tplsettings:{wid}", style="primary")]
    if has_prev:
        row.append(kb_btn("в¬†пёЏ", callback_data=f"pg:{wid}:tpr:x:x:prev"))
    if has_next:
        row.append(kb_btn("в¬‡пёЏ", callback_data=f"pg:{wid}:tpr:x:x:next"))
    kb.row(*row)
    return kb

def template_report_interval_kb(wid: str, interval_idx: int, interval: dict | None = None):
    kb = InlineKeyboardMarkup(row_width=1)
    if interval and interval.get("kind") != "on_done":
        kb.add(kb_btn("РР·РјРµРЅРёС‚СЊ РІСЂРµРјСЏ РѕС‚С‡РµС‚Р°", callback_data=f"tplreportedit:{wid}:{interval_idx}", style=False))
        kb.add(kb_btn("РР·РјРµРЅРёС‚СЊ РёРЅС‚РµСЂРІР°Р» РЅР°РєРѕРїР»РµРЅРёСЏ", callback_data=f"tplreportaccedit:{wid}:{interval_idx}", style=False))
    kb.add(kb_btn("рџ—‘ РЈРґР°Р»РёС‚СЊ", callback_data=f"tplreportdelask:{wid}:{interval_idx}", style="danger"))
    kb.add(kb_btn("в¬…пёЏ", callback_data=f"tplreport:{wid}", style="primary"))
    return kb

def template_report_interval_kind_kb(wid: str, flow: str, interval_idx: int | None):
    kb = InlineKeyboardMarkup(row_width=1)
    token = "x" if interval_idx is None else str(interval_idx)
    kb.row(
        kb_btn("РџРѕРЅРµРґРµР»СЊРЅРёРє", callback_data=f"tplreportweek:{wid}:{token}:{flow}:0", style=False),
        kb_btn("Р’С‚РѕСЂРЅРёРє", callback_data=f"tplreportweek:{wid}:{token}:{flow}:1", style=False),
    )
    kb.row(
        kb_btn("РЎСЂРµРґР°", callback_data=f"tplreportweek:{wid}:{token}:{flow}:2", style=False),
        kb_btn("Р§РµС‚РІРµСЂРі", callback_data=f"tplreportweek:{wid}:{token}:{flow}:3", style=False),
    )
    kb.row(
        kb_btn("РџСЏС‚РЅРёС†Р°", callback_data=f"tplreportweek:{wid}:{token}:{flow}:4", style=False),
        kb_btn("РЎСѓР±Р±РѕС‚Р°", callback_data=f"tplreportweek:{wid}:{token}:{flow}:5", style=False),
    )
    kb.add(kb_btn("Р’РѕСЃРєСЂРµСЃРµРЅРёРµ", callback_data=f"tplreportweek:{wid}:{token}:{flow}:6", style=False))
    kb.add(kb_btn("рџ“† РљР°Р¶РґС‹Р№ РґРµРЅСЊ", callback_data=f"tplreportdaily:{wid}:{token}:{flow}", style=False))
    kb.add(kb_btn("рџ—“ РљР°Р¶РґС‹Р№ РјРµСЃСЏС†", callback_data=f"tplreportmonth:{wid}:{token}:{flow}", style=False))
    kb.add(kb_btn("рџ“† РЎСЂР°Р·Сѓ РїРѕСЃР»Рµ РІС‹РїРѕР»РЅРµРЅРёСЏ", callback_data=f"tplreportinstant:{wid}:{token}:{flow}", style=False))
    back_cb = f"tplreportitem:{wid}:{interval_idx}" if flow == "edit" and interval_idx is not None else f"tplreport:{wid}"
    kb.add(kb_btn("в¬…пёЏ", callback_data=back_cb, style="primary"))
    return kb

def confirm_kb(confirm_cb: str, back_cb: str):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("Р”Р°!", callback_data=confirm_cb, style="danger"))
    kb.add(kb_btn("в¬…пёЏ", callback_data=back_cb, style="primary"))
    return kb

# =========================
# VIEW HELPERS
# =========================

async def upsert_ws_menu(data: dict, wid: str, text: str, reply_markup, disable_web_page_preview: bool = False):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return False

    MENU_LOCKS.setdefault(wid, asyncio.Lock())
    async with MENU_LOCKS[wid]:
        current_id = RUNTIME_MENU_IDS.get(wid) or ws.get("menu_msg_id")
        if current_id:
            ws["menu_msg_id"] = current_id
            ok = await try_edit_text(
                ws["chat_id"],
                current_id,
                text,
                reply_markup=reply_markup,
                disable_web_page_preview=disable_web_page_preview,
            )
            if ok:
                RUNTIME_MENU_IDS[wid] = current_id
                return False

        fresh_id = ws.get("menu_msg_id")
        if fresh_id and fresh_id != current_id:
            ok = await try_edit_text(
                ws["chat_id"],
                fresh_id,
                text,
                reply_markup=reply_markup,
                disable_web_page_preview=disable_web_page_preview,
            )
            if ok:
                RUNTIME_MENU_IDS[wid] = fresh_id
                return False

        msg = await send_message(
            ws["chat_id"],
            text,
            reply_markup=reply_markup,
            thread_id=ws["thread_id"],
            disable_web_page_preview=disable_web_page_preview,
        )
        ws["menu_msg_id"] = msg.message_id
        RUNTIME_MENU_IDS[wid] = msg.message_id
        async with FILE_LOCK:
            await save_data_unlocked(data)
        return True

async def update_pm_menu(user_id: str, data: dict):
    user = ensure_user(data, user_id)
    text = pm_main_text(user_id, data)
    kb = pm_main_kb(user_id, data)
    if user.get("pm_menu_msg_id"):
        if await try_edit_text(int(user_id), user["pm_menu_msg_id"], text, reply_markup=kb):
            return
        user["pm_menu_msg_id"] = None
    try:
        msg = await send_message(int(user_id), text, reply_markup=kb)
        user["pm_menu_msg_id"] = msg.message_id
    except Exception:
        pass

async def edit_pm_workspace_view(data: dict, user_id: str, wid: str, message_id: int | None = None):
    user = ensure_user(data, user_id)
    target_message_id = message_id or user.get("pm_menu_msg_id")
    if not target_message_id:
        return
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected") or wid not in user.get("workspaces", []):
        await safe_edit_text(int(user_id), target_message_id, pm_main_text(user_id, data), reply_markup=pm_main_kb(user_id, data))
        return
    title = ws.get("name") or binding_place_label(
        data,
        ws.get("chat_id"),
        ws.get("thread_id") or 0,
        fallback_label="Workspace",
    )
    view_ws = dict(ws)
    view_ws["name"] = title
    await safe_edit_text(int(user_id), target_message_id, workspace_title_label(view_ws), reply_markup=pm_workspace_kb(wid))

async def upsert_company_card(ws: dict, company_idx: int, card_text: str | None = None):
    if company_idx < 0 or company_idx >= len(ws["companies"]):
        return False
    company = ws["companies"][company_idx]
    text = card_text or company_card_text(company)
    card_msg_id = company.get("card_msg_id")
    if card_msg_id:
        ok = await try_edit_text(ws["chat_id"], card_msg_id, text)
        if ok:
            return False
    msg = await send_message(ws["chat_id"], text, thread_id=ws["thread_id"])
    if card_msg_id and card_msg_id != msg.message_id:
        await safe_delete_message(ws["chat_id"], card_msg_id)
    company["card_msg_id"] = msg.message_id
    return True

async def upsert_company_mirror(mirror: dict, company: dict, card_text: str | None = None):
    if not mirror:
        return False
    text = card_text or company_card_text(company)
    msg_id = mirror.get("message_id")
    if msg_id:
        ok = await try_edit_text(mirror["chat_id"], msg_id, text)
        if ok:
            return False
    msg = await send_message(mirror["chat_id"], text, thread_id=mirror.get("thread_id") or 0)
    if msg_id and msg_id != msg.message_id:
        await safe_delete_message(mirror["chat_id"], msg_id)
    mirror["message_id"] = msg.message_id
    return True

async def publish_initial_company_mirror(company: dict, chat_id: int, thread_id: int = 0, card_text: str | None = None) -> int | None:
    text = card_text or company_card_text(company)
    msg = await send_message(chat_id, text, thread_id=thread_id)
    return msg.message_id

async def sync_company_everywhere(ws: dict, company_idx: int, recreate_menu: bool = True, return_details: bool = False):
    company = ws["companies"][company_idx]
    card_text = company_card_text(company)
    mirrors = company.get("mirrors", [])
    sync_jobs = [upsert_company_card(ws, company_idx, card_text)]
    sync_jobs.extend(upsert_company_mirror(mirror, company, card_text) for mirror in mirrors)
    sync_results = await asyncio.gather(*sync_jobs)
    recreated_card = sync_results[0]
    changed = any(sync_results)
    if recreated_card and ws.get("is_connected"):
        old_menu_id = ws.get("menu_msg_id")
        ws["menu_msg_id"] = None
        RUNTIME_MENU_IDS.pop(ws["id"], None)
        await safe_delete_message(ws["chat_id"], old_menu_id)
        if recreate_menu:
            msg = await send_message(ws["chat_id"], workspace_home_title(ws), reply_markup=ws_home_kb(ws["id"], ws), thread_id=ws["thread_id"])
            ws["menu_msg_id"] = msg.message_id
            RUNTIME_MENU_IDS[ws["id"]] = msg.message_id
        changed = True
    if return_details:
        return changed, recreated_card
    return changed

async def sync_company_and_refresh_view(data: dict, wid: str, company_idx: int, editor, *editor_args):
    ws = data["workspaces"].get(wid)
    if not ws:
        return False
    menu_task = asyncio.create_task(editor(data, wid, *editor_args))
    changed, recreated_card = await sync_company_everywhere(ws, company_idx, recreate_menu=False, return_details=True)
    menu_error = None
    try:
        await menu_task
    except Exception as exc:
        menu_error = exc
    if recreated_card:
        await editor(data, wid, *editor_args)
    elif menu_error is not None:
        raise menu_error
    return changed

async def sync_company_and_show_back_view(data: dict, wid: str, company_idx: int, back_to: dict):
    view = (back_to or {}).get("view", "ws")
    if view == "company":
        return await sync_company_and_refresh_view(data, wid, company_idx, edit_company_menu, company_idx)
    if view == "company_settings":
        return await sync_company_and_refresh_view(data, wid, company_idx, edit_company_settings_menu, company_idx)
    if view == "category":
        return await sync_company_and_refresh_view(data, wid, company_idx, edit_category_menu, company_idx, back_to["category_idx"])
    if view == "category_settings":
        return await sync_company_and_refresh_view(data, wid, company_idx, edit_category_settings_menu, company_idx, back_to["category_idx"])
    if view == "task":
        return await sync_company_and_refresh_view(data, wid, company_idx, edit_task_menu, company_idx, back_to["task_idx"])
    if view == "task_deadline":
        return await sync_company_and_refresh_view(data, wid, company_idx, edit_task_deadline_menu, company_idx, back_to["task_idx"])
    changed = await sync_company_everywhere(data["workspaces"][wid], company_idx, recreate_menu=False)
    await show_back_view(data, wid, back_to)
    return changed

async def publish_company_reports(ws: dict, company_idx: int, now_value: int) -> bool:
    if company_idx < 0 or company_idx >= len(ws.get("companies", [])):
        return False

    company = ws["companies"][company_idx]
    normalize_company_report_target_keys(company)
    targets = get_effective_report_targets(company)
    changed = False
    ws_id_value = str(ws.get("id") or "")
    company_id_value = str(company.get("id") or company_idx)
    intervals_by_target_key: dict[str, list[dict]] = {}
    for interval in get_report_intervals(company):
        target_key = interval.get("target_key")
        if not target_key or interval.get("kind") == "on_done":
            continue
        intervals_by_target_key.setdefault(target_key, []).append(interval)

    pending_reports = []
    report_texts_by_period: dict[tuple[int, int], str] = {}
    for target in targets:
        target_key = report_target_key(target)
        intervals = intervals_by_target_key.get(target_key, [])
        for interval in intervals:
            interval_key = (
                ws_id_value,
                company_id_value,
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
            period_key = (start_at, end_at)
            text = report_texts_by_period.get(period_key)
            if text is None:
                text = build_report_message(company, start_at, end_at)
                report_texts_by_period[period_key] = text
            pending_reports.append((target, text, interval, occurrence, interval_key))

    if not pending_reports:
        return changed

    send_results = await asyncio.gather(
        *(send_message(target["chat_id"], text, thread_id=target.get("thread_id") or 0) for target, text, _, _, _ in pending_reports),
        return_exceptions=True,
    )
    for (_, _, interval, occurrence, interval_key), result in zip(pending_reports, send_results):
        if isinstance(result, Exception):
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
    on_done_intervals_by_target_key: dict[str, list[dict]] = {}
    for interval in get_report_intervals(company):
        target_key = interval.get("target_key")
        if not target_key or interval.get("kind") != "on_done":
            continue
        on_done_intervals_by_target_key.setdefault(target_key, []).append(interval)

    if not on_done_intervals_by_target_key:
        return False

    targets_to_publish = []
    for target in get_effective_report_targets(company):
        target_key = report_target_key(target)
        target_intervals = on_done_intervals_by_target_key.get(target_key, [])
        if not target_intervals:
            continue
        targets_to_publish.append((target, target_intervals))

    if not targets_to_publish:
        return False

    text = build_task_completion_report_message(company, task)
    now_value = now_ts()
    changed = False
    ws_id_value = str(ws.get("id") or "")
    company_id_value = str(company.get("id") or company_idx)
    send_results = await asyncio.gather(
        *(send_message(target["chat_id"], text, thread_id=target.get("thread_id") or 0) for target, _ in targets_to_publish),
        return_exceptions=True,
    )
    for (_, target_intervals), result in zip(targets_to_publish, send_results):
        if isinstance(result, Exception):
            continue
        for interval in target_intervals:
            interval["last_report_at"] = now_value
            interval_key = (
                ws_id_value,
                company_id_value,
                str(interval.get("id") or ""),
            )
            RUNTIME_REPORT_OCCURRENCES[interval_key] = now_value
            changed = True
    return changed

def prompt_menu_kb(wid: str):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("в¬…пёЏ", callback_data=f"cancel:{wid}"))
    return kb

def set_prompt_state(ws: dict, awaiting_payload: dict):
    ws["awaiting"] = awaiting_payload

async def show_prompt_menu(data: dict, ws: dict, prompt_text: str):
    await upsert_ws_menu(data, ws["id"], prompt_text, prompt_menu_kb(ws["id"]))

async def show_instruction_menu(data: dict, wid: str, text: str, back_cb: str):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("в¬…пёЏ", callback_data=back_cb, style="primary"))
    await upsert_ws_menu(data, wid, text, kb, disable_web_page_preview=True)

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
        set_prompt_state(ws, awaiting_payload)
        await save_data_unlocked(data)
    await show_prompt_menu(data, ws, prompt_text)

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
        set_prompt_state(ws, payload_factory(company_idx))
        await save_data_unlocked(data)
    await show_prompt_menu(data, ws, prompt_text)

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
        set_prompt_state(ws, payload_factory(company_idx, category_idx))
        await save_data_unlocked(data)
    await show_prompt_menu(data, ws, prompt_text)

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
        set_prompt_state(ws, payload_factory(ws, company_idx, task_idx))
        await save_data_unlocked(data)
    await show_prompt_menu(data, ws, prompt_text)

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
        set_prompt_state(ws, payload_factory(category_idx))
        await save_data_unlocked(data)
    await show_prompt_menu(data, ws, prompt_text)

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
        set_prompt_state(ws, payload_factory(ws, task_idx))
        await save_data_unlocked(data)
    await show_prompt_menu(data, ws, prompt_text)

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
    await upsert_ws_menu(data, wid, workspace_home_title(ws), ws_home_kb(wid, ws))

async def edit_ws_home_menu(data: dict, wid: str):
    ws = get_connected_ws(data, wid)
    if not ws:
        return
    await upsert_ws_menu(data, wid, workspace_home_title(ws), ws_home_kb(wid, ws))

async def edit_ws_settings_menu(data: dict, wid: str):
    ws = get_connected_ws(data, wid)
    if not ws:
        return
    await upsert_ws_menu(data, wid, workspace_path_title(ws, "вљ™пёЏ РќР°СЃС‚СЂРѕР№РєРё Workspace"), ws_settings_kb(wid))

async def edit_mirror_item_menu(data: dict, wid: str, company_idx: int, mirror_idx: int):
    ws, company = await get_connected_company(data, wid, company_idx)
    if not ws:
        return
    mirrors = company.get("mirrors", [])
    if mirror_idx < 0 or mirror_idx >= len(mirrors):
        await upsert_ws_menu(data, wid, workspace_path_title(ws, rich_display_company_name(company), "рџ“¤ Р”СѓР±Р»РёСЂРѕРІР°РЅРёРµ СЃРїРёСЃРєР°"), mirrors_menu_kb(wid, company_idx, company))
        return
    mirror = mirrors[mirror_idx]
    label = mirror.get("label") or f"{mirror.get('chat_id')}/{mirror.get('thread_id') or 0}"
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("вњЌрџЏ» РџРµСЂРµРёРјРµРЅРѕРІР°С‚СЊ РЎРІСЏР·РєСѓ", callback_data=f"mirrorren:{wid}:{company_idx}:{mirror_idx}", style=False))
    kb.add(kb_btn("рџ’…рџЏ» РџРµСЂРµРїСЂРёСЃРІРѕРёС‚СЊ СЃРјР°Р№Р»РёРє", callback_data=f"mirroremoji:{wid}:{company_idx}:{mirror_idx}", style=False))
    kb.add(kb_btn("рџ”Њ РћС‚РІСЏР·Р°С‚СЊ СЃРїРёСЃРѕРє", callback_data=f"mirroroff:{wid}:{company_idx}:{mirror_idx}"))
    kb.add(kb_btn("в¬…пёЏ", callback_data=f"mirrors:{wid}:{company_idx}", style="primary"))
    await upsert_ws_menu(data, wid, workspace_path_title(ws, display_company_name(company), "рџ“¤ Р”СѓР±Р»РёСЂРѕРІР°РЅРёРµ СЃРїРёСЃРєР°", esc(label)), kb)

async def edit_company_create_menu(data: dict, wid: str):
    ws = get_connected_ws(data, wid)
    if not ws:
        return
    await upsert_ws_menu(data, wid, "вћ• РЎРїРёСЃРѕРє", company_create_mode_kb(wid, ws))

async def edit_company_menu(data: dict, wid: str, company_idx: int):
    ws, company = await get_connected_company(data, wid, company_idx)
    if not ws:
        return
    await upsert_ws_menu(data, wid, workspace_path_title(ws, rich_display_company_name(company)), company_menu_kb(wid, company_idx, company))

async def edit_company_settings_menu(data: dict, wid: str, company_idx: int):
    ws, company = await get_connected_company(data, wid, company_idx)
    if not ws:
        return
    await upsert_ws_menu(data, wid, workspace_path_title(ws, rich_display_company_name(company), "вљ™пёЏ РќР°СЃС‚СЂРѕР№РєРё СЃРїРёСЃРєР°"), company_settings_kb(wid, company_idx, company))

async def edit_report_menu(data: dict, wid: str, company_idx: int, target_idx: int):
    ws, company, target = await get_report_target_context(data, wid, company_idx, target_idx)
    if not ws:
        return
    title = [rich_display_company_name(company), "рџ§ѕ РћС‚С‡РµС‚РЅРѕСЃС‚СЊ"]
    if target:
        title.append(esc(target.get("label") or f"{target.get('chat_id')}/{target.get('thread_id') or 0}"))
    await upsert_ws_menu(data, wid, workspace_path_title(ws, *title), report_menu_kb(wid, company_idx, target_idx, company))

async def edit_report_settings_menu(data: dict, wid: str, company_idx: int, target_idx: int):
    ws, company, target = await get_report_target_context(data, wid, company_idx, target_idx)
    if not ws:
        return
    title = [rich_display_company_name(company), "рџ§ѕ РћС‚С‡РµС‚РЅРѕСЃС‚СЊ"]
    if target:
        title.append(esc(target.get("label") or f"{target.get('chat_id')}/{target.get('thread_id') or 0}"))
    title.append("вљ™пёЏ РћС‚С‡РµС‚РЅРѕСЃС‚СЊ")
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("вњЌрџЏ» РџРµСЂРµРёРјРµРЅРѕРІР°С‚СЊ РЎРІСЏР·РєСѓ", callback_data=f"reportren:{wid}:{company_idx}:{target_idx}", style=False))
    kb.add(kb_btn("рџ’…рџЏ» РџРµСЂРµРїСЂРёСЃРІРѕРёС‚СЊ СЃРјР°Р№Р»РёРє", callback_data=f"reportemoji:{wid}:{company_idx}:{target_idx}", style=False))
    kb.add(kb_btn("рџ”Њ РћС‚РІСЏР·Р°С‚СЊ", callback_data=f"reportbindoff:{wid}:{company_idx}:{target_idx}", style="danger"))
    kb.add(kb_btn("рџ§№ РћС‡РёСЃС‚РёС‚СЊ РіСЂР°С„РёРє", callback_data=f"reportclearask:{wid}:{company_idx}:{target_idx}", style="danger"))
    kb.add(kb_btn("в¬…пёЏ", callback_data=f"reportmenu:{wid}:{company_idx}:{target_idx}", style="primary"))
    await upsert_ws_menu(data, wid, workspace_path_title(ws, *title), kb)

async def edit_report_interval_menu(data: dict, wid: str, company_idx: int, target_idx: int, interval_idx: int):
    ws, company, target = await get_report_target_context(data, wid, company_idx, target_idx)
    if not ws:
        return
    interval = find_report_interval(company, interval_idx)
    if not interval or interval.get("target_key") != report_target_key(target):
        await edit_report_menu(data, wid, company_idx, target_idx)
        return
    title = [rich_display_company_name(company), "рџ§ѕ РћС‚С‡РµС‚РЅРѕСЃС‚СЊ"]
    if target:
        title.append(esc(target.get("label") or f"{target.get('chat_id')}/{target.get('thread_id') or 0}"))
    title.append(format_report_schedule_label(interval))
    if interval.get("kind") != "on_done":
        start_at, end_at = resolve_report_period(interval, report_preview_occurrence(interval), company)
        title.append(format_report_period_preview(interval, start_at, end_at))
    await upsert_ws_menu(data, wid, workspace_path_title(ws, *title), report_interval_kb(wid, company_idx, target_idx, interval_idx, interval))

async def edit_report_interval_kind_menu(data: dict, wid: str, company_idx: int, target_idx: int, flow: str, interval_idx: int | None):
    ws, company, target = await get_report_target_context(data, wid, company_idx, target_idx)
    if not ws:
        return
    label = "РР·РјРµРЅРёС‚СЊ РІСЂРµРјСЏ РѕС‚С‡РµС‚Р°" if flow == "edit" and interval_idx is not None else "Р”РѕР±Р°РІРёС‚СЊ РІСЂРµРјСЏ РѕС‚С‡РµС‚Р°"
    await upsert_ws_menu(
        data,
        wid,
        workspace_path_title(ws, rich_display_company_name(company), "рџ§ѕ РћС‚С‡РµС‚РЅРѕСЃС‚СЊ", esc(target.get("label") or f"{target.get('chat_id')}/{target.get('thread_id') or 0}"), label),
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
            "вљ™пёЏ РЁР°Р±Р»РѕРЅС‹ Р·Р°РґР°С‡",
            rich_display_template_name(active),
            "рџ§ѕ РћС‚С‡РµС‚РЅРѕСЃС‚СЊ",
            format_report_schedule_label(draft_interval),
            "РљРѕРїРёС‚СЊ Р·Р°РґР°С‡Рё:",
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
            "рџ§ѕ РћС‚С‡РµС‚РЅРѕСЃС‚СЊ",
            format_report_schedule_label(draft_interval),
            "РљРѕРїРёС‚СЊ Р·Р°РґР°С‡Рё:",
        )
    await upsert_ws_menu(data, wid, title, report_accumulation_kb(wid, draft_interval))

async def edit_report_targets_menu(data: dict, wid: str, company_idx: int):
    ws, company = await get_connected_company(data, wid, company_idx)
    if not ws:
        return
    await upsert_ws_menu(data, wid, workspace_path_title(ws, rich_display_company_name(company), "рџ§ѕ РћС‚С‡РµС‚РЅРѕСЃС‚СЊ", "рџ“Ћ РџСЂРёРІСЏР·РєР°"), report_targets_kb(wid, company_idx, company))

async def edit_category_menu(data: dict, wid: str, company_idx: int, category_idx: int):
    ws, company, category = await get_company_category_context(data, wid, company_idx, category_idx)
    if not ws:
        return
    await upsert_ws_menu(data, wid, workspace_path_title(ws, rich_display_company_name(company), rich_display_category_name(category)), category_menu_kb(wid, company_idx, category_idx, category, company))

async def edit_category_settings_menu(data: dict, wid: str, company_idx: int, category_idx: int):
    ws, company, category = await get_company_category_context(data, wid, company_idx, category_idx)
    if not ws:
        return
    await upsert_ws_menu(data, wid, workspace_path_title(ws, rich_display_company_name(company), rich_display_category_name(category), "вљ™пёЏ РџРѕРґРіСЂСѓРїРїР°"), category_settings_kb(wid, company_idx, category_idx, category))

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
    kb.add(kb_btn("вЏ° РџРѕРјРµРЅСЏС‚СЊ РґРµРґР»Р°Р№РЅ", callback_data=f"taskdeadline:{wid}:{company_idx}:{task_idx}", style=False))
    kb.add(kb_btn("рџ—‘ РЈРґР°Р»РёС‚СЊ РґРµРґР»Р°Р№РЅ", callback_data=f"taskdeadel:{wid}:{company_idx}:{task_idx}", style="danger"))
    kb.add(kb_btn("в¬…пёЏ", callback_data=f"task:{wid}:{company_idx}:{task_idx}", style="primary"))
    await upsert_ws_menu(data, wid, task_menu_title(ws, company, task, category), kb)

async def edit_task_move_menu(data: dict, wid: str, company_idx: int, task_idx: int):
    ws, company, task, _ = await get_company_task_context(data, wid, company_idx, task_idx)
    if not ws:
        return
    await upsert_ws_menu(data, wid, f"рџ“Ґ {task['text']}", task_move_kb(wid, company_idx, task_idx, company, task))

async def edit_templates_root_menu(data: dict, wid: str):
    ws = get_connected_ws(data, wid)
    if not ws:
        return
    await upsert_ws_menu(data, wid, workspace_path_title(ws, "вљ™пёЏ РЁР°Р±Р»РѕРЅС‹ Р·Р°РґР°С‡"), templates_root_kb(wid, ws))

async def edit_template_menu(data: dict, wid: str):
    ws, active = get_connected_active_template(data, wid)
    if not ws:
        return
    await upsert_ws_menu(data, wid, workspace_path_title(ws, "вљ™пёЏ РЁР°Р±Р»РѕРЅС‹ Р·Р°РґР°С‡", rich_display_template_name(active)), template_menu_kb(wid, ws))

async def edit_template_category_menu(data: dict, wid: str, category_idx: int):
    ws, active, category = await get_template_category_context(data, wid, category_idx)
    if not ws:
        return
    await upsert_ws_menu(data, wid, workspace_path_title(ws, "вљ™пёЏ РЁР°Р±Р»РѕРЅС‹ Р·Р°РґР°С‡", rich_display_template_name(active), rich_display_category_name(category)), template_category_menu_kb(wid, category_idx, category, active))

async def edit_template_category_settings_menu(data: dict, wid: str, category_idx: int):
    ws, active, category = await get_template_category_context(data, wid, category_idx)
    if not ws:
        return
    await upsert_ws_menu(data, wid, workspace_path_title(ws, "вљ™пёЏ РЁР°Р±Р»РѕРЅС‹ Р·Р°РґР°С‡", rich_display_template_name(active), rich_display_category_name(category), "вљ™пёЏ РџРѕРґРіСЂСѓРїРїР°"), template_category_settings_kb(wid, category_idx))

async def edit_template_settings_menu(data: dict, wid: str):
    ws, active = get_connected_active_template(data, wid)
    if not ws:
        return
    await upsert_ws_menu(data, wid, workspace_path_title(ws, "вљ™пёЏ РЁР°Р±Р»РѕРЅС‹ Р·Р°РґР°С‡", rich_display_template_name(active), "вљ™пёЏ РЁР°Р±Р»РѕРЅ"), template_settings_kb(wid))

async def edit_template_report_menu(data: dict, wid: str):
    ws, active = get_connected_active_template(data, wid)
    if not ws:
        return
    await upsert_ws_menu(data, wid, workspace_path_title(ws, "вљ™пёЏ РЁР°Р±Р»РѕРЅС‹ Р·Р°РґР°С‡", rich_display_template_name(active), "рџ§ѕ РћС‚С‡РµС‚РЅРѕСЃС‚СЊ"), template_report_menu_kb(wid, ws))

async def edit_template_report_settings_menu(data: dict, wid: str):
    ws, active = get_connected_active_template(data, wid)
    if not ws:
        return
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("рџ§№ РћС‡РёСЃС‚РёС‚СЊ РіСЂР°С„РёРє", callback_data=f"tplreportclearask:{wid}", style="danger"))
    kb.add(kb_btn("в¬…пёЏ", callback_data=f"tplreport:{wid}", style="primary"))
    await upsert_ws_menu(data, wid, workspace_path_title(ws, "вљ™пёЏ РЁР°Р±Р»РѕРЅС‹ Р·Р°РґР°С‡", rich_display_template_name(active), "рџ§ѕ РћС‚С‡РµС‚РЅРѕСЃС‚СЊ", "вљ™пёЏ РћС‚С‡РµС‚РЅРѕСЃС‚СЊ"), kb)

async def edit_template_report_interval_menu(data: dict, wid: str, interval_idx: int):
    ws, active = get_connected_active_template(data, wid)
    if not ws:
        return
    interval = find_report_interval(active, interval_idx)
    if not interval:
        await edit_template_report_menu(data, wid)
        return
    title = [
        "вљ™пёЏ РЁР°Р±Р»РѕРЅС‹ Р·Р°РґР°С‡",
        rich_display_template_name(active),
        "рџ§ѕ РћС‚С‡РµС‚РЅРѕСЃС‚СЊ",
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
    label = "РР·РјРµРЅРёС‚СЊ РІСЂРµРјСЏ РѕС‚С‡РµС‚Р°" if flow == "edit" and interval_idx is not None else "Р”РѕР±Р°РІРёС‚СЊ РІСЂРµРјСЏ РѕС‚С‡РµС‚Р°"
    await upsert_ws_menu(
        data,
        wid,
        workspace_path_title(ws, "вљ™пёЏ РЁР°Р±Р»РѕРЅС‹ Р·Р°РґР°С‡", rich_display_template_name(active), "рџ§ѕ РћС‚С‡РµС‚РЅРѕСЃС‚СЊ", label),
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
    kb.add(kb_btn("вЏ° РџРѕРјРµРЅСЏС‚СЊ РґРµРґР»Р°Р№РЅ", callback_data=f"tpltaskdeadline:{wid}:{task_idx}", style=False))
    kb.add(kb_btn("рџ—‘ РЈРґР°Р»РёС‚СЊ РґРµРґР»Р°Р№РЅ", callback_data=f"tpltaskdeadel:{wid}:{task_idx}", style="danger"))
    kb.add(kb_btn("в¬…пёЏ", callback_data=f"tpltask:{wid}:{task_idx}", style="primary"))
    await upsert_ws_menu(data, wid, template_task_title(ws, active, task, category), kb)

async def edit_template_task_move_menu(data: dict, wid: str, task_idx: int):
    ws, _, task, _ = await get_template_task_context(data, wid, task_idx)
    if not ws:
        return
    await upsert_ws_menu(data, wid, f"рџ“Ґ {task['text']}", template_task_move_kb(wid, task_idx, ws, task))

async def clear_workspace_contents(ws: dict):
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
        user = ensure_user(data, uid)
        user["pm_menu_msg_id"] = cb.message.message_id
        user["pm_awaiting"] = None
        await save_data_unlocked(data)
    await safe_edit_text(int(uid), cb.message.message_id, pm_main_text(uid, data), reply_markup=pm_main_kb(uid, data))

@dp.callback_query_handler(lambda c: c.data == "pmhelp:root")
async def pm_help(cb: types.CallbackQuery):
    await cb.answer()
    if cb.message.chat.type != "private" or should_ignore_callback(cb):
        return
    uid = str(cb.from_user.id)
    await safe_edit_text(
        int(uid),
        cb.message.message_id,
        workspace_connect_instruction_text(),
        reply_markup=InlineKeyboardMarkup(row_width=1).add(kb_btn("в¬…пёЏ", callback_data="pmrefresh:root", style="primary")),
        disable_web_page_preview=True,
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
                "name": "Р›РёС‡РЅС‹Р№ workspace",
                "chat_title": "Р›РёС‡РЅС‹Р№ workspace",
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
    await edit_ws_home_menu(data, wid)

@dp.callback_query_handler(lambda c: c.data.startswith("pmws:"))
async def pm_open_workspace(cb: types.CallbackQuery):
    await cb.answer()
    if cb.message.chat.type != "private" or should_ignore_callback(cb):
        return
    uid = str(cb.from_user.id)
    wid = cb.data.split(":", 1)[1]
    async with FILE_LOCK:
        data = await load_data_unlocked()
        user = ensure_user(data, uid)
        user["pm_menu_msg_id"] = cb.message.message_id
        user["pm_awaiting"] = None
        await save_data_unlocked(data)
    await edit_pm_workspace_view(data, uid, wid, cb.message.message_id)

@dp.callback_query_handler(lambda c: c.data.startswith("pmwsren:"))
async def pm_rename_workspace_prompt(cb: types.CallbackQuery):
    await cb.answer()
    if cb.message.chat.type != "private" or should_ignore_callback(cb):
        return
    uid = str(cb.from_user.id)
    wid = cb.data.split(":", 1)[1]
    show_root = False
    async with FILE_LOCK:
        data = await load_data_unlocked()
        user = ensure_user(data, uid)
        user["pm_menu_msg_id"] = cb.message.message_id
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected") or wid not in user.get("workspaces", []):
            user["pm_awaiting"] = None
            show_root = True
        else:
            user["pm_awaiting"] = {"type": "rename_workspace_label", "wid": wid}
        await save_data_unlocked(data)
    if show_root:
        await safe_edit_text(int(uid), cb.message.message_id, pm_main_text(uid, data), reply_markup=pm_main_kb(uid, data))
        return
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("в¬…пёЏ", callback_data=f"pmws:{wid}", style="primary"))
    await safe_edit_text(int(uid), cb.message.message_id, "вњЌрџЏ» Р’РІРµРґРё РЅРѕРІРѕРµ РёРјСЏ Workspace:", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith("pmwsemoji:"))
async def pm_workspace_emoji_prompt(cb: types.CallbackQuery):
    await cb.answer()
    if cb.message.chat.type != "private" or should_ignore_callback(cb):
        return
    uid = str(cb.from_user.id)
    wid = cb.data.split(":", 1)[1]
    show_root = False
    async with FILE_LOCK:
        data = await load_data_unlocked()
        user = ensure_user(data, uid)
        user["pm_menu_msg_id"] = cb.message.message_id
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected") or wid not in user.get("workspaces", []):
            user["pm_awaiting"] = None
            show_root = True
        else:
            user["pm_awaiting"] = {"type": "workspace_label_emoji", "wid": wid}
        await save_data_unlocked(data)
    if show_root:
        await safe_edit_text(int(uid), cb.message.message_id, pm_main_text(uid, data), reply_markup=pm_main_kb(uid, data))
        return
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(kb_btn("в¬…пёЏ", callback_data=f"pmws:{wid}", style="primary"))
    await safe_edit_text(int(uid), cb.message.message_id, "рџ’…рџЏ» РџСЂРёС€Р»Рё РѕРґРёРЅ СЃРјР°Р№Р»РёРє РґР»СЏ Workspace:", reply_markup=kb)

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
    await upsert_ws_menu(data, wid, workspace_path_title(ws, "вљ™пёЏ РќР°СЃС‚СЂРѕР№РєРё Workspace", "рџ§№ РћС‡РёСЃС‚РёС‚СЊ workspace?"), confirm_kb(f"wsclear:{wid}", f"wsset:{wid}"))

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
    await edit_ws_home_menu(data, wid)

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
    await safe_edit_text(int(cb.from_user.id), cb.message.message_id, f"{workspace_title_label(ws)}\n\nРћС‡РёСЃС‚РёС‚СЊ workspace?", reply_markup=confirm_kb(f"pmwsclear:{wid}", f"pmws:{wid}"))

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
    ws = data["workspaces"].get(wid)
    if ws and ws.get("is_connected"):
        await edit_ws_home_menu(data, wid)
    await safe_edit_text(int(uid), cb.message.message_id, pm_main_text(uid, data), reply_markup=pm_main_kb(uid, data))

@dp.callback_query_handler(lambda c: c.data.startswith("pmwsdelask:"))
async def pm_delete_workspace_ask(cb: types.CallbackQuery):
    await cb.answer()
    if cb.message.chat.type != "private" or should_ignore_callback(cb):
        return
    wid = cb.data.split(":", 1)[1]
    data = await load_data()
    uid = str(cb.from_user.id)
    ws = data["workspaces"].get(wid)
    title = f"РЈРґР°Р»РёС‚СЊ workspace В«{esc(ws.get('name') or 'Workspace')}В»?" if ws else "РЈРґР°Р»РёС‚СЊ workspace?"
    await safe_edit_text(int(uid), cb.message.message_id, title, reply_markup=confirm_kb(f"pmwsdel:{wid}", f"pmws:{wid}"))

@dp.callback_query_handler(lambda c: c.data.startswith("pmwsdel:"))
async def pm_delete_workspace(cb: types.CallbackQuery):
    await cb.answer()
    if cb.message.chat.type != "private" or should_ignore_callback(cb):
        return
    current_uid = str(cb.from_user.id)
    wid = cb.data.split(":", 1)[1]
    show_root_only = False

    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws:
            await save_data_unlocked(data)
            show_root_only = True
        else:
            ws_name = ws["name"]
            chat_id = ws["chat_id"]
            thread_id = ws["thread_id"]
            menu_msg_id = ws.get("menu_msg_id")
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

    if show_root_only:
        await safe_edit_text(int(current_uid), cb.message.message_id, pm_main_text(current_uid, data), reply_markup=pm_main_kb(current_uid, data))
        return

    await safe_delete_message(chat_id, menu_msg_id)
    await safe_edit_text(int(current_uid), cb.message.message_id, pm_main_text(current_uid, data), reply_markup=pm_main_kb(current_uid, data))
    for uid in affected_users:
        if uid != current_uid:
            await update_pm_menu(uid, data)
    for uid in affected_users:
        await send_temp_message(int(uid), f"Workspace В«{ws_name}В» РѕС‚РєР»СЋС‡РµРЅ", delay=10)
    await send_temp_message(chat_id, f"Workspace В«{ws_name}В» РѕС‚РєР»СЋС‡РµРЅ", thread_id, delay=10)

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
    old_menu_id = None
    old_company_card_ids = []

    async with FILE_LOCK:
        data = await load_data_unlocked()
        ensure_user(data, uid)
        existing_ws = data["workspaces"].get(wid)
        topic_title, topic_title_source = resolve_message_topic_title(data, message)
        chat_title, topic_title = remember_binding_place(data, message.chat.id, thread_id, chat_title, topic_title, topic_title_source)
        ws_name = refresh_binding_labels(data, message.chat.id, thread_id)
        if existing_ws and existing_ws.get("is_connected"):
            existing_ws["chat_title"] = chat_title
            existing_ws["topic_title"] = topic_title
            existing_ws["name"] = ws_name
            await save_data_unlocked(data)
            asyncio.create_task(send_temp_message(message.chat.id, f"Workspace В«{existing_ws.get('name') or 'Workspace'}В» СѓР¶Рµ РїРѕРґРєР»СЋС‡С‘РЅ", thread_id, delay=10))
            asyncio.create_task(try_delete_user_message(message))
            return

        old_companies = existing_ws["companies"] if existing_ws else []
        old_templates = existing_ws.get("templates") if existing_ws else [make_template(tasks=default_template_tasks())]
        old_active_template_id = existing_ws.get("active_template_id") if existing_ws else None

        if existing_ws:
            old_menu_id = existing_ws.get("menu_msg_id")
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
    for card_msg_id in old_company_card_ids:
        await safe_delete_message(message.chat.id, card_msg_id)

    fresh = data
    ws = fresh["workspaces"].get(wid)
    if not ws:
        return

    for idx, company in enumerate(ws.get("companies", [])):
        card_text = company_card_text(company)
        await upsert_company_card(ws, idx, card_text)
        for mirror in company.get("mirrors", []):
            await upsert_company_mirror(mirror, company, card_text)
    await edit_ws_home_menu(fresh, wid)
    await update_pm_menu(uid, fresh)
    await save_data(fresh)
    await try_delete_user_message(message)
    try:
        await send_temp_message(int(uid), f"Workspace В«{ws['name']}В» РїРѕРґРєР»СЋС‡С‘РЅ", delay=10)
    except Exception:
        pass

@dp.message_handler(lambda message: bool(getattr(message, "forum_topic_created", None) or getattr(message, "forum_topic_edited", None)), content_types=types.ContentTypes.ANY)
async def track_forum_topic_updates(message: types.Message):
    if message.chat.type == "private":
        return
    thread_id = message.message_thread_id or 0
    if not thread_id:
        return
    topic_title = extract_message_topic_title(message)
    if not topic_title:
        return
    topic_title_source = "edited" if getattr(message, "forum_topic_edited", None) else "created"

    async with FILE_LOCK:
        data = await load_data_unlocked()
        remember_binding_place(data, message.chat.id, thread_id, message.chat.title or "Workspace", topic_title, topic_title_source)
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
    await upsert_ws_menu(data, wid, workspace_path_title(ws, rich_display_company_name(company), "рџ“¤ Р”СѓР±Р»РёСЂРѕРІР°РЅРёРµ СЃРїРёСЃРєР°"), mirrors_menu_kb(wid, company_idx, company))

@dp.callback_query_handler(lambda c: c.data.startswith("mirroritem:"))
async def open_mirror_item(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, mirror_idx = cb.data.split(":")
    data = await load_data()
    await edit_mirror_item_menu(data, wid, int(company_idx), int(mirror_idx))

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
    await upsert_ws_menu(data, wid, workspace_path_title(ws, rich_display_company_name(company), "рџ“¤ Р”СѓР±Р»РёСЂРѕРІР°РЅРёРµ СЃРїРёСЃРєР°"), mirrors_menu_kb(wid, company_idx, company))

@dp.callback_query_handler(lambda c: c.data.startswith("mirrorren:"))
async def mirror_rename_binding_prompt(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, mirror_idx = cb.data.split(":")
    company_idx = int(company_idx)
    mirror_idx = int(mirror_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws, company = await get_connected_company(data, wid, company_idx)
        if not ws:
            return
        ws["menu_msg_id"] = cb.message.message_id
        RUNTIME_MENU_IDS[wid] = cb.message.message_id
        mirrors = company.get("mirrors", [])
        if mirror_idx < 0 or mirror_idx >= len(mirrors):
            return
        mirror = mirrors[mirror_idx]
        prompt_text = "вњЌрџЏ» Р’РІРµРґРё РЅРѕРІРѕРµ РёРјСЏ СЃРІСЏР·РєРё:"
        set_prompt_state(
            ws,
            {
                "type": "rename_binding_label",
                "chat_id": mirror.get("chat_id"),
                "thread_id": mirror.get("thread_id") or 0,
                "back_to": {"view": "mirror_item", "company_idx": company_idx, "mirror_idx": mirror_idx},
            },
        )
        await save_data_unlocked(data)
    await show_prompt_menu(data, ws, prompt_text)

@dp.callback_query_handler(lambda c: c.data.startswith("mirroremoji:"))
async def mirror_binding_emoji_prompt(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, mirror_idx = cb.data.split(":")
    company_idx = int(company_idx)
    mirror_idx = int(mirror_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws, company = await get_connected_company(data, wid, company_idx)
        if not ws:
            return
        ws["menu_msg_id"] = cb.message.message_id
        RUNTIME_MENU_IDS[wid] = cb.message.message_id
        mirrors = company.get("mirrors", [])
        if mirror_idx < 0 or mirror_idx >= len(mirrors):
            return
        mirror = mirrors[mirror_idx]
        prompt_text = "рџ’…рџЏ» РџСЂРёС€Р»Рё РѕРґРёРЅ СЃРјР°Р№Р»РёРє РґР»СЏ СЃРІСЏР·РєРё:"
        set_prompt_state(
            ws,
            {
                "type": "binding_emoji",
                "chat_id": mirror.get("chat_id"),
                "thread_id": mirror.get("thread_id") or 0,
                "back_to": {"view": "mirror_item", "company_idx": company_idx, "mirror_idx": mirror_idx},
            },
        )
        await save_data_unlocked(data)
    await show_prompt_menu(data, ws, prompt_text)

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

    fresh = data
    ws2 = fresh["workspaces"].get(wid)
    if not ws2 or not (0 <= company_idx < len(ws2.get("companies", []))):
        return
    company2 = ws2["companies"][company_idx]
    if show_import_menu:
        await upsert_ws_menu(
            fresh,
            wid,
            workspace_path_title(ws2, rich_display_company_name(company2), "рџ“¤ Р”СѓР±Р»РёСЂРѕРІР°РЅРёРµ СЃРїРёСЃРєР°", "вћ• Р”РѕР±Р°РІРёС‚СЊ СЃРІСЏР·РєСѓ"),
            mirror_import_candidates_kb(wid, company_idx, company2),
        )
        return
    await show_instruction_menu(
        fresh,
        wid,
        binding_instruction_text("рџ“¤ РљР°Рє РґРѕР±Р°РІРёС‚СЊ СЃРІСЏР·РєСѓ", token),
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

    fresh = data
    ws2 = fresh["workspaces"].get(wid)
    if ws2 and 0 <= company_idx < len(ws2.get("companies", [])):
        await show_instruction_menu(
            fresh,
            wid,
            binding_instruction_text("рџ“¤ РљР°Рє РґРѕР±Р°РІРёС‚СЊ СЃРІСЏР·РєСѓ", token),
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

    fresh = data
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
        await upsert_ws_menu(fresh, wid, workspace_path_title(ws2, rich_display_company_name(company2), "рџ“¤ Р”СѓР±Р»РёСЂРѕРІР°РЅРёРµ СЃРїРёСЃРєР°"), mirrors_menu_kb(wid, company_idx, company2))

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
    fresh = data
    ws2 = fresh["workspaces"].get(wid)
    if ws2 and 0 <= company_idx < len(ws2.get("companies", [])):
        company2 = ws2["companies"][company_idx]
        await upsert_ws_menu(fresh, wid, f"рџ“¤ Р”СѓР±Р»РёСЂРѕРІР°РЅРёРµ СЃРїРёСЃРєР°: {display_company_name(company2)}", mirrors_menu_kb(wid, company_idx, company2))

@dp.message_handler(commands=["mirror"])
async def cmd_mirror(message: types.Message):
    if message.chat.type == "private":
        return
    code = (message.get_args() or "").strip().upper()
    if not code:
        await send_temp_message(message.chat.id, "РЈРєР°Р¶Рё РєРѕРґ: /mirror CODE", message.message_thread_id or 0, delay=10)
        await try_delete_user_message(message)
        return

    async with FILE_LOCK:
        data = await load_data_unlocked()
        payload = data.get("mirror_tokens", {}).get(code)
        if not payload:
            await save_data_unlocked(data)
            asyncio.create_task(send_temp_message(message.chat.id, "РљРѕРґ РЅРµ РЅР°Р№РґРµРЅ РёР»Рё СѓР¶Рµ РёСЃРїРѕР»СЊР·РѕРІР°РЅ.", message.message_thread_id or 0, delay=10))
            asyncio.create_task(try_delete_user_message(message))
            return
        source_wid = payload["source_wid"]
        company_id = payload["company_id"]
        ws = data["workspaces"].get(source_wid)
        if not ws:
            data["mirror_tokens"].pop(code, None)
            await save_data_unlocked(data)
            asyncio.create_task(send_temp_message(message.chat.id, "РСЃС…РѕРґРЅС‹Р№ workspace РЅРµ РЅР°Р№РґРµРЅ.", message.message_thread_id or 0, delay=10))
            asyncio.create_task(try_delete_user_message(message))
            return
        company_idx = find_company_index_by_id(ws, company_id)
        if company_idx is None:
            data["mirror_tokens"].pop(code, None)
            await save_data_unlocked(data)
            asyncio.create_task(send_temp_message(message.chat.id, "РЎРїРёСЃРѕРє РЅРµ РЅР°Р№РґРµРЅ.", message.message_thread_id or 0, delay=10))
            asyncio.create_task(try_delete_user_message(message))
            return
        company = ws["companies"][company_idx]
        token_kind = payload.get("kind") or "mirror"
        thread_id = message.message_thread_id or 0
        topic_title, topic_title_source = resolve_message_topic_title(data, message)
        remember_binding_place(data, message.chat.id, thread_id, message.chat.title or "Р§Р°С‚", topic_title, topic_title_source)
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

    fresh = data
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
            await sync_company_everywhere(ws, company_idx, recreate_menu=False)
        await save_data(fresh)
    await try_delete_user_message(message)
    if source_wid in fresh.get("workspaces", {}):
        if token_kind == "report_target":
            await edit_report_targets_menu(fresh, source_wid, company_idx)
        else:
            ws2 = fresh["workspaces"].get(source_wid)
            if ws2 and 0 <= company_idx < len(ws2.get("companies", [])):
                company2 = ws2["companies"][company_idx]
                await upsert_ws_menu(fresh, source_wid, workspace_path_title(ws2, rich_display_company_name(company2), "рџ“¤ Р”СѓР±Р»РёСЂРѕРІР°РЅРёРµ СЃРїРёСЃРєР°"), mirrors_menu_kb(source_wid, company_idx, company2))
    if token_kind == "report_target":
        await send_temp_message(ws["chat_id"], f"рџ§ѕ РћС‚С‡РµС‚С‹ РїРѕ СЃРїРёСЃРєСѓ В«{company['title']}В» С‚РµРїРµСЂСЊ Р±СѓРґСѓС‚ РІС‹РіСЂСѓР¶Р°С‚СЊСЃСЏ РµС‰Рµ РІ РѕРґРёРЅ С‚СЂРµРґ/С‡Р°С‚", source_thread_id, delay=10)
    else:
        await send_temp_message(ws["chat_id"], f"рџ“¤ РЎРїРёСЃРѕРє В«{company['title']}В» РґСѓР±Р»РёСЂСѓРµС‚СЃСЏ РµС‰С‘ РІ РѕРґРёРЅ С‚СЂРµРґ/С‡Р°С‚", source_thread_id, delay=10)

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

        prompt_text = report_schedule_prompt_text(kind)
        set_prompt_state(
            ws,
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
    await show_prompt_menu(data, ws, prompt_text)

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

    fresh = data
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

    await edit_template_report_menu(data, wid)

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
        prompt_text = report_schedule_prompt_text(kind)
        set_prompt_state(
            ws,
            {
                "type": "template_report_schedule_time",
                "interval_idx": interval_idx,
                "flow": flow,
                "draft_interval": draft,
                "back_to": {"view": "template_report_interval_kind", "interval_idx": interval_idx, "flow": flow},
            },
        )
        await save_data_unlocked(data)
    await show_prompt_menu(data, ws, prompt_text)

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

@dp.callback_query_handler(lambda c: c.data.startswith("reportren:"))
async def report_rename_binding_prompt(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, target_idx = cb.data.split(":")
    company_idx = int(company_idx)
    target_idx = int(target_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws, _, target = await get_report_target_context(data, wid, company_idx, target_idx)
        if not ws or not target:
            return
        ws["menu_msg_id"] = cb.message.message_id
        RUNTIME_MENU_IDS[wid] = cb.message.message_id
        prompt_text = "вњЌрџЏ» Р’РІРµРґРё РЅРѕРІРѕРµ РёРјСЏ СЃРІСЏР·РєРё:"
        set_prompt_state(
            ws,
            {
                "type": "rename_binding_label",
                "chat_id": target.get("chat_id"),
                "thread_id": target.get("thread_id") or 0,
                "back_to": {"view": "report_settings", "company_idx": company_idx, "target_idx": target_idx},
            },
        )
        await save_data_unlocked(data)
    await show_prompt_menu(data, ws, prompt_text)

@dp.callback_query_handler(lambda c: c.data.startswith("reportemoji:"))
async def report_binding_emoji_prompt(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, company_idx, target_idx = cb.data.split(":")
    company_idx = int(company_idx)
    target_idx = int(target_idx)
    async with FILE_LOCK:
        data = await load_data_unlocked()
        ws, _, target = await get_report_target_context(data, wid, company_idx, target_idx)
        if not ws or not target:
            return
        ws["menu_msg_id"] = cb.message.message_id
        RUNTIME_MENU_IDS[wid] = cb.message.message_id
        prompt_text = "рџ’…рџЏ» РџСЂРёС€Р»Рё РѕРґРёРЅ СЃРјР°Р№Р»РёРє РґР»СЏ СЃРІСЏР·РєРё:"
        set_prompt_state(
            ws,
            {
                "type": "binding_emoji",
                "chat_id": target.get("chat_id"),
                "thread_id": target.get("thread_id") or 0,
                "back_to": {"view": "report_settings", "company_idx": company_idx, "target_idx": target_idx},
            },
        )
        await save_data_unlocked(data)
    await show_prompt_menu(data, ws, prompt_text)

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

    await edit_report_menu(data, wid, company_idx_value, target_idx_value)

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

    await edit_report_accumulation_menu(data, wid)

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
                prompt_text = "рџ§ѕ РџСЂРёС€Р»Рё С‚РѕС‡РЅСѓСЋ РґР°С‚Сѓ Рё РІСЂРµРјСЏ РЅР°С‡Р°Р»Р° РЅР°РєРѕРїР»РµРЅРёСЏ"
            elif draft_interval.get("kind") == "monthly":
                prompt_text = "рџ§ѕ РџСЂРёС€Р»Рё С‡РёСЃР»Рѕ Рё РІСЂРµРјСЏ РЅР°С‡Р°Р»Р° РЅР°РєРѕРїР»РµРЅРёСЏ, РЅР°РїСЂРёРјРµСЂ: 15 08:30"
            else:
                prompt_text = "рџ§ѕ РџСЂРёС€Р»Рё С‚РѕС‡РЅСѓСЋ РґР°С‚Сѓ Рё РІСЂРµРјСЏ РЅР°С‡Р°Р»Р° РЅР°РєРѕРїР»РµРЅРёСЏ"
            set_prompt_state(
                ws,
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
    if mode == "specific":
        await show_prompt_menu(data, ws, prompt_text)
        return
    if is_template:
        await finalize_template_report_interval(wid, draft_interval, flow, interval_idx)
    else:
        await finalize_report_interval(wid, company_idx, draft_interval, flow, interval_idx)

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
        "РЈРґР°Р»РёС‚СЊ РёРЅС‚РµСЂРІР°Р» РѕС‚С‡РµС‚Р°?",
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
    await edit_report_menu(data, wid, company_idx, target_idx)

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
        "РћС‡РёСЃС‚РёС‚СЊ РІРµСЃСЊ РіСЂР°С„РёРє РѕС‚С‡РµС‚РЅРѕСЃС‚Рё?",
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
    await edit_report_menu(data, wid, company_idx, target_idx)

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

    fresh = data
    ws2 = fresh["workspaces"].get(wid)
    if not ws2 or not (0 <= company_idx < len(ws2.get("companies", []))):
        return
    company2 = ws2["companies"][company_idx]
    if show_import_menu:
        await upsert_ws_menu(
            fresh,
            wid,
            workspace_path_title(ws2, rich_display_company_name(company2), "рџ§ѕ РћС‚С‡РµС‚РЅРѕСЃС‚СЊ", "рџ“Ћ РџСЂРёРІСЏР·РєР°", "вћ• Р”РѕР±Р°РІРёС‚СЊ СЃРІСЏР·РєСѓ"),
            report_import_candidates_kb(wid, company_idx, company2),
        )
        return
    await show_instruction_menu(
        fresh,
        wid,
        binding_instruction_text("рџ§ѕ РљР°Рє РґРѕР±Р°РІРёС‚СЊ РїСЂРёРІСЏР·РєСѓ РґР»СЏ РѕС‚С‡РµС‚РЅРѕСЃС‚Рё", token),
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

    await show_instruction_menu(
        data,
        wid,
        binding_instruction_text("рџ§ѕ РљР°Рє РґРѕР±Р°РІРёС‚СЊ РїСЂРёРІСЏР·РєСѓ РґР»СЏ РѕС‚С‡РµС‚РЅРѕСЃС‚Рё", token),
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

    await edit_report_targets_menu(data, wid, company_idx)

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

    await edit_report_targets_menu(data, wid, company_idx)

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

    await edit_template_report_menu(data, wid)

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
    await edit_report_accumulation_menu(data, wid)

@dp.callback_query_handler(lambda c: c.data.startswith("tplreportdelask:"))
async def template_report_delete_ask(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    _, wid, interval_idx = cb.data.split(":")
    data = await load_data()
    await upsert_ws_menu(data, wid, "РЈРґР°Р»РёС‚СЊ РРЅС‚РµСЂРІР°Р» РћС‚С‡РµС‚Р°?", confirm_kb(f"tplreportdel:{wid}:{interval_idx}", f"tplreportitem:{wid}:{interval_idx}"))

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
    await edit_template_report_menu(data, wid)

@dp.callback_query_handler(lambda c: c.data.startswith("tplreportclearask:"))
async def template_report_clear_ask(cb: types.CallbackQuery):
    await cb.answer()
    if should_ignore_callback(cb):
        return
    wid = cb.data.split(":", 1)[1]
    data = await load_data()
    await upsert_ws_menu(data, wid, "РћС‡РёСЃС‚РёС‚СЊ РІРµСЃСЊ РіСЂР°С„РёРє РѕС‚С‡РµС‚РЅРѕСЃС‚Рё?", confirm_kb(f"tplreportclear:{wid}", f"tplreportsettings:{wid}"))

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
    await edit_template_report_menu(data, wid)

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
            workspace_path_title(ws, rich_display_company_name(company), "рџ“¤ Р”СѓР±Р»РёСЂРѕРІР°РЅРёРµ СЃРїРёСЃРєР°"),
            mirrors_menu_kb(wid, company_idx, company),
        )
    elif view == "mic" and a != "x":
        company_idx = int(a)
        company = ws["companies"][company_idx]
        await upsert_ws_menu(
            data,
            wid,
            workspace_path_title(ws, rich_display_company_name(company), "рџ“¤ Р”СѓР±Р»РёСЂРѕРІР°РЅРёРµ СЃРїРёСЃРєР°", "вћ• Р”РѕР±Р°РІРёС‚СЊ СЃРІСЏР·РєСѓ"),
            mirror_import_candidates_kb(wid, company_idx, company),
        )
    elif view == "ric" and a != "x":
        company_idx = int(a)
        company = ws["companies"][company_idx]
        await upsert_ws_menu(
            data,
            wid,
            workspace_path_title(ws, rich_display_company_name(company), "рџ§ѕ РћС‚С‡РµС‚РЅРѕСЃС‚СЊ", "рџ“Ћ РџСЂРёРІСЏР·РєР°", "вћ• Р”РѕР±Р°РІРёС‚СЊ СЃРІСЏР·РєСѓ"),
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
        prompt_text = "вњЏпёЏ РќР°РїРёС€Рё РЅР°Р·РІР°РЅРёРµ СЃРїРёСЃРєР°:"
        set_prompt_state(ws, {"type": "new_company", "use_template": mode == "tpl", "template_id": template_id, "back_to": {"view": "ws"}})
        await save_data_unlocked(data)
    await show_prompt_menu(data, ws, prompt_text)

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
    elif view == "mirror_item":
        await edit_mirror_item_menu(data, wid, back_to["company_idx"], back_to["mirror_idx"])
    elif view == "report":
        await edit_report_menu(data, wid, back_to["company_idx"], back_to["target_idx"])
    elif view == "report_settings":
        await edit_report_settings_menu(data, wid, back_to["company_idx"], back_to["target_idx"])
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
        back_to = awaiting.get("back_to", {"view": "ws"})
        ws["awaiting"] = back_to.get("restore_awaiting")
        await save_data_unlocked(data)
    if ws.get("is_connected"):
        await show_back_view(data, wid, back_to)

# =========================
# COMPANY / CATEGORY ACTIONS
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("cmpren:"))
async def rename_company_prompt(cb: types.CallbackQuery):
    await open_company_prompt_from_callback(
        cb,
        "вњЌрџЏ» Р’РІРµРґРё РЅРѕРІРѕРµ РЅР°Р·РІР°РЅРёРµ СЃРїРёСЃРєР°:",
        lambda company_idx: {"type": "rename_company", "company_idx": company_idx, "back_to": {"view": "company_settings", "company_idx": company_idx}},
    )

@dp.callback_query_handler(lambda c: c.data.startswith("cmpemoji:"))
async def company_emoji_prompt(cb: types.CallbackQuery):
    await open_company_prompt_from_callback(
        cb,
        "рџ’…рџЏ» РџСЂРёС€Р»Рё РѕРґРёРЅ СЃРјР°Р№Р»РёРє РґР»СЏ СЃРїРёСЃРєР°:",
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
    await upsert_ws_menu(data, wid, workspace_path_title(ws, display_company_name(company), "рџ—‘ РЈРґР°Р»РµРЅРёРµ СЃРїРёСЃРєР°?"), confirm_kb(f"cmpdel:{wid}:{company_idx}", f"cmpset:{wid}:{company_idx}"))

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
    await edit_ws_home_menu(data, wid)

@dp.callback_query_handler(lambda c: c.data.startswith("catnew:"))
async def add_category_prompt(cb: types.CallbackQuery):
    await open_company_prompt_from_callback(
        cb,
        "вњЏпёЏ Р’РІРµРґРё РЅР°Р·РІР°РЅРёРµ РїРѕРґРіСЂСѓРїРїС‹:",
        lambda company_idx: {"type": "new_category", "company_idx": company_idx, "back_to": {"view": "company", "company_idx": company_idx}},
    )

@dp.callback_query_handler(lambda c: c.data.startswith("catren:"))
async def rename_category_prompt(cb: types.CallbackQuery):
    await open_company_category_prompt_from_callback(
        cb,
        "вњЌрџЏ» Р’РІРµРґРё РЅРѕРІРѕРµ РЅР°Р·РІР°РЅРёРµ РїРѕРґРіСЂСѓРїРїС‹:",
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
        "рџ’…рџЏ» РџСЂРёС€Р»Рё РѕРґРёРЅ СЃРјР°Р№Р»РёРє РґР»СЏ РїРѕРґРіСЂСѓРїРїС‹:",
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
        workspace_path_title(ws, rich_display_company_name(company), rich_display_category_name(category), "рџ—‘ РЈРґР°Р»РёС‚СЊ РїРѕРґРіСЂСѓРїРїСѓ СЃ Р·Р°РґР°С‡Р°РјРё?"),
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
    fresh = data
    await sync_company_and_refresh_view(fresh, wid, company_idx, edit_company_menu, company_idx)

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
        workspace_path_title(ws, rich_display_company_name(company), rich_display_category_name(category), "рџ—‘ РЈРґР°Р»РёС‚СЊ РїРѕРґРіСЂСѓРїРїСѓ?"),
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
    fresh = data
    await sync_company_and_refresh_view(fresh, wid, company_idx, edit_company_menu, company_idx)

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
        prompt_text = "вњЏпёЏ Р’РІРµРґРё С‚РµРєСЃС‚ РЅРѕРІРѕР№ Р·Р°РґР°С‡Рё:"
        set_prompt_state(ws, {"type": "new_task", "company_idx": company_idx, "category_idx": category_idx, "back_to": back_to})
        await save_data_unlocked(data)
    await show_prompt_menu(data, ws, prompt_text)

@dp.callback_query_handler(lambda c: c.data.startswith("taskren:"))
async def rename_task_prompt(cb: types.CallbackQuery):
    await open_company_task_prompt_from_callback(
        cb,
        "вњЌрџЏ» Р’РІРµРґРё РЅРѕРІРѕРµ РЅР°Р·РІР°РЅРёРµ Р·Р°РґР°С‡Рё:",
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
        "вЏ° РџСЂРёС€Р»Рё РјРЅРµ РґР°С‚Сѓ РёР»Рё СЃСЂРѕРє РґР»СЏ РґРµРґР»Р°Р№РЅР°",
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
    fresh = data
    await sync_company_and_refresh_view(fresh, wid, company_idx, edit_task_menu, company_idx, task_idx)

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
    fresh = data
    if category_id:
        cat_idx = find_category_index(fresh["workspaces"][wid]["companies"][company_idx].get("categories", []), category_id)
        if cat_idx is not None:
            await sync_company_and_refresh_view(fresh, wid, company_idx, edit_category_menu, company_idx, cat_idx)
            return
    await sync_company_and_refresh_view(fresh, wid, company_idx, edit_company_menu, company_idx)

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
        became_done = False
        if task.get("done"):
            task["done"] = False
            cancel_task_completion_event(company, task)
        else:
            task["done"] = True
            add_task_completion_event(company, task)
            became_done = True
        category_id = task.get("category_id")
        await save_data_unlocked(data)
    fresh = data
    ws_fresh = fresh["workspaces"][wid]
    if category_id:
        cat_idx = find_category_index(fresh["workspaces"][wid]["companies"][company_idx].get("categories", []), category_id)
    else:
        cat_idx = None
    if cat_idx is not None:
        view_task = asyncio.create_task(sync_company_and_refresh_view(fresh, wid, company_idx, edit_category_menu, company_idx, cat_idx))
    else:
        view_task = asyncio.create_task(sync_company_and_refresh_view(fresh, wid, company_idx, edit_company_menu, company_idx))
    instant_changed = False
    if became_done:
        instant_task = asyncio.create_task(publish_company_done_reports(ws_fresh, company_idx, task_idx))
        instant_changed = await instant_task
    await view_task
    if instant_changed:
        await save_data(fresh)

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
    fresh = data
    if prev_category_id:
        prev_idx = find_category_index(fresh["workspaces"][wid]["companies"][company_idx].get("categories", []), prev_category_id)
        if prev_idx is not None:
            await sync_company_and_refresh_view(fresh, wid, company_idx, edit_category_menu, company_idx, category_idx)
            return
    await sync_company_and_refresh_view(fresh, wid, company_idx, edit_company_menu, company_idx)

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
    fresh = data
    if prev_category_id:
        prev_idx = find_category_index(fresh["workspaces"][wid]["companies"][company_idx].get("categories", []), prev_category_id)
        if prev_idx is not None:
            await sync_company_and_refresh_view(fresh, wid, company_idx, edit_category_menu, company_idx, prev_idx)
            return
    await sync_company_and_refresh_view(fresh, wid, company_idx, edit_company_menu, company_idx)

# =========================
# TEMPLATE ACTIONS
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("tplcatnew:"))
async def add_template_category_prompt(cb: types.CallbackQuery):
    await open_wid_prompt_from_callback(cb, "вњЏпёЏ Р’РІРµРґРё РЅР°Р·РІР°РЅРёРµ РїРѕРґРіСЂСѓРїРїС‹ С€Р°Р±Р»РѕРЅР°:", {"type": "new_template_category", "back_to": {"view": "template"}})

@dp.callback_query_handler(lambda c: c.data.startswith("tplcatren:"))
async def rename_template_category_prompt(cb: types.CallbackQuery):
    await open_template_category_prompt_from_callback(
        cb,
        "вњЌрџЏ» Р’РІРµРґРё РЅРѕРІРѕРµ РЅР°Р·РІР°РЅРёРµ РїРѕРґРіСЂСѓРїРїС‹ С€Р°Р±Р»РѕРЅР°:",
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
        "рџ’…рџЏ» РџСЂРёС€Р»Рё РѕРґРёРЅ СЃРјР°Р№Р»РёРє РґР»СЏ РїРѕРґРіСЂСѓРїРїС‹ С€Р°Р±Р»РѕРЅР°:",
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
        workspace_path_title(ws, "вљ™пёЏ РЁР°Р±Р»РѕРЅС‹ Р·Р°РґР°С‡", rich_display_template_name(active), rich_display_category_name(category), "рџ—‘ РЈРґР°Р»РёС‚СЊ РїРѕРґРіСЂСѓРїРїСѓ СЃ Р·Р°РґР°С‡Р°РјРё?"),
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
    await edit_template_menu(data, wid)

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
        workspace_path_title(ws, "вљ™пёЏ РЁР°Р±Р»РѕРЅС‹ Р·Р°РґР°С‡", rich_display_template_name(active), rich_display_category_name(category), "рџ—‘ РЈРґР°Р»РёС‚СЊ РїРѕРґРіСЂСѓРїРїСѓ?"),
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
    await edit_template_menu(data, wid)

@dp.callback_query_handler(lambda c: c.data.startswith("tplcatcopy:"))
async def copy_template_category_prompt(cb: types.CallbackQuery):
    await open_template_category_prompt_from_callback(
        cb,
        "вњЏпёЏ Р’РІРµРґРё РёРјСЏ РЅРѕРІРѕР№ РїРѕРґРіСЂСѓРїРїС‹-РєРѕРїРёРё:",
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
        prompt_text = "вњЏпёЏ Р’РІРµРґРё РЅР°Р·РІР°РЅРёРµ РЅРѕРІРѕР№ Р·Р°РґР°С‡Рё С€Р°Р±Р»РѕРЅР°:"
        set_prompt_state(ws, {"type": "new_template_task", "category_idx": category_idx, "back_to": back_to})
        await save_data_unlocked(data)
    await show_prompt_menu(data, ws, prompt_text)

@dp.callback_query_handler(lambda c: c.data.startswith("tpltaskren:"))
async def rename_template_task_prompt(cb: types.CallbackQuery):
    await open_template_task_prompt_from_callback(
        cb,
        "вњЌрџЏ» Р’РІРµРґРё РЅРѕРІРѕРµ РЅР°Р·РІР°РЅРёРµ Р·Р°РґР°С‡Рё С€Р°Р±Р»РѕРЅР°:",
        lambda ws, task_idx: {"type": "rename_template_task", "task_idx": task_idx, "back_to": {"view": "template_task", "task_idx": task_idx}},
    )

@dp.callback_query_handler(lambda c: c.data.startswith("tpltaskdeadline:"))
async def template_task_deadline_prompt(cb: types.CallbackQuery):
    await open_template_task_prompt_from_callback(
        cb,
        "вЏ° РџСЂРёС€Р»Рё СЃСЂРѕРє РґР»СЏ РґРµРґР»Р°Р№РЅР°, РЅР°РїСЂРёРјРµСЂ: 3 РґРЅСЏ, 7С‡20Рј, 45 РјРёРЅСѓС‚.",
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
    await edit_template_task_menu(data, wid, task_idx)

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
    fresh = data
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
    fresh = data
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
    fresh = data
    if prev_category_id:
        cat_idx = find_category_index(fresh["workspaces"][wid].get("template_categories", []), prev_category_id)
        if cat_idx is not None:
            await edit_template_category_menu(fresh, wid, cat_idx)
            return
    await edit_template_menu(fresh, wid)

# =========================
# TEXT INPUT
# =========================

async def handle_private_pm_text_input(message: types.Message) -> bool:
    if message.chat.type != "private":
        return False

    pm_uid = str(message.from_user.id)
    pm_rename_wid = None
    pm_message_id = None

    async with FILE_LOCK:
        data = await load_data_unlocked()
        user = ensure_user(data, pm_uid)
        pm_awaiting = user.get("pm_awaiting") or {}
        if not pm_awaiting:
            return False

        text = clean_text(message.text)
        if not text:
            await save_data_unlocked(data)
            asyncio.create_task(try_delete_user_message(message))
            return True

        mode = pm_awaiting.get("type")
        if mode == "rename_workspace_label":
            wid = pm_awaiting.get("wid")
            ws = data["workspaces"].get(wid)
            user["pm_awaiting"] = None
            if ws and ws.get("is_connected") and wid in user.get("workspaces", []):
                set_binding_custom_label(data, ws["chat_id"], ws.get("thread_id") or 0, text)
                refresh_binding_labels(data, ws["chat_id"], ws.get("thread_id") or 0)
            await save_data_unlocked(data)
            pm_rename_wid = wid
            pm_message_id = user.get("pm_menu_msg_id")
        elif mode == "workspace_label_emoji":
            if not is_single_emoji(text):
                await save_data_unlocked(data)
                asyncio.create_task(send_temp_message(int(pm_uid), "РџСЂРёС€Р»Рё РѕРґРёРЅ СЃРјР°Р№Р»РёРє, Р±Р°Р»РґР°Р±С‘Р±!", delay=6))
                asyncio.create_task(try_delete_user_message(message))
                return True
            wid = pm_awaiting.get("wid")
            ws = data["workspaces"].get(wid)
            user["pm_awaiting"] = None
            if ws and ws.get("is_connected") and wid in user.get("workspaces", []):
                set_binding_emoji(data, ws["chat_id"], ws.get("thread_id") or 0, text)
                refresh_binding_labels(data, ws["chat_id"], ws.get("thread_id") or 0)
            await save_data_unlocked(data)
            pm_rename_wid = wid
            pm_message_id = user.get("pm_menu_msg_id")
        else:
            await save_data_unlocked(data)
            asyncio.create_task(try_delete_user_message(message))
            return True

    asyncio.create_task(try_delete_user_message(message))
    if pm_rename_wid is None:
        return True
    if pm_message_id:
        await edit_pm_workspace_view(data, pm_uid, pm_rename_wid, pm_message_id)
    else:
        await update_pm_menu(pm_uid, data)
    return True

async def finish_text_input_silently(finish_and_save, state: dict):
    state["stop"] = True
    await finish_and_save()

async def handle_company_text_mode(ws: dict, awaiting: dict, text: str, reject_input, finish_and_save, state: dict) -> bool:
    mode = awaiting.get("type")
    if mode == "new_company":
        if company_exists(ws, text):
            await reject_input("РўР°РєРѕР№ СЃРїРёСЃРѕРє СѓР¶Рµ СЃСѓС‰РµСЃС‚РІСѓРµС‚.")
            return True
        company = make_company(text, awaiting.get("use_template", False), ws, awaiting.get("template_id"))
        ws["companies"].append(company)
        state["created_company_idx"] = len(ws["companies"]) - 1
        await finish_and_save()
        return True

    if mode == "rename_company":
        company_idx = awaiting["company_idx"]
        if company_idx < 0 or company_idx >= len(ws["companies"]):
            await finish_text_input_silently(finish_and_save, state)
            return True
        if company_exists(ws, text, exclude_idx=company_idx):
            await reject_input("РўР°РєРѕР№ СЃРїРёСЃРѕРє СѓР¶Рµ СЃСѓС‰РµСЃС‚РІСѓРµС‚.")
            return True
        ws["companies"][company_idx]["title"] = text
        await finish_and_save()
        return True

    if mode == "company_emoji":
        if not is_single_emoji(text):
            await reject_input("РџСЂРёС€Р»Рё РѕРґРёРЅ СЃРјР°Р№Р»РёРє, Р±Р°Р»РґР°Р±С‘Р±!")
            return True
        company_idx = awaiting["company_idx"]
        if company_idx < 0 or company_idx >= len(ws["companies"]):
            await finish_text_input_silently(finish_and_save, state)
            return True
        ws["companies"][company_idx]["emoji"] = text
        await finish_and_save()
        return True

    if mode == "new_category":
        company_idx = awaiting["company_idx"]
        if company_idx < 0 or company_idx >= len(ws["companies"]):
            await finish_text_input_silently(finish_and_save, state)
            return True
        company = ws["companies"][company_idx]
        if category_exists(company.get("categories", []), text):
            await reject_input("РўР°РєР°СЏ РїРѕРґРіСЂСѓРїРїР° СѓР¶Рµ СЃСѓС‰РµСЃС‚РІСѓРµС‚.")
            return True
        company.setdefault("categories", []).append({"id": uuid.uuid4().hex, "title": text, "emoji": "рџ“Ѓ"})
        await finish_and_save()
        return True

    if mode == "rename_category":
        company_idx = awaiting["company_idx"]
        category_idx = awaiting["category_idx"]
        if company_idx < 0 or company_idx >= len(ws["companies"]):
            await finish_text_input_silently(finish_and_save, state)
            return True
        company = ws["companies"][company_idx]
        if category_idx < 0 or category_idx >= len(company.get("categories", [])):
            await finish_text_input_silently(finish_and_save, state)
            return True
        category = company["categories"][category_idx]
        if category_exists(company.get("categories", []), text, exclude_id=category["id"]):
            await reject_input("РўР°РєР°СЏ РїРѕРґРіСЂСѓРїРїР° СѓР¶Рµ СЃСѓС‰РµСЃС‚РІСѓРµС‚.")
            return True
        category["title"] = text
        await finish_and_save()
        return True

    if mode == "category_emoji":
        if not is_single_emoji(text):
            await reject_input("РџСЂРёС€Р»Рё РѕРґРёРЅ СЃРјР°Р№Р»РёРє, Р±Р°Р»РґР°Р±С‘Р±!")
            return True
        company_idx = awaiting["company_idx"]
        category_idx = awaiting["category_idx"]
        if company_idx < 0 or company_idx >= len(ws["companies"]):
            await finish_text_input_silently(finish_and_save, state)
            return True
        company = ws["companies"][company_idx]
        if category_idx < 0 or category_idx >= len(company.get("categories", [])):
            await finish_text_input_silently(finish_and_save, state)
            return True
        company["categories"][category_idx]["emoji"] = text
        await finish_and_save()
        return True

    if mode == "new_task":
        company_idx = awaiting["company_idx"]
        category_idx = awaiting.get("category_idx")
        if company_idx < 0 or company_idx >= len(ws["companies"]):
            await finish_text_input_silently(finish_and_save, state)
            return True
        company = ws["companies"][company_idx]
        category_id = None
        if category_idx is not None:
            if category_idx < 0 or category_idx >= len(company.get("categories", [])):
                await finish_text_input_silently(finish_and_save, state)
                return True
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
        await finish_and_save()
        return True

    if mode == "rename_task":
        company_idx = awaiting["company_idx"]
        task_idx = awaiting["task_idx"]
        if company_idx < 0 or company_idx >= len(ws["companies"]):
            await finish_text_input_silently(finish_and_save, state)
            return True
        company = ws["companies"][company_idx]
        if task_idx < 0 or task_idx >= len(company.get("tasks", [])):
            await finish_text_input_silently(finish_and_save, state)
            return True
        company["tasks"][task_idx]["text"] = text
        entry = find_completion_entry(get_report_history(company), company["tasks"][task_idx].get("done_event_id"))
        if entry and entry.get("canceled_at") is None:
            entry["task_text"] = company["tasks"][task_idx].get("text") or ""
        await finish_and_save()
        return True

    if mode == "task_deadline":
        company_idx = awaiting["company_idx"]
        task_idx = awaiting["task_idx"]
        if company_idx < 0 or company_idx >= len(ws["companies"]):
            await finish_text_input_silently(finish_and_save, state)
            return True
        company = ws["companies"][company_idx]
        if task_idx < 0 or task_idx >= len(company.get("tasks", [])):
            await finish_text_input_silently(finish_and_save, state)
            return True
        task = company["tasks"][task_idx]
        started_at, due_at, err = parse_deadline_input(text, task.get("deadline_started_at"))
        if err:
            await reject_input(err)
            return True
        task["deadline_started_at"] = started_at
        task["deadline_due_at"] = due_at
        await finish_and_save()
        return True

    if mode == "copy_company":
        source_idx = awaiting["company_idx"]
        if source_idx < 0 or source_idx >= len(ws.get("companies", [])) or company_exists(ws, text):
            await reject_input("РўР°РєРѕР№ СЃРїРёСЃРѕРє СѓР¶Рµ СЃСѓС‰РµСЃС‚РІСѓРµС‚.")
            return True
        ws["companies"].append(copy_company_payload(ws["companies"][source_idx], text))
        state["created_company_idx"] = len(ws["companies"]) - 1
        await finish_and_save()
        return True

    if mode == "copy_category":
        company_idx = awaiting["company_idx"]
        category_idx = awaiting["category_idx"]
        if company_idx < 0 or company_idx >= len(ws.get("companies", [])):
            await finish_text_input_silently(finish_and_save, state)
            return True
        company = ws["companies"][company_idx]
        if category_idx < 0 or category_idx >= len(company.get("categories", [])) or category_exists(company.get("categories", []), text):
            await reject_input("РўР°РєР°СЏ РїРѕРґРіСЂСѓРїРїР° СѓР¶Рµ СЃСѓС‰РµСЃС‚РІСѓРµС‚.")
            return True
        copy_category_into_company(company, category_idx, text)
        await finish_and_save()
        return True

    return False

async def handle_binding_text_mode(data: dict, awaiting: dict, text: str, reject_input, finish_and_save, state: dict) -> bool:
    mode = awaiting.get("type")
    if mode == "rename_binding_label":
        chat_id = awaiting.get("chat_id")
        thread_id = awaiting.get("thread_id") or 0
        if chat_id is None:
            await finish_text_input_silently(finish_and_save, state)
            return True
        set_binding_custom_label(data, chat_id, thread_id, text)
        refresh_binding_labels(data, chat_id, thread_id)
        await finish_and_save()
        return True

    if mode == "binding_emoji":
        chat_id = awaiting.get("chat_id")
        thread_id = awaiting.get("thread_id") or 0
        if chat_id is None:
            await finish_text_input_silently(finish_and_save, state)
            return True
        if not is_single_emoji(text):
            await reject_input("РџСЂРёС€Р»Рё РѕРґРёРЅ СЃРјР°Р№Р»РёРє, Р±Р°Р»РґР°Р±С‘Р±!")
            return True
        set_binding_emoji(data, chat_id, thread_id, text)
        refresh_binding_labels(data, chat_id, thread_id)
        await finish_and_save()
        return True

    return False

async def handle_report_text_mode(data: dict, ws: dict, awaiting: dict, text: str, reject_input, finish_and_save, state: dict) -> bool:
    mode = awaiting.get("type")
    if mode == "report_schedule_time":
        company_idx = awaiting["company_idx"]
        target_idx = awaiting.get("target_idx")
        draft_interval = clone_report_interval(awaiting.get("draft_interval") or {})
        kind = draft_interval.get("kind")
        flow = awaiting.get("flow")
        interval_idx = awaiting.get("interval_idx")
        if company_idx < 0 or company_idx >= len(ws["companies"]) or kind not in {"weekly", "daily", "monthly", "once"}:
            await finish_text_input_silently(finish_and_save, state)
            return True

        error_text = apply_report_schedule_input(draft_interval, text)
        if error_text:
            await reject_input(error_text)
            return True

        normalized_draft = ensure_report_interval(draft_interval) or draft_interval
        if flow == "edit" and interval_idx is not None:
            await finish_and_save()
            state["report_followup"] = "report_finalize"
            state["report_followup_payload"] = {
                "company_idx": company_idx,
                "interval_idx": interval_idx,
                "flow": flow,
                "draft_interval": normalized_draft,
            }
            return True

        ws["awaiting"] = {
            "type": "report_accumulation_choice",
            "company_idx": company_idx,
            "target_idx": target_idx,
            "interval_idx": interval_idx,
            "flow": flow,
            "draft_interval": normalized_draft,
        }
        await save_data_unlocked(data)
        state["report_followup"] = "report_accumulation"
        return True

    if mode == "report_accumulation_value":
        company_idx = awaiting["company_idx"]
        draft_interval = clone_report_interval(awaiting.get("draft_interval") or {})
        kind = draft_interval.get("kind")
        if company_idx < 0 or company_idx >= len(ws["companies"]) or kind not in {"weekly", "daily", "monthly", "once"}:
            await finish_text_input_silently(finish_and_save, state)
            return True

        error_text = apply_report_accumulation_input(draft_interval, text)
        if error_text:
            await reject_input(error_text)
            return True

        await finish_and_save()
        state["report_followup"] = "report_finalize"
        state["report_followup_payload"] = {
            "company_idx": company_idx,
            "interval_idx": awaiting.get("interval_idx"),
            "flow": awaiting.get("flow"),
            "draft_interval": draft_interval,
        }
        return True

    if mode == "template_report_schedule_time":
        draft_interval = clone_report_interval(awaiting.get("draft_interval") or {})
        kind = draft_interval.get("kind")
        flow = awaiting.get("flow")
        interval_idx = awaiting.get("interval_idx")
        if kind not in {"weekly", "daily", "monthly"}:
            await finish_text_input_silently(finish_and_save, state)
            return True

        error_text = apply_report_schedule_input(draft_interval, text)
        if error_text:
            await reject_input(error_text)
            return True

        normalized_draft = ensure_report_interval(draft_interval) or draft_interval
        if flow == "edit" and interval_idx is not None:
            await finish_and_save()
            state["report_followup"] = "template_report_finalize"
            state["report_followup_payload"] = {
                "interval_idx": interval_idx,
                "flow": flow,
                "draft_interval": normalized_draft,
            }
            return True

        ws["awaiting"] = {
            "type": "template_report_accumulation_choice",
            "interval_idx": interval_idx,
            "flow": flow,
            "draft_interval": normalized_draft,
        }
        await save_data_unlocked(data)
        state["report_followup"] = "template_report_accumulation"
        return True

    if mode == "template_report_accumulation_value":
        draft_interval = clone_report_interval(awaiting.get("draft_interval") or {})
        error_text = apply_report_accumulation_input(draft_interval, text)
        if error_text:
            await reject_input(error_text)
            return True

        await finish_and_save()
        state["report_followup"] = "template_report_finalize"
        state["report_followup_payload"] = {
            "interval_idx": awaiting.get("interval_idx"),
            "flow": awaiting.get("flow"),
            "draft_interval": draft_interval,
        }
        return True

    return False

async def handle_template_text_mode(ws: dict, awaiting: dict, text: str, reject_input, finish_and_save, state: dict) -> bool:
    mode = awaiting.get("type")
    if mode == "new_template_category":
        if category_exists(ws.get("template_categories", []), text):
            await reject_input("РўР°РєР°СЏ РїРѕРґРіСЂСѓРїРїР° СѓР¶Рµ СЃСѓС‰РµСЃС‚РІСѓРµС‚.")
            return True
        ws.setdefault("template_categories", []).append({"id": uuid.uuid4().hex, "title": text, "emoji": "рџ“Ѓ"})
        await finish_and_save()
        return True

    if mode == "rename_template_category":
        category_idx = awaiting["category_idx"]
        if category_idx < 0 or category_idx >= len(ws.get("template_categories", [])):
            await finish_text_input_silently(finish_and_save, state)
            return True
        category = ws["template_categories"][category_idx]
        if category_exists(ws.get("template_categories", []), text, exclude_id=category["id"]):
            await reject_input("РўР°РєР°СЏ РїРѕРґРіСЂСѓРїРїР° СѓР¶Рµ СЃСѓС‰РµСЃС‚РІСѓРµС‚.")
            return True
        category["title"] = text
        await finish_and_save()
        return True

    if mode == "template_category_emoji":
        if not is_single_emoji(text):
            await reject_input("РџСЂРёС€Р»Рё РѕРґРёРЅ СЃРјР°Р№Р»РёРє, Р±Р°Р»РґР°Р±С‘Р±!")
            return True
        category_idx = awaiting["category_idx"]
        if category_idx < 0 or category_idx >= len(ws.get("template_categories", [])):
            await finish_text_input_silently(finish_and_save, state)
            return True
        ws["template_categories"][category_idx]["emoji"] = text
        await finish_and_save()
        return True

    if mode == "new_template_task":
        category_idx = awaiting.get("category_idx")
        category_id = None
        if category_idx is not None:
            if category_idx < 0 or category_idx >= len(ws.get("template_categories", [])):
                await finish_text_input_silently(finish_and_save, state)
                return True
            category_id = ws["template_categories"][category_idx]["id"]
        ws.setdefault("template_tasks", []).append({
            "id": uuid.uuid4().hex,
            "text": text,
            "category_id": category_id,
            "created_at": now_ts(),
            "deadline_seconds": None,
        })
        await finish_and_save()
        return True

    if mode == "rename_template_task":
        task_idx = awaiting["task_idx"]
        if task_idx < 0 or task_idx >= len(ws.get("template_tasks", [])):
            await finish_text_input_silently(finish_and_save, state)
            return True
        ws["template_tasks"][task_idx]["text"] = text
        await finish_and_save()
        return True

    if mode == "template_task_deadline":
        task_idx = awaiting["task_idx"]
        if task_idx < 0 or task_idx >= len(ws.get("template_tasks", [])):
            await finish_text_input_silently(finish_and_save, state)
            return True
        seconds, err = parse_template_deadline_seconds(text)
        if err:
            await reject_input(err)
            return True
        ws["template_tasks"][task_idx]["deadline_seconds"] = seconds
        await finish_and_save()
        return True

    if mode == "new_template_set":
        if template_exists(ws.get("templates", []), text):
            await reject_input("РўР°РєРѕР№ С€Р°Р±Р»РѕРЅ СѓР¶Рµ СЃСѓС‰РµСЃС‚РІСѓРµС‚.")
            return True
        tpl = make_template(text)
        ws.setdefault("templates", []).append(tpl)
        set_active_template(ws, tpl["id"])
        await finish_and_save()
        return True

    if mode == "rename_template_set":
        tpl = get_active_template(ws)
        if template_exists(ws.get("templates", []), text, exclude_id=tpl["id"]):
            await reject_input("РўР°РєРѕР№ С€Р°Р±Р»РѕРЅ СѓР¶Рµ СЃСѓС‰РµСЃС‚РІСѓРµС‚.")
            return True
        tpl["title"] = text
        await finish_and_save()
        return True

    if mode == "template_set_emoji":
        if not is_single_emoji(text):
            await reject_input("РџСЂРёС€Р»Рё РѕРґРёРЅ СЃРјР°Р№Р»РёРє, Р±Р°Р»РґР°Р±С‘Р±!")
            return True
        tpl = get_active_template(ws)
        tpl["emoji"] = text
        set_active_template(ws, tpl["id"])
        await finish_and_save()
        return True

    if mode == "copy_template_set":
        tpl = get_active_template(ws)
        if template_exists(ws.get("templates", []), text):
            await reject_input("РўР°РєРѕР№ С€Р°Р±Р»РѕРЅ СѓР¶Рµ СЃСѓС‰РµСЃС‚РІСѓРµС‚.")
            return True
        new_tpl = copy_template_payload(tpl, text)
        ws.setdefault("templates", []).append(new_tpl)
        set_active_template(ws, new_tpl["id"])
        await finish_and_save()
        return True

    if mode == "copy_template_category":
        category_idx = awaiting["category_idx"]
        if category_idx < 0 or category_idx >= len(ws.get("template_categories", [])) or category_exists(ws.get("template_categories", []), text):
            await reject_input("РўР°РєР°СЏ РїРѕРґРіСЂСѓРїРїР° СѓР¶Рµ СЃСѓС‰РµСЃС‚РІСѓРµС‚.")
            return True
        tpl = get_active_template(ws)
        copy_template_category(tpl, category_idx, text)
        set_active_template(ws, tpl["id"])
        await finish_and_save()
        return True

    return False

async def handle_group_text_followup(data: dict, wid: str, ws: dict, awaiting: dict, state: dict):
    report_followup = state.get("report_followup")
    report_followup_payload = state.get("report_followup_payload") or {}
    if report_followup in {"report_accumulation", "template_report_accumulation"}:
        await edit_report_accumulation_menu(data, wid)
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

    mode = awaiting.get("type")
    if mode in {"new_company", "copy_company"}:
        created_company_idx = state.get("created_company_idx")
        if created_company_idx is not None and 0 <= created_company_idx < len(ws.get("companies", [])):
            await sync_company_everywhere(ws, created_company_idx, recreate_menu=False)
        await recreate_ws_home_menu(data, wid)
        return

    if mode == "copy_category":
        company_idx = awaiting.get("company_idx")
        if company_idx is not None and 0 <= company_idx < len(ws.get("companies", [])):
            company = ws["companies"][company_idx]
            if await sync_company_and_refresh_view(data, wid, company_idx, edit_category_menu, company_idx, len(company.get("categories", [])) - 1):
                await save_data(data)
            return

    company_modes = {"rename_company", "company_emoji", "new_category", "rename_category", "category_emoji", "new_task", "rename_task", "task_deadline"}
    if mode in company_modes:
        company_idx = awaiting.get("company_idx")
        if company_idx is not None and 0 <= company_idx < len(ws.get("companies", [])):
            if await sync_company_and_show_back_view(data, wid, company_idx, awaiting.get("back_to", {"view": "ws"})):
                await save_data(data)
            return

    if mode in {"new_template_set", "rename_template_set", "template_set_emoji", "copy_template_set"}:
        await edit_templates_root_menu(data, wid)
        return

    if mode == "copy_template_category":
        await edit_template_category_menu(data, wid, len(ws.get("template_categories", [])) - 1)
        return

    await show_back_view(data, wid, awaiting.get("back_to", {"view": "ws"}))

@dp.message_handler(content_types=types.ContentTypes.TEXT)
async def handle_group_text(message: types.Message):
    if message.text and message.text.startswith("/") and message.text.split()[0].lower() in {"/start", "/connect", "/mirror"}:
        return

    if await handle_private_pm_text_input(message):
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
        text = clean_text(message.text)
        if not text:
            asyncio.create_task(try_delete_user_message(message))
            return

        state = {
            "created_company_idx": None,
            "report_followup": None,
            "report_followup_payload": {},
            "stop": False,
        }

        def finish():
            ws["awaiting"] = None

        async def reject_input(error_text: str):
            await save_data_unlocked(data)
            state["stop"] = True
            asyncio.create_task(send_temp_message(ws["chat_id"], error_text, ws["thread_id"], delay=6))
            asyncio.create_task(try_delete_user_message(message))

        async def finish_and_save():
            finish()
            await save_data_unlocked(data)

        handled = await handle_company_text_mode(ws, awaiting, text, reject_input, finish_and_save, state)
        if not handled:
            handled = await handle_binding_text_mode(data, awaiting, text, reject_input, finish_and_save, state)
        if not handled:
            handled = await handle_report_text_mode(data, ws, awaiting, text, reject_input, finish_and_save, state)
        if not handled:
            handled = await handle_template_text_mode(ws, awaiting, text, reject_input, finish_and_save, state)
        if not handled:
            await save_data_unlocked(data)
            state["stop"] = True
            asyncio.create_task(try_delete_user_message(message))
            return

    if state["stop"]:
        return

    await try_delete_user_message(message)
    fresh = data
    ws = fresh["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    await handle_group_text_followup(fresh, wid, ws, awaiting, state)
    return

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
    fresh = data
    await sync_company_and_refresh_view(fresh, wid, company_idx, edit_company_settings_menu, company_idx)

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
        prompt_text = "вњЏпёЏ Р’РІРµРґРё РёРјСЏ РЅРѕРІРѕР№ СЃРїРёСЃРєР°-РєРѕРїРёРё:"
        set_prompt_state(ws, {"type": "copy_company", "company_idx": company_idx, "back_to": {"view": "company_settings", "company_idx": company_idx}})
        await save_data_unlocked(data)
    await show_prompt_menu(data, ws, prompt_text)

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
        prompt_text = "вњЏпёЏ Р’РІРµРґРё РёРјСЏ РЅРѕРІРѕР№ РїРѕРґРіСЂСѓРїРїС‹-РєРѕРїРёРё:"
        set_prompt_state(ws, {"type": "copy_category", "company_idx": company_idx, "category_idx": category_idx, "back_to": {"view": "category_settings", "company_idx": company_idx, "category_idx": category_idx}})
        await save_data_unlocked(data)
    await show_prompt_menu(data, ws, prompt_text)

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
    fresh = data
    await sync_company_and_refresh_view(fresh, wid, company_idx, edit_category_settings_menu, company_idx, category_idx)

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
    await edit_template_menu(data, wid)

@dp.callback_query_handler(lambda c: c.data.startswith("tplsettings:"))
async def open_template_settings(cb: types.CallbackQuery):
    await open_wid_menu_from_callback(cb, edit_template_settings_menu)

@dp.callback_query_handler(lambda c: c.data.startswith("tplnewset:"))
async def add_template_set_prompt(cb: types.CallbackQuery):
    await open_wid_prompt_from_callback(cb, "вњЏпёЏ Р’РІРµРґРё РЅР°Р·РІР°РЅРёРµ РЅРѕРІРѕРіРѕ С€Р°Р±Р»РѕРЅР°:", {"type": "new_template_set", "back_to": {"view": "template_root"}})

@dp.callback_query_handler(lambda c: c.data.startswith("tplrenameset:"))
async def rename_template_set_prompt(cb: types.CallbackQuery):
    await open_wid_prompt_from_callback(cb, "вњЌрџЏ» Р’РІРµРґРё РЅРѕРІРѕРµ РЅР°Р·РІР°РЅРёРµ С€Р°Р±Р»РѕРЅР°:", {"type": "rename_template_set", "back_to": {"view": "template_settings"}})

@dp.callback_query_handler(lambda c: c.data.startswith("tplemojiset:"))
async def template_set_emoji_prompt(cb: types.CallbackQuery):
    await open_wid_prompt_from_callback(cb, "рџ’…рџЏ» РџСЂРёС€Р»Рё РѕРґРёРЅ СЃРјР°Р№Р»РёРє РґР»СЏ С€Р°Р±Р»РѕРЅР°:", {"type": "template_set_emoji", "back_to": {"view": "template_settings"}})

@dp.callback_query_handler(lambda c: c.data.startswith("tplcopy:"))
async def copy_template_set_prompt(cb: types.CallbackQuery):
    await open_wid_prompt_from_callback(cb, "вњЏпёЏ Р’РІРµРґРё РЅР°Р·РІР°РЅРёРµ РєРѕРїРёРё С€Р°Р±Р»РѕРЅР°:", {"type": "copy_template_set", "back_to": {"view": "template_settings"}})

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
        workspace_path_title(ws, "вљ™пёЏ РЁР°Р±Р»РѕРЅС‹ Р·Р°РґР°С‡", rich_display_template_name(active), "рџ—‘ РЈРґР°Р»РёС‚СЊ С€Р°Р±Р»РѕРЅ?"),
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
    await edit_templates_root_menu(data, wid)

async def deadline_refresh_worker():
    last_report_tick = None
    last_deadline_tick = None
    while True:
        now = datetime.now(TIMEZONE)
        report_tick = now.replace(second=0, microsecond=0)
        deadline_tick = (now.year, now.month, now.day, now.hour, now.minute // 10)
        report_due = report_tick != last_report_tick
        deadline_due = now.minute % 10 == 0 and deadline_tick != last_deadline_tick

        if report_due or deadline_due:
            if report_due:
                last_report_tick = report_tick
            if deadline_due:
                last_deadline_tick = deadline_tick
            try:
                data = await load_data()
                changed = False
                report_jobs = []
                deadline_jobs = []
                connected_workspaces = [ws for ws in data.get("workspaces", {}).values() if ws.get("is_connected")]
                for ws in connected_workspaces:
                    for idx, company in enumerate(ws.get("companies", [])):
                        if report_due:
                            intervals = get_report_intervals(company)
                            if any(interval.get("kind") != "on_done" for interval in intervals):
                                report_jobs.append((ws, idx))
                        if deadline_due and any(
                            isinstance(task.get("deadline_due_at"), int) and not task.get("done")
                            for task in company.get("tasks", [])
                        ):
                            deadline_jobs.append((ws, idx))

                if report_due:
                    now_value = int(report_tick.timestamp())
                    for ws, idx in report_jobs:
                        if await publish_company_reports(ws, idx, now_value):
                            changed = True

                if deadline_due:
                    for ws, idx in deadline_jobs:
                        if await sync_company_everywhere(ws, idx):
                            changed = True

                if changed:
                    await save_data(data)
            except Exception:
                pass

        await asyncio.sleep(5 if now.second >= 55 else max(1, 60 - now.second))

async def drain_startup_updates():
    offset = None
    pending_topic_titles: dict[tuple[int, int], tuple[str | None, str, str]] = {}

    for _ in range(20):
        updates = await bot.get_updates(offset=offset, limit=100, timeout=0)
        if not updates:
            break
        offset = updates[-1].update_id + 1
        for update in updates:
            for field_name in ("message", "edited_message", "channel_post", "edited_channel_post"):
                message = getattr(update, field_name, None)
                if not message or message.chat.type == "private":
                    continue
                thread_id = message.message_thread_id or 0
                if not thread_id:
                    continue
                topic_title = extract_message_topic_title(message)
                if not topic_title:
                    continue
                topic_title_source = "edited" if getattr(message, "forum_topic_edited", None) else "created"
                pending_topic_titles[(message.chat.id, thread_id)] = (message.chat.title, topic_title, topic_title_source)

    if not pending_topic_titles:
        return

    async with FILE_LOCK:
        data = await load_data_unlocked()
        for (chat_id, thread_id), (chat_title, topic_title, topic_title_source) in pending_topic_titles.items():
            remember_binding_place(data, chat_id, thread_id, chat_title, topic_title, topic_title_source)
            refresh_binding_labels(data, chat_id, thread_id)
        await save_data_unlocked(data)

async def on_startup_polling(_):
    await load_data()
    await drain_startup_updates()
    asyncio.create_task(deadline_refresh_worker())

# =========================
# RUN
# =========================

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=False, on_startup=on_startup_polling)

