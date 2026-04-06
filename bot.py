import os
import json
import asyncio
import uuid
from datetime import datetime, date, timedelta

from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils import executor
from aiogram.utils.exceptions import MessageNotModified, RetryAfter, TelegramAPIError

TOKEN = os.getenv("API_TOKEN")
if not TOKEN:
    raise RuntimeError("API_TOKEN is not set")

bot = Bot(token=TOKEN)
dp = Dispatcher(bot)

DATA_FILE = "data.json"
lock = asyncio.Lock()
menu_locks: dict[str, asyncio.Lock] = {}


# =========================
# DATA
# =========================

def default_data():
    return {
        "users": {},
        "workspaces": {},
        "mirror_tokens": {},
    }


def make_task(text: str, done: bool = False, *, created_at: int | None = None,
              deadline_due_date: str | None = None, deadline_started_at: int | None = None,
              template_deadline_days: int | None = None):
    return {
        "id": uuid.uuid4().hex,
        "text": text,
        "done": done,
        "created_at": created_at or int(datetime.now().timestamp()),
        "deadline_due_date": deadline_due_date,
        "deadline_started_at": deadline_started_at,
        "template_deadline_days": template_deadline_days,
    }


def ensure_task(task):
    if not isinstance(task, dict):
        task = make_task(str(task))
    task.setdefault("id", uuid.uuid4().hex)
    task.setdefault("text", "")
    task.setdefault("done", False)
    task.setdefault("created_at", int(datetime.now().timestamp()))
    task.setdefault("deadline_due_date", None)
    task.setdefault("deadline_started_at", None)
    task.setdefault("template_deadline_days", None)
    if task.get("template_deadline_days") is not None:
        try:
            days = int(task.get("template_deadline_days"))
            task["template_deadline_days"] = days if days > 0 else None
        except Exception:
            task["template_deadline_days"] = None
    return task


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
        ws.setdefault("template", [])
        ws.setdefault("companies", [])
        ws.setdefault("awaiting", None)
        ws.setdefault("is_connected", True)

        if isinstance(ws.get("template"), dict):
            ws["template"] = ws["template"].get("tasks", [])
        if not isinstance(ws["template"], list):
            ws["template"] = []
        ws["template"] = [ensure_task(t) for t in ws["template"]]

        if not isinstance(ws["companies"], list):
            ws["companies"] = []

        for idx, company in enumerate(ws["companies"]):
            if not isinstance(company, dict):
                ws["companies"][idx] = {}
                company = ws["companies"][idx]

            company.setdefault("id", uuid.uuid4().hex)
            company.setdefault("name", "Компания")
            company.setdefault("tasks", [])
            company.setdefault("card_msg_id", None)
            company.setdefault("mirror", None)
            company.setdefault("deadline_view_mode", "days")

            if not isinstance(company["tasks"], list):
                company["tasks"] = []

            company["tasks"] = [ensure_task(t) for t in company["tasks"]]

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

        company = ws["companies"][company_idx]
        payload["company_id"] = company.get("id") or uuid.uuid4().hex
        company["id"] = payload["company_id"]
        payload.pop("company_idx", None)
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
    head = text.split()[0].lower()
    return head in {"/start", "/connect", "/mirror"}


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


def today_local() -> date:
    return date.today()


def parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except Exception:
        return None


def format_deadline_suffix(task: dict, mode: str = "days") -> str:
    if task.get("done"):
        return ""
    due = parse_iso_date(task.get("deadline_due_date"))
    if not due:
        return ""
    if mode == "date":
        return f" (до {due.strftime('%d.%m.%Y')})"
    delta = (due - today_local()).days
    if delta < 0:
        return " (срок вышел)"
    if delta == 0:
        return " (сегодня)"
    if delta == 1:
        return " (до завтра)"
    return f" (<{delta} д.)"


def task_deadline_icon(task: dict) -> str:
    if task.get("done"):
        return "✔"
    due = parse_iso_date(task.get("deadline_due_date"))
    if not due:
        return "⬜"
    today = today_local()
    if due < today:
        return "🟥"
    started_at = task.get("deadline_started_at")
    if not isinstance(started_at, int):
        return "🟩"
    start_date = datetime.fromtimestamp(started_at).date()
    total = max(1, (due - start_date).days)
    elapsed = max(0, (today - start_date).days)
    ratio = elapsed / total
    if ratio >= 0.75:
        return "🟫"
    if ratio >= 0.5:
        return "🟧"
    if ratio >= 0.25:
        return "🟨"
    return "🟩"


def task_sort_key(task: dict):
    due = parse_iso_date(task.get("deadline_due_date"))
    has_due = 0 if due else 1
    due_ord = due.toordinal() if due else 10**10
    created = int(task.get("created_at") or 0)
    return (has_due, due_ord, created)


def sorted_task_entries(tasks: list[dict]):
    return sorted(list(enumerate(tasks)), key=lambda item: task_sort_key(item[1]))


def template_task_sort_key(task: dict):
    days = task.get("template_deadline_days")
    has_due = 0 if isinstance(days, int) and days > 0 else 1
    return (has_due, days if has_due == 0 else 10**10, int(task.get("created_at") or 0))


def sorted_template_entries(tasks: list[dict]):
    return sorted(list(enumerate(tasks)), key=lambda item: template_task_sort_key(item[1]))


def template_task_suffix(task: dict) -> str:
    days = task.get("template_deadline_days")
    return f" ({days} д.)" if isinstance(days, int) and days > 0 else ""


def task_button_text(task: dict, include_days: bool = False, deadline_mode: str = "days") -> str:
    icon = task_deadline_icon(task)
    suffix = format_deadline_suffix(task, deadline_mode) if include_days else ""
    return f"{icon} {task.get('text', '')}{suffix}"


def template_task_button_text(task: dict) -> str:
    return f"{task.get('text', '')}{template_task_suffix(task)}"


def company_card_text(company: dict) -> str:
    lines = [f"📁 {company['name']}:"]
    tasks = [task for _, task in sorted_task_entries(company.get("tasks", []))]
    if tasks:
        mode = company.get("deadline_view_mode") or "days"
        for task in tasks:
            lines.append(task_button_text(task, include_days=True, deadline_mode=mode))
    else:
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


def parse_deadline_input(text: str, keep_started_at: int | None = None):
    value = clean_text(text)
    today = today_local()
    if value.isdigit():
        days = int(value)
        if days < 0:
            return None, None, "Введите дату ДД.ММ.ГГГГ или неотрицательное количество дней."
        due = today + timedelta(days=days)
        return due.isoformat(), keep_started_at or int(datetime.now().timestamp()), None
    try:
        due = datetime.strptime(value, "%d.%m.%Y").date()
    except Exception:
        return None, None, "Введите дату в формате ДД.ММ.ГГГГ или количество дней, например 3."
    if due < today:
        return None, None, "Дата дедлайна не может быть в прошлом."
    return due.isoformat(), keep_started_at or int(datetime.now().timestamp()), None


def parse_template_deadline_input(text: str):
    value = clean_text(text)
    if not value.isdigit():
        return None, "Введите целое количество дней, например 3."
    days = int(value)
    if days <= 0:
        return None, "Количество дней должно быть больше 0."
    return days, None


def get_menu_lock(wid: str) -> asyncio.Lock:
    menu_locks.setdefault(wid, asyncio.Lock())
    return menu_locks[wid]
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


def company_menu_kb(wid: str, company_idx: int, company: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    for task_idx, task in sorted_task_entries(company["tasks"]):
        kb.add(InlineKeyboardButton(task_button_text(task), callback_data=f"task:{wid}:{company_idx}:{task_idx}"))
    kb.add(InlineKeyboardButton("➕ Добавить задачу", callback_data=f"tasknew:{wid}:{company_idx}"))
    kb.add(InlineKeyboardButton("⚙️ Настройки компании", callback_data=f"cmpset:{wid}:{company_idx}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"backws:{wid}"))
    return kb


def company_settings_kb(wid: str, company_idx: int, company: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("✍️ Переименовать компанию", callback_data=f"cmpren:{wid}:{company_idx}"))
    mode = company.get("deadline_view_mode") or "days"
    kb.add(InlineKeyboardButton(f"🗓 Формат дедлайнов: {'даты' if mode == 'date' else 'дни'}", callback_data=f"cmpddlmode:{wid}:{company_idx}"))
    if company.get("mirror"):
        kb.add(InlineKeyboardButton("🔌 Отвязать список", callback_data=f"mirroroff:{wid}:{company_idx}"))
    else:
        kb.add(InlineKeyboardButton("📤 Дублировать список", callback_data=f"mirroron:{wid}:{company_idx}"))
    kb.add(InlineKeyboardButton("🗑 Удалить компанию", callback_data=f"cmpdel:{wid}:{company_idx}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"cmp:{wid}:{company_idx}"))
    return kb


def task_menu_kb(wid: str, company_idx: int, task_idx: int, task: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    if task["done"]:
        kb.add(InlineKeyboardButton("❌ Отменить выполнение", callback_data=f"taskdone:{wid}:{company_idx}:{task_idx}"))
    else:
        kb.add(InlineKeyboardButton("✔ Отметить выполненной", callback_data=f"taskdone:{wid}:{company_idx}:{task_idx}"))
        if task.get("deadline_due_date"):
            kb.add(InlineKeyboardButton("⏰ Поменять дедлайн", callback_data=f"taskdl:{wid}:{company_idx}:{task_idx}"))
            kb.add(InlineKeyboardButton("🗑 Удалить дедлайн", callback_data=f"taskdldel:{wid}:{company_idx}:{task_idx}"))
        else:
            kb.add(InlineKeyboardButton("⏰ Установить дедлайн", callback_data=f"taskdl:{wid}:{company_idx}:{task_idx}"))
    kb.add(InlineKeyboardButton("✍️ Переименовать", callback_data=f"taskren:{wid}:{company_idx}:{task_idx}"))
    kb.add(InlineKeyboardButton("🗑 Удалить задачу", callback_data=f"taskdel:{wid}:{company_idx}:{task_idx}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"cmp:{wid}:{company_idx}"))
    return kb


def template_menu_kb(wid: str, ws: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    for idx, task in sorted_template_entries(ws["template"]):
        kb.add(InlineKeyboardButton(template_task_button_text(task), callback_data=f"tplitem:{wid}:{idx}"))
    kb.add(InlineKeyboardButton("➕ Добавить задачу", callback_data=f"tplnew:{wid}"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"backws:{wid}"))
    return kb


def template_item_kb(wid: str, template_idx: int, task: dict):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("✍️ Переименовать", callback_data=f"tplren:{wid}:{template_idx}"))
    if task.get("template_deadline_days"):
        kb.add(InlineKeyboardButton("⏰ Поменять дедлайн", callback_data=f"tpldl:{wid}:{template_idx}"))
        kb.add(InlineKeyboardButton("🗑 Удалить дедлайн", callback_data=f"tpldldel:{wid}:{template_idx}"))
    else:
        kb.add(InlineKeyboardButton("⏰ Установить дедлайн", callback_data=f"tpldl:{wid}:{template_idx}"))
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

async def tg_call(factory, retries: int = 2):
    last_exc = None
    for attempt in range(retries + 1):
        try:
            return await factory()
        except RetryAfter as e:
            wait = int(getattr(e, "timeout", 1) or 1)
            await asyncio.sleep(wait + 1)
            last_exc = e
        except TelegramAPIError as e:
            last_exc = e
            if attempt >= retries:
                raise
            await asyncio.sleep(1 + attempt)
    if last_exc:
        raise last_exc


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
    return await tg_call(lambda: bot.send_message(chat_id, text, reply_markup=reply_markup, **thread_kwargs(thread_id)), retries=2)


async def send_temp_message(chat_id: int, text: str, thread_id: int = 0, delay: int = 8):
    msg = await send_message(chat_id, text, thread_id=thread_id)

    async def remover():
        await asyncio.sleep(delay)
        try:
            await bot.delete_message(chat_id, msg.message_id)
        except Exception:
            pass

    asyncio.create_task(remover())


async def send_week_notice_pm(user_id: str, text: str):
    msg = await send_message(int(user_id), text)

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
            await tg_call(lambda: bot.edit_message_text(text, int(user_id), user["pm_menu_msg_id"], reply_markup=kb), retries=1)
            return
        except MessageNotModified:
            return
        except Exception:
            user["pm_menu_msg_id"] = None

    try:
        old_id = user.get("pm_menu_msg_id")
        if old_id:
            await safe_delete_message(int(user_id), old_id)
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

    msg = await send_message(mirror["chat_id"], text, thread_id=(mirror.get("thread_id") or 0))
    mirror["message_id"] = msg.message_id


async def ensure_all_company_cards(ws: dict):
    for idx in range(len(ws["companies"])):
        await upsert_company_card(ws, idx)


async def sync_company_everywhere(ws: dict, company_idx: int):
    await upsert_company_card(ws, company_idx)
    company = ws["companies"][company_idx]
    await upsert_company_mirror(company)


async def delete_old_prompt_if_any(ws: dict):
    awaiting = ws.get("awaiting") or {}
    if awaiting.get("prompt_msg_id"):
        await safe_delete_message(ws["chat_id"], awaiting["prompt_msg_id"])


async def set_prompt(ws: dict, prompt_text: str, awaiting_payload: dict):
    await delete_old_prompt_if_any(ws)
    msg = await send_message(ws["chat_id"], prompt_text, reply_markup=prompt_kb(ws["id"]), thread_id=ws["thread_id"])
    awaiting_payload["prompt_msg_id"] = msg.message_id
    ws["awaiting"] = awaiting_payload


async def upsert_ws_menu(ws: dict, text: str, reply_markup):
    if not ws or not ws.get("is_connected"):
        return False
    async with get_menu_lock(ws["id"]):
        menu_msg_id = ws.get("menu_msg_id")
        if menu_msg_id:
            ok = await try_edit_text(ws["chat_id"], menu_msg_id, text, reply_markup=reply_markup)
            if ok:
                return False
        msg = await send_message(ws["chat_id"], text, reply_markup=reply_markup, thread_id=ws["thread_id"])
        ws["menu_msg_id"] = msg.message_id
        return True


async def send_or_replace_ws_home_menu(data: dict, wid: str):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return

    created = await upsert_ws_menu(ws, "📂 Меню workspace", ws_home_kb(wid, ws))


async def edit_ws_home_menu(data: dict, wid: str):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return

    created = await upsert_ws_menu(ws, "📂 Меню workspace", ws_home_kb(wid, ws))


async def edit_company_menu(data: dict, wid: str, company_idx: int):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return

    if company_idx < 0 or company_idx >= len(ws["companies"]):
        await edit_ws_home_menu(data, wid)
        return

    company = ws["companies"][company_idx]
    created = await upsert_ws_menu(ws, f"📁 {company['name']}", company_menu_kb(wid, company_idx, company))


async def edit_company_settings_menu(data: dict, wid: str, company_idx: int):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return

    if company_idx < 0 or company_idx >= len(ws["companies"]):
        await edit_ws_home_menu(data, wid)
        return

    company = ws["companies"][company_idx]
    created = await upsert_ws_menu(ws, f"⚙️ {company['name']}", company_settings_kb(wid, company_idx, company))


async def edit_task_menu(data: dict, wid: str, company_idx: int, task_idx: int):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return

    if company_idx < 0 or company_idx >= len(ws["companies"]):
        await edit_ws_home_menu(data, wid)
        return

    company = ws["companies"][company_idx]
    if task_idx < 0 or task_idx >= len(company["tasks"]):
        await edit_company_menu(data, wid, company_idx)
        return

    task = company["tasks"][task_idx]
    created = await upsert_ws_menu(ws, f"📌 {task['text']}", task_menu_kb(wid, company_idx, task_idx, task))


async def edit_template_menu(data: dict, wid: str):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return

    created = await upsert_ws_menu(ws, "⚙️ Шаблон задач", template_menu_kb(wid, ws))


async def edit_template_item_menu(data: dict, wid: str, template_idx: int):
    ws = data["workspaces"].get(wid)
    if not ws or not ws.get("is_connected"):
        return
    if template_idx < 0 or template_idx >= len(ws["template"]):
        await edit_template_menu(data, wid)
        return
    task = ws["template"][template_idx]
    created = await upsert_ws_menu(ws, f"⚙️ {task['text']}", template_item_kb(wid, template_idx, task))


def company_exists(ws: dict, name: str, exclude_idx: int | None = None) -> bool:
    target = name.casefold()
    for idx, company in enumerate(ws["companies"]):
        if exclude_idx is not None and idx == exclude_idx:
            continue
        if company["name"].casefold() == target:
            return True
    return False


def clear_pending_mirror_tokens_for_company(data: dict, wid: str, company_id: str):
    to_delete = []
    for token, payload in data.get("mirror_tokens", {}).items():
        if payload.get("source_wid") == wid and payload.get("company_id") == company_id:
            to_delete.append(token)
    for token in to_delete:
        data["mirror_tokens"].pop(token, None)


# =========================
# PM HANDLERS
# =========================

@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    if message.chat.type != "private":
        return

    async with lock:
        data = await load_data_unlocked()
        uid = str(message.from_user.id)
        user = ensure_user(data, uid)
        text_pm = pm_main_text(uid, data)
        kb = pm_main_kb(uid, data)
        pm_id = user.get("pm_menu_msg_id")
        if pm_id:
            ok = await try_edit_text(int(uid), pm_id, text_pm, reply_markup=kb)
            if ok:
                await save_data_unlocked(data)
                return
            await safe_delete_message(int(uid), pm_id)
            user["pm_menu_msg_id"] = None
        msg = await send_message(int(uid), text_pm, reply_markup=kb)
        user["pm_menu_msg_id"] = msg.message_id
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data == "pmrefresh:root")
async def pm_refresh(cb: types.CallbackQuery):
    await cb.answer()
    if cb.message.chat.type != "private":
        return

    async with lock:
        data = await load_data_unlocked()
        uid = str(cb.from_user.id)
        user = ensure_user(data, uid)
        user["pm_menu_msg_id"] = cb.message.message_id

        await safe_edit_text(
            int(uid),
            cb.message.message_id,
            pm_main_text(uid, data),
            reply_markup=pm_main_kb(uid, data),
        )
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data == "pmhelp:root")
async def pm_help(cb: types.CallbackQuery):
    await cb.answer()
    if cb.message.chat.type != "private":
        return

    async with lock:
        data = await load_data_unlocked()
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
        await save_data_unlocked(data)


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
        await safe_edit_text(
            int(uid),
            cb.message.message_id,
            pm_main_text(uid, data),
            reply_markup=pm_main_kb(uid, data),
        )
        return

    await safe_edit_text(
        int(uid),
        cb.message.message_id,
        f"📂 {ws['name']}",
        reply_markup=pm_ws_manage_kb(wid),
    )


@dp.callback_query_handler(lambda c: c.data.startswith("pmwsdel:"))
async def pm_delete_workspace(cb: types.CallbackQuery):
    await cb.answer()
    if cb.message.chat.type != "private":
        return

    async with lock:
        data = await load_data_unlocked()
        current_uid = str(cb.from_user.id)
        wid = cb.data.split(":", 1)[1]
        ws = data["workspaces"].get(wid)

        if not ws:
            await safe_edit_text(
                int(current_uid),
                cb.message.message_id,
                pm_main_text(current_uid, data),
                reply_markup=pm_main_kb(current_uid, data),
            )
            await save_data_unlocked(data)
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

        affected_users = []
        for uid, user in data["users"].items():
            if wid in user.get("workspaces", []):
                user["workspaces"].remove(wid)
                affected_users.append(uid)

        ensure_user(data, current_uid)
        data["users"][current_uid]["pm_menu_msg_id"] = cb.message.message_id

        await safe_edit_text(
            int(current_uid),
            cb.message.message_id,
            pm_main_text(current_uid, data),
            reply_markup=pm_main_kb(current_uid, data),
        )

        for uid in affected_users:
            if uid != current_uid:
                await update_pm_menu(uid, data)

        await save_data_unlocked(data)

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

    uid = str(message.from_user.id)
    thread_id = message.message_thread_id or 0
    wid = make_ws_id(message.chat.id, thread_id)
    old_menu_msg_id = None
    old_prompt_msg_id = None
    help_msg_id = None
    ws_name = None
    already_connected = False

    async with lock:
        data = await load_data_unlocked()
        ensure_user(data, uid)
        existing_ws = data["workspaces"].get(wid)
        if existing_ws and existing_ws.get("is_connected"):
            already_connected = True
            ws_name = existing_ws.get("name") or workspace_full_name(message.chat.title or "Workspace", extract_topic_title(message), thread_id)
            help_msg_id = data["users"][uid].get("help_msg_id")
            if help_msg_id:
                data["users"][uid]["help_msg_id"] = None
            await save_data_unlocked(data)
        else:
            topic_title = extract_topic_title(message)
            if not topic_title and existing_ws:
                topic_title = existing_ws.get("topic_title")
            chat_title = message.chat.title or "Workspace"
            ws_name = workspace_full_name(chat_title, topic_title, thread_id)
            old_companies = existing_ws.get("companies", []) if existing_ws else []
            old_template = existing_ws.get("template", []) if existing_ws else [make_task("Создать договор"), make_task("Выставить счёт")]
            old_menu_msg_id = existing_ws.get("menu_msg_id") if existing_ws else None
            old_prompt_msg_id = (existing_ws.get("awaiting") or {}).get("prompt_msg_id") if existing_ws else None
            help_msg_id = data["users"][uid].get("help_msg_id")
            data["users"][uid]["help_msg_id"] = None
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
            if wid not in data["users"][uid]["workspaces"]:
                data["users"][uid]["workspaces"].append(wid)
            await save_data_unlocked(data)

    if help_msg_id:
        await safe_delete_message(int(uid), help_msg_id)

    if already_connected:
        await try_delete_user_message(message)
        await send_temp_message(message.chat.id, f"Workspace «{ws_name}» уже подключён в этом треде.", thread_id, delay=8)
        fresh = await load_data()
        await update_pm_menu(uid, fresh)
        await save_data(fresh)
        return

    if old_menu_msg_id:
        await safe_delete_message(message.chat.id, old_menu_msg_id)
    if old_prompt_msg_id:
        await safe_delete_message(message.chat.id, old_prompt_msg_id)

    fresh = await load_data()
    ws = fresh["workspaces"][wid]
    await ensure_all_company_cards(ws)
    for company in ws["companies"]:
        if company.get("mirror"):
            await upsert_company_mirror(company)
    await send_or_replace_ws_home_menu(fresh, wid)
    await update_pm_menu(uid, fresh)
    await save_data(fresh)

    try:
        await send_week_notice_pm(uid, f"Workspace «{ws_name}» подключён")
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

    async with lock:
        data = await load_data_unlocked()
        wid = make_ws_id(message.chat.id, thread_id)
        ws = data["workspaces"].get(wid)
        if not ws:
            return

        ws["topic_title"] = topic_title
        ws["chat_title"] = message.chat.title or ws.get("chat_title") or "Workspace"
        ws["name"] = workspace_full_name(ws["chat_title"], ws["topic_title"], thread_id)

        for uid, user in data["users"].items():
            if wid in user.get("workspaces", []):
                await update_pm_menu(uid, data)

        await save_data_unlocked(data)


# =========================
# MIRROR BINDING
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("mirroron:"))
async def mirror_on(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx = cb.data.split(":")
    company_idx = int(company_idx)

    async with lock:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        if company_idx < 0 or company_idx >= len(ws["companies"]):
            return

        company = ws["companies"][company_idx]
        if company.get("mirror"):
            await send_temp_message(ws["chat_id"], "Этот список уже дублируется.", ws["thread_id"], delay=8)
            await save_data_unlocked(data)
            return

        company_id = company["id"]
        clear_pending_mirror_tokens_for_company(data, wid, company_id)
        token = generate_mirror_token()
        data["mirror_tokens"][token] = {
            "source_wid": wid,
            "company_id": company_id,
            "created_by": cb.from_user.id,
        }

        await send_temp_message(
            ws["chat_id"],
            "📤 Чтобы привязать дубликат:\n"
            "1) Перейдите в целевой чат/тред\n"
            f"2) Отправьте команду:\n/mirror {token}",
            ws["thread_id"],
            delay=60,
        )
        await edit_company_menu(data, wid, company_idx)
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("mirroroff:"))
async def mirror_off(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx = cb.data.split(":")
    company_idx = int(company_idx)

    async with lock:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws:
            return
        if company_idx < 0 or company_idx >= len(ws["companies"]):
            return

        company = ws["companies"][company_idx]
        company["mirror"] = None
        clear_pending_mirror_tokens_for_company(data, wid, company["id"])
        await edit_company_menu(data, wid, company_idx)
        await save_data_unlocked(data)

    await send_temp_message(ws["chat_id"], "🔌 Список отвязан", ws["thread_id"], delay=8)


@dp.message_handler(commands=["mirror"])
async def cmd_mirror(message: types.Message):
    if message.chat.type == "private":
        return

    code = (message.get_args() or "").strip().upper()
    if not code:
        await send_temp_message(message.chat.id, "Укажите код: /mirror CODE", message.message_thread_id or 0, delay=10)
        return

    async with lock:
        data = await load_data_unlocked()
        payload = data.get("mirror_tokens", {}).get(code)
        if not payload:
            await send_temp_message(message.chat.id, "Код не найден или уже использован.", message.message_thread_id or 0, delay=10)
            return

        source_wid = payload["source_wid"]
        company_id = payload.get("company_id")

        ws = data["workspaces"].get(source_wid)
        if not ws:
            data["mirror_tokens"].pop(code, None)
            await save_data_unlocked(data)
            await send_temp_message(message.chat.id, "Исходный workspace не найден.", message.message_thread_id or 0, delay=10)
            return

        company_idx = find_company_index_by_id(ws, company_id)
        if company_idx is None:
            data["mirror_tokens"].pop(code, None)
            await save_data_unlocked(data)
            await send_temp_message(message.chat.id, "Компания не найдена.", message.message_thread_id or 0, delay=10)
            return

        company = ws["companies"][company_idx]

        company["mirror"] = {
            "chat_id": message.chat.id,
            "thread_id": message.message_thread_id or 0,
            "message_id": None,
        }

        await upsert_company_mirror(company)
        data["mirror_tokens"].pop(code, None)
        await try_delete_user_message(message)

        await send_temp_message(
            ws["chat_id"],
            f"📤 Список «{company['name']}» дублируется в другой тред/чат",
            ws["thread_id"],
            delay=10,
        )

        if ws.get("is_connected"):
            await edit_company_menu(data, source_wid, company_idx)

        await save_data_unlocked(data)


# =========================
# NAVIGATION
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("backws:"))
async def back_to_ws(cb: types.CallbackQuery):
    await cb.answer()
    wid = cb.data.split(":", 1)[1]

    async with lock:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        await edit_ws_home_menu(data, wid)
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("cmp:"))
async def open_company(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx = cb.data.split(":")

    async with lock:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        await edit_company_menu(data, wid, int(company_idx))
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("cmpset:"))
async def open_company_settings(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx = cb.data.split(":")

    async with lock:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        await edit_company_settings_menu(data, wid, int(company_idx))
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("cmpddlmode:"))
async def toggle_company_deadline_mode(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx = cb.data.split(":")
    company_idx = int(company_idx)
    async with lock:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected") or company_idx < 0 or company_idx >= len(ws["companies"]):
            return
        company = ws["companies"][company_idx]
        company["deadline_view_mode"] = "date" if company.get("deadline_view_mode") != "date" else "days"
        await sync_company_everywhere(ws, company_idx)
        await edit_company_settings_menu(data, wid, company_idx)
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("task:"))
async def open_task_menu(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx, task_idx = cb.data.split(":")

    async with lock:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        await edit_task_menu(data, wid, int(company_idx), int(task_idx))
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("tplitem:"))
async def open_template_item(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, template_idx = cb.data.split(":")

    async with lock:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        await edit_template_item_menu(data, wid, int(template_idx))
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("tpl:"))
async def open_template_menu(cb: types.CallbackQuery):
    await cb.answer()
    wid = cb.data.split(":", 1)[1]

    async with lock:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        await edit_template_menu(data, wid)
        await save_data_unlocked(data)


# =========================
# PROMPT / CANCEL
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("cancel:"))
async def cancel_input(cb: types.CallbackQuery):
    await cb.answer()
    wid = cb.data.split(":", 1)[1]

    async with lock:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws:
            return

        awaiting = ws.get("awaiting") or {}
        if awaiting.get("prompt_msg_id"):
            await safe_delete_message(ws["chat_id"], awaiting["prompt_msg_id"])

        back_to = awaiting.get("back_to", {"view": "ws"})
        ws["awaiting"] = None

        if ws.get("is_connected"):
            if back_to["view"] == "company":
                await edit_company_menu(data, wid, back_to["company_idx"])
            elif back_to["view"] == "company_settings":
                await edit_company_settings_menu(data, wid, back_to["company_idx"])
            elif back_to["view"] == "task":
                await edit_task_menu(data, wid, back_to["company_idx"], back_to["task_idx"])
            elif back_to["view"] == "template_item":
                await edit_template_item_menu(data, wid, back_to["template_idx"])
            elif back_to["view"] == "template":
                await edit_template_menu(data, wid)
            else:
                await edit_ws_home_menu(data, wid)

        await save_data_unlocked(data)


# =========================
# COMPANY ACTIONS
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("cmpnew:"))
async def create_company_prompt(cb: types.CallbackQuery):
    await cb.answer()
    wid = cb.data.split(":", 1)[1]

    async with lock:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return

        await set_prompt(
            ws,
            "✏️ Напишите название компании:",
            {
                "type": "new_company",
                "back_to": {"view": "ws"},
            },
        )
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("cmpren:"))
async def rename_company_prompt(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx = cb.data.split(":")
    company_idx = int(company_idx)

    async with lock:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
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
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("cmpdel:"))
async def delete_company(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx = cb.data.split(":")
    company_idx = int(company_idx)

    async with lock:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        if company_idx < 0 or company_idx >= len(ws["companies"]):
            return

        company = ws["companies"][company_idx]
        company_id = company["id"]
        company = ws["companies"].pop(company_idx)
        await safe_delete_message(ws["chat_id"], company.get("card_msg_id"))

        mirror = company.get("mirror") or {}
        if mirror.get("message_id"):
            await safe_delete_message(mirror["chat_id"], mirror["message_id"])

        clear_pending_mirror_tokens_for_company(data, wid, company_id)
        await send_or_replace_ws_home_menu(data, wid)
        await save_data_unlocked(data)


# =========================
# TASK ACTIONS
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("tasknew:"))
async def add_task_prompt(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx = cb.data.split(":")
    company_idx = int(company_idx)

    async with lock:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
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
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("taskren:"))
async def rename_task_prompt(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx, task_idx = cb.data.split(":")
    company_idx = int(company_idx)
    task_idx = int(task_idx)

    async with lock:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
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
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("taskdl:"))
async def task_deadline_prompt(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx, task_idx = cb.data.split(":")
    company_idx = int(company_idx)
    task_idx = int(task_idx)
    async with lock:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        await set_prompt(ws, "⏰ Пришлите дату в формате ДД.ММ.ГГГГ или число дней, например 3.", {
            "type": "task_deadline",
            "company_idx": company_idx,
            "task_idx": task_idx,
            "back_to": {"view": "task", "company_idx": company_idx, "task_idx": task_idx},
        })
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("taskdldel:"))
async def delete_task_deadline(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx, task_idx = cb.data.split(":")
    company_idx = int(company_idx)
    task_idx = int(task_idx)
    async with lock:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected") or company_idx < 0 or company_idx >= len(ws["companies"]):
            return
        company = ws["companies"][company_idx]
        if task_idx < 0 or task_idx >= len(company["tasks"]):
            return
        task = company["tasks"][task_idx]
        task["deadline_due_date"] = None
        task["deadline_started_at"] = None
        await sync_company_everywhere(ws, company_idx)
        await edit_task_menu(data, wid, company_idx, task_idx)
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("taskdel:"))
async def delete_task(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx, task_idx = cb.data.split(":")
    company_idx = int(company_idx)
    task_idx = int(task_idx)

    async with lock:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        if company_idx < 0 or company_idx >= len(ws["companies"]):
            return

        company = ws["companies"][company_idx]
        if task_idx < 0 or task_idx >= len(company["tasks"]):
            return

        company["tasks"].pop(task_idx)
        await sync_company_everywhere(ws, company_idx)
        await edit_company_menu(data, wid, company_idx)
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("taskdone:"))
async def toggle_task_done(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, company_idx, task_idx = cb.data.split(":")
    company_idx = int(company_idx)
    task_idx = int(task_idx)

    async with lock:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        if company_idx < 0 or company_idx >= len(ws["companies"]):
            return

        company = ws["companies"][company_idx]
        if task_idx < 0 or task_idx >= len(company["tasks"]):
            return

        company["tasks"][task_idx]["done"] = not company["tasks"][task_idx]["done"]
        await sync_company_everywhere(ws, company_idx)
        await edit_task_menu(data, wid, company_idx, task_idx)
        await save_data_unlocked(data)


# =========================
# TEMPLATE ACTIONS
# =========================

@dp.callback_query_handler(lambda c: c.data.startswith("tplnew:"))
async def add_template_task_prompt(cb: types.CallbackQuery):
    await cb.answer()
    wid = cb.data.split(":", 1)[1]

    async with lock:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return

        await set_prompt(
            ws,
            "✏️ Введите название новой задачи шаблона:",
            {
                "type": "new_template_task",
                "back_to": {"view": "template"},
            },
        )
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("tplren:"))
async def rename_template_task_prompt(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, template_idx = cb.data.split(":")
    template_idx = int(template_idx)

    async with lock:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
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
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("tpldl:"))
async def template_deadline_prompt(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, template_idx = cb.data.split(":")
    template_idx = int(template_idx)
    async with lock:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        await set_prompt(ws, "⏰ Пришлите количество дней, например 3.", {
            "type": "template_deadline",
            "template_idx": template_idx,
            "back_to": {"view": "template_item", "template_idx": template_idx},
        })
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("tpldldel:"))
async def delete_template_deadline(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, template_idx = cb.data.split(":")
    template_idx = int(template_idx)
    async with lock:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected") or template_idx < 0 or template_idx >= len(ws["template"]):
            return
        ws["template"][template_idx]["template_deadline_days"] = None
        await edit_template_item_menu(data, wid, template_idx)
        await save_data_unlocked(data)


@dp.callback_query_handler(lambda c: c.data.startswith("tpldel:"))
async def delete_template_task(cb: types.CallbackQuery):
    await cb.answer()
    _, wid, template_idx = cb.data.split(":")
    template_idx = int(template_idx)

    async with lock:
        data = await load_data_unlocked()
        ws = data["workspaces"].get(wid)
        if not ws or not ws.get("is_connected"):
            return
        if template_idx < 0 or template_idx >= len(ws["template"]):
            return

        ws["template"].pop(template_idx)
        await edit_template_menu(data, wid)
        await save_data_unlocked(data)


# =========================
# GROUP TEXT INPUT
# =========================

@dp.message_handler(content_types=types.ContentTypes.TEXT)
async def handle_group_text(message: types.Message):
    if message.chat.type == "private":
        return

    if is_known_command(message.text):
        return

    async with lock:
        data = await load_data_unlocked()
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
                await save_data_unlocked(data)
                return
            now_ts = int(datetime.now().timestamp())
            new_tasks = []
            for tpl in ws["template"]:
                due_date = None
                started_at = None
                days = tpl.get("template_deadline_days")
                if isinstance(days, int) and days > 0:
                    due_date = (today_local() + timedelta(days=days)).isoformat()
                    started_at = now_ts
                new_tasks.append(make_task(tpl.get("text", ""), False, created_at=now_ts, deadline_due_date=due_date, deadline_started_at=started_at))
            company = {
                "id": uuid.uuid4().hex,
                "name": text,
                "tasks": new_tasks,
                "card_msg_id": None,
                "mirror": None,
                "deadline_view_mode": "days",
            }
            ws["companies"].append(company)
            ws["awaiting"] = None
            await sync_company_everywhere(ws, len(ws["companies"]) - 1)
            await safe_delete_message(ws["chat_id"], prompt_msg_id)
            await try_delete_user_message(message)
            await send_or_replace_ws_home_menu(data, wid)
            await save_data_unlocked(data)
            return

        if mode == "rename_company":
            company_idx = awaiting["company_idx"]
            if company_idx < 0 or company_idx >= len(ws["companies"]):
                ws["awaiting"] = None
                await save_data_unlocked(data)
                return
            if company_exists(ws, text, exclude_idx=company_idx):
                await send_temp_message(ws["chat_id"], "Такая компания уже существует.", ws["thread_id"], delay=6)
                await save_data_unlocked(data)
                return
            ws["companies"][company_idx]["name"] = text
            ws["awaiting"] = None
            await safe_delete_message(ws["chat_id"], prompt_msg_id)
            await try_delete_user_message(message)
            await sync_company_everywhere(ws, company_idx)
            await edit_company_settings_menu(data, wid, company_idx)
            await save_data_unlocked(data)
            return

        if mode == "new_task":
            company_idx = awaiting["company_idx"]
            if company_idx < 0 or company_idx >= len(ws["companies"]):
                ws["awaiting"] = None
                await save_data_unlocked(data)
                return
            ws["companies"][company_idx]["tasks"].append(make_task(text))
            ws["awaiting"] = None
            await safe_delete_message(ws["chat_id"], prompt_msg_id)
            await try_delete_user_message(message)
            await sync_company_everywhere(ws, company_idx)
            await edit_company_menu(data, wid, company_idx)
            await save_data_unlocked(data)
            return

        if mode == "rename_task":
            company_idx = awaiting["company_idx"]
            task_idx = awaiting["task_idx"]
            if company_idx < 0 or company_idx >= len(ws["companies"]):
                ws["awaiting"] = None
                await save_data_unlocked(data)
                return
            company = ws["companies"][company_idx]
            if task_idx < 0 or task_idx >= len(company["tasks"]):
                ws["awaiting"] = None
                await save_data_unlocked(data)
                return
            company["tasks"][task_idx]["text"] = text
            ws["awaiting"] = None
            await safe_delete_message(ws["chat_id"], prompt_msg_id)
            await try_delete_user_message(message)
            await sync_company_everywhere(ws, company_idx)
            await edit_company_menu(data, wid, company_idx)
            await save_data_unlocked(data)
            return

        if mode == "task_deadline":
            company_idx = awaiting["company_idx"]
            task_idx = awaiting["task_idx"]
            if company_idx < 0 or company_idx >= len(ws["companies"]):
                ws["awaiting"] = None
                await save_data_unlocked(data)
                return
            company = ws["companies"][company_idx]
            if task_idx < 0 or task_idx >= len(company["tasks"]):
                ws["awaiting"] = None
                await save_data_unlocked(data)
                return
            task = company["tasks"][task_idx]
            due_date, started_at, error = parse_deadline_input(text, task.get("deadline_started_at"))
            if error:
                await send_temp_message(ws["chat_id"], error, ws["thread_id"], delay=7)
                await save_data_unlocked(data)
                return
            task["deadline_due_date"] = due_date
            task["deadline_started_at"] = started_at
            ws["awaiting"] = None
            await safe_delete_message(ws["chat_id"], prompt_msg_id)
            await try_delete_user_message(message)
            await sync_company_everywhere(ws, company_idx)
            await edit_task_menu(data, wid, company_idx, task_idx)
            await save_data_unlocked(data)
            return

        if mode == "new_template_task":
            ws["template"].append(make_task(text))
            ws["awaiting"] = None
            await safe_delete_message(ws["chat_id"], prompt_msg_id)
            await try_delete_user_message(message)
            await edit_template_menu(data, wid)
            await save_data_unlocked(data)
            return

        if mode == "rename_template_task":
            template_idx = awaiting["template_idx"]
            if template_idx < 0 or template_idx >= len(ws["template"]):
                ws["awaiting"] = None
                await save_data_unlocked(data)
                return
            ws["template"][template_idx]["text"] = text
            ws["awaiting"] = None
            await safe_delete_message(ws["chat_id"], prompt_msg_id)
            await try_delete_user_message(message)
            await edit_template_item_menu(data, wid, template_idx)
            await save_data_unlocked(data)
            return

        if mode == "template_deadline":
            template_idx = awaiting["template_idx"]
            if template_idx < 0 or template_idx >= len(ws["template"]):
                ws["awaiting"] = None
                await save_data_unlocked(data)
                return
            days, error = parse_template_deadline_input(text)
            if error:
                await send_temp_message(ws["chat_id"], error, ws["thread_id"], delay=7)
                await save_data_unlocked(data)
                return
            ws["template"][template_idx]["template_deadline_days"] = days
            ws["awaiting"] = None
            await safe_delete_message(ws["chat_id"], prompt_msg_id)
            await try_delete_user_message(message)
            await edit_template_item_menu(data, wid, template_idx)
            await save_data_unlocked(data)
            return


# =========================
# RUN
# =========================

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
