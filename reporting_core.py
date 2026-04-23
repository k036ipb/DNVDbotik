import copy
import time
import uuid
from calendar import monthrange
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

TIMEZONE = ZoneInfo("Europe/Riga")
NORMALIZED_REPORTING_IDS: set[int] = set()

WEEKDAY_NAMES = [
    "понедельник",
    "вторник",
    "среда",
    "четверг",
    "пятница",
    "суббота",
    "воскресение",
]


def now_ts() -> int:
    return int(time.time())


def clear_reporting_runtime_cache():
    NORMALIZED_REPORTING_IDS.clear()


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
    elif kind != "on_done":
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
    NORMALIZED_REPORTING_IDS.add(id(reporting))
    return reporting


def get_reporting(company: dict) -> dict:
    reporting = company.get("reporting")
    if isinstance(reporting, dict):
        reporting.setdefault("intervals", [])
        reporting.setdefault("targets", None)
        reporting.setdefault("history", [])
        if id(reporting) in NORMALIZED_REPORTING_IDS:
            return reporting
    company["reporting"] = ensure_reporting(reporting)
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
    if not isinstance(targets, list):
        return []

    normalized_targets = []
    changed = False
    for item in targets:
        if isinstance(item, dict) and item.get("chat_id") is not None:
            thread_id = int(item.get("thread_id") or 0)
            if item.get("thread_id") != thread_id:
                item["thread_id"] = thread_id
                changed = True
            normalized_targets.append(item)
            continue

        normalized = ensure_report_target(item)
        if normalized:
            normalized_targets.append(normalized)
        changed = True

    if changed or len(normalized_targets) != len(targets):
        reporting["targets"] = normalized_targets
    return normalized_targets


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


def missing_report_targets_for_mirrors(company: dict) -> list[tuple[int, dict]]:
    mirror_keys = {
        report_target_key(mirror)
        for mirror in company.get("mirrors", [])
        if isinstance(mirror, dict) and mirror.get("chat_id") is not None
    }
    result = []
    for idx, target in enumerate(get_effective_report_targets(company)):
        if report_target_key(target) in mirror_keys:
            continue
        result.append((idx, target))
    return result


def missing_mirrors_for_report_targets(company: dict) -> list[tuple[int, dict]]:
    target_keys = {report_target_key(target) for target in get_effective_report_targets(company)}
    result = []
    for idx, mirror in enumerate(company.get("mirrors", [])):
        if not isinstance(mirror, dict) or mirror.get("chat_id") is None:
            result.append((idx, mirror))
            continue
        if report_target_key(mirror) in target_keys:
            continue
        result.append((idx, mirror))
    return result


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
