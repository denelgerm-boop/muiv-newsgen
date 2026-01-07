# -*- coding: utf-8 -*-
"""
Автор:  Андуганов Д. Г
Тема практики: Автоматическая генерация новостных сообщений из плана мероприятий организации с помощью нейронных сетей

Назначение:
- Скачивает события (events) из публичного API KudaGo
- Нормализует поля (даты/тексты/URL)
- Сохраняет:
  1) raw-события: JSONL
  2) обучающие пары "план -> новость": JSONL + CSV (опционально)

Важно:
- Никаких случайных данных: все тексты основаны на реальных событиях.
- Защита от падений на "кривых" датах (сек/мс/None/слишком большие числа).
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm

try:
    import pandas as pd  # type: ignore
except Exception:
    pd = None


KUDAGO_EVENTS_URL = "https://kudago.com/public-api/v1.4/events/"


# -----------------------------
# Утилиты (текст/даты)
# -----------------------------

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


def strip_html(text: str) -> str:
    """Грубое удаление HTML-тегов (без внешних зависимостей)."""
    if not text:
        return ""
    text = _TAG_RE.sub(" ", text)
    text = text.replace("&nbsp;", " ").replace("&amp;", "&")
    text = _WS_RE.sub(" ", text).strip()
    return text


def safe_int(x: Any) -> Optional[int]:
    """Пытается привести к int; возвращает None при неуспехе."""
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        return None


def normalize_unix_ts(ts: Any) -> Optional[int]:
    """
    Нормализует unix timestamp:
    - допускает секунды или миллисекунды
    - отсекает мусор/None
    Возвращает timestamp в СЕКУНДАХ или None.
    """
    v = safe_int(ts)
    if v is None:
        return None

    # Частая причина Errno 22 на Windows: слишком большие числа (например миллисекунды).
    # Если число похоже на миллисекунды — делим на 1000.
    # 1e12 ~ 2001-09-09 в миллисекундах, 1e10 ~ 2286-11-20 в секундах.
    if v > 10_000_000_000:  # скорее миллисекунды
        v = v // 1000

    # Фильтр диапазона (примерно 1970..2100, чтобы не улетать в ошибки)
    if v < 0 or v > 4_102_444_800:  # 2100-01-01
        return None

    return v


def ts_to_local_str(ts_sec: Optional[int]) -> str:
    """Переводит timestamp (сек) в строку 'YYYY-MM-DD HH:MM' в локальном времени."""
    if ts_sec is None:
        return ""
    try:
        dt = datetime.fromtimestamp(ts_sec)  # локальное время
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return ""


def pick_best_date_range(dates: Any) -> Tuple[Optional[int], Optional[int]]:
    """
    KudaGo возвращает dates как список объектов:
    [{"start": 123, "end": 456}, ...]
    Берём самый ранний start и соответствующий end (или минимальный end >= start).
    """
    if not isinstance(dates, list) or not dates:
        return None, None

    norm_ranges: List[Tuple[int, Optional[int]]] = []
    for d in dates:
        if not isinstance(d, dict):
            continue
        s = normalize_unix_ts(d.get("start"))
        e = normalize_unix_ts(d.get("end"))
        if s is None:
            continue
        norm_ranges.append((s, e))

    if not norm_ranges:
        return None, None

    # Сортируем по start
    norm_ranges.sort(key=lambda x: x[0])
    start = norm_ranges[0][0]

    # Ищем "разумный" end: первый end, который >= start
    end: Optional[int] = None
    for s, e in norm_ranges:
        if s == start and e is not None and e >= start:
            end = e
            break
    if end is None:
        # fallback: минимальный end среди тех, что >= start
        candidates = [e for s, e in norm_ranges if e is not None and e >= start]
        end = min(candidates) if candidates else None

    return start, end


def safe_filename(s: str) -> str:
    """Делает строку безопасной для имени файла."""
    s = re.sub(r"[^a-zA-Z0-9_\-]+", "_", s.strip())
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "file"


# -----------------------------
# HTTP / API
# -----------------------------

def build_session(timeout_sec: int = 30) -> requests.Session:
    """
    Создаёт Session с ретраями на сетевые ошибки и 429/5xx.
    """
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0 Safari/537.36"
        }
    )
    # timeout будем передавать в запрос
    session._timeout_sec = timeout_sec  # type: ignore[attr-defined]
    return session


def get_json(session: requests.Session, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    timeout = getattr(session, "_timeout_sec", 30)
    r = session.get(url, params=params, timeout=timeout)
    # Даже если статус не 200, попробуем разобрать JSON, а потом уже ругаться.
    try:
        data = r.json()
    except Exception as e:
        raise RuntimeError(f"Ответ не JSON. status={r.status_code}, url={r.url}, err={e}") from e

    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code} при запросе {r.url}. Ответ: {str(data)[:500]}")
    return data


# -----------------------------
# Логика датасета
# -----------------------------

@dataclass
class EventRecord:
    id: int
    title: str
    description: str
    short_title: str
    location: str
    site_url: str
    start_ts: Optional[int]
    end_ts: Optional[int]
    start_str: str
    end_str: str
    place: str
    address: str
    categories: str
    tags: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "short_title": self.short_title,
            "description": self.description,
            "location": self.location,
            "site_url": self.site_url,
            "start_ts": self.start_ts,
            "end_ts": self.end_ts,
            "start_str": self.start_str,
            "end_str": self.end_str,
            "place": self.place,
            "address": self.address,
            "categories": self.categories,
            "tags": self.tags,
        }


def parse_event(raw: Dict[str, Any], location: str) -> Optional[EventRecord]:
    """
    Приводит raw-event к нормализованной структуре.
    """
    eid = safe_int(raw.get("id"))
    if eid is None:
        return None

    title = strip_html(str(raw.get("title") or "")).strip()
    if not title:
        return None

    short_title = strip_html(str(raw.get("short_title") or "")).strip()
    desc = strip_html(str(raw.get("description") or "")).strip()
    site_url = str(raw.get("site_url") or "").strip()

    start_ts, end_ts = pick_best_date_range(raw.get("dates"))
    start_str = ts_to_local_str(start_ts)
    end_str = ts_to_local_str(end_ts)

    place = ""
    address = ""
    place_obj = raw.get("place")
    if isinstance(place_obj, dict):
        place = strip_html(str(place_obj.get("title") or "")).strip()
        address = strip_html(str(place_obj.get("address") or "")).strip()

    cats = raw.get("categories")
    if isinstance(cats, list):
        categories = ", ".join([str(x) for x in cats if x])
    else:
        categories = str(cats or "")

    tags = raw.get("tags")
    if isinstance(tags, list):
        tags_s = ", ".join([str(x) for x in tags if x])
    else:
        tags_s = str(tags or "")

    return EventRecord(
        id=eid,
        title=title,
        short_title=short_title,
        description=desc,
        location=location,
        site_url=site_url,
        start_ts=start_ts,
        end_ts=end_ts,
        start_str=start_str,
        end_str=end_str,
        place=place,
        address=address,
        categories=categories,
        tags=tags_s,
    )


def make_plan_text(ev: EventRecord) -> str:
    """
    Источник (input) для генерации: структурированный "план мероприятия".
    """
    parts = []
    parts.append(f"Событие: {ev.title}")
    if ev.location:
        parts.append(f"Город: {ev.location}")
    if ev.place:
        parts.append(f"Место: {ev.place}")
    if ev.address:
        parts.append(f"Адрес: {ev.address}")
    if ev.start_str:
        if ev.end_str and ev.end_str != ev.start_str:
            parts.append(f"Дата и время: {ev.start_str} — {ev.end_str}")
        else:
            parts.append(f"Дата и время: {ev.start_str}")
    if ev.categories:
        parts.append(f"Категории: {ev.categories}")
    if ev.tags:
        parts.append(f"Теги: {ev.tags}")

    # Короткое описание (как “пояснение в плане”)
    if ev.description:
        teaser = ev.description[:320].strip()
        parts.append(f"Описание: {teaser}")

    return "\n".join(parts).strip()


def make_news_text(ev: EventRecord) -> str:
    """
    Цель (target) для обучения: "новостная заметка" на основе фактов события.
    Это не случайный текст: собирается из реальных полей мероприятия.
    """
    base = ev.short_title or ev.title
    chunk = ev.description[:420].strip() if ev.description else ""
    when = ""
    if ev.start_str:
        when = ev.start_str if not ev.end_str else f"{ev.start_str}—{ev.end_str}"

    pieces = []
    pieces.append(base.rstrip(".") + ".")
    if when:
        pieces.append(f"Дата: {when}.")
    if ev.place:
        pieces.append(f"Место: {ev.place}.")
    if chunk:
        pieces.append(chunk.rstrip(".") + ".")
    if ev.site_url:
        pieces.append(f"Подробнее: {ev.site_url}")

    text = " ".join(pieces)
    text = _WS_RE.sub(" ", text).strip()
    return text


def iter_events(
    session: requests.Session,
    location: str,
    pages: int,
    page_size: int,
    lang: str,
) -> Iterable[Dict[str, Any]]:
    """
    Итератор по событиям KudaGo.
    """
    fields = ",".join([
        "id",
        "title",
        "short_title",
        "description",
        "dates",
        "place",
        "site_url",
        "categories",
        "tags",
    ])

    for page in range(1, pages + 1):
        params = {
            "location": location,
            "lang": lang,
            "page": page,
            "page_size": page_size,
            "fields": fields,
            "text_format": "text",  # попросим API отдать без HTML (если поддерживается)
        }
        data = get_json(session, KUDAGO_EVENTS_URL, params=params)
        results = data.get("results")
        if not isinstance(results, list) or not results:
            break
        for item in results:
            if isinstance(item, dict):
                yield item


def ensure_dirs() -> None:
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    Path("data/processed").mkdir(parents=True, exist_ok=True)


def save_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def save_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


def build_dataset(
    location: str,
    pages: int,
    page_size: int,
    lang: str,
    make_pairs: bool,
) -> Tuple[int, Path, Optional[Path], Optional[Path]]:
    """
    Основная функция:
    - скачивает
    - парсит
    - сохраняет
    """
    ensure_dirs()
    session = build_session(timeout_sec=45)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    loc_tag = safe_filename(location)
    raw_path = Path(f"data/raw/kudago_events__{loc_tag}__{ts}.jsonl")
    pairs_jsonl_path = Path(f"data/processed/kudago_pairs__{loc_tag}__{ts}.jsonl") if make_pairs else None
    pairs_csv_path = Path(f"data/processed/kudago_pairs__{loc_tag}__{ts}.csv") if make_pairs else None

    raw_events: List[Dict[str, Any]] = []
    pairs: List[Dict[str, Any]] = []

    iterator = iter_events(session, location=location, pages=pages, page_size=page_size, lang=lang)
    for item in tqdm(iterator, total=pages * page_size, desc="Downloading events", unit="ev"):
        ev = parse_event(item, location=location)
        if ev is None:
            continue

        raw_events.append(ev.to_dict())

        if make_pairs:
            src = make_plan_text(ev)
            tgt = make_news_text(ev)
            if src and tgt:
                pairs.append({
                    "id": ev.id,
                    "location": ev.location,
                    "source": src,
                    "target": tgt,
                    "site_url": ev.site_url,
                    "start_str": ev.start_str,
                    "end_str": ev.end_str,
                })

    save_jsonl(raw_path, raw_events)

    if make_pairs and pairs_jsonl_path and pairs_csv_path:
        save_jsonl(pairs_jsonl_path, pairs)
        save_csv(
            pairs_csv_path,
            pairs,
            fieldnames=["id", "location", "source", "target", "site_url", "start_str", "end_str"]
        )

        # если pandas доступен — ещё и компактный “чистый” CSV
        if pd is not None:
            try:
                df = pd.DataFrame(pairs)
                df.to_csv(pairs_csv_path, index=False, encoding="utf-8")
            except Exception:
                pass

    return len(raw_events), raw_path, pairs_jsonl_path, pairs_csv_path


# -----------------------------
# CLI
# -----------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Скачивание мероприятий из KudaGo и формирование датасета для генерации новостей."
    )
    p.add_argument("--location", type=str, default="msk", help="Локация KudaGo: msk, spb, ekb, etc.")
    p.add_argument("--pages", type=int, default=5, help="Сколько страниц скачивать.")
    p.add_argument("--page-size", type=int, default=100, help="Сколько событий на страницу (обычно до 100).")
    p.add_argument("--lang", type=str, default="ru", help="Язык (ru/en).")
    p.add_argument("--make-pairs", action="store_true", help="Сформировать пары plan->news (source/target).")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    n, raw_path, pairs_jsonl, pairs_csv = build_dataset(
        location=args.location,
        pages=max(1, args.pages),
        page_size=max(1, args.page_size),
        lang=args.lang,
        make_pairs=args.make_pairs,
    )

    print()
    print(f"Saved events: {n}")
    print(f"RAW: {raw_path}")
    if pairs_jsonl:
        print(f"PAIRS JSONL: {pairs_jsonl}")
    if pairs_csv:
        print(f"PAIRS CSV:  {pairs_csv}")


if __name__ == "__main__":
    main()
