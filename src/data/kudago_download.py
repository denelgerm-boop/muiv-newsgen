# -*- coding: utf-8 -*-
"""
Автор: <ФИО>
Тема: Автоматическая генерация новостных сообщений из плана мероприятий организации с помощью нейронных сетей

Модуль загрузки событий из KudaGo API и сохранения датасета для обучения.
"""

from __future__ import annotations

import csv
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup


KUDAGO_EVENTS_URL = "https://kudago.com/public-api/v1.4/events/"


def _strip_html(html: str) -> str:
    """Удаляет HTML-теги и нормализует пробелы."""
    if not html:
        return ""
    text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()
    return text


@dataclass
class EventItem:
    event_id: int
    title: str
    description: str
    site_url: str
    place_title: str
    place_address: str
    first_date: Optional[int]


def fetch_events(location: str = "msk", pages: int = 5, page_size: int = 100, sleep_sec: float = 0.3) -> List[Dict[str, Any]]:
    """
    Загружает события из KudaGo API.
    location: например msk, spb и т.д.
    pages: сколько страниц забрать
    """
    results: List[Dict[str, Any]] = []

    params = {
        "location": location,
        "lang": "ru",
        "page_size": page_size,
        "fields": "id,title,description,site_url,dates,place,price",
        "expand": "place",
        "order_by": "-publication_date",
    }

    for page in range(1, pages + 1):
        params["page"] = page
        r = requests.get(KUDAGO_EVENTS_URL, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        chunk = data.get("results", [])
        results.extend(chunk)
        time.sleep(sleep_sec)

    return results


def normalize_event(raw: Dict[str, Any]) -> EventItem:
    place = raw.get("place") or {}
    dates = raw.get("dates") or []
    first_date = dates[0].get("start") if dates else None

    return EventItem(
        event_id=int(raw.get("id")),
        title=str(raw.get("title") or "").strip(),
        description=_strip_html(str(raw.get("description") or "")),
        site_url=str(raw.get("site_url") or "").strip(),
        place_title=str(place.get("title") or "").strip(),
        place_address=str(place.get("address") or "").strip(),
        first_date=first_date,
    )


def save_csv(items: List[EventItem], out_path: str) -> None:
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["event_id", "title", "description", "site_url", "place_title", "place_address", "first_date"])
        for it in items:
            w.writerow([it.event_id, it.title, it.description, it.site_url, it.place_title, it.place_address, it.first_date])


def build_dataset(out_path: str = "data/processed/events.csv", location: str = "msk") -> int:
    raw = fetch_events(location=location, pages=8, page_size=100)
    items = [normalize_event(x) for x in raw]
    # минимальная фильтрация: убираем пустые тексты
    items = [x for x in items if x.title and len(x.description) >= 80]
    save_csv(items, out_path)
    return len(items)


if __name__ == "__main__":
    n = build_dataset()
    print(f"Saved: {n} items")
