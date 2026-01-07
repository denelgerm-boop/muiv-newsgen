"""
Автор: Андуганов Д.Г.
Тема практики: Автоматическая генерация новостных сообщений из плана мероприятий организации с помощью нейронных сетей

Скрейпер новостей МУИВ:
- обходит /about/news/ и пагинацию
- собирает ссылки на статьи
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import List
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


NEWS_INDEX = "https://www.muiv.ru/about/news/"


@dataclass
class NewsItem:
    url: str
    title: str
    date_str: str


def _get(url: str, timeout: int = 30) -> str:
    headers = {"User-Agent": "Mozilla/5.0 (compatible; muiv-newsgen/1.0)"}
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.text


def parse_index_page(html: str) -> List[NewsItem]:
    soup = BeautifulSoup(html, "lxml")
    items: List[NewsItem] = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href.startswith("/about/news/"):
            continue
        if href.rstrip("/") == "/about/news":
            continue

        title = a.get_text(" ", strip=True)
        if len(title) < 5:
            continue

        parent_text = a.parent.get_text(" ", strip=True) if a.parent else ""
        m = re.search(r"(\d{2}\.\d{2}\.\d{4})", parent_text)
        date_str = m.group(1) if m else ""

        items.append(
            NewsItem(
                url=urljoin(NEWS_INDEX, href),
                title=title,
                date_str=date_str,
            )
        )

    uniq = {it.url: it for it in items}
    return list(uniq.values())


def collect_news_items(max_pages: int = 5, sleep_sec: float = 0.5) -> List[NewsItem]:
    all_items: List[NewsItem] = []

    for page in range(1, max_pages + 1):
        url = NEWS_INDEX if page == 1 else f"{NEWS_INDEX}?PAGEN_1={page}"
        html = _get(url)
        all_items.extend(parse_index_page(html))
        time.sleep(sleep_sec)

    uniq = {it.url: it for it in all_items}
    return list(uniq.values())
