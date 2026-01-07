"""
Автор: Андуганов Д.Г.
Тема: Автоматическая генерация новостных сообщений из плана мероприятий организации с помощью нейронных сетей.

http_client.py — единая точка загрузки HTML с обработкой антибот-страниц.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict, Tuple
import re
import time

import requests


DEFAULT_HEADERS: Dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
}


@dataclass
class FetchResult:
    url: str
    final_url: str
    status_code: int
    content_type: str
    text: str
    used_cloudscraper: bool
    looks_like_antibot: bool


def _looks_like_antibot(html: str) -> bool:
    """
    Эвристики антибот-страницы МУИВ:
    - meta robots noindex/noarchive
    - странная "заглушка" с base64 gif
    - характерный класс gorizontal-vertikal
    """
    h = (html or "").lower()
    if not h:
        return False

    signals = 0
    if "noindex" in h and "noarchive" in h:
        signals += 1
    if "gorizontal-vertikal" in h:
        signals += 1
    if "data:image/gif;base64" in h:
        signals += 1
    if "enable javascript" in h or "please enable javascript" in h:
        signals += 1

    return signals >= 2


def fetch_html(
    url: str,
    timeout: int = 30,
    use_cloudscraper_if_needed: bool = True,
    sleep_seconds: float = 0.0,
) -> FetchResult:
    """
    Пытаемся получить реальную HTML-страницу.
    Сначала requests, если похоже на антибот — пробуем cloudscraper (если включено).
    """
    if sleep_seconds > 0:
        time.sleep(sleep_seconds)

    # 1) Обычный requests
    r = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout, allow_redirects=True)
    ct = r.headers.get("Content-Type", "")
    text = r.text or ""
    antibot = _looks_like_antibot(text)

    if (not antibot) or (not use_cloudscraper_if_needed):
        return FetchResult(
            url=url,
            final_url=str(r.url),
            status_code=int(r.status_code),
            content_type=ct,
            text=text,
            used_cloudscraper=False,
            looks_like_antibot=antibot,
        )

    # 2) Фолбэк cloudscraper
    try:
        import cloudscraper  # type: ignore
    except Exception:
        # cloudscraper не установлен — возвращаем то, что есть
        return FetchResult(
            url=url,
            final_url=str(r.url),
            status_code=int(r.status_code),
            content_type=ct,
            text=text,
            used_cloudscraper=False,
            looks_like_antibot=antibot,
        )

    scraper = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "desktop": True}
    )
    r2 = scraper.get(url, headers=DEFAULT_HEADERS, timeout=timeout, allow_redirects=True)
    ct2 = r2.headers.get("Content-Type", "")
    text2 = r2.text or ""
    antibot2 = _looks_like_antibot(text2)

    return FetchResult(
        url=url,
        final_url=str(r2.url),
        status_code=int(r2.status_code),
        content_type=ct2,
        text=text2,
        used_cloudscraper=True,
        looks_like_antibot=antibot2,
    )
