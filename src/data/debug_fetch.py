"""
Автор: Андуганов Д.Г.
Тема: Автоматическая генерация новостных сообщений из плана мероприятий организации с помощью нейронных сетей.

debug_fetch.py — диагностика загрузки /about/news/.
"""

from __future__ import annotations

from pathlib import Path

from src.data.http_client import fetch_html


def main() -> None:
    url = "https://www.muiv.ru/about/news/"
    res = fetch_html(url, timeout=40, use_cloudscraper_if_needed=True)

    cache_dir = Path("data/cache")
    cache_dir.mkdir(parents=True, exist_ok=True)
    out = cache_dir / "news_index.html"
    out.write_text(res.text, encoding="utf-8")

    print(f"status: {res.status_code}")
    print(f"final_url: {res.final_url}")
    print(f"content_type: {res.content_type}")
    print(f"used_cloudscraper: {res.used_cloudscraper}")
    print(f"looks_like_antibot: {res.looks_like_antibot}")
    print(f"saved: {out.resolve()}")
    print("head(250):")
    print(res.text[:250])


if __name__ == "__main__":
    main()
