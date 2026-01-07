"""
Автор: Андуганов Д.Г.
Тема: Автоматическая генерация новостных сообщений из плана мероприятий организации с помощью нейронных сетей.

sitemap_muiv.py — получение ссылок на новости через robots.txt и sitemap.
Устойчиво к антибот-заглушкам (HTML вместо XML).
"""

from __future__ import annotations

from typing import List, Set
import re

from lxml import etree

from src.data.http_client import fetch_html

BASE = "https://www.muiv.ru"
ROBOTS_URL = f"{BASE}/robots.txt"
NEWS_PREFIX = f"{BASE}/about/news/"


def _is_xml_like(text: str) -> bool:
    t = (text or "").lstrip()
    return t.startswith("<?xml") or t.startswith("<urlset") or t.startswith("<sitemapindex")


def _parse_sitemap_xml(xml_text: str) -> tuple[str, List[str]]:
    root = etree.fromstring(xml_text.encode("utf-8", errors="ignore"))
    tag = root.tag.split("}")[-1].lower()

    locs: List[str] = []
    for el in root.findall(".//{*}loc"):
        if el.text:
            locs.append(el.text.strip())

    return tag, locs


def _is_news_url(url: str) -> bool:
    if not url.startswith(NEWS_PREFIX):
        return False
    if url.rstrip("/") == NEWS_PREFIX.rstrip("/"):
        return False
    return True


def _extract_sitemaps_from_robots(robots_text: str) -> List[str]:
    sitemaps: List[str] = []
    for line in (robots_text or "").splitlines():
        line = line.strip()
        if line.lower().startswith("sitemap:"):
            sm = line.split(":", 1)[1].strip()
            if sm:
                sitemaps.append(sm)
    return sitemaps


def discover_sitemaps() -> List[str]:
    """
    Берём robots.txt и достаём из него ссылки Sitemap:
    """
    res = fetch_html(ROBOTS_URL, timeout=60, use_cloudscraper_if_needed=True)
    text = res.text or ""
    sitemaps = _extract_sitemaps_from_robots(text)

    # Фолбэк: иногда robots пустой/режется — пробуем стандартный путь
    if not sitemaps:
        sitemaps = [f"{BASE}/sitemap.xml"]

    # дедуп
    seen = []
    for s in sitemaps:
        if s not in seen:
            seen.append(s)
    return seen


def collect_news_urls_from_sitemap(max_urls: int = 500) -> List[str]:
    """
    1) ищем sitemap через robots.txt
    2) обходим sitemapindex/urlset
    3) собираем ссылки на /about/news/...
    """
    sitemap_candidates = discover_sitemaps()

    urls: Set[str] = set()

    for sm_url in sitemap_candidates:
        sm = fetch_html(sm_url, timeout=60, use_cloudscraper_if_needed=True)

        # Если пришёл не XML — пропускаем
        if sm.status_code != 200 or not sm.text or (not _is_xml_like(sm.text)):
            continue

        try:
            tag, locs = _parse_sitemap_xml(sm.text)
        except Exception:
            continue

        if tag == "urlset":
            for u in locs:
                if _is_news_url(u):
                    urls.add(u)
                    if len(urls) >= max_urls:
                        return sorted(urls)[:max_urls]

        elif tag == "sitemapindex":
            # обходим вложенные sitemap
            for child_url in locs:
                child = fetch_html(child_url, timeout=60, use_cloudscraper_if_needed=True, sleep_seconds=0.2)

                if child.status_code != 200 or not child.text or (not _is_xml_like(child.text)):
                    continue

                try:
                    ctag, clocs = _parse_sitemap_xml(child.text)
                except Exception:
                    continue

                if ctag == "urlset":
                    for u in clocs:
                        if _is_news_url(u):
                            urls.add(u)
                            if len(urls) >= max_urls:
                                return sorted(urls)[:max_urls]

    return sorted(urls)[:max_urls]
