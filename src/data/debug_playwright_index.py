from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

URL = "https://www.muiv.ru/about/news/"

def fetch(url: str) -> str:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(url, wait_until="networkidle", timeout=60000)
        html = page.content()
        browser.close()
        return html

def looks_like_antibot(html: str) -> bool:
    h = (html or "").lower()
    return ("gorizontal-vertikal" in h) or ("data:image/gif;base64" in h) or ("noindex, noarchive" in h)

if __name__ == "__main__":
    html = fetch(URL)
    print("antibot:", looks_like_antibot(html))
    soup = BeautifulSoup(html, "lxml")
    links = set()
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if href.startswith("/about/news/") and href not in ("/about/news/", "/about/news"):
            links.add("https://www.muiv.ru" + href)
    print("links:", len(links))
    for u in sorted(links)[:10]:
        print(u)
