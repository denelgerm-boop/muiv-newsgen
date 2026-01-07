from src.data.scrape_muiv import collect_news_items

if __name__ == "__main__":
    items = collect_news_items(max_pages=2)
    print("items:", len(items))
    for it in items[:5]:
        print(it.date_str, it.title, it.url)
