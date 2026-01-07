from src.data.sitemap_muiv import discover_sitemaps, collect_news_urls_from_sitemap

if __name__ == "__main__":
    sitemaps = discover_sitemaps()
    print("sitemaps:")
    for s in sitemaps:
        print(" -", s)

    urls = collect_news_urls_from_sitemap(max_urls=50)
    print("\nnews_urls:", len(urls))
    for u in urls[:10]:
        print(u)
