import os, json, requests, time
import html2text

URLS_PATH = "urls.csv"
OUT_DIR = "docs"
os.makedirs(OUT_DIR, exist_ok=True)
OUT_FILE = os.path.join(OUT_DIR, "data.jsonl")

def fetch_all_html(url: str) -> dict:
    r = requests.get(url, timeout=40, headers={"User-Agent":"Mozilla/5.0"})
    r.raise_for_status()
    html = r.text
    # HTMLをまるごとMarkdownに変換（見出し/リスト/リンク等も保持）
    h = html2text.HTML2Text()
    h.ignore_links = False
    h.body_width = 0
    md = h.handle(html)
    return {"url": url, "title": url, "content": md}

def main():
    urls = [u.strip() for u in open(URLS_PATH, encoding="utf-8") if u.strip()]
    rows = []
    for url in urls:
        print("fetch:", url)
        rows.append(fetch_all_html(url))
        time.sleep(1)  # 優しく
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print("OK:", len(rows), "pages")

if __name__ == "__main__":
    main()
