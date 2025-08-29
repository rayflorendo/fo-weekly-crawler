# ページのHTMLを「そのまま」Markdown化して保存
import os, json, time, requests, re
import html2text

URLS_PATH = "urls.csv"
OUT_DIR   = "docs"
OUT_FILE  = os.path.join(OUT_DIR, "data.jsonl")
os.makedirs(OUT_DIR, exist_ok=True)

UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36"

def get_title(html: str, fallback: str) -> str:
    m = re.search(r"<title[^>]*>(.*?)</title>", html, re.I|re.S)
    if m: return re.sub(r"\s+", " ", m.group(1)).strip()
    return fallback

def fetch_markdown(url: str) -> dict:
    r = requests.get(url, timeout=60, headers={"User-Agent": UA})
    r.raise_for_status()
    html = r.text
    # HTML → Markdown（要約なし・全文化）
    h = html2text.HTML2Text()
    h.ignore_links = False       # リンク残す
    h.body_width = 0             # 折り返し無効（読みやすい）
    h.images_to_alt = True       # 画像はaltを残す
    md = h.handle(html)
    title = get_title(html, url)
    # 軽い整形（余計な空行を減らす）
    md = "\n".join([line.rstrip() for line in md.splitlines() if line.strip() != ""])
    return {"url": url, "title": title, "content": md}

def main():
    urls = [u.strip() for u in open(URLS_PATH, encoding="utf-8") if u.strip()]
    rows = []
    for u in urls:
        try:
            print("fetch:", u, flush=True)
            rows.append(fetch_markdown(u))
            time.sleep(1)  # 優しめレート
        except Exception as e:
            rows.append({"url": u, "title": "(取得失敗)", "content": str(e)})
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"OK: {len(rows)} pages -> {OUT_FILE}", flush=True)

if __name__ == "__main__":
    main()
