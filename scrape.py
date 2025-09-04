# scrape.py
import csv
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# ---- 設定 ----
URLS_CSV = os.getenv("URLS_CSV", "urls.csv")
OUT_JSON = os.getenv("OUT_JSON", "docs/data.json")
OUT_JSONL = os.getenv("OUT_JSONL", "docs/data.jsonl")
CONCURRENCY = int(os.getenv("CONCURRENCY", "6"))
TIMEOUT = int(os.getenv("TIMEOUT", "20"))
RETRIES = int(os.getenv("RETRIES", "2"))

HEADERS = {
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Safari/537.36",
    "accept": "text/html,application/xhtml+xml",
    "accept-language": "ja,en;q=0.9",
}

# 見出しを含む「最低限のタグ」だけを残すホワイトリスト
ALLOWED_TAGS = {
    "h1", "h2", "h3", "h4",
    "p", "ul", "ol", "li",
    "table", "thead", "tbody", "tr", "th", "td",
    "pre", "code", "strong", "em", "a", "br"
}

def load_urls(path: str):
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} が見つかりません")
    urls = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.reader(f):
            if not row:
                continue
            url = row[0].strip()
            if url:
                urls.append(url)
    return urls

def normalize_text(text: str) -> str:
    if not text:
        return ""
    t = text.replace("\r", "\n").replace("\t", " ").replace("\xa0", " ")
    # 3つ以上の改行 → 2個に圧縮
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()

def fetch_html(url: str) -> str:
    last_err = None
    for i in range(RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_err = e
            time.sleep(0.5 * (i + 1))  # 簡易バックオフ
    raise last_err

def clean_section_keep_headings(sec: BeautifulSoup) -> str:
    """section要素内をクリーンアップし、見出し等の最低限のタグは保持してHTMLとして返す。"""
    # 完全削除対象
    for tag in sec.find_all(["script", "style", "noscript", "iframe"]):
        tag.decompose()

    # <a>以外の属性は基本落とす（安全＆軽量化）
    for tag in sec.find_all(True):
        if tag.name == "a":
            href = tag.get("href")
            tag.attrs = {}
            if href:
                tag["href"] = href
        else:
            # 不要な巨大classやdata属性を削除
            tag.attrs = {}

    # ホワイトリスト外のタグは unwrap（中身だけ残す）
    for tag in list(sec.find_all(True)):
        if tag.name not in ALLOWED_TAGS:
            tag.unwrap()

    html = str(sec)
    html = normalize_text(html)
    return html

def extract_title_and_text(html: str, url: str):
    soup = BeautifulSoup(html, "lxml")

    # 1) 共通UIの除去
    for header in soup.find_all("header"):
        header.decompose()
    for cls in ["ft_custom01", "breadcrumbs", "contents_row"]:
        for div in soup.find_all("div", class_=cls):
            div.decompose()

    # 2) 対象は <section class="content-element"> のみ
    sections = soup.find_all("section", class_="content-element")
    if sections:
        cleaned = [clean_section_keep_headings(sec) for sec in sections]
        text_html = "\n\n".join(cleaned)
    else:
        # フォールバック：本文全体を同様にクリーンして出す
        root = soup.body or soup
        text_html = clean_section_keep_headings(root)

    # 3) タイトル
    title = (soup.title.string if soup.title else "") or ""
    title = title.strip() if title else ""
    if not title:
        h1 = soup.find("h1")
        title = (h1.get_text(strip=True) if h1 else "") or "記事の詳細"

    return {"title": title or "記事の詳細", "url": url, "html": text_html}

def main():
    urls = load_urls(URLS_CSV)
    if not urls:
        print("URLが0件でした。空の配列を書き出します。")
        os.makedirs(os.path.dirname(OUT_JSON), exist_ok=True)
        with open(OUT_JSON, "w", encoding="utf-8") as f:
            json.dump([], f, ensure_ascii=False, indent=2)
        with open(OUT_JSONL, "w", encoding="utf-8") as f:
            pass
        return

    results = [None] * len(urls)

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        futures = {}
        for idx, url in enumerate(urls):
            futures[ex.submit(fetch_html, url)] = (idx, url)

        for fut in tqdm(as_completed(futures), total=len(futures), desc="fetch"):
            idx, url = futures[fut]
            try:
                html = fut.result()
                obj = extract_title_and_text(html, url)
            except Exception as e:
                obj = {"title": "記事の詳細", "url": url, "html": ""}
                print(f"[warn] {url}: {e}")
            results[idx] = obj

    # 出力（順序・重複は入力通り）
    os.makedirs(os.path.dirname(OUT_JSON), exist_ok=True)
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # JSONL も併産（互換用）
    with open(OUT_JSONL, "w", encoding="utf-8") as f:
        for obj in results:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")

    print(f"written: {OUT_JSON}  ({len(results)} items)")
    print(f"written: {OUT_JSONL}")

if __name__ == "__main__":
    main()
