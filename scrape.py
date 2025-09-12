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
CONCURRENCY = int(os.getenv("CONCURRENCY", "6"))
TIMEOUT = int(os.getenv("TIMEOUT", "20"))
RETRIES = int(os.getenv("RETRIES", "2"))

HEADERS = {
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Safari/537.36",
    "accept": "text/html,application/xhtml+xml",
    "accept-language": "ja,en;q=0.9",
}

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
            time.sleep(0.5 * (i + 1))
    raise last_err


def clean_section_keep_headings(sec: BeautifulSoup) -> str:
    """section要素内をクリーンアップし、見出し等の最低限のタグは保持してHTMLとして返す。"""

    # 完全削除対象
    for tag in sec.find_all(["script", "style", "noscript", "iframe"]):
        tag.decompose()

    # <a>以外の属性は基本落とす
    for tag in sec.find_all(True):
        if tag.name == "a":
            href = tag.get("href")
            tag.attrs = {}
            if href:
                tag["href"] = href
        else:
            tag.attrs = {}

    # ✅ <code> タグは常に残す
    for tag in list(sec.find_all(True)):
        if tag.name not in ALLOWED_TAGS:
            if tag.name == "code":
                continue  # 常に残す
            tag.unwrap()

    html = str(sec)
    return normalize_text(html)


def extract_title_and_text(html: str, url: str):
    soup = BeautifulSoup(html, "lxml")

    # 共通UI除去
    for header in soup.find_all("header"):
        header.decompose()
    for cls in ["ft_custom01", "breadcrumbs", "contents_row"]:
        for div in soup.find_all("div", class_=cls):
            div.decompose()

    # メインセクション抽出
    sections = soup.find_all("section", class_="content-element")
    if sections:
        cleaned = [clean_section_keep_headings(sec) for sec in sections]
        text_html = "\n\n".join(cleaned)
    else:
        root = soup.body or soup
        text_html = clean_section_keep_headings(root)

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
        os.makedirs("docs", exist_ok=True)
        with open("docs/fo-manual.json", "w", encoding="utf-8") as f:
            json.dump([], f, ensure_ascii=False, indent=2)
        with open("docs/js-part.json", "w", encoding="utf-8") as f:
            json.dump([], f, ensure_ascii=False, indent=2)
        return

    results_fo = []
    results_js = []

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        futures = {ex.submit(fetch_html, url): url for url in urls}

        for fut in tqdm(as_completed(futures), total=len(futures), desc="fetch"):
            url = futures[fut]
            try:
                html = fut.result()
                obj = extract_title_and_text(html, url)
            except Exception as e:
                obj = {"title": "記事の詳細", "url": url, "html": ""}
                print(f"[warn] {url}: {e}")

            if "fo-guidebook.hmup.jp" in url:
                results_fo.append(obj)
            elif "js-part.hmup.jp" in url:
                results_js.append(obj)
            else:
                print(f"[warn] 未分類のURL: {url}")

    os.makedirs("docs", exist_ok=True)

    with open("docs/fo-manual.json", "w", encoding="utf-8") as f:
        json.dump(results_fo, f, ensure_ascii=False, indent=2)

    with open("docs/js-part.json", "w", encoding="utf-8") as f:
        json.dump(results_js, f, ensure_ascii=False, indent=2)

    print(f"✅ written: docs/fo-manual.json  ({len(results_fo)} items)")
    print(f"✅ written: docs/js-part.json    ({len(results_js)} items)")


if __name__ == "__main__":
    main()
