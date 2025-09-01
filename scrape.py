# -*- coding: utf-8 -*-
# 指定URLを取得し、header と .ft_custom01 を除いた“ほぼ生テキスト”を Markdown に変換
# 出力: docs/data.jsonl（1行1レコード）
# フィールド: url, section, page, title, h1, headings, content_md

import os, json, time, re, urllib.parse, requests
from bs4 import BeautifulSoup
import html2text

URLS_PATH = "urls.csv"
OUT_DIR   = "docs"
OUT_FILE  = os.path.join(OUT_DIR, "data.jsonl")
os.makedirs(OUT_DIR, exist_ok=True)

UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36"

def to_md(html: str) -> str:
    h = html2text.HTML2Text()
    h.ignore_links = False
    h.images_to_alt = True
    h.body_width = 0  # 改行で折り返さない
    md = h.handle(html or "")
    # 余計な空行を軽く整える（内容は削らない）
    return "\n".join([line.rstrip() for line in md.splitlines() if line.strip() != ""])

def get_title_from_html(html: str) -> str:
    m = re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.S)
    if m:
        return re.sub(r"\s+", " ", m.group(1)).strip()
    return ""

def guess_section_and_page(url: str) -> tuple[str, str]:
    p = urllib.parse.urlparse(url)
    parts = [s for s in p.path.split("/") if s]
    section = parts[0] if parts else ""
    page = parts[-1] if parts else ""
    return section, page

def remove_globals(soup: BeautifulSoup) -> None:
    # 1) すべての <header> を除外
    for node in soup.find_all("header"):
        node.decompose()
    # 2) クラスに ft_custom01 を含む div を除外（複数クラス対応）
    def has_ft_custom01(cls):
        if not cls: return False
        if isinstance(cls, str): return "ft_custom01" in cls.split()
        # class_ はlistになることもある
        return "ft_custom01" in cls
    for node in soup.find_all("div", class_=has_ft_custom01):
        node.decompose()

def extract_meta(soup: BeautifulSoup, html: str) -> tuple[str, str, list[str]]:
    # h1
    h1 = ""
    h1_tag = soup.find("h1")
    if h1_tag:
        h1 = h1_tag.get_text(strip=True)
    # h2/h3 見出し
    headings = [t.get_text(strip=True) for t in soup.select("h2, h3")]
    # title
    title = h1 or get_title_from_html(html)
    return title, h1, headings

def fetch_record(url: str) -> dict:
    r = requests.get(url, timeout=60, headers={"User-Agent": UA})
    r.raise_for_status()
    html = r.text

    soup = BeautifulSoup(html, "html.parser")
    remove_globals(soup)  # header と .ft_custom01 を除外
    title, h1, headings = extract_meta(soup, html)

    # 省略・要約なしで全体（＝除外後のDOM全体）をMarkdown化
    content_md = to_md(str(soup))

    section, page = guess_section_and_page(url)

    return {
        "url": url,
        "section": section,
        "page": page,
        "title": title or url,
        "h1": h1,
        "headings": headings,
        "content_md": content_md,  # ← ChatGPTが読む本文（要約なし）
    }

def main():
    urls = [u.strip() for u in open(URLS_PATH, encoding="utf-8") if u.strip()]
    with open(OUT_FILE, "w", encoding="utf-8") as out:
        for u in urls:
            print("fetch:", u, flush=True)
            try:
                rec = fetch_record(u)
            except Exception as e:
                rec = {"url": u, "error": str(e)}
            out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            time.sleep(1)  # サーバに優しく
    print(f"OK: {len(urls)} pages -> {OUT_FILE}", flush=True)

if __name__ == "__main__":
    main()
