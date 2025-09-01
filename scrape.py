# -*- coding: utf-8 -*-
import os, json, time, re, urllib.parse, requests
from bs4 import BeautifulSoup
import html2text

URLS_PATH = "urls.csv"
OUT_DIR   = "docs"
OUT_FILE  = os.path.join(OUT_DIR, "data.jsonl")
os.makedirs(OUT_DIR, exist_ok=True)

UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36"

NOISE_LINES = {
    "メインコンテンツへスキップ",
    "検索を展開", "読み込み中",
    "ferret One help center",
    "テクニカルサポートへ問い合わせ",
}

TITLE_SUFFIX_PATTERNS = [
    r"\s*\|\s*ferret One help center\s*$",
    r"\s*\|\s*FOガイドブック\s*$",
]

def clean_title(t: str) -> str:
    if not t: return ""
    t = re.sub(r"\s+", " ", t).strip()
    for pat in TITLE_SUFFIX_PATTERNS:
        t = re.sub(pat, "", t, flags=re.I)
    return t

def html_to_md(html: str) -> str:
    h = html2text.HTML2Text()
    h.ignore_links = False
    h.images_to_alt = True
    h.body_width = 0
    md = h.handle(html or "")
    lines = []
    for line in md.splitlines():
        t = line.strip()
        if not t: continue
        if t in NOISE_LINES: continue
        lines.append(line.rstrip())
    return "\n".join(lines)

def remove_globals(soup: BeautifulSoup) -> None:
    for n in soup.find_all("header"):
        n.decompose()
    for n in soup.select("div.ft_custom01"):
        n.decompose()

def best_title(soup, html, url) -> str:
    m = soup.find("meta", property="og:title")
    if m and m.get("content"): return clean_title(m["content"])
    m = soup.find("meta", attrs={"name":"twitter:title"})
    if m and m.get("content"): return clean_title(m["content"])
    h1 = soup.find("h1")
    if h1: return clean_title(h1.get_text(strip=True))
    m = re.search(r"<title[^>]*>(.*?)</title>", html, re.I|re.S)
    if m: return clean_title(m.group(1))
    return url

def canonical_url(soup, url) -> str:
    link = soup.find("link", rel=lambda v: v and "canonical" in v.lower())
    if link and link.get("href"):
        return urllib.parse.urljoin(url, link["href"].strip())
    return url

def fetch(url: str) -> dict:
    r = requests.get(url, timeout=60, headers={"User-Agent": UA})
    r.raise_for_status()
    html = r.text
    soup = BeautifulSoup(html, "html.parser")
    remove_globals(soup)

    page_title = best_title(soup, html, url)
    page_url   = canonical_url(soup, url)

    # 見出しでセクション分解（h1/h2/h3）
    headings = soup.select("h1, h2, h3")
    sections = []

    if not headings:
        sections.append({"heading": page_title, "content": html_to_md(str(soup))})
    else:
        # 見出しノードごとに「次の見出し直前まで」を本文として抽出
        for i, h in enumerate(headings):
            content_nodes = []
            node = h.next_sibling
            while node and not (getattr(node, "name", None) in ["h1","h2","h3"]):
                content_nodes.append(str(node))
                node = node.next_sibling
            sec_html = str(h) + "".join(content_nodes)
            md = html_to_md(sec_html)
            if md.strip():
                sections.append({
                    "heading": h.get_text(strip=True),
                    "content": md
                })

    return {
        "page_title": page_title or page_url,
        "url": page_url,
        "sections": sections
    }

def main():
    urls = [u.strip() for u in open(URLS_PATH, encoding="utf-8") if u.strip()]
    with open(OUT_FILE, "w", encoding="utf-8") as out:
        for u in urls:
            print("fetch:", u, flush=True)
            try:
                rec = fetch(u)
            except Exception as e:
                rec = {"url": u, "error": str(e), "page_title": "(取得失敗)", "sections": []}
            out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            time.sleep(1)
    print("OK ->", OUT_FILE, flush=True)

if __name__ == "__main__":
    main()
