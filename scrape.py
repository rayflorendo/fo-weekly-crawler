# -*- coding: utf-8 -*-
"""
fo-weekly-crawler / scrape.py  (safe-full)
- 1ページ=1行 JSONL を docs/data.jsonl に出力
- schema: "v2-sections"（新フォーマット目印）
- <header> / <nav> / <footer> などを除去（※ .ft_custom01 はデフォルトで残す）
- id/class は保存しない（見出しは人が読むテキストのみ）
- 見出しの本文は「次の同格以上の見出し」までを next_elements で深く収集
- もしセクション抽出で空になったら、ページ全体を丸ごと1セクションにフォールバック
"""

import os
import re
import json
import time
import urllib.parse
from typing import List, Dict, Any, Iterable

import requests
from bs4 import BeautifulSoup, Tag, NavigableString
import html2text

URLS_PATH = "urls.csv"
OUT_DIR = "docs"
OUT_FILE = os.path.join(OUT_DIR, "data.jsonl")
os.makedirs(OUT_DIR, exist_ok=True)

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123 Safari/537.36"
)

# ====== 調整ポイント ======
REMOVE_FT_CUSTOM01 = False  # ← 最初は False。本文が無事取れるのを確認してから True でも可
# =========================

NOISE_LINES = {
    "メインコンテンツへスキップ",
    "検索を展開",
    "読み込み中",
    "ferret One help center",
    "テクニカルサポートへ問い合わせ",
}

TITLE_SUFFIX_PATTERNS = [
    r"\s*\|\s*ferret One help center\s*$",
    r"\s*\|\s*FOガイドブック\s*$",
]

HEADING_TAGS = ("h1", "h2", "h3")


def _clean_title(t: str) -> str:
    if not t:
        return ""
    t = re.sub(r"\s+", " ", t).strip()
    for pat in TITLE_SUFFIX_PATTERNS:
        t = re.sub(pat, "", t, flags=re.I)
    return t


def _html2md(html: str) -> str:
    h = html2text.HTML2Text()
    h.ignore_links = False
    h.images_to_alt = True
    h.body_width = 0
    md = h.handle(html or "")

    lines: List[str] = []
    for line in md.splitlines():
        t = line.strip()
        if not t:
            continue
        if t in NOISE_LINES:
            continue
        lines.append(line.rstrip())
    return "\n".join(lines)


def _remove_globals(soup: BeautifulSoup) -> None:
    # 共通っぽい領域を削除（本文を巻き込まない程度に控えめ）
    for sel in ["header", "nav", "footer", "script", "style"]:
        for n in soup.find_all(sel):
            n.decompose()
    if REMOVE_FT_CUSTOM01:
        for n in soup.select("div.ft_custom01"):
            n.decompose()


def _best_title(soup: BeautifulSoup, html: str, url: str) -> str:
    m = soup.find("meta", property="og:title")
    if m and m.get("content"):
        return _clean_title(m["content"])
    m = soup.find("meta", attrs={"name": "twitter:title"})
    if m and m.get("content"):
        return _clean_title(m["content"])
    h1 = soup.find("h1")
    if h1:
        return _clean_title(h1.get_text(strip=True))
    m = re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.S)
    if m:
        return _clean_title(m.group(1))
    return url


def _canonical_url(soup: BeautifulSoup, url: str) -> str:
    link = soup.find("link", rel=lambda v: v and "canonical" in v.lower())
    if link and link.get("href"):
        return urllib.parse.urljoin(url, link["href"].strip())
    return url


def _heading_level(tag: Tag) -> int:
    # 'h1' -> 1, 'h2' -> 2, ...
    return int(tag.name[1]) if tag and tag.name in HEADING_TAGS else 7


def _collect_until_next_heading(h: Tag) -> str:
    """
    見出し h から次の「同格以上(h1/h2/h3)の見出し」が来るまで、
    next_elements を辿って HTML を収集（深い入れ子も拾う）。
    """
    html_parts: List[str] = []
    cur_level = _heading_level(h)
    it = h.next_elements  # h の直後から DOM を深く辿る

    for el in it:
        if isinstance(el, Tag) and el.name in HEADING_TAGS:
            # 次の見出しに到達。自分と同格以上なら終了
            if _heading_level(el) <= cur_level:
                break
        if isinstance(el, Tag):
            if el.name in ("script", "style", "header", "nav", "footer"):
                continue
            # 見出し自身は除外
            if el.name in HEADING_TAGS:
                continue
            html_parts.append(str(el))
        elif isinstance(el, NavigableString):
            # テキストノードも拾う
            text = str(el).strip()
            if text:
                html_parts.append(text)

    return "".join(html_parts)


def _fetch_html(url: str) -> str:
    r = requests.get(url, timeout=60, headers={"User-Agent": UA})
    r.raise_for_status()
    return r.text


def fetch(url: str) -> Dict[str, Any]:
    html = _fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")
    _remove_globals(soup)

    page_title = _best_title(soup, html, url)
    page_url = _canonical_url(soup, url)

    sections: List[Dict[str, str]] = []
    headings = soup.select(",".join(HEADING_TAGS))

    if not headings:
        # 見出しが無い場合は全文を1セクションに
        sections.append({"heading": page_title, "content": _html2md(str(soup))})
    else:
        for h in headings:
            heading_text = h.get_text(strip=True)
            raw_html = _collect_until_next_heading(h)
            md = _html2md(str(h) + raw_html)
            if md.strip():
                sections.append({"heading": heading_text, "content": md})

    # もし何らかの理由で空になったら、最終フォールバック
    if not sections:
        sections.append({"heading": page_title, "content": _html2md(str(soup))})

    return {
        "schema": "v2-sections",
        "page_title": page_title or page_url,
        "url": page_url,
        "sections": sections,
    }


def main() -> None:
    if not os.path.exists(URLS_PATH):
        raise FileNotFoundError(f"{URLS_PATH} が見つかりません")

    with open(URLS_PATH, encoding="utf-8") as f:
        urls = [u.strip() for u in f if u.strip()]

    with open(OUT_FILE, "w", encoding="utf-8") as out:
        for u in urls:
            print(f"[scrape] {u}", flush=True)
            try:
                rec = fetch(u)
            except Exception as e:
                rec = {
                    "schema": "v2-sections",
                    "url": u,
                    "page_title": "(取得失敗)",
                    "error": str(e),
                    "sections": [],
                }
            out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            time.sleep(1)

    print(f"OK -> {OUT_FILE}", flush=True)


if __name__ == "__main__":
    main()
