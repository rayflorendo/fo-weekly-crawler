# -*- coding: utf-8 -*-
"""
fo-weekly-crawler / scrape.py
- 1ページ=1行の JSONL（data.jsonl）を生成
- schema: "v2-sections" を付与（新フォーマットの目印）
- header／.ft_custom01 を除去
- id/class は保存せず、人間が読む「見出しテキスト」だけを sections[].heading に入れる
- 追加のノイズ行も削除（"メインコンテンツへスキップ" 等）
- タイトルは og:title > twitter:title > h1 > <title> > URL の優先順
- URL は <link rel="canonical"> があればそれを採用
"""

import os
import re
import json
import time
import urllib.parse
from typing import List, Dict, Any

import requests
from bs4 import BeautifulSoup
import html2text

URLS_PATH = "urls.csv"
OUT_DIR = "docs"
OUT_FILE = os.path.join(OUT_DIR, "data.jsonl")
os.makedirs(OUT_DIR, exist_ok=True)

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123 Safari/537.36"
)

# サイト共通ノイズ（この行は捨てる）
NOISE_LINES = {
    "メインコンテンツへスキップ",
    "検索を展開",
    "読み込み中",
    "ferret One help center",
    "テクニカルサポートへ問い合わせ",
}

# ページタイトル末尾のサイト名などを削る
TITLE_SUFFIX_PATTERNS = [
    r"\s*\|\s*ferret One help center\s*$",
    r"\s*\|\s*FOガイドブック\s*$",
]


# ---------------------------
#          Utils
# ---------------------------

def _clean_title(t: str) -> str:
    if not t:
        return ""
    t = re.sub(r"\s+", " ", t).strip()
    for pat in TITLE_SUFFIX_PATTERNS:
        t = re.sub(pat, "", t, flags=re.I)
    return t


def _html2md(html: str) -> str:
    """HTML→Markdown（ノイズ行削除）"""
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
    """共通ヘッダー等を削除"""
    for n in soup.find_all("header"):
        n.decompose()
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
        href = link["href"].strip()
        return urllib.parse.urljoin(url, href)
    return url


def _fetch_html(url: str) -> str:
    """単純GET（必要ならここにリトライを追加）"""
    r = requests.get(url, timeout=60, headers={"User-Agent": UA})
    r.raise_for_status()
    # 一部サイトで charset が meta 指定のみの場合もあるが、requests が推測してくれる
    return r.text


# ---------------------------
#        Core logic
# ---------------------------

def fetch(url: str) -> Dict[str, Any]:
    html = _fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")
    _remove_globals(soup)

    page_title = _best_title(soup, html, url)
    page_url = _canonical_url(soup, url)

    # h1/h2/h3 でセクション分割（見出しテキストだけ採用）
    headings = soup.select("h1, h2, h3")
    sections: List[Dict[str, str]] = []

    if not headings:
        # 見出しが無いページは全体を1セクションに
        sections.append({"heading": page_title, "content": _html2md(str(soup))})
    else:
        for h in headings:
            heading_text = h.get_text(strip=True)  # ← id/class は使わない
            contents: List[str] = []
            node = h.next_sibling
            # 次の見出しまでを本文として集める
            while node and not (getattr(node, "name", None) in ["h1", "h2", "h3"]):
                contents.append(str(node))
                node = node.next_sibling
            sec_html = str(h) + "".join(contents)
            md = _html2md(sec_html)
            if md.strip():
                sections.append({"heading": heading_text, "content": md})

    return {
        "schema": "v2-sections",             # ★ 新フォーマットの目印
        "page_title": page_title or page_url,
        "url": page_url,
        "sections": sections,
    }


def main() -> None:
    # URLリストの読み込み
    if not os.path.exists(URLS_PATH):
        raise FileNotFoundError(f"{URLS_PATH} が見つかりません")
    with open(URLS_PATH, encoding="utf-8") as f:
        urls = [u.strip() for u in f if u.strip()]

    # 生成
    with open(OUT_FILE, "w", encoding="utf-8") as out:
        for u in urls:
            print(f"[scrape] {u}", flush=True)
            try:
                rec = fetch(u)
            except Exception as e:
                # 失敗しても1行は出す（何が落ちたか後で追える）
                rec = {
                    "schema": "v2-sections",
                    "url": u,
                    "page_title": "(取得失敗)",
                    "error": str(e),
                    "sections": [],
                }
            out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            time.sleep(1)  # 過負荷回避

    print(f"OK -> {OUT_FILE}", flush=True)


if __name__ == "__main__":
    main()
