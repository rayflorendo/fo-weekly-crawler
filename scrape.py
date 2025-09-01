# -*- coding: utf-8 -*-
import os, json, time, re, urllib.parse, requests
from bs4 import BeautifulSoup
import html2text
from datetime import datetime, timezone

URLS_PATH = "urls.csv"
OUT_DIR   = "docs"
OUT_FILE  = os.path.join(OUT_DIR, "data.jsonl")
os.makedirs(OUT_DIR, exist_ok=True)

UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36"

# ブロックしたい定型ノイズ
NOISE_LINES = {
    "メインコンテンツへスキップ",
    "検索を展開",
    "読み込み中",
    "ferret One help center",
    "テクニカルサポートへ問い合わせ",
}

def html_to_md(html: str) -> str:
    h = html2text.HTML2Text()
    h.ignore_links = False
    h.images_to_alt = True
    h.body_width = 0
    md = h.handle(html or "")
    # 余計な空行削減＆ノイズ行除去（内容は削らない）
    lines = []
    for line in md.splitlines():
        t = line.strip()
        if not t: 
            continue
        if t in NOISE_LINES:
            continue
        lines.append(line.rstrip())
    return "\n".join(lines)

def remove_globals(soup: BeautifulSoup) -> None:
    # <header> 全除去
    for node in soup.find_all("header"):
        node.decompose()
    # .ft_custom01 を含む div を除去
    for node in soup.select("div.ft_custom01"):
        node.decompose()

def guess_section_and_page(url: str):
    p = urllib.parse.urlparse(url)
    parts = [s for s in p.path.split("/") if s]
    section = parts[0] if parts else ""
    page = parts[-1] if parts else ""
    return section, page

def fetch_html(url: str) -> str:
    r = requests.get(url, timeout=60, headers={"User-Agent": UA})
    r.raise_for_status()
    return r.text

def extract_chunks(url: str, html: str):
    """H1/H2/H3単位で分割したチャンクをyield"""
    soup = BeautifulSoup(html, "html.parser")
    remove_globals(soup)

    # ページタイトル/見出し
    h1 = (soup.find("h1").get_text(strip=True) if soup.find("h1") else "")
    title = h1
    if not title:
        m = re.search(r"<title[^>]*>(.*?)</title>", html, re.I|re.S)
        if m: title = re.sub(r"\s+", " ", m.group(1)).strip()

    # 見出しのノード列を抽出（H1→H2→H3…）
    headings = soup.select("h1, h2, h3")
    if not headings:
        # 見出しが無い場合はページ全体を1チャンクに
        md = html_to_md(str(soup))
        yield {"url": url, "page_url": url, "title": title or url, "h_path":[title] if title else [],
               "chunk": md, "anchors": [], "content_len": len(md)}
        return

    # セクションをHタグの「次の見出し直前まで」で区切る
    def anchor_id(tag):
        # id があればそれ
        if tag.has_attr("id"): return tag["id"]
        # テキストから疑似ID
        t = re.sub(r"\s+", "-", tag.get_text(strip=True))
        return "h-" + re.sub(r"[^0-9A-Za-z\-ぁ-んァ-ヶ一-龠]", "", t)[:40]

    for i, h in enumerate(headings):
        level = int(h.name[1])
        # セクション範囲
        contents = []
        node = h.next_sibling
        while node and not (getattr(node, "name", None) in ["h1","h2","h3"]):
            contents.append(str(node))
            node = node.next_sibling

        # サブ見出しのパス(H1→H2→H3)
        # 直前の上位見出しをたどる
        h_text = h.get_text(strip=True)
        path = [h_text]
        # 上へ遡ってH1を拾う
        prev = h
        while prev and prev.previous_sibling:
            prev = prev.previous_sibling
        # BeautifulSoupで厳密な階層追跡は難しいので、H1をページ代表として先頭に
        if h.name != "h1" and h1:
            path = [h1, h_text]

        # チャンクMarkdown
        sec_html = str(h) + "".join(contents)
        md = html_to_md(sec_html)
        if not md.strip():
            continue

        # アンカーURL
        aid = anchor_id(h)
        page_url = url
        anchor_url = url.rstrip("/") + "#" + aid

        yield {
            "url": anchor_url,
            "page_url": page_url,
            "title": title or h1 or page_url,
            "h_path": path,
            "chunk": md,
            "anchors": list({h_text}),
            "content_len": len(md),
        }

def main():
    urls = [u.strip() for u in open(URLS_PATH, encoding="utf-8") if u.strip()]
    section, page = "", ""
    with open(OUT_FILE, "w", encoding="utf-8") as out:
        for src in urls:
            print("fetch:", src, flush=True)
            try:
                html = fetch_html(src)
                section, page = guess_section_and_page(src)
                for rec in extract_chunks(src, html):
                    rec["section"], rec["page"] = section, page
                    rec["updated_at"] = datetime.now(timezone.utc).isoformat()
                    out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            except Exception as e:
                out.write(json.dumps({"url": src, "error": str(e) }, ensure_ascii=False) + "\n")
            time.sleep(1)
    print("OK ->", OUT_FILE, flush=True)

if __name__ == "__main__":
    main()
