import os, json, time, re, requests
from typing import List, Dict, Any, Tuple
from fastapi import FastAPI, Query, Header, HTTPException
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel

DATA_URL = os.getenv("DATA_URL")
API_KEY  = os.getenv("API_KEY", "")

app = FastAPI()

# メモリ内の「仮想チャンク」構造
# chunks[i] = {"page_idx": int, "page_url": str, "page_title": str,
#              "subhead": str, "text": str}
pages: List[Dict[str, Any]] = []
chunks: List[Dict[str, Any]] = []
vec, X, last = None, None, 0

# ---- ユーティリティ ----
HEADING_PAT = re.compile(r'^(#{1,6}\s+|[0-9０-９]+\.\s+|[-•・]\s+|【.+?】)', re.MULTILINE)

def split_into_virtual_chunks(title: str, url: str, md: str) -> List[Tuple[str,str]]:
    """
    1ページのMarkdownを「見出し or 空行 or 句点」でざっくり分割して
    （保存形式を変えずに）仮想チャンクを返す。
    戻り値: [(subhead, text), ...]
    """
    # まず大見出しっぽい位置で粗くスプリット
    parts = HEADING_PAT.split(md)
    # splitの結果は ["本文", "見出し記号", "見出し本文", "本文", ...] の交互になりがちなので整形
    blocks: List[str] = []
    buf = ""
    for seg in parts:
        if HEADING_PAT.match(seg or ""):
            if buf.strip():
                blocks.append(buf.strip())
            buf = seg
        else:
            buf += ("\n" + seg) if seg else ""
    if buf.strip():
        blocks.append(buf.strip())

    # さらに大きすぎるブロックは空行や句点で小割り
    out: List[Tuple[str,str]] = []
    for b in blocks:
        subhead = ""
        lines = [ln for ln in b.splitlines() if ln.strip()]
        if not lines:
            continue
        # 先頭行をサブ見出し候補に
        subhead_candidate = lines[0].strip()
        if len(subhead_candidate) <= 60:
            subhead = subhead_candidate.lstrip("#-•・ ").strip()

        text = "\n".join(lines)
        # 2000字超なら句点で小割り
        if len(text) > 2000:
            sentences = re.split(r"(?<=[。．!?])\s*", text)
            cur = ""
            for s in sentences:
                if len(cur) + len(s) > 800:
                    out.append((subhead, cur.strip()))
                    cur = s
                else:
                    cur += s
            if cur.strip():
                out.append((subhead, cur.strip()))
        else:
            out.append((subhead, text))
    # フォールバック
    if not out:
        out = [("", md[:1200])]
    return out[:30]  # 1ページあたり最大30チャンクで十分

def build_corpus_str(page_title: str, subhead: str, text: str) -> str:
    # タイトル＆見出しを先頭に置いてブースト
    head = " ".join([page_title or "", subhead or ""]).strip()
    return (head + "\n" + text).strip()

def refresh():
    global pages, chunks, vec, X, last
    if time.time()-last < 600 and chunks:
        return
    r = requests.get(DATA_URL, timeout=40); r.raise_for_status()
    lines = [l for l in r.text.splitlines() if l.strip()]
    raw = [json.loads(l) for l in lines]

    pages = []
    chunks = []
    for pi, d in enumerate(raw):
        page_title = d.get("title") or d.get("display_title") or d.get("page") or d.get("url")
        page_url   = d.get("display_url") or d.get("url")
        md         = d.get("content_md") or d.get("html") or d.get("content") or ""
        if not (page_url and md.strip()):
            continue
        pages.append({"title": page_title, "url": page_url})
        # ここで “オンメモリ仮想チャンク化”
        for subhead, text in split_into_virtual_chunks(page_title, page_url, md):
            chunks.append({
                "page_idx": pi,
                "page_url": page_url,
                "page_title": page_title,
                "subhead": subhead,
                "text": text
            })

    # 日本語に強い char n-gram
    corpus = [build_corpus_str(c["page_title"], c["subhead"], c["text"]) for c in chunks]
    vec = TfidfVectorizer(analyzer="char_wb", ngram_range=(2,5), max_features=150000).fit(corpus)
    X = vec.transform(corpus)
    last = time.time()

def mmr(query_vec, X, top=12, lambda_div=0.5):
    sims_q = linear_kernel(query_vec, X).ravel()
    selected, cand = [], sims_q.argsort()[::-1].tolist()
    while cand and len(selected) < top:
        if not selected:
            selected.append(cand.pop(0)); continue
        best, best_i = -1e9, None
        for idx, c in enumerate(cand[:200]):
            sim_q = sims_q[c]
            sim_red = max(linear_kernel(X[c], X[j]).ravel()[0] for j in selected)
            score = lambda_div*sim_q - (1-lambda_div)*sim_red
            if score > best:
                best, best_i, best_idx = score, c, idx
        selected.append(best_i); cand.pop(best_idx)
    return selected

@app.get("/search")
def search(q: str = Query(..., description="自然文でOK"),
           top_k: int = 12,
           diversity: float = 0.5,
           authorization: str | None = Header(None),
           key: str | None = Query(None)):
    # 認証（?key= も可：ブラウザ検証用）
    if API_KEY:
        if not (authorization == f"Bearer {API_KEY}" or key == API_KEY):
            raise HTTPException(401, "bad token")
    refresh()
    if not chunks:
        return {"results": []}

    qv = vec.transform([q])
    cand = mmr(qv, X, top=min(max(top_k,1), 20), lambda_div=max(0.0, min(diversity,1.0)))

    results = []
    seen_pages = set()
    for i in cand:
        c = chunks[i]
        # ページ単位での重複を軽く抑制（多様なソースを返す）
        if c["page_url"] in seen_pages and len(results) >= 6:
            continue
        seen_pages.add(c["page_url"])
        # 表示用タイトル：ページタイトル + サブ見出し
        disp_title = c["page_title"]
        if c["subhead"]:
            disp_title = f"{disp_title}｜{c['subhead']}"  # GPTがそのまま表示

        results.append({
            "url": c["page_url"],                  # 実在保証のため #アンカーは付けない
            "title": disp_title,                   # 表示用タイトル（改変禁止）
            "snippet": c["text"][:700]             # 部分抜粋
        })
        if len(results) >= top_k:
            break

    return {"results": results}
