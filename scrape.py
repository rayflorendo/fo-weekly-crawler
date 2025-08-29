# URLを読み、本文だけ取り出して data.jsonl を作る
import csv, json, time, requests, os
from readability import Document
import html2text

os.makedirs("docs", exist_ok=True)
OUT = "docs/data.jsonl"

def fetch(url):
    res = requests.get(url, timeout=30, headers={"User-Agent":"Mozilla/5.0"})
    res.raise_for_status()
    doc = Document(res.text)
    title = doc.short_title() or url
    md = html2text.HTML2Text(); md.ignore_links=False; md.body_width=0
    content = md.handle(doc.summary())
    content = "\n".join([l.rstrip() for l in content.splitlines() if l.strip()])
    return {"url":url, "title":title, "content":content[:200000]}

rows=[]
for url in [u.strip() for u in open("urls.csv", encoding="utf-8") if u.strip()]:
    try:
        rows.append(fetch(url)); time.sleep(1)
    except Exception as e:
        rows.append({"url":url,"title":"(取得失敗)","content":str(e)})

with open(OUT,"w",encoding="utf-8") as f:
    for r in rows: f.write(json.dumps(r, ensure_ascii=False)+"\n")

print(f"OK: {len(rows)} pages -> {OUT}")
