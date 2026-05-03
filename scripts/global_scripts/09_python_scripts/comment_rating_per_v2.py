# comment_per.py
import openai

# comment2duck_raw.py
import re, time, openai, duckdb, pandas as pd
from typing import List, Dict, Tuple

DB_PATH, TABLE_NAME = "amazon.duckdb", "comment_score"

# ──────────────────────────────────────────────────────────────
def rate_comment(title: str, body: str, product_line_name: str,
                 propertyname: str, propertytype: str,
                 gpt_key: str, model: str = "o4-mini"
) -> Tuple[int | None, str, str]:
    """
    使用 GPT 模型對單筆評論進行評分。

    參數:
      title: 評論標題
      body: 評論內容
      product_line_name: 生產線名稱
      propertyname: 要評分的屬性名稱
      propertytype: 評分類別，例如 "屬性" 或 "品牌個性"
      gpt_key: GPT API 金鑰
      model: 使用的 GPT 模型，預設為 "gpt-4o-mini"

    回傳:
      GPT 回應的評分結果字串，格式預期為 "[分數,理由]"，
      若發生錯誤則回傳錯誤訊息。
     (score, reason, raw_resp)
    """
    client = openai.OpenAI(api_key=gpt_key)

    # ↓↓↓ 你的訊息文字：完全保留
    message_text = f"""
The following is a comment on a {product_line_name} product:
Title: {title}
Body: {body}
Evaluate the comment regarding the product's '{propertyname}', which is categorized as a {propertytype} feature.
Use the following rules to respond:
1. If the comment does not demonstrate the stated characteristic in any way, reply exactly [NaN,NaN] without any additional reasoning or explanation.
2. Otherwise, rate your agreement with the statement on a scale from 1 to 5:
- ‘5’ for Strongly Agree
- ‘4’ for Agree
- ‘3’ for Neither Agree nor Disagree
- ‘2’ for Disagree
- ‘1’ for Strongly Disagree
Provide your rationale in the format: [Score, Reason].
** Please double-check that if the comment does not demonstrate the stated characteristic in any way, your reply is exactly [NaN,NaN] with no extra explanation.
"""

    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "Forget any previous information."},
                {"role": "user",   "content": message_text}
            ]
        )
        raw = resp.choices[0].message.content.strip()

        if raw == "[NaN,NaN]":
            return None, "", raw
        m = re.match(r"\[\s*(\d)\s*,\s*(.+?)\s*\]$", raw)
        if not m:
            raise ValueError(f"Unexpected format: {raw}")
        return int(m.group(1)), m.group(2), raw
    except Exception as e:
        print("GPT error:", e)
        return None, "", f"Error: {e}"

# ──────────────────────────────────────────────────────────────
def init_db(db_path: str = "amazon.duckdb",
            table_name: str = "comment_score",
            overwrite: bool = False):
    import os, pathlib
    if overwrite and pathlib.Path(db_path).exists():
        os.remove(db_path)

    con = duckdb.connect(db_path)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
          id BIGINT GENERATED ALWAYS AS IDENTITY,
          title     TEXT,
          body      TEXT,
          property  TEXT,
          score     INTEGER,
          reason    TEXT,
          raw_resp  TEXT,
          scored_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    return con

def write_rows(con: duckdb.DuckDBPyConnection,
               rows: List[Dict],
               table_name: str = "comment_score"):
    if not rows:
        return
    df = pd.DataFrame(rows)
    con.register("tmp", df)
    con.execute(f"""
        INSERT INTO {table_name}
        SELECT title, body, property, score, reason, raw_resp FROM tmp
    """)
    con.unregister("tmp")

# ──────────────────────────────────────────────────────────────
def main(db_path: str = "amazon.duckdb",
         table_name: str = "comment_score",
         overwrite: bool = False):
    GPT_KEY = "sk-..."   # ← 填入你的 key
    PRODUCT, PROP_NAME, PROP_TYPE = "Sympt-X", "balance", "brand personality"

    comments = [
        {"title": "Love it",   "body": "Finally broke down and bought…"},
        {"title": "Too salty", "body": "I can’t take it every day…"},
    ]

    con  = init_db(db_path, table_name, overwrite)
    rows = []

    for c in comments:
        score, reason, raw = rate_comment(
            c["title"], c["body"],
            PRODUCT, PROP_NAME, PROP_TYPE,
            gpt_key=GPT_KEY
        )
        rows.append({
            "title": c["title"], "body": c["body"],
            "property": PROP_NAME,
            "score": score, "reason": reason,
            "raw_resp": raw
        })
        time.sleep(1.2)

    write_rows(con, rows, table_name)
    print(con.execute(f"SELECT id,title,score FROM {table_name}").fetch_df())
    con.close()

# ───────────── CLI 入口：可帶參數 ─────────────
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="GPT rate & save to DuckDB")
    ap.add_argument("--db_path",    default="amazon.duckdb")
    ap.add_argument("--table_name", default="comment_score")
    ap.add_argument("--overwrite",  action="store_true",
                    help="Delete existing DB before run")
    args = ap.parse_args()

    main(db_path=args.db_path,
         table_name=args.table_name,
         overwrite=args.overwrite)
