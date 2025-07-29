import sqlite3
import os
from typing import List, Dict, Any, Optional
from tabulate import tabulate
import re

def get_db_path(base_dir: str, db_id: str) -> str:
    """
    base_dir 이하의
    dataset/bird_benchmark/dev_databases/{db_id}/{db_id}.sqlite
    경로를 리턴합니다.
    """
    path = os.path.join(
        base_dir,
        "dataset",
        "bird_benchmark",
        "dev_databases",
        db_id,
        f"{db_id}.sqlite"
    )
    if not os.path.exists(path):
        raise FileNotFoundError(f"DB 파일이 없습니다: {path}")
    return path

def execute_query(
    db_path: str,
    query: str,
    params: Optional[tuple] = None
) -> List[Dict[str, Any]]:
    """
    SQLite에 접속해 query를 실행하고,
    결과를 [{col: val, …}, …] 형태로 반환합니다.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    try:
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)
        rows = cur.fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()

def format_results_for_llm(
    results: List[Dict[str, Any]],
    sort_keys: Optional[List[str]] = None,
    row_limit: Optional[int] = 10
) -> str:
    """
    1) sort_keys로 정렬  
    2) row_limit로 truncate  
    3) Markdown 테이블로 포매팅해서 반환  
    (LLM에 넣기 좋은 형태)
    """
    if not results:
        return "```\n(결과 없음)\n```"

    # --- 1) 정렬
    if sort_keys:
        try:
            results = sorted(
                results,
                key=lambda r: tuple(r.get(k) if r.get(k) is not None else "" for k in sort_keys)
            )
        except Exception as e:
            print(f"⚠️ 정렬 중 오류 (무시됨): {e}")


    total = len(results)

    # --- 2) 자르기
    if row_limit is not None and total > row_limit:
        truncated = True
        results = results[:row_limit]
    else:
        truncated = False

    # --- 3) Markdown 테이블 생성
    headers = list(results[0].keys())
    sep = ["---"] * len(headers)

    lines = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(sep)     + " |")
    for row in results:
        lines.append(
            "| " + " | ".join(str(row.get(h, "")) for h in headers) + " |"
        )

    if truncated:
        lines.append(f"\n> (총 {total}개 중 앞 {row_limit}개만 표시)")

    return "```\n" + "\n".join(lines) + "\n```"

# def print_markdown_table(md_str: str, title: str = ""):
#     """
#     Markdown 코드 블럭(```) 형식으로 된 테이블을 파싱해
#     터미널에 예쁘게 출력합니다 (tabulate 사용).

#     Parameters:
#     - md_str: Markdown 포맷의 테이블 (`| 헤더 | ... |`)
#     - title: 출력 전에 보여줄 제목 (옵션)
#     """
#     if not md_str or not md_str.startswith("```"):
#         print("(❗유효한 마크다운 테이블이 아닙니다)")
#         return

#     lines = md_str.strip("`\n").split("\n")

#     # 헤더, 구분자 제거
#     data_lines = [line.strip() for line in lines if line.strip() and not line.startswith("---")]
#     if len(data_lines) < 2:
#         print("(⚠️ 데이터 없음)")
#         return

#     headers = [h.strip() for h in data_lines[0].split("|")[1:-1]]
#     rows = []
#     for line in data_lines[2:]:  # skip header & separator
#         cols = [c.strip() for c in line.split("|")[1:-1]]
#         rows.append(cols)

#     if title:
#         print(f"\n📄 {title}")
#         print("-" * (len(title) + 4))

#     print(tabulate(rows, headers=headers, tablefmt="grid"))
def print_markdown_table(md_str: str, title: str = ""):

    if not md_str:
        print("(❗유효한 마크다운 테이블이 아닙니다)")
        return

    # 1) 역슬래시 \n 들을 진짜 줄바꿈으로 바꿔줌
    md_str = md_str.encode().decode('unicode_escape')

    # 2) 코드블럭 기호 제거
    md_str = md_str.strip().strip("`")

    lines = md_str.split("\n")
    data_lines = [line.strip() for line in lines if line.strip() and not line.startswith("---")]
    
    if len(data_lines) < 2:
        print("(⚠️ 데이터 없음)")
        return

    headers = [h.strip() for h in data_lines[0].split("|")[1:-1]]
    rows = []
    for line in data_lines[2:]:  # skip header & separator
        cols = [c.strip() for c in line.split("|")[1:-1]]
        rows.append(cols)

    if title:
        print(f"\n📄 {title}")
        print("-" * (len(title) + 4))

    print(tabulate(rows, headers=headers, tablefmt="grid"))
