import sqlite3
import os
from typing import List, Dict, Any, Optional
from tabulate import tabulate
import re

def get_db_path(base_dir: str, db_id: str) -> str:
    """
    Returns the path to the SQLite file located at:
    {base_dir}/dataset/bird_benchmark/dev_databases/{db_id}/{db_id}.sqlite
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
        raise FileNotFoundError(f"Database file not found: {path}")
    return path

def execute_query(
    db_path: str,
    query: str,
    params: Optional[tuple] = None
) -> List[Dict[str, Any]]:
    """
    Connects to SQLite and executes the query.
    Returns the results as a list of dictionaries: [{col: val, …}, …]
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
    1) Sort by `sort_keys`
    2) Truncate with `row_limit`
    3) Format the result as a Markdown table
    (Suitable format for LLM input)
    """
    if not results:
        return "```\n(No results)\n```"

    # 1) Sorting
    if sort_keys:
        try:
            results = sorted(
                results,
                key=lambda r: tuple(r.get(k) if r.get(k) is not None else "" for k in sort_keys)
            )
        except Exception as e:
            print(f"Warning: Error occurred during sorting (ignored): {e}")

    total = len(results)

    # 2) Truncating
    if row_limit is not None and total > row_limit:
        truncated = True
        results = results[:row_limit]
    else:
        truncated = False

    # 3) Markdown table formatting
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
        lines.append(f"\n> (Showing first {row_limit} of {total} rows)")

    return "```\n" + "\n".join(lines) + "\n```"


def print_markdown_table(md_str: str, title: str = ""):
    if not md_str:
        print("(Invalid markdown table)")
        return

    # 1) Replace literal \n with actual newlines
    md_str = md_str.encode().decode('unicode_escape')

    # 2) Remove code block markers
    md_str = md_str.strip().strip("`")

    lines = md_str.split("\n")
    data_lines = [line.strip() for line in lines if line.strip() and not line.startswith("---")]
    
    if len(data_lines) < 2:
        print("(No data)")
        return

    headers = [h.strip() for h in data_lines[0].split("|")[1:-1]]
    rows = []
    for line in data_lines[2:]:  # Skip header & separator
        cols = [c.strip() for c in line.split("|")[1:-1]]
        rows.append(cols)

    if title:
        print(f"\n{title}")
        print("-" * (len(title) + 4))

    print(tabulate(rows, headers=headers, tablefmt="grid"))
