import re
from typing import Any, Dict, List, Tuple, Union
from nltk import word_tokenize

# --- 상수 ---
CLAUSE_KEYWORDS = ('select', 'from', 'where', 'group', 'order', 'limit', 'intersect', 'union', 'except')
UNIT_OPS        = ('-', '+', '*', '/')
AGG_OPS         = ('max', 'min', 'count', 'sum', 'avg')
COND_OPS        = ('and', 'or')
SQL_OPS         = ('intersect', 'union', 'except')
ORDER_OPS       = ('desc', 'asc')

# # --- 토크나이저 (리터럴 보존 + 소문자 정규화) ---
# def tokenize(query: str) -> List[str]:
#     # 1) 작은따옴표 → 큰따옴표
#     s = query.replace("'", '"')
#     # 2) CAST(... AS REAL) 단순화
#     s = re.sub(r'cast\s*\((.*?)\s+as\s+real\)', r'(\1)', s, flags=re.IGNORECASE)
#     # 3) 리터럴("...")을 꺼내서 보존
#     literals: Dict[str, str] = {}
#     def repl(m):
#         key = f"__lit{len(literals)}__"
#         literals[key] = m.group(1).lower()  # 실제 값 소문자화
#         return key
#     s = re.sub(r'"([^"]*)"', repl, s)
#     # 4) 일반 토크나이징
#     toks = word_tokenize(s)
#     out: List[str] = []
#     for tok in toks:
#         if tok in literals:
#             out.append(literals[tok])      # 자리표시자 → 실제 리터럴
#         else:
#             out.append(tok.lower())        # 나머지만 소문자
#     return out
# --- 새 토크나이저 (공백·쉼표·괄호·연산자만 기준으로 split) ---
_ID   = r'[a-zA-Z_][a-zA-Z0-9_]*'
_NUM  = r'\d+(?:\.\d+)?'
_STR  = r'"[^"]*"|\'[^\']*\''
_OP   = r'<=|>=|!=|<>|=|<|>|\.|,|\(|\)'
regex = re.compile(f'({_STR}|{_NUM}|{_ID}|{_OP})', re.I)

def tokenize(query: str) -> List[str]:
    q = query.strip()
    # CAST 제거, 따옴표 통일
    q = re.sub(r"cast\s*\((.*?)\s+as\s+real\)", r"(\1)", q, flags=re.I)
    q = q.replace("'", '"')
    toks = [t.lower() for t in regex.findall(q) if t.strip()]
    return toks

def scan_alias(toks: List[str]) -> Dict[str, str]:
    alias: Dict[str, str] = {}
    for idx, tok in enumerate(toks):
        if tok == 'as' and idx + 2 < len(toks):
            alias[toks[idx+1]] = toks[idx-1]
    return alias

def get_tables_with_alias(toks: List[str]) -> Dict[str, str]:
    return scan_alias(toks)

parse_sql = None  # forward declaration

def parse_col(toks: List[str], i: int, tables_with_alias: Dict[str,str], schema: Any, default_tables: List[str]=None) -> Tuple[int, Tuple[Any, ...]]:
    if i >= len(toks):
        return i, ('*',)
    tok = toks[i]
    if tok == '*':
        return i+1, ('*',)
    if '.' in tok:
        alias, col = tok.split('.',1)
        return i+1, (alias, col)
    return i+1, (None, tok)

def parse_val_unit(toks: List[str], i: int, tables_with_alias: Dict[str,str], schema: Any, default_tables: List[str]=None) -> Tuple[int, Any]:
    stack: List[Any] = []
    while i < len(toks):
        tok = toks[i]
        if tok in UNIT_OPS:
            stack.append(tok)
            i += 1
        elif tok in AGG_OPS[1:]:
            agg = tok; i += 1
            if i < len(toks) and toks[i] == '(': i += 1
            i, inner = parse_val_unit(toks, i, tables_with_alias, schema, default_tables)
            if i < len(toks) and toks[i] == ')': i += 1
            stack.append((agg, inner))
        elif tok == '(':
            i += 1
            i, inner = parse_val_unit(toks, i, tables_with_alias, schema, default_tables)
            if i < len(toks) and toks[i] == ')': i += 1
            stack.append(inner)
        else:
            i, col = parse_col(toks, i, tables_with_alias, schema, default_tables)
            stack.append(('none', col))
        if i >= len(toks) or toks[i] in CLAUSE_KEYWORDS + (',', ')'):
            break
    return i, tuple(stack) if len(stack) > 1 else (stack[0] if stack else ('none', None))

def parse_table_unit(toks: List[str], i: int, tables_with_alias: Dict[str,str], schema: Any) -> Tuple[int, str]:
    if i >= len(toks): return i, ''
    tbl = toks[i]; i += 1
    if i < len(toks) and toks[i] == 'as': i += 2
    return i, tbl

def parse_condition(toks: List[str], i: int, tables_with_alias: Dict[str,str], schema: Any, default_tables: List[str]=None) -> Tuple[int, List[Any]]:
    conds: List[Any] = []
    while i < len(toks):
        i, vu = parse_val_unit(toks, i, tables_with_alias, schema, default_tables)
        not_op = False
        if i < len(toks) and toks[i] == 'not': not_op = True; i += 1
        if i >= len(toks): break
        op = toks[i]; i += 1
        if i < len(toks) and toks[i] == 'select':
            i, val = parse_sql(toks, i, tables_with_alias, schema)
        else:
            val = toks[i] if i < len(toks) else None; i += 1
        conds.append((not_op, op, vu, val))
        if i < len(toks) and toks[i] in COND_OPS:
            conds.append(toks[i]); i += 1
        else:
            break
    return i, conds

def parse_select(toks: List[str], i: int, tables_with_alias: Dict[str,str], schema: Any, default_tables: List[str]=None) -> Tuple[int, Tuple[bool, List[Any]]]:
    if i >= len(toks) or toks[i] != 'select':
        return i, (False, [])
    i += 1
    distinct = False
    if i < len(toks) and toks[i] == 'distinct': distinct = True; i += 1
    cols: List[Any] = []
    while i < len(toks) and toks[i] != 'from':
        i, vu = parse_val_unit(toks, i, tables_with_alias, schema, default_tables)
        cols.append(vu)
        if i < len(toks) and toks[i] == ',': i += 1
        else: break
    return i, (distinct, cols)

def parse_from(toks: List[str], i: int, tables_with_alias: Dict[str,str], schema: Any) -> Tuple[int, List[str], List[Any]]:
    if i >= len(toks) or toks[i] != 'from':
        return i, [], []
    i += 1
    tbls: List[str] = []
    conds: List[Any] = []
    while i < len(toks) and toks[i] not in CLAUSE_KEYWORDS:
        i, tbl = parse_table_unit(toks, i, tables_with_alias, schema)
        tbls.append(tbl)
        if i < len(toks) and toks[i] == 'on':
            i += 1
            i, cs = parse_condition(toks, i, tables_with_alias, schema, tbls)
            conds += cs
        if i < len(toks) and toks[i] == ',': i += 1
        else: break
    return i, tbls, conds

def parse_group_by(toks: List[str], i: int, tables_with_alias: Dict[str,str], schema: Any, default_tables: List[str]=None) -> Tuple[int, List[Any]]:
    if i >= len(toks) or toks[i] != 'group':
        return i, []
    i += 2  # skip 'group by'
    cols: List[Any] = []
    while i < len(toks) and toks[i] not in CLAUSE_KEYWORDS:
        i, cu = parse_col(toks, i, tables_with_alias, schema, default_tables)
        cols.append(cu)
        if i < len(toks) and toks[i] == ',': i += 1
        else: break
    return i, cols

def parse_order_by(toks: List[str], i: int, tables_with_alias: Dict[str,str], schema: Any, default_tables: List[str]=None) -> Tuple[int, Tuple[str, List[Any]]]:
    if i >= len(toks) or toks[i] != 'order':
        return i, ('', [])
    i += 2  # skip 'order by'
    vals: List[Any] = []
    dir = 'asc'
    while i < len(toks) and toks[i] not in CLAUSE_KEYWORDS:
        i, vu = parse_val_unit(toks, i, tables_with_alias, schema, default_tables)
        vals.append(vu)
        if i < len(toks) and toks[i] in ORDER_OPS: dir = toks[i]; i += 1
        if i < len(toks) and toks[i] == ',': i += 1
        else: break
    return i, (dir, vals)

def parse_limit(toks: List[str], i: int) -> Tuple[int, Union[int, None]]:
    if i < len(toks) and toks[i] == 'limit' and i+1 < len(toks):
        try:
            val = int(toks[i+1])
            i += 2
            return i, val
        except:
            pass
    return i, None

# def parse_sql(toks: List[str], i: int, tables_with_alias: Dict[str,str], schema: Any) -> Tuple[int, Dict[str, Any]]:
#     sql: Dict[str, Any] = {}
#     i, sel = parse_select(toks, i, tables_with_alias, schema, [])
#     sql['select'] = sel
#     # locate FROM
#     if 'from' in toks[i:]:
#         j = toks.index('from', i)
#         i, tbls, fr_conds = parse_from(toks, j, tables_with_alias, schema)
#         sql['from'] = {'table_units': tbls, 'conds': fr_conds}
#     else:
#         sql['from'] = {'table_units': [], 'conds': []}
#     # WHERE
#     if i < len(toks) and toks[i] == 'where':
#         i += 1                       # ← where 키워드 건너뛰기
#         i, wh = parse_condition(toks, i, tables_with_alias, schema,
#                             sql['from']['table_units'])
#     else:
#         wh = []
#     sql['where'] = wh
#     # GROUP BY
#     i, gb = parse_group_by(toks, i, tables_with_alias, schema, sql['from']['table_units'])
#     sql['groupBy'] = gb
#     # HAVING
#     if i < len(toks) and toks[i] == 'having':
#         i += 1                       # ← having 키워드 건너뛰기
#         i, hv = parse_condition(toks, i, tables_with_alias, schema,
#                                 sql['from']['table_units'])
#     else:
#         hv = []
#     sql['having'] = hv
#     # ORDER BY
#     i, ob = parse_order_by(toks, i, tables_with_alias, schema, sql['from']['table_units'])
#     sql['orderBy'] = ob
#     # LIMIT
#     i, lm = parse_limit(toks, i)
#     sql['limit'] = lm
#     # set IUEN
#     for op in SQL_OPS:
#         sql[op] = None
#     return i, sql
def parse_sql(toks: List[str], i: int, tables_with_alias: Dict[str,str], schema: Any) -> Tuple[int, Dict[str, Any]]:
    sql: Dict[str, Any] = {}

    # 1) SELECT
    i, sel = parse_select(toks, i, tables_with_alias, schema, [])
    sql['select'] = sel

    # 2) FROM (기존 로직 유지)
    if 'from' in toks[i:]:
        j = toks.index('from', i)
        i, tbls, fr_conds = parse_from(toks, j, tables_with_alias, schema)
        sql['from'] = {'table_units': tbls, 'conds': fr_conds}
    else:
        sql['from'] = {'table_units': [], 'conds': []}

    # 3) WHERE (robust detection)
    wh: List[Any] = []
    if 'where' in toks[i:]:
        # 현재 i 이후에서 첫 번째 where 위치 찾기
        where_idx = toks.index('where', i)
        # 그 다음 토큰부터 조건 파싱
        _, wh = parse_condition(toks, where_idx + 1, tables_with_alias, schema,
                                sql['from']['table_units'])
        # i 위치를 where 절 직후로 갱신
        i = where_idx + 1
    sql['where'] = wh

    # 4) GROUP BY
    i, gb = parse_group_by(toks, i, tables_with_alias, schema, sql['from']['table_units'])
    sql['groupBy'] = gb

    # 5) HAVING
    hv: List[Any] = []
    if 'having' in toks[i:]:
        having_idx = toks.index('having', i)
        _, hv = parse_condition(toks, having_idx + 1, tables_with_alias, schema,
                                sql['from']['table_units'])
        i = having_idx + 1
    sql['having'] = hv

    # 6) ORDER BY
    i, ob = parse_order_by(toks, i, tables_with_alias, schema, sql['from']['table_units'])
    sql['orderBy'] = ob

    # 7) LIMIT
    i, lm = parse_limit(toks, i)
    sql['limit'] = lm

    # 8) set IUEN placeholders
    for op in SQL_OPS:
        sql[op] = None

    return i, sql


parse_sql = parse_sql
def get_sql(query: str) -> Dict[str, Any]:
    """Parse raw SQL string into structured dict."""
    toks = tokenize(query)
    tables_with_alias = get_tables_with_alias(toks)
    _, sql = parse_sql(toks, 0, tables_with_alias, schema={})
    return sql

# recursive extraction
def extract_sqls(sql: Dict[str, Any]) -> List[Dict[str, Any]]:
    lst: List[Dict[str, Any]] = [sql]
    for cond in sql.get('from', {}).get('conds', []) + sql.get('where', []) + sql.get('having', []):
        if isinstance(cond, tuple) and len(cond) >= 4:
            val = cond[3]
            if isinstance(val, dict):
                lst += extract_sqls(val)
    for op in SQL_OPS:
        sub = sql.get(op)
        if isinstance(sub, dict): lst += extract_sqls(sub)
    return lst
# --- 컴포넌트 수집기에 DISTINCT 추가 ---
def collect_components(sql_list: List[Dict[str, Any]]) -> Dict[str, List[Any]]:
    comps = {
        'distinct': [],
        'select': [],
        'from': [],
        'where': [],
        'groupBy': [],
        'having': [],
        'orderBy': [],
        'limit': []
    }
    for s in sql_list:
        # DISTINCT 플래그 저장
        distinct_flag, cols = s['select']
        comps['distinct'].append(distinct_flag)
        # 나머지 컴포넌트
        comps['select']  += cols
        comps['from']    += s['from']['table_units']
        comps['where']   += s['from']['conds'] + s['where']
        comps['groupBy'] += s['groupBy']
        comps['having']  += s['having']
        dir_, vals = s['orderBy']
        if vals:
            comps['orderBy'].append((dir_, tuple(vals)))
        if s['limit'] is not None:
            comps['limit'].append(s['limit'])
    return comps

# --- 개선된 ExactMatchEvaluator ---
class ExactMatchEvaluator:
    def __call__(self, pred_sql: str, gold_sql: str) -> bool:
        try:
            # 파싱 & 중첩 subquery까지 모두 추출
            p_sqls = extract_sqls(get_sql(pred_sql))
            g_sqls = extract_sqls(get_sql(gold_sql))
            # 컴포넌트별 집합으로 변환
            p_comp = collect_components(p_sqls)
            g_comp = collect_components(g_sqls)
        except Exception:
            return False

        # 모든 키가 order‐agnostic 하게 일치해야 True
        for key in p_comp:
            if set(p_comp[key]) != set(g_comp.get(key, [])):
                return False
        return True

class ExactMatchEvaluatorStrict:
    def __call__(self, pred_sql: str, gold_sql: str) -> bool:
        try:
            # 파싱 & 중첩 subquery까지 모두 추출
            p_sqls = extract_sqls(get_sql(pred_sql))
            g_sqls = extract_sqls(get_sql(gold_sql))
            # 컴포넌트별 추출
            p_comp = collect_components(p_sqls)
            g_comp = collect_components(g_sqls)
        except Exception as e:
            print(f"[Parse Error] {e}")
            return False

        # 모든 키가 값까지 포함하여 일치해야 함 (순서도 고려)
        for key in p_comp:
            p_vals = p_comp[key]
            g_vals = g_comp.get(key, [])
            
            # 정렬된 문자열 비교 (튜플 → 문자열로)
            def normalize(val):
                if isinstance(val, (tuple, list)):
                    return str(tuple(normalize(v) for v in val))
                return str(val).lower()
            
            p_norm = sorted([normalize(v) for v in p_vals])
            g_norm = sorted([normalize(v) for v in g_vals])
            
            if p_norm != g_norm:
                return False

        return True

class ExactMatchEvaluatorStrictWithLog:
    def __call__(self, pred_sql: str, gold_sql: str) -> bool:
        try:
            # 파싱 및 중첩 subquery 추출
            p_sqls = extract_sqls(get_sql(pred_sql))
            g_sqls = extract_sqls(get_sql(gold_sql))
            # 컴포넌트 추출
            p_comp = collect_components(p_sqls)
            g_comp = collect_components(g_sqls)
        except Exception as e:
            print(f"[❌ Parse Error] {e}")
            return False

        # normalize 함수 정의
        def normalize(val):
            if isinstance(val, (tuple, list)):
                return str(tuple(normalize(v) for v in val))
            return str(val).lower()

        print("\n[🔍 Component-by-Component Comparison]")
        all_match = True
        for key in p_comp:
            p_vals = p_comp[key]
            g_vals = g_comp.get(key, [])

            p_norm = sorted([normalize(v) for v in p_vals])
            g_norm = sorted([normalize(v) for v in g_vals])

            if p_norm != g_norm:
                all_match = False
                print(f"❌ Mismatch in component [{key}]")
                print(f"  🔻 Pred: {p_norm}")
                print(f"  🔺 Gold: {g_norm}")
            else:
                print(f"✅ Match in component [{key}]")

        return all_match

class ExactMatchEvaluatorStrictWithFullLog:
    def __call__(self, pred_sql: str, gold_sql: str) -> bool:
        try:
            # 파싱 및 중첩 subquery 추출
            p_sqls = extract_sqls(get_sql(pred_sql))
            g_sqls = extract_sqls(get_sql(gold_sql))

            # 컴포넌트 추출
            p_comp = collect_components(p_sqls)
            g_comp = collect_components(g_sqls)

        except Exception as e:
            print(f"[❌ Parse Error] {e}")
            return False

        # normalize 함수
        def normalize(val):
            if isinstance(val, (tuple, list)):
                return str(tuple(normalize(v) for v in val))
            return str(val).lower()

        print("\n📦 Parsed SQL Components (Pred)")
        for k, v in p_comp.items():
            print(f"  [{k}]: {v}")

        print("\n📦 Parsed SQL Components (Gold)")
        for k, v in g_comp.items():
            print(f"  [{k}]: {v}")

        print("\n[🔍 Component-by-Component Comparison]")
        all_match = True
        for key in p_comp:
            p_vals = p_comp[key]
            g_vals = g_comp.get(key, [])

            p_norm = sorted([normalize(v) for v in p_vals])
            g_norm = sorted([normalize(v) for v in g_vals])

            if p_norm != g_norm:
                all_match = False
                print(f"❌ Mismatch in component [{key}]")
                print(f"  🔻 Pred: {p_norm}")
                print(f"  🔺 Gold: {g_norm}")
            else:
                print(f"✅ Match in component [{key}]")

        print(f"\n🎯 Final Result: {'✅ Exact Match' if all_match else '❌ Not Match'}")
        return all_match
