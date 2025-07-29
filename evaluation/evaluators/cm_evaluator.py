import re
from typing import Any, Dict, List, Tuple, Union
from nltk import word_tokenize

# --- Constants ---
CLAUSE_KEYWORDS = ('select', 'from', 'where', 'group', 'order', 'limit', 'intersect', 'union', 'except')
JOIN_KEYWORDS = ('join', 'on', 'as')
WHERE_OPS = ('not', 'between', '=', '>', '<', '>=', '<=', '!=', 'in', 'like', 'is', 'exists')
UNIT_OPS = ('none', '-', '+', '*', '/')
AGG_OPS = ('none', 'max', 'min', 'count', 'sum', 'avg')
COND_OPS = ('and', 'or')
SQL_OPS = ('intersect', 'union', 'except')
ORDER_OPS = ('desc', 'asc')

# --- Basic SQL parser ---

def tokenize(string: str) -> List[str]:
    s = string.replace("'", '"')
    s = re.sub(r'cast\s*\((.*?)\s+as\s+real\)', r'(\1)', s, flags=re.IGNORECASE)
    vals: Dict[str, str] = {}
    quote_idxs = [i for i, c in enumerate(s) if c == '"']
    assert len(quote_idxs) % 2 == 0, 'Unmatched quotes'
    for j in range(len(quote_idxs) - 1, 0, -2):
        a, b = quote_idxs[j-1], quote_idxs[j]
        key = f"__VAL_{a}_{b}__"
        vals[key] = s[a:b+1]
        s = s[:a] + key + s[b+1:]
    toks = [w.lower() for w in word_tokenize(s)]
    return [vals.get(tok, tok) for tok in toks]

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

def parse_sql(toks: List[str], i: int, tables_with_alias: Dict[str,str], schema: Any) -> Tuple[int, Dict[str, Any]]:
    sql: Dict[str, Any] = {}
    i, sel = parse_select(toks, i, tables_with_alias, schema, [])
    sql['select'] = sel
    # locate FROM
    if 'from' in toks[i:]:
        j = toks.index('from', i)
        i, tbls, fr_conds = parse_from(toks, j, tables_with_alias, schema)
        sql['from'] = {'table_units': tbls, 'conds': fr_conds}
    else:
        sql['from'] = {'table_units': [], 'conds': []}
    # WHERE
    if i < len(toks) and toks[i] == 'where':
        i, wh = parse_condition(toks, i, tables_with_alias, schema, sql['from']['table_units'])
    else:
        wh = []
    sql['where'] = wh
    # GROUP BY
    i, gb = parse_group_by(toks, i, tables_with_alias, schema, sql['from']['table_units'])
    sql['groupBy'] = gb
    # HAVING
    if i < len(toks) and toks[i] == 'having':
        i, hv = parse_condition(toks, i, tables_with_alias, schema, sql['from']['table_units'])
    else:
        hv = []
    sql['having'] = hv
    # ORDER BY
    i, ob = parse_order_by(toks, i, tables_with_alias, schema, sql['from']['table_units'])
    sql['orderBy'] = ob
    # LIMIT
    i, lm = parse_limit(toks, i)
    sql['limit'] = lm
    # set IUEN
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

def collect_components(sql_list: List[Dict[str, Any]]) -> Dict[str, List[Any]]:
    comps = {'select': [], 'from': [], 'where': [], 'groupBy': [], 'having': [], 'orderBy': [], 'limit': []}
    for s in sql_list:
        comps['select'] += s['select'][1]
        comps['from'] += s['from']['table_units']
        comps['where'] += s['from']['conds'] + s['where']
        comps['groupBy'] += s['groupBy']
        comps['having'] += s['having']
        ob = s['orderBy']
        if ob and ob[1]: comps['orderBy'].append((ob[0], tuple(ob[1])))  # Fix for unhashable list
        if s['limit'] is not None: comps['limit'].append(s['limit'])
    return comps

class ComponentMatchingEvaluator:
    def __call__(self, pred_sql: str, gold_sql: str) -> Tuple[float, Dict[str, bool]]:
        p_sqls = extract_sqls(get_sql(pred_sql))
        g_sqls = extract_sqls(get_sql(gold_sql))
        p_comp = collect_components(p_sqls)
        g_comp = collect_components(g_sqls)
        def make_hashable(v):
            if isinstance(v, list):
                return tuple(make_hashable(i) for i in v)
            elif isinstance(v, dict):
                return tuple(sorted((k, make_hashable(val)) for k, val in v.items()))
            else:
                return v

        details: Dict[str, bool] = {}
        score = 0
        total = len(p_comp)
        for k, pv in p_comp.items():
            gv = g_comp.get(k, [])
            ok = set(map(make_hashable, pv)) == set(map(make_hashable, gv))
            details[k] = ok
            score += ok
        return score/total, details
