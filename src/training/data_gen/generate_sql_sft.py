# Project: logs-ai-reporting-model-train — File: src/training/data_gen/generate_sql_sft.py
# Purpose: Generate 20k schema-aware NL→SQL examples (JSONL) for LoRA SFT

import json, os, random, datetime

# ----------------------------- CONFIG (explicit constants) -----------------------------
OUT_DIR                      = "data"
OUT_FILE_TRAIN               = os.path.join(OUT_DIR, "sql_sft_examples.jsonl")
OUT_FILE_TEST                = os.path.join(OUT_DIR, "sql_sft_examples_test.jsonl")
N_TOTAL                      = 20000
N_TEST                       = 3000   # held-out; remaining go to train
SEED                         = 42

SCHEMA_COLUMNS = [
    "user_id TEXT",
    "id TEXT",
    "subseq_id TEXT",
    "message TEXT",
    "audit_time TIMESTAMPTZ",
    "action_raw TEXT",
    "type TEXT",
    "label TEXT",
    "version TEXT",
    "recipe_id TEXT",
    "recipe_name TEXT",
    "material_name TEXT",
    "material_id TEXT",
    "name1 TEXT",
    "name2 TEXT",
    "username TEXT",
    "action_derived TEXT",
    "session_start TIMESTAMPTZ",
    "session_end TIMESTAMPTZ",
    "session_duration INTEGER"
]

SYSTEM_BASE = (
    "Return exactly one SQL statement. Read-only (no DML/DDL). "
    "Use ILIKE or trim(lower(col)) for case-insensitive matches. "
    "Time = SUM(session_duration). When counting recipes, use COUNT(DISTINCT recipe_id). "
    "Use COALESCE(session_duration,0) > 0 when summing time."
)

# Inputs for templating (explicit sets; add more as needed)
KEYWORDS = [
    "Trastuzumab","Tocilizumab","Obinutuzumab","Prasinezumab","Faricimab",
    "Trontinemab","Astegolimab","Mircera","gtVA2D","Blueprint"
]
USERNAMES = ["farhat","liu106","sitarskk","skvar","ciszewsk","lek8","dfquicho","luol12"]
TYPES_CANON = ["recipe"]  # filter with trim(lower(type))='recipe'
# date ranges (ISO string boundaries, UTC)
DATE_RANGES = [
    ("2025-07-01T00:00:00Z","2025-10-01T00:00:00Z"),
    ("2025-01-01T00:00:00Z","2025-04-01T00:00:00Z"),
    ("2024-10-01T00:00:00Z","2025-01-01T00:00:00Z"),
    ("2024-01-01T00:00:00Z","2025-01-01T00:00:00Z"),
]

# Category weights (sum to N_TOTAL via sampling)
CATS = [
    ("basic_agg_time_or_count", 5000),
    ("text_filters_case_ins",   3000),
    ("time_windows",            3000),
    ("group_topn",              3000),
    ("hard_combos",             4000),
    ("edge_no_match",           2000),
]

# ----------------------------- TEMPLATES -----------------------------

def tpl_total_time_keyword(kw:str)->tuple[str,str]:
    uq = f"Total time spent on {kw} overall (seconds and hours)."
    sql = (
        "SELECT SUM(session_duration) AS total_seconds,\n"
        "       SUM(session_duration)/3600.0 AS total_hours\n"
        "FROM logs_pkm\n"
        f"WHERE recipe_name ILIKE '%{kw}%'\n"
        "  AND COALESCE(session_duration,0) > 0;"
    )
    return uq, sql

def tpl_count_distinct_recipes_keyword(kw:str)->tuple[str,str]:
    uq = f"How many distinct {kw} recipes do we have?"
    sql = (
        "SELECT COUNT(DISTINCT recipe_id) AS unique_recipes\n"
        "FROM logs_pkm\n"
        "WHERE recipe_id IS NOT NULL AND recipe_id <> ''\n"
        f"  AND recipe_name ILIKE '%{kw}%';"
    )
    return uq, sql

def tpl_type_recipe_nonzero(limit:int)->tuple[str,str]:
    uq = f"Return {limit} rows where type is Recipe and session_duration > 0, newest first."
    sql = (
        "SELECT *\n"
        "FROM logs_pkm\n"
        "WHERE trim(lower(type)) = 'recipe'\n"
        "  AND COALESCE(session_duration,0) > 0\n"
        "ORDER BY audit_time DESC\n"
        f"LIMIT {limit};"
    )
    return uq, sql

def tpl_topn_recipes_events(n:int, dr:tuple[str,str]|None)->tuple[str,str]:
    if dr:
        start, end = dr
        uq = f"Top {n} recipes by number of log events between {start} and {end}."
        where_time = f"AND audit_time >= '{start}' AND audit_time < '{end}'\n"
    else:
        uq = f"Top {n} recipes by number of log events overall."
        where_time = ""
    sql = (
        "SELECT recipe_id, recipe_name, COUNT(*) AS events\n"
        "FROM logs_pkm\n"
        "WHERE recipe_id IS NOT NULL AND recipe_id <> ''\n"
        f"{where_time}"
        "GROUP BY recipe_id, recipe_name\n"
        "ORDER BY events DESC\n"
        f"LIMIT {n};"
    )
    return uq, sql

def tpl_user_time_on_keyword(user:str, kw:str, dr:tuple[str,str])->tuple[str,str]:
    start, end = dr
    uq = (f"How many hours did username={user} work on recipes containing '{kw}' "
          f"between {start} and {end}?")
    sql = (
        "SELECT SUM(session_duration)/3600.0 AS total_hours\n"
        "FROM logs_pkm\n"
        f"WHERE username = '{user}'\n"
        f"  AND recipe_name ILIKE '%{kw}%'\n"
        "  AND COALESCE(session_duration,0) > 0\n"
        f"  AND audit_time >= '{start}' AND audit_time < '{end}';"
    )
    return uq, sql

def tpl_keyword_no_match(kw:str)->tuple[str,str]:
    uq = f"List distinct recipes for keyword '{kw}??' (likely none)."
    sql = (
        "SELECT DISTINCT recipe_id, recipe_name\n"
        "FROM logs_pkm\n"
        "WHERE recipe_id IS NOT NULL AND recipe_id <> ''\n"
        f"  AND recipe_name ILIKE '%{kw}??%'\n"
        "ORDER BY recipe_id;"
    )
    return uq, sql

# ----------------------------- SAMPLING -----------------------------

def sample_pair(cat:str)->dict:
    if cat == "basic_agg_time_or_count":
        if random.random() < 0.5:
            kw = random.choice(KEYWORDS)
            uq, sql = tpl_total_time_keyword(kw)
        else:
            kw = random.choice(KEYWORDS)
            uq, sql = tpl_count_distinct_recipes_keyword(kw)
    elif cat == "text_filters_case_ins":
        # mix type filter vs keyword filter
        if random.random() < 0.5:
            lim = random.choice([50,100,200])
            uq, sql = tpl_type_recipe_nonzero(lim)
        else:
            kw = random.choice(KEYWORDS)
            uq, sql = tpl_count_distinct_recipes_keyword(kw)
    elif cat == "time_windows":
        dr = random.choice(DATE_RANGES)
        n  = random.choice([10,20,50])
        uq, sql = tpl_topn_recipes_events(n, dr)
    elif cat == "group_topn":
        n = random.choice([10,20,50])
        uq, sql = tpl_topn_recipes_events(n, None)
    elif cat == "hard_combos":
        user = random.choice(USERNAMES)
        kw   = random.choice(KEYWORDS)
        dr   = random.choice(DATE_RANGES)
        uq, sql = tpl_user_time_on_keyword(user, kw, dr)
    elif cat == "edge_no_match":
        kw = random.choice(KEYWORDS)
        uq, sql = tpl_keyword_no_match(kw)
    else:
        raise ValueError(f"unknown category: {cat}")

    obj = {
        "schema": {
            "engine": "postgres",
            "tables": {
                "logs_pkm": { "columns": SCHEMA_COLUMNS }
            }
        },
        "system": SYSTEM_BASE,
        "user_query": uq,
        "gold_sql": sql,
        "notes": []
    }
    return obj

def gen_all()->tuple[list, list]:
    random.seed(SEED)
    plan = []
    for cat, k in CATS:
        plan.extend([cat]*k)
    # If sums differ for any reason, trim or pad explicitly
    if len(plan) > N_TOTAL:
        plan = plan[:N_TOTAL]
    elif len(plan) < N_TOTAL:
        plan += [CATS[-1][0]]*(N_TOTAL - len(plan))

    random.shuffle(plan)
    pairs = [sample_pair(cat) for cat in plan]

    # split
    test = pairs[:N_TEST]
    train = pairs[N_TEST:]
    return train, test

def validate(obj:dict)->None:
    s = obj["gold_sql"]
    forbidden = ["UPDATE", "DELETE", "INSERT", "ALTER", "DROP TABLE", ";--", "/*"]
    for bad in forbidden:
        if bad.lower() in s.lower():
            raise ValueError(f"forbidden token in SQL: {bad}")
    # one statement heuristic: avoid multiple semicolons
    if s.strip().count(";") > 1:
        raise ValueError("multiple statements detected")

def main()->None:
    train, test = gen_all()
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(OUT_FILE_TRAIN, "w", encoding="utf-8") as f:
        for obj in train:
            validate(obj)
            f.write(json.dumps(obj, ensure_ascii=False)+"\n")
    with open(OUT_FILE_TEST, "w", encoding="utf-8") as f:
        for obj in test:
            validate(obj)
            f.write(json.dumps(obj, ensure_ascii=False)+"\n")
    print(f"Wrote {len(train)} train → {OUT_FILE_TRAIN}")
    print(f"Wrote {len(test)}  test  → {OUT_FILE_TEST}")

if __name__ == "__main__":
    main()