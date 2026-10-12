
import re
from datetime import datetime
import pandas as pd
from databricks import sql as dbsql

DATABRICKS_HOST  = "dbc-6c5d05f5-fce9.cloud.databricks.com"
DATABRICKS_PATH  = "/sql/1.0/warehouses/96dabf34bd066df3"
DATABRICKS_TOKEN = "dapi5dc60a6525fa7d86116ec57093f7e60a"
CATALOG          = "wieland"
SCHEMA           = "data"
TBL_BOM          = f"{CATALOG}.{SCHEMA}.bom_master"
TBL_RTG          = f"{CATALOG}.{SCHEMA}.routing_master"
TBL_APPROVED     = f"{CATALOG}.{SCHEMA}.approved_rules"
TBL_VAL_RUNS     = f"{CATALOG}.{SCHEMA}.validation_runs"
TBL_VAL_FLAGS    = f"{CATALOG}.{SCHEMA}.validation_flags"


def get_conn():
    return dbsql.connect(server_hostname=DATABRICKS_HOST,
                         http_path=DATABRICKS_PATH, access_token=DATABRICKS_TOKEN)


def load_approved_rules():
    conn = get_conn(); cursor = conn.cursor()
    cursor.execute(f"SELECT id, field, rule, confidence, pattern_type, evidence_count, source FROM {TBL_APPROVED} ORDER BY CAST(id AS INT) ASC")
    rows = cursor.fetchall()
    cursor.close(); conn.close()
    return [
        {
            "id": str(r[0]),
            "field": r[1],
            "rule": r[2],
            "confidence": int(r[3]) if r[3] and str(r[3]).isdigit() else 0,
            "pattern_type": r[4] or "",
            "evidence_count": int(r[5]) if r[5] and str(r[5]).isdigit() else 0,
            "source": r[6] or "Agent",
        }
        for r in rows
    ]


def normalize_field(field):
    return field.split(".")[-1].strip().lower()


def extract_table(field):
    parts = field.split(".")
    return parts[-2].lower() if len(parts) >= 2 else ""


def ensure_df(df):
    if df is None:
        return pd.DataFrame()
    return df.copy()


def build_check(rule):
    field = rule["field"]
    f = normalize_field(field)
    pattern = (rule.get("pattern_type") or "").upper()
    text = (rule.get("rule") or "").lower()

    def check(df):
        df = ensure_df(df)
        if df.empty or f not in [c.lower() for c in df.columns]:
            return pd.DataFrame(columns=["field", "rule_id", "record_count", "note"])

        col = next(c for c in df.columns if c.lower() == f)
        s = df[col]
        mask = pd.Series(False, index=df.index)
        note = ""

        if pattern == "HIGH_NULL_RATE" or "null" in text or "missing" in text:
            mask = s.isna() | (s.astype(str).str.strip() == "")
            note = "Null or missing values"
        elif pattern == "DELETION_FLAG_MISMATCH" or "deletion" in text:
            mask = s.astype(str).str.upper().isin(["X", "1", "TRUE", "Y"])
            note = "Deletion flag set"
        elif pattern == "STALE_VALIDITY_DATE" or "valid_to" in f or "valid" in text:
            mask = s.astype(str).eq("9999-12-31")
            note = "Open-ended validity date"
        elif pattern == "DUPLICATE_KEY" or any(k in f for k in ["matnr", "plnnr", "stlnr", "id", "nr", "key"]):
            mask = s.duplicated(keep=False)
            note = "Duplicate values"
        elif pattern == "CONCURRENT_EDIT" or "changed_by" in f:
            mask = s.astype(str).str.strip() != ""
            note = "Changed by value present; review manually"
        elif pattern == "CONSTANT_FIELD":
            mask = s.notna() & (s.astype(str).str.strip() != "")
            note = "Constant field check"
        else:
            mask = s.isna()
            note = "Fallback null check"

        flagged = df.loc[mask, [col]].copy()
        if flagged.empty:
            return pd.DataFrame(columns=["field", "rule_id", "record_count", "note"])
        return pd.DataFrame([{
            "field": field,
            "rule_id": rule["id"],
            "record_count": int(len(flagged)),
            "note": note,
        }])

    return check


def build_checks_from_approved_rules():
    rules = load_approved_rules()
    return [(r, build_check(r)) for r in rules]


def get_table_df(table_name, cutoff_date=None):
    conn = get_conn(); cursor = conn.cursor()
    if cutoff_date:
        cursor.execute(f"SELECT * FROM {table_name} WHERE record_date >= ?", (cutoff_date,))
    else:
        cursor.execute(f"SELECT * FROM {table_name}")
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    cursor.close(); conn.close()
    return pd.DataFrame(rows, columns=cols)


def run_validations(triggered_by="Manual", cutoff_date=None):
    if cutoff_date is None:
        cutoff_date = (datetime.now() - pd.Timedelta(days=7)).strftime("%Y-%m-%d")
    checks = build_checks_from_approved_rules()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    run_id = f"RUN-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

    frames = []
    total_flags = 0
    total_rules = len(checks)
    records_checked = 0

    for table in [TBL_BOM, TBL_RTG]:
        try:
            df = get_table_df(table, cutoff_date=cutoff_date)
            records_checked += len(df)
            for rule, check in checks:
                if rule["field"].startswith(table):
                    out = check(df)
                    if not out.empty:
                        out["run_id"] = run_id
                        out["created_at"] = now
                        frames.append(out)
                        total_flags += int(out["record_count"].sum())
        except Exception:
            continue

    flags_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["field", "rule_id", "record_count", "note", "run_id", "created_at"])

    conn = get_conn(); cursor = conn.cursor()
    cursor.execute(f"INSERT INTO {TBL_VAL_RUNS} (run_id, run_ts, triggered_by, records_checked, flags_found, rules_ran, report_status) VALUES (?,?,?,?,?,?,?)",
                   (run_id, now, triggered_by, str(records_checked), str(total_flags), str(total_rules), "Sent"))
    for _, row in flags_df.iterrows():
        cursor.execute(f"INSERT INTO {TBL_VAL_FLAGS} (run_id, flag_id, field, rule_id, record_count, note, created_at) VALUES (?,?,?,?,?,?,?)",
                       (run_id, f"F-{run_id}-{row['rule_id']}", row["field"], str(row["rule_id"]), str(row["record_count"]), row["note"], row["created_at"]))
    cursor.close(); conn.close()

    return run_id, flags_df


def manual_validation_report(triggered_by="Manual"):
    run_id, flags_df = run_validations(triggered_by=triggered_by)
    return {
        "run_id": run_id,
        "flags": flags_df,
        "rules_count": len(load_approved_rules())
    }
