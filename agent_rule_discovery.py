"""
agent_rule_discovery.py
AI Agent reads 2-year Databricks data, detects patterns,
asks Ollama to formulate rules and writes tickets to master_data_rules.
"""
import re, requests, pandas as pd
from datetime import datetime, timedelta
from databricks import sql as dbsql

DATABRICKS_HOST  = "dbc-6c5d05f5-fce9.cloud.databricks.com"
DATABRICKS_PATH  = "/sql/1.0/warehouses/96dabf34bd066df3"
DATABRICKS_TOKEN = "dapi5dc60a6525fa7d86116ec57093f7e60a"
OLLAMA_URL       = "http://localhost:11434"
OLLAMA_MODEL     = "llama3.2"
CATALOG          = "wieland"
SCHEMA           = "data"
TBL_BOM          = f"{CATALOG}.{SCHEMA}.bom_master"
TBL_RTG          = f"{CATALOG}.{SCHEMA}.routing_master"
TBL_RULES        = f"{CATALOG}.{SCHEMA}.master_data_rules"
CUTOFF_DATE      = (datetime.today() - timedelta(days=730)).strftime("%Y-%m-%d")

def get_conn():
    return dbsql.connect(server_hostname=DATABRICKS_HOST,
                         http_path=DATABRICKS_PATH, access_token=DATABRICKS_TOKEN)

def run_query(sql):
    conn = get_conn(); cursor = conn.cursor()
    cursor.execute(sql)
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    cursor.close(); conn.close()
    return pd.DataFrame(rows, columns=cols)

def load_data(table):
    print(f"[Agent] Loading {table} since {CUTOFF_DATE}...")
    df = run_query(f"SELECT * FROM {table} WHERE record_date >= '{CUTOFF_DATE}'")
    print(f"[Agent] {len(df)} rows, {len(df.columns)} columns loaded.")
    return df

def detect_patterns(df, table_name):
    findings = []
    total = len(df)
    if total == 0:
        return findings
    for col in df.columns:
        series = df[col]
        
        null_rate = series.isna().sum() / total
        if null_rate > 0.15:
            findings.append({"table": table_name, "field": col,
                "pattern": "HIGH_NULL_RATE",
                "detail": f"{null_rate*100:.1f}% of values are null or missing",
                "evidence_count": int(series.isna().sum()),
                "confidence": min(99, int(60 + null_rate * 100))})
        
        if any(k in col.lower() for k in ["id","nr","key","matnr","plnnr","stlnr"]):
            dup_rate = series.duplicated().sum() / total
            if dup_rate > 0.05:
                findings.append({"table": table_name, "field": col,
                    "pattern": "DUPLICATE_KEY",
                    "detail": f"{dup_rate*100:.1f}% duplicate values in key field",
                    "evidence_count": int(series.duplicated().sum()),
                    "confidence": min(99, int(65 + dup_rate * 80))})
        
        if "valid" in col.lower():
            try:
                parsed = pd.to_datetime(series, errors="coerce")
                stale = (parsed == pd.Timestamp("9999-12-31")).sum()
                if stale / total > 0.4:
                    findings.append({"table": table_name, "field": col,
                        "pattern": "STALE_VALIDITY_DATE",
                        "detail": f"{stale} records have valid_to = 9999-12-31, no expiry governance",
                        "evidence_count": int(stale), "confidence": 88})
            except Exception:
                pass
        
        if "deletion" in col.lower():
            flagged = series.fillna("").astype(str).str.strip().isin(["X","x","1"]).sum()
            if flagged > 0:
                findings.append({"table": table_name, "field": col,
                    "pattern": "DELETION_FLAG_MISMATCH",
                    "detail": f"{flagged} records have deletion flag set but may still be active",
                    "evidence_count": int(flagged), "confidence": 91})
        
        if "changed_by" in col.lower():
            date_col = next((c for c in df.columns if "changed_on" in c.lower()), None)
            if date_col:
                try:
                    df["_dt"] = pd.to_datetime(df[date_col], errors="coerce")
                    concurrent = (df.groupby("_dt")[col].nunique() > 2).sum()
                    if concurrent > 0:
                        findings.append({"table": table_name, "field": col,
                            "pattern": "CONCURRENT_EDIT",
                            "detail": f"{concurrent} time windows with >2 users editing same records",
                            "evidence_count": int(concurrent), "confidence": 87})
                except Exception:
                    pass
        
        if series.dtype == object and series.nunique() == 1:
            findings.append({"table": table_name, "field": col,
                "pattern": "CONSTANT_FIELD",
                "detail": "Field has only 1 unique value across all records",
                "evidence_count": total, "confidence": 82})
    print(f"[Agent] {len(findings)} patterns detected in {table_name}.")
    return findings

def formulate_rule(finding):
    prompt = (
        f"You are a master data quality expert.\n"
        f"Pattern found in SAP master data:\n"
        f"- Table: {finding['table']}\n"
        f"- Field: {finding['field']}\n"
        f"- Pattern: {finding['pattern']}\n"
        f"- Detail: {finding['detail']}\n"
        f"- Evidence: {finding['evidence_count']} records\n\n"
        f"Write one clear data quality rule (1-2 sentences) a data steward can approve. Start with: If ..."
    )
    try:
        r = requests.post(f"{OLLAMA_URL}/api/generate",
                          json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False}, timeout=60)
        rule = re.sub(r"\n+", " ", r.json().get("response","").strip())
        return rule if rule else f"Auto-rule for {finding['pattern']} in {finding['field']}"
    except Exception:
        return f"If {finding['field']} shows {finding['pattern'].lower().replace('_',' ')}, flag for review."

def write_tickets(tickets):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(f"SELECT MAX(CAST(id AS INT)) FROM {TBL_RULES}")
    max_id = cursor.fetchone()[0] or 0
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for i, t in enumerate(tickets):
        cursor.execute(
            f"INSERT INTO {TBL_RULES} (id, field, rule, confidence, pattern_type, evidence_count, source, status, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (str(int(max_id) + i + 1), t["field"], t["rule"], str(t["confidence"]),
             t["pattern"], str(t["evidence_count"]), "Agent", "Waiting for Approval", now))
    cursor.close()
    conn.close()
    print(f"[Agent] {len(tickets)} tickets written to {TBL_RULES}.")

def run_discovery():
    print("="*60)
    print(f"[Agent] Discovery started at {datetime.now()}")
    print(f"[Agent] Scanning data since {CUTOFF_DATE}")
    print("="*60)
    all_tickets = []
    for table in [TBL_BOM, TBL_RTG]:
        try:
            df = load_data(table)
            for finding in detect_patterns(df, table):
                print(f"[Agent] Formulating rule: {finding['pattern']} in {finding['field']}...")
                rule = formulate_rule(finding)
                all_tickets.append({
                    "field":          f"{finding['table']}.{finding['field']}",
                    "rule":           rule,
                    "confidence":     finding["confidence"],
                    "pattern":        finding["pattern"],
                    "evidence_count": finding["evidence_count"],
                })
        except Exception as e:
            print(f"[Agent] Error on {table}: {e}")
    if all_tickets:
        write_tickets(all_tickets)
        print(f"[Agent] Done. {len(all_tickets)} rules proposed.")
    else:
        print("[Agent] No patterns found.")

if __name__ == "__main__":
    run_discovery()