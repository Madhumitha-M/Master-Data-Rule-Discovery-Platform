import re
import requests
import streamlit as st
from databricks import sql as dbsql
from datetime import datetime
import pandas as pd
from rule_engine import load_approved_rules, run_validations

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
TBL_APPROVED     = f"{CATALOG}.{SCHEMA}.approved_rules"
TBL_VAL_RUNS  = f"{CATALOG}.{SCHEMA}.validation_runs"
TBL_VAL_FLAGS = f"{CATALOG}.{SCHEMA}.validation_flags"

FIELDS = ["Select a field...",
    f"{TBL_BOM}.deletion_flag", f"{TBL_BOM}.valid_to", f"{TBL_BOM}.stlal",
    f"{TBL_BOM}.comp_deletion", f"{TBL_BOM}.changed_by",
    f"{TBL_RTG}.deletion_flag", f"{TBL_RTG}.valid_to",
    f"{TBL_RTG}.plnnr", f"{TBL_RTG}.changed_by"]

STYLE = """
<style>
html, body, [class*="css"] { font-family: Arial, sans-serif; }
.block-container { padding-top: 1.2rem; padding-bottom: 1rem; }
.stat-card {
    background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 10px;
    padding: 18px 24px; text-align: center;
}
.stat-number { font-size: 2.4rem; font-weight: 700; margin: 0; }
.stat-label  { font-size: 0.85rem; color: #6c757d; margin: 0; }
.stat-pending  .stat-number { color: #0d6efd; }
.stat-approved .stat-number { color: #198754; }
.stat-rejected .stat-number { color: #dc3545; }
.section-divider { border-top: 2px solid #e9ecef; margin: 1.2rem 0; }
</style>
"""

def get_conn():
    return dbsql.connect(server_hostname=DATABRICKS_HOST,
                         http_path=DATABRICKS_PATH, access_token=DATABRICKS_TOKEN)

def ensure_tables():
    try:
        conn = get_conn(); cursor = conn.cursor()
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS {TBL_APPROVED} (
            id STRING, field STRING, rule STRING, confidence STRING,
            pattern_type STRING, evidence_count STRING,
            source STRING, approved_at STRING, comment STRING)""")
        try:
            cursor.execute(f"ALTER TABLE {TBL_RULES} ADD COLUMNS (rejection_reason STRING)")
        except Exception:
            pass
        cursor.close(); conn.close()
    except Exception:
        pass

def load_tickets(status_filter):
    try:
        conn = get_conn(); cursor = conn.cursor()
        status_map = {"pending": "Waiting for Approval", "approved": "Approved", "rejected": "Rejected"}
        cursor.execute(
            f"SELECT id, field, rule, confidence, source, status, evidence_count, pattern_type, rejection_reason FROM {TBL_RULES} WHERE status = ?",
            (status_map[status_filter],))
        rows = cursor.fetchall(); cursor.close(); conn.close()
        tickets = []
        for i, r in enumerate(rows):
            raw_id = r[0]
            tid = f"RD-{raw_id}" if raw_id and str(raw_id).strip() not in ["","None","null"] else f"RD-{i+1}"
            tickets.append({
                "id": tid, "raw_id": str(raw_id) if raw_id else str(i+1),
                "field": r[1] or "", "rule": r[2] or "",
                "confidence": int(r[3]) if r[3] and str(r[3]).isdigit() else 0,
                "source": r[4] or "Agent", "status": r[5] or "Waiting for Approval",
                "evidence_count": int(r[6]) if r[6] and str(r[6]).isdigit() else 0,
                "pattern_type": r[7] or "", "rejection_reason": r[8] or "",
            })
        return tickets
    except Exception as e:
        st.error(f"DB error: {e}"); return []

def save_status(raw_id, status, reason=""):
    try:
        conn = get_conn(); cursor = conn.cursor()
        if reason:
            cursor.execute(f"UPDATE {TBL_RULES} SET status=?, rejection_reason=? WHERE id=?", (status, reason, raw_id))
        else:
            cursor.execute(f"UPDATE {TBL_RULES} SET status=? WHERE id=?", (status, raw_id))
        cursor.close(); conn.close()
    except Exception as e:
        st.error(f"Update error: {e}")

def move_to_approved(ticket, comment=""):
    try:
        conn = get_conn(); cursor = conn.cursor()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute(f"SELECT MAX(CAST(id AS INT)) FROM {TBL_APPROVED}")
        max_id = cursor.fetchone()[0] or 0
        cursor.execute(
            f"INSERT INTO {TBL_APPROVED} (id,field,rule,confidence,pattern_type,evidence_count,source,approved_at,comment) VALUES (?,?,?,?,?,?,?,?,?)",
            (str(int(max_id)+1), ticket["field"], ticket["rule"], str(ticket["confidence"]),
             ticket["pattern_type"], str(ticket["evidence_count"]), ticket["source"], now, comment))
        cursor.close(); conn.close()
    except Exception as e:
        st.error(f"Approve error: {e}")

def compare_with_db(field, rule_idea):
    try:
        f = field.split(".")[-1]
        conn = get_conn(); cursor = conn.cursor()
        cursor.execute(f"SELECT field, rule, confidence FROM {TBL_RULES} WHERE LOWER(field) LIKE ? LIMIT 5", (f"%{f}%",))
        rows = cursor.fetchall(); cursor.close(); conn.close()
        if rows:
            best = max(rows, key=lambda r: int(r[2]) if r[2] and str(r[2]).isdigit() else 0)
            return best[0], best[1], int(best[2]) if best[2] and str(best[2]).isdigit() else 72
    except Exception:
        pass
    return field, rule_idea, 72
def normalize_field(field):
    return field.split(".")[-1].strip().lower()


def check_duplicate_rule(field, rule_idea):
    text = rule_idea.strip().lower()
    keywords = set(re.findall(r"[a-z_]+", text))
    target_field = normalize_field(field)

    try:
        conn = get_conn(); cursor = conn.cursor()

        cursor.execute(f"SELECT field, rule, status FROM {TBL_RULES} WHERE status != 'Rejected'")
        pending_rows = [(r[0], r[1], r[2]) for r in cursor.fetchall()]

        cursor.execute(f"SELECT field, rule FROM {TBL_APPROVED}")
        approved_rows = [(r[0], r[1], "Approved") for r in cursor.fetchall()]

        cursor.close(); conn.close()

        for existing_field, existing_rule, status in pending_rows + approved_rows:
            if not existing_field or normalize_field(existing_field) != target_field:
                continue
            if not existing_rule:
                continue
            existing_keywords = set(re.findall(r"[a-z_]+", existing_rule.lower()))
            overlap = len(keywords & existing_keywords) / max(len(keywords | existing_keywords), 1)
            if overlap >= 0.3:
                return True, status
        return False, None
    except Exception as e:
        st.error(f"Duplicate check error: {e}")
        return False, None

def ask_ollama(field, rule_idea, db_rule, confidence):
    prompt = (f"A user wants to add a master data quality rule for field '{field}'.\n"
              f"Their description: {rule_idea}\n"
              f"Similar rule in DB ({confidence}%): {db_rule}\n"
              f"In one sentence ask if this is what they meant. Be concise and friendly.")
    try:
        r = requests.post(f"{OLLAMA_URL}/api/generate",
                          json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False}, timeout=30)
        return r.json().get("response","").strip()
    except Exception:
        return f"Databricks match ({confidence}%): \"{db_rule}\" — is this what you meant?"

def add_human_ticket(field, rule, confidence):
    try:
        conn = get_conn(); cursor = conn.cursor()
        cursor.execute(f"SELECT MAX(CAST(id AS INT)) FROM {TBL_RULES}")
        max_id = cursor.fetchone()[0] or 0
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute(
            f"INSERT INTO {TBL_RULES} (id,field,rule,confidence,source,status,created_at) VALUES (?,?,?,?,?,?,?)",
            (str(int(max_id)+1), field, rule, str(confidence), "Human", "Waiting for Approval", now))
        cursor.close(); conn.close()
    except Exception as e:
        st.error(f"Insert error: {e}")

def ci(c): return "🟢" if c >= 85 else "🟡" if c >= 70 else "🔴"

def render_detail(t, from_rejected=False):
    c1, c2 = st.columns(2)
    c1.markdown(f"**Source:** {'🤖 AI Agent' if t['source']=='Agent' else '👤 Human'}")
    c2.markdown(f"**Pattern:** `{t['pattern_type']}`")
    st.markdown(f"**Field:** `{t['field']}`")
    st.progress(t["confidence"]/100, text=f"Confidence: {t['confidence']}%")
    st.markdown(f"**Evidence:** {t['evidence_count']} records flagged")
    st.markdown("**Rule**")
    st.info(t["rule"])
    if t.get("rejection_reason"):
        st.error(f"**Rejection reason:** {t['rejection_reason']}")
    if from_rejected:
        st.markdown("---")
        cmt = st.text_input("Comment (optional)", key=f"rcmt_{t['id']}")
        if st.button("✅ Approve this rule", key=f"rapr_{t['id']}", use_container_width=True):
            save_status(t["raw_id"], "Approved")
            move_to_approved(t, cmt)
            st.session_state.modal = None
            st.success("Approved!"); st.rerun()

# ── App ───
st.set_page_config(page_title="Master Data Quality Platform", layout="wide")
st.markdown(STYLE, unsafe_allow_html=True)
ensure_tables()

for k, v in [("selected_id", None), ("modal", None), ("rej_sel", 0),
             ("apr_sel", 0), ("agent_reply", ""), ("pending_check", None),
             ("reject_confirm_id", None)]:
    st.session_state.setdefault(k, v)

pending  = load_tickets("pending")
approved = load_tickets("approved")
rejected = load_tickets("rejected")

st.title("Master Data Quality Platform")
tab1, tab2 = st.tabs(["Master Data — Rule Approval", "Data Validation Dashboard"])

with tab1:
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(f'''<div class="stat-card stat-pending">
            <p class="stat-number">{len(pending)}</p>
            <p class="stat-label">⏳ Pending Approval</p></div>''', unsafe_allow_html=True)
        if st.button("Open Pending ▼", key="btn_pending", use_container_width=True):
            st.session_state.modal = "pending" if st.session_state.modal != "pending" else None
    with c2:
        st.markdown(f'''<div class="stat-card stat-approved">
            <p class="stat-number">{len(approved)}</p>
            <p class="stat-label">✅ Approved Rules</p></div>''', unsafe_allow_html=True)
        if st.button("View Approved ▼", key="btn_approved", use_container_width=True):
            st.session_state.modal = "approved" if st.session_state.modal != "approved" else None
    with c3:
        st.markdown(f'''<div class="stat-card stat-rejected">
            <p class="stat-number">{len(rejected)}</p>
            <p class="stat-label">❌ Rejected Rules</p></div>''', unsafe_allow_html=True)
        if st.button("View Rejected ▼", key="btn_rejected", use_container_width=True):
            st.session_state.modal = "rejected" if st.session_state.modal != "rejected" else None

    st.markdown("<div class='section-divider'></div>", unsafe_allow_html=True)

    if st.session_state.modal == "pending":
        with st.expander("⏳ Pending Tickets", expanded=True):
            if not pending:
                st.info("No pending tickets!")
            else:
                pl, pr = st.columns([1, 1.4])
                with pl:
                    for t in pending:
                        icon = "🤖" if t["source"] == "Agent" else "👤"
                        if st.button(f"{icon} {t['id']}  |  {t['field'].split('.')[-1]}  {ci(t['confidence'])} {t['confidence']}%",
                                    key=f"psel_{t['id']}", use_container_width=True):
                            st.session_state.selected_id = t["id"]
                            st.session_state.reject_confirm_id = None
                with pr:
                    sel = next((t for t in pending if t["id"] == st.session_state.selected_id), pending[0])
                    st.subheader(f"🎫 {sel['id']}")
                    render_detail(sel)
                    cmt = st.text_area("Comment", placeholder="Optional...", key=f"pcmt_{sel['id']}")
                    ac, rc = st.columns(2)
                    if ac.button("✅ Approve", use_container_width=True, key=f"papr_{sel['id']}"):
                        save_status(sel["raw_id"], "Approved"); move_to_approved(sel, cmt)
                        st.session_state.selected_id = None; st.success("Approved!"); st.rerun()
                    if rc.button("❌ Reject", use_container_width=True, key=f"prej_{sel['id']}"):
                        st.session_state.reject_confirm_id = sel["id"]
                    if st.session_state.reject_confirm_id == sel["id"]:
                        st.warning("⚠️ A reason is required.")
                        reason = st.text_input("Rejection reason*", key=f"preason_{sel['id']}")
                        if st.button("Confirm Rejection", key=f"pconfrej_{sel['id']}"):
                            if not reason.strip(): st.error("Enter a reason.")
                            else:
                                save_status(sel["raw_id"], "Rejected", reason.strip())
                                st.session_state.selected_id = None
                                st.session_state.reject_confirm_id = None
                                st.warning("Rejected."); st.rerun()

    elif st.session_state.modal == "approved":
        with st.expander("✅ Approved Rules", expanded=True):
            if not approved:
                st.info("No approved rules yet.")
            else:
                al, ar = st.columns([1, 1.4])
                with al:
                    for i, t in enumerate(approved):
                        if st.button(f"✅ {t['id']}  |  {t['field'].split('.')[-1]}  {ci(t['confidence'])} {t['confidence']}%",
                                    key=f"asel_{i}", use_container_width=True):
                            st.session_state.apr_sel = i
                with ar:
                    t = approved[min(st.session_state.apr_sel, len(approved)-1)]
                    st.subheader(f"✅ {t['id']}")
                    render_detail(t)
                    try:
                        conn = get_conn(); cursor = conn.cursor()
                        cursor.execute(f"SELECT approved_at, comment FROM {TBL_APPROVED} WHERE field=? ORDER BY CAST(id AS INT) DESC LIMIT 1", (t["field"],))
                        row = cursor.fetchone(); cursor.close(); conn.close()
                        if row:
                            if row[1]: st.markdown(f"**Comment:** {row[1]}")
                            st.caption(f"Approved at: {row[0]}")
                    except Exception:
                        pass

    elif st.session_state.modal == "rejected":
        with st.expander("❌ Rejected Rules", expanded=True):
            if not rejected:
                st.info("No rejected rules.")
            else:
                rl, rr = st.columns([1, 1.4])
                with rl:
                    for i, t in enumerate(rejected):
                        if st.button(f"❌ {t['id']}  |  {t['field'].split('.')[-1]}  {ci(t['confidence'])} {t['confidence']}%",
                                    key=f"rsel_{i}", use_container_width=True):
                            st.session_state.rej_sel = i
                with rr:
                    t = rejected[min(st.session_state.rej_sel, len(rejected)-1)]
                    st.subheader(f"❌ {t['id']}")
                    render_detail(t, from_rejected=True)

    st.markdown("<div class='section-divider'></div>", unsafe_allow_html=True)

  
    st.subheader("📋 Pending Tickets")
    if not pending:
        st.info("No pending tickets. All caught up! ✅")
    else:
        left, right = st.columns([1, 1.4])
        with left:
            for t in pending:
                icon = "🤖" if t["source"] == "Agent" else "👤"
                if st.button(f"{icon} {t['id']}  |  {t['field'].split('.')[-1]}  {ci(t['confidence'])} {t['confidence']}%",
                            key=f"msel_{t['id']}", use_container_width=True):
                    st.session_state.selected_id = t["id"]
                    st.session_state.reject_confirm_id = None
        with right:
            sel = next((t for t in pending if t["id"] == st.session_state.selected_id), pending[0])
            st.subheader(f"🎫 {sel['id']}")
            render_detail(sel)
            cmt = st.text_area("Comment", placeholder="Optional comment...", key=f"mcmt_{sel['id']}")
            ac, rc = st.columns(2)
            if ac.button("✅ Approve", use_container_width=True, key=f"mapr_{sel['id']}"):
                save_status(sel["raw_id"], "Approved"); move_to_approved(sel, cmt)
                st.session_state.selected_id = None; st.success("Approved!"); st.rerun()
            if rc.button("❌ Reject", use_container_width=True, key=f"mrej_{sel['id']}"):
                st.session_state.reject_confirm_id = sel["id"]
            if st.session_state.reject_confirm_id == sel["id"]:
                st.warning("⚠️ A reason is required to reject.")
                reason = st.text_input("Rejection reason*", key=f"mreason_{sel['id']}")
                if st.button("Confirm Rejection", key=f"mconfrej_{sel['id']}"):
                    if not reason.strip(): st.error("Enter a reason.")
                    else:
                        save_status(sel["raw_id"], "Rejected", reason.strip())
                        st.session_state.selected_id = None
                        st.session_state.reject_confirm_id = None
                        st.warning("Rejected."); st.rerun()

    st.markdown("<div class='section-divider'></div>", unsafe_allow_html=True)

    # ── SUGGEST A RULE ──────
    st.subheader("➕ Suggest a Rule")
    scol1, scol2 = st.columns([1, 1])
    with scol1:
        field_name = st.selectbox("Field name", FIELDS, index=0)
        rule_idea  = st.text_area("Rule idea", placeholder="Describe your rule idea...", height=100)
        if st.button("🔍 Check with Databricks + AI", use_container_width=True):
            if field_name == "Select a field..." or not rule_idea.strip():
                st.error("Please select a field and enter a rule idea.")
            else:
                is_dup, dup_status = check_duplicate_rule(field_name, rule_idea)
                if is_dup:
                    if dup_status == "Approved":
                        st.session_state.agent_reply = f"✅ An approved rule already exists for `{field_name}` covering this exact scenario. No new ticket was created."
                    else:
                        st.session_state.agent_reply = f"⚠️ A similar rule for `{field_name}` is already {dup_status.lower()}. No duplicate ticket was created."
                        st.session_state.pending_check = None
                else:
                    with st.spinner("Querying Databricks and asking Ollama..."):
                        db_field, db_rule, conf = compare_with_db(field_name, rule_idea)
                        msg = ask_ollama(field_name, rule_idea, db_rule, conf)
                    st.session_state.agent_reply = msg
                    st.session_state.pending_check = {"field": field_name, "rule": rule_idea, "confidence": conf}
    with scol2:
        if st.session_state.agent_reply:
            st.markdown("**Agent feedback**")
            if st.session_state.pending_check is None:
                st.warning(st.session_state.agent_reply)
            else:
                st.info(st.session_state.agent_reply)
                st.progress(st.session_state.pending_check["confidence"]/100,
                            text=f"Confidence: {st.session_state.pending_check['confidence']}%")
                b1, b2 = st.columns(2)
            if b1.button("✅ Add to Pending", use_container_width=True):
                p = st.session_state.pending_check
                add_human_ticket(p["field"], p["rule"], p["confidence"])
                st.session_state.agent_reply = ""
                st.session_state.pending_check = None
                st.success("Ticket added!")
            if b2.button("✏️ Revise", use_container_width=True):
                st.session_state.agent_reply = ""
                st.session_state.pending_check = None

with tab2:
    def load_validation_runs():
        try:
            conn = get_conn(); cursor = conn.cursor()
            cursor.execute(f"SELECT run_id, run_ts, triggered_by, records_checked, flags_found, rules_ran, report_status FROM {TBL_VAL_RUNS} ORDER BY run_ts DESC LIMIT 10")
            rows = cursor.fetchall(); cursor.close(); conn.close()
            return rows
        except Exception:
            return []

    def load_today_flags():
        try:
            conn = get_conn(); cursor = conn.cursor()
            cursor.execute(f"""
                SELECT field, rule_id, record_count
                FROM {TBL_VAL_FLAGS}
                WHERE run_id = (SELECT run_id FROM {TBL_VAL_RUNS} ORDER BY run_ts DESC LIMIT 1)
            """)
            rows = cursor.fetchall(); cursor.close(); conn.close()
            return [{"field": r[0], "rule": r[1], "count": int(r[2])} for r in rows]
        except Exception:
            return []

    def load_trend_data():
        dates = pd.date_range(end=pd.Timestamp.today(), periods=30)
        vals = [25,24,27,30,28,26,24,22,21,23,20,18,19,17,16,15,14,16,15,14,13,12,11,12,13,12,10,9,8,7]
        return pd.DataFrame({"date": dates, "flags": vals})

    approved_rules = load_tickets("approved")
    runs = load_validation_runs()
    flags = load_today_flags()
    trend = load_trend_data()

    last_run_records = int(runs[0][3]) if runs else 0
    last_run_flags = int(runs[0][4]) if runs else 0

    a, b, c, d = st.columns(4)
    a.metric("Active Rules", len(approved_rules))
    b.metric("Records Checked", f"{last_run_records:,}")
    c.metric("Flags Today", last_run_flags)
    d.metric("Last Run", runs[0][1] if runs else "Never")

    st.markdown("<div class='section-divider'></div>", unsafe_allow_html=True)
    ctrl1, ctrl2, ctrl3 = st.columns([1, 1, 1.4])
    with ctrl1:
        if st.button("Run Validation Now", use_container_width=True):
            run_validations("Manual")
            st.success("Validation completed")
    with ctrl2:
        st.toggle("Daily 6AM Automation", value=True)
    with ctrl3:
        st.text_input("Report Recipient", value="planning-team@company.com")
        st.button("Save", use_container_width=True)

    st.markdown("<div class='section-divider'></div>", unsafe_allow_html=True)
    st.subheader("All Dashboard Items")
    st.dataframe(pd.DataFrame(flags), use_container_width=True, hide_index=True)

    st.markdown("<div class='section-divider'></div>", unsafe_allow_html=True)
    left, right = st.columns([1.1, 1])
    with left:
        st.subheader("Flag Trend — Last 30 Days")
        st.line_chart(trend.set_index("date")["flags"])
    with right:
        st.subheader("Today's Flags")
        flags_df = pd.DataFrame(flags, columns=["field", "rule", "count"])
        st.dataframe(flags_df, use_container_width=True, hide_index=True)

        st.markdown("<div class='section-divider'></div>", unsafe_allow_html=True)
        st.subheader("Audit Log — Run History")
        if runs:
            st.dataframe(pd.DataFrame(runs, columns=["Run ID", "Timestamp", "Triggered By", "Records Checked", "Flags Found", "Rules Ran", "Report Status"]), use_container_width=True, hide_index=True)
        else:
            st.info("No runs yet.")