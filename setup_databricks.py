import random, csv, io, requests
from datetime import datetime, timedelta
from databricks import sql as dbsql

DATABRICKS_HOST  = "dbc-6c5d05f5-fce9.cloud.databricks.com"
DATABRICKS_PATH  = "/sql/1.0/warehouses/96dabf34bd066df3"
DATABRICKS_TOKEN = "dapi5dc60a6525fa7d86116ec57093f7e60a"
VOLUME_PATH      = "/Volumes/wieland/data/uploads"
CATALOG          = "wieland"
SCHEMA           = "data"
TBL_BOM          = f"{CATALOG}.{SCHEMA}.bom_master"
TBL_RTG          = f"{CATALOG}.{SCHEMA}.routing_master"
TBL_RULES        = f"{CATALOG}.{SCHEMA}.master_data_rules"

START_DATE = datetime(2024, 1, 1)
DAYS       = (datetime(2026, 7, 1) - START_DATE).days

def rand_date():
    return (START_DATE + timedelta(days=random.randint(0, DAYS))).strftime("%Y-%m-%d")

def rand_user():
    return random.choice(["JSMITH","MRUHL","ABAUER","TCOOK","LMEYER","ADMIN01"])

def connect():
    return dbsql.connect(server_hostname=DATABRICKS_HOST,
                         http_path=DATABRICKS_PATH, access_token=DATABRICKS_TOKEN)

def make_csv(rows, headers):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(headers)
    w.writerows(rows)
    return buf.getvalue().encode("utf-8")

def generate_bom(n=2000):
    plants = ["DE01","DE02","US01","CN01","PL01"]
    rows = []
    for i in range(n):
        werks = random.choice(plants)
        rows.append([
            str(i+1), rand_date(),
            f"MAT-{random.randint(1000,9999)}", werks,
            f"ALT-{random.randint(1,5)}", f"BOM-{random.randint(100,999)}",
            random.choice(["1","1","1","7","7","X"]),
            random.choice(["","","","","X"]),
            rand_date(),
            random.choice(["9999-12-31","9999-12-31","9999-12-31","2025-12-31","2024-06-30"]),
            rand_user(), rand_user(), rand_date(),
            f"COMP-{random.randint(100,999)}",
            random.choice(["","","","X"]),
            str(round(random.uniform(0.1,100),2)),
            f"Plant {werks}",
        ])
    return rows

def generate_routing(n=2000):
    plants = ["DE01","DE02","US01","CN01","PL01"]
    rows = []
    for i in range(n):
        rows.append([
            str(i+1), rand_date(),
            f"MAT-{random.randint(1000,9999)}", f"RTG-{random.randint(100,999)}",
            random.choice(["N","N","N","R","M"]),
            random.choice(plants), rand_date(),
            random.choice(["9999-12-31","9999-12-31","9999-12-31","2025-06-30"]),
            random.choice(["","","","","X"]),
            rand_user(), rand_user(), rand_date(),
            f"OP-{random.randint(10,99)}",
            random.choice(["ZM01","ZM02","PP01",""]),
            str(round(random.uniform(0.5,8.0),2)),
            random.choice(["1","1","1","7"]),
            f"VEND-{random.randint(100,999)}" if random.random() > 0.7 else "",
        ])
    return rows

def upload_csv(csv_bytes, filename):
    url = f"https://{DATABRICKS_HOST}/api/2.0/fs/files{VOLUME_PATH}/{filename}"
    r = requests.put(url, data=csv_bytes,
                     headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}",
                               "Content-Type": "application/octet-stream"})
    r.raise_for_status()
    print(f"  Uploaded {filename} ({len(csv_bytes)//1024} KB)")

BOM_HEADERS = ["record_id","record_date","matnr","werks","stlal","stlnr","stlst",
               "deletion_flag","valid_from","valid_to","created_by","changed_by",
               "changed_on","component_id","comp_deletion","base_qty","plant_name"]
RTG_HEADERS = ["record_id","record_date","matnr","plnnr","plnty","werks","valid_from",
               "valid_to","deletion_flag","created_by","changed_by","changed_on",
               "operation_no","control_key","std_value","status","supplier"]

conn   = connect()
cursor = conn.cursor()

print("Creating schema and tables...")
cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {TBL_BOM} (
        record_id STRING, record_date STRING, matnr STRING, werks STRING,
        stlal STRING, stlnr STRING, stlst STRING, deletion_flag STRING,
        valid_from STRING, valid_to STRING, created_by STRING, changed_by STRING,
        changed_on STRING, component_id STRING, comp_deletion STRING,
        base_qty STRING, plant_name STRING)""")
cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {TBL_RTG} (
        record_id STRING, record_date STRING, matnr STRING, plnnr STRING,
        plnty STRING, werks STRING, valid_from STRING, valid_to STRING,
        deletion_flag STRING, created_by STRING, changed_by STRING,
        changed_on STRING, operation_no STRING, control_key STRING,
        std_value STRING, status STRING, supplier STRING)""")
cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {TBL_RULES} (
        id STRING, field STRING, rule STRING, confidence STRING,
        pattern_type STRING, evidence_count STRING,
        source STRING, status STRING, created_at STRING)""")
print("Tables created.")

cursor.execute(f"SELECT COUNT(*) FROM {TBL_BOM}")
if cursor.fetchone()[0] == 0:
    print("Uploading BOM data...")
    upload_csv(make_csv(generate_bom(), BOM_HEADERS), "bom_master.csv")
    cursor.execute(f"""
        COPY INTO {TBL_BOM} FROM '{VOLUME_PATH}/bom_master.csv'
        FILEFORMAT = CSV
        FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')
        COPY_OPTIONS ('mergeSchema' = 'false')""")
    print("bom_master: 2000 rows loaded.")
else:
    print("bom_master already seeded. Skipping.")

cursor.execute(f"SELECT COUNT(*) FROM {TBL_RTG}")
if cursor.fetchone()[0] == 0:
    print("Uploading Routing data...")
    upload_csv(make_csv(generate_routing(), RTG_HEADERS), "routing_master.csv")
    cursor.execute(f"""
        COPY INTO {TBL_RTG} FROM '{VOLUME_PATH}/routing_master.csv'
        FILEFORMAT = CSV
        FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')
        COPY_OPTIONS ('mergeSchema' = 'false')""")
    print("routing_master: 2000 rows loaded.")
else:
    print("routing_master already seeded. Skipping.")

cursor.close()
conn.close()