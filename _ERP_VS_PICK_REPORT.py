import sqlite3
import pandas as pd
import paramiko
from scp import SCPClient
import os

# === CONFIGURATION ===
PICK_AGING_DB_PATH = r"C:\Users\andrew.beattie\AppData\Local\Programs\Python\Python313\Lib\Projects_2\shop_erp_recon\aging_orders\pick_aging.db"
LOCAL_PICKED_CSV = "picked_aging_lines.csv"

ERP_HOST = "cidb1"
ERP_PORT = 22
ERP_USER = "cutsey"
ERP_PASS = "cuts1978"
ERP_PROGRAM = "processShopifyLines.p"
ERP_PROGRAM_DIR = "adb"
ERP_REMOTE_DIR = "/u/live/code/"
ERP_REMOTE_CSV_PATH = f"/home/cutsey/andrewb/python/local/shopify/reports/{LOCAL_PICKED_CSV}"
ERP_OUTPUT_CSV_PATH = f"/home/cutsey/andrewb/python/local/shopify/reports/erp_order_lines.csv"
LOCAL_OUTPUT_CSV = "erp_order_lines.csv"
LOCAL_DB_PATH = PICK_AGING_DB_PATH
TARGET_TABLE = "erp_order_lines_oms_report"

# === Initialize the DB if needed ===
def initialize_db():
    if not os.path.exists(PICK_AGING_DB_PATH):
        print(f"[→] Creating new database: {PICK_AGING_DB_PATH}")
    else:
        print(f"[✓] Database already exists: {PICK_AGING_DB_PATH}")

    conn = sqlite3.connect(PICK_AGING_DB_PATH)
    cursor = conn.cursor()

    # Create picked_aging_report table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS picked_aging_report (
            order_id TEXT,
            order_name TEXT,
            product_id TEXT,
            location_id TEXT,
            order_shipment_line_units INTEGER
        )
    """)

    conn.commit()
    conn.close()
    print("[✓] Database initialized with picked_aging_report table (if not already present)")

# === Export picked aging lines to CSV ===
def export_picked_lines_to_csv():
    conn = sqlite3.connect(LOCAL_DB_PATH)
    query = """
        SELECT DISTINCT 
            order_name, 
            product_id, 
            location_id, 
            order_shipment_line_units
        FROM picked_aging_report
    """
    df = pd.read_sql_query(query, conn)
    df.to_csv(LOCAL_PICKED_CSV, index=False)
    conn.close()
    print(f"[✓] Exported picked aging lines to CSV: {LOCAL_PICKED_CSV}")

# === Push CSV to ERP and run program ===
def push_csv_and_run_erp():
    print("[→] Connecting to ERP server via SSH...")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ERP_HOST, port=ERP_PORT, username=ERP_USER, password=ERP_PASS)
    scp = SCPClient(ssh.get_transport())

    # Upload CSV
    print(f"[→] Uploading CSV to ERP: {ERP_REMOTE_CSV_PATH}")
    scp.put(LOCAL_PICKED_CSV, ERP_REMOTE_CSV_PATH)

    # Run ERP .p program
    erp_cmd = f"cd {ERP_REMOTE_DIR} && /usr/dlc117/bin/mpro -b -pf connect.pf -p {ERP_PROGRAM_DIR}/{ERP_PROGRAM}"
    print("[→] Executing ERP .p script remotely...")
    stdin, stdout, stderr = ssh.exec_command(erp_cmd)

    for line in stdout:
        print("[ERP]", line.strip())
    for err in stderr:
        print("[!] ERP Error:", err.strip())

    # Download ERP output CSV
    print(f"[→] Fetching ERP output CSV: {ERP_OUTPUT_CSV_PATH}")
    scp.get(ERP_OUTPUT_CSV_PATH, LOCAL_OUTPUT_CSV)

    scp.close()
    ssh.close()
    print(f"[✓] ERP output CSV downloaded: {LOCAL_OUTPUT_CSV}")

# === Load ERP CSV into local DB with warehouse normalization and order-date ===
def load_erp_csv_to_db():
    df = pd.read_csv(LOCAL_OUTPUT_CSV)

    # Normalize Warehouse column if it exists
    if 'Warehouse' in df.columns:
        df['Warehouse'] = df['Warehouse'].astype(str).str.replace(r'^AYS', '10', regex=True)
        print("[✓] Normalized Warehouse column (AYS → 10)")

    # Check for OrderDate column
    if 'orderDate' in df.columns or 'OrderDate' in df.columns:
        print("[✓] OrderDate column found and will be stored.")
    else:
        print("[⚠️] OrderDate column NOT found — please verify CSV output from ERP.")

    conn = sqlite3.connect(LOCAL_DB_PATH)
    cursor = conn.cursor()

    # Drop table if it exists
    cursor.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
    conn.commit()

    # Save new data
    df.to_sql(TARGET_TABLE, conn, if_exists='replace', index=False)

    conn.commit()
    conn.close()
    print(f"[✓] ERP data (with OrderDate) loaded into table: {TARGET_TABLE} in {LOCAL_DB_PATH}")

# === Main execution ===
def main():
    initialize_db()
    export_picked_lines_to_csv()
    push_csv_and_run_erp()
    load_erp_csv_to_db()
    print("[✅] Full ERP workflow completed successfully.")

if __name__ == "__main__":
    main()
