import os
import time
import sqlite3
import pandas as pd
from datetime import datetime
import paramiko
from scp import SCPClient

# === CONFIGURATION ===
CSV_FILENAME = "erpAgingReport.csv"
LOCAL_CSV_PATH = f"shop_erp_recon/aging_data/{CSV_FILENAME}"
DB_PATH = "mad_recon.db"

ERP_HOST = "cidb1"
ERP_PORT = 22
ERP_USER = "xxxx"
ERP_PASS = "xxxxx"
ERP_PROGRAM = "erpAgingLinesReport.p"
ERP_PROGRAM_DIR = "adp"
ERP_REMOTE_DIR = "/u/live/code/"
ERP_REMOTE_CSV_PATH = f"/home/cutsey/andrewb/python/local/shopify/reports/{CSV_FILENAME}"

# === EXECUTE REMOTE ERP SCRIPT ===
def run_erp_job_and_fetch_csv():
    print("[→] Connecting to ERP server via SSH...")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ERP_HOST, port=ERP_PORT, username=ERP_USER, password=ERP_PASS)
    scp = SCPClient(ssh.get_transport())

    print("[→] Executing ERP .p script remotely...")
    erp_cmd = f"cd {ERP_REMOTE_DIR} && /usr/dlc117/bin/mpro -b -pf connect.pf -p {ERP_PROGRAM_DIR}/{ERP_PROGRAM}"
    stdin, stdout, stderr = ssh.exec_command(erp_cmd)

    for line in stdout:
        print("[ERP]", line.strip())
    for err in stderr:
        print("[!] ERP Error:", err.strip())

    print(f"[→] Fetching CSV file: {ERP_REMOTE_CSV_PATH}")
    scp.get(ERP_REMOTE_CSV_PATH, LOCAL_CSV_PATH)

    scp.close()
    ssh.close()
    print(f"[✓] Downloaded ERP report to: {LOCAL_CSV_PATH}")

# === LOAD CSV INTO SQLITE ===
def load_csv_to_sqlite():
    if not os.path.exists(LOCAL_CSV_PATH):
        print("[X] Local ERP CSV not found.")
        return

    print("[→] Loading CSV into memory...")
    df_new = pd.read_csv(LOCAL_CSV_PATH, header=None, names=[
        "order-number", "bo-number", "warehouse", "cShopifyOrderNumber",
        "cShopifyOrderID", "warehouse-status", "hold-code",
        "lHasFulfillment", "lHasShipment", "iAge"
    ])
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    df_new["timestamp"] = timestamp

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Ensure table exists
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS erp_aging_data (
        timestamp TEXT,
        "order-number" TEXT,
        "bo-number" TEXT,
        "warehouse" TEXT,
        "cShopifyOrderNumber" TEXT,
        "cShopifyOrderID" TEXT,
        "warehouse-status" TEXT,
        "hold-code" TEXT,
        "lHasFulfillment" TEXT,
        "lHasShipment" TEXT,
        "iAge" INTEGER
    )
    """)

    # Fetch all existing order-numbers in DB
    existing_df = pd.read_sql_query("SELECT * FROM erp_aging_data", conn)
    existing_orders = set(existing_df["order-number"].tolist())
    new_orders = set(df_new["order-number"].tolist())

    # 1. DELETE records no longer present in ERP report
    orders_to_delete = existing_orders - new_orders
    if orders_to_delete:
        cursor.executemany(
            "DELETE FROM erp_aging_data WHERE `order-number` = ?",
            [(order,) for order in orders_to_delete]
        )
        print(f"[−] Deleted {len(orders_to_delete)} obsolete orders from DB")

    # 2. UPSERT: insert new or update changed rows
    for _, row in df_new.iterrows():
        # Always delete + insert for simplicity
        cursor.execute("DELETE FROM erp_aging_data WHERE `order-number` = ?", (row["order-number"],))
        cursor.execute("""
            INSERT INTO erp_aging_data (
                timestamp, "order-number", "bo-number", "warehouse",
                "cShopifyOrderNumber", "cShopifyOrderID", "warehouse-status",
                "hold-code", "lHasFulfillment", "lHasShipment", "iAge"
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row["timestamp"], row["order-number"], row["bo-number"], row["warehouse"],
            row["cShopifyOrderNumber"], row["cShopifyOrderID"], row["warehouse-status"],
            row["hold-code"], row["lHasFulfillment"], row["lHasShipment"], row["iAge"]
        ))

    conn.commit()
    conn.close()
    print(f"[✓] ERP data synced. Inserted/updated {len(df_new)} records.")

# === MAIN ===
if __name__ == "__main__":
    run_erp_job_and_fetch_csv()
    load_csv_to_sqlite()
