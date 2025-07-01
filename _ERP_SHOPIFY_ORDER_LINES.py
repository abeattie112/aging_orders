import sqlite3
import pandas as pd
import paramiko
from scp import SCPClient

# === CONFIGURATION ===
SHOPIFY_DB_PATH = r"C:\Users\andrew.beattie\AppData\Local\Programs\Python\Python313\Lib\Projects_2\open_shopify.db"
LOCAL_SHOPIFY_CSV = "shopify_unfulfilled_lines.csv"

ERP_HOST = "cidb1"
ERP_PORT = 22
ERP_USER = "cutsey"
ERP_PASS = "cuts1978"
ERP_PROGRAM = "processShopifyLines.p"
ERP_PROGRAM_DIR = "adb"
ERP_REMOTE_DIR = "/u/live/code/"
ERP_REMOTE_CSV_PATH = f"/home/cutsey/andrewb/python/local/shopify/reports/{LOCAL_SHOPIFY_CSV}"
ERP_OUTPUT_CSV_PATH = f"/home/cutsey/andrewb/python/local/shopify/reports/erp_order_lines.csv"
LOCAL_OUTPUT_CSV = "erp_order_lines.csv"
LOCAL_DB_PATH = r"C:\Users\andrew.beattie\AppData\Local\Programs\Python\Python313\Lib\Projects_2\open_shopify.db"

# === Export Shopify order lines to CSV ===
def export_shopify_lines_to_csv():
    conn = sqlite3.connect(SHOPIFY_DB_PATH)
    query = "SELECT DISTINCT order_name, sku FROM unfulfilled_lines"
    df = pd.read_sql_query(query, conn)
    df.to_csv(LOCAL_SHOPIFY_CSV, index=False)
    conn.close()
    print(f"[✓] Exported Shopify lines to CSV: {LOCAL_SHOPIFY_CSV}")

# === Push CSV to ERP and run program ===
def push_csv_and_run_erp():
    print("[→] Connecting to ERP server via SSH...")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ERP_HOST, port=ERP_PORT, username=ERP_USER, password=ERP_PASS)
    scp = SCPClient(ssh.get_transport())

    # Upload CSV
    print(f"[→] Uploading CSV to ERP: {ERP_REMOTE_CSV_PATH}")
    scp.put(LOCAL_SHOPIFY_CSV, ERP_REMOTE_CSV_PATH)

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

    # Delete remote CSVs
    ssh.exec_command(f"rm -f {ERP_REMOTE_CSV_PATH} {ERP_OUTPUT_CSV_PATH}")
    print(f"[🗑️] Deleted remote CSVs: {ERP_REMOTE_CSV_PATH}, {ERP_OUTPUT_CSV_PATH}")

    scp.close()
    ssh.close()
    print(f"[✓] ERP output CSV downloaded: {LOCAL_OUTPUT_CSV}")

# === Load ERP CSV into local DB ===
def load_erp_csv_to_db():
    df = pd.read_csv(LOCAL_OUTPUT_CSV)

    conn = sqlite3.connect(LOCAL_DB_PATH)
    cursor = conn.cursor()

    # Delete all rows (keeps table structure)
    cursor.execute("DELETE FROM erp_order_lines")
    conn.commit()

    # Insert new data
    df.to_sql("erp_order_lines", conn, if_exists='append', index=False)

    conn.commit()
    conn.close()
    print(f"[✓] ERP data loaded into table: erp_order_lines in {LOCAL_DB_PATH}")


# === Main execution ===
def main():
    export_shopify_lines_to_csv()
    push_csv_and_run_erp()
    load_erp_csv_to_db()
    print("[✅] Full ERP workflow completed successfully.")

if __name__ == "__main__":
    main()
