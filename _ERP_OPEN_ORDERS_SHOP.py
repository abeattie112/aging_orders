import os
import json
import requests
import paramiko
from scp import SCPClient
import pandas as pd

# === CONFIGURATION ===
RUN_FULL_PROCESS = False  # <<< Set to True to run ERP program remotely

ERP_HOST = "cidb1"
ERP_PORT = 22
ERP_USER = "cutsey"
ERP_PASS = "cuts1978"
ERP_PROGRAM = "erpAgingReport.p"
ERP_PROGRAM_DIR = "bhm"
ERP_REMOTE_DIR = "/u/live/code/"
ERP_OUTPUT_FILE = "erpAgingReport.csv"
REMOTE_CSV_PATH = f"/home/cutsey/bradm/erpAgingReport/reports/{ERP_OUTPUT_FILE}"

LOCAL_DIR = os.path.join(os.path.dirname(__file__), "aging_data")
os.makedirs(LOCAL_DIR, exist_ok=True)
LOCAL_CSV_PATH = os.path.join(LOCAL_DIR, ERP_OUTPUT_FILE)

SHOPIFY_ENDPOINT = "https://alo-yoga.myshopify.com/admin/api/2023-10/graphql.json"
SHOPIFY_HEADERS = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": "7904b3cc654fa017c25b62f8c16bc6fc"
}


def query_shopify_batch(order_ids):
    results = {}
    for shopify_id in order_ids:
        gid = f"gid://shopify/Order/{shopify_id}"
        query = '''
        query getOrder($id: ID!) {
          order(id: $id) {
            id
            name
            displayFinancialStatus
            fulfillments {
              status
              location {
                name
              }
            }
          }
        }
        '''
        variables = {"id": gid}
        response = requests.post(
            SHOPIFY_ENDPOINT,
            headers=SHOPIFY_HEADERS,
            json={"query": query, "variables": variables}
        )

        print(f"[→] Shopify call for {shopify_id}: HTTP {response.status_code}")

        if response.status_code != 200:
            print(f"[!] Shopify API error for ID {shopify_id}: {response.status_code}")
            continue

        data = response.json()
        order_data = data.get("data", {}).get("order")
        if order_data:
            results[shopify_id] = {
                "shopify_status": ", ".join(
                    f.get("status", "") for f in order_data.get("fulfillments", [])
                ),
                "shopify_location": ", ".join(
                    f.get("location", {}).get("name", "")
                    for f in order_data.get("fulfillments", []) if f.get("location")
                ),
                "shopify_order_id": order_data.get("id")
            }
        else:
            print(f"[!] Order not found for ID {shopify_id}")
    return results


def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def main():
    # === SSH & SCP SETUP ===
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ERP_HOST, port=ERP_PORT, username=ERP_USER, password=ERP_PASS)
    scp = SCPClient(ssh.get_transport())

    if RUN_FULL_PROCESS:
        erp_cmd = f"cd {ERP_REMOTE_DIR} && /usr/dlc117/bin/mpro -b -pf connect.pf -p {ERP_PROGRAM_DIR}/{ERP_PROGRAM}"
        print("[→] Running:", erp_cmd)
        stdin, stdout, stderr = ssh.exec_command(erp_cmd)
        for line in stdout:
            print(line.strip())
        for err in stderr:
            print("[!] Error:", err.strip())
    else:
        print("[~] Skipping ERP execution. Pulling latest CSV only.")

    try:
        scp.get(REMOTE_CSV_PATH, LOCAL_CSV_PATH)
        print(f"[✓] Downloaded ERP CSV to: {LOCAL_CSV_PATH}")
    except Exception as e:
        print("[X] Could not download ERP report:", str(e))
        return

    scp.close()
    ssh.close()

    # === LOAD CSV ===
    erp_columns = [
        "order-number", "bo-number", "warehouse", "cShopifyOrderNumber",
        "cShopifyOrderID", "warehouse-status", "hold-code",
        "lHasFulfillment", "lHasShipment", "iAge"
    ]
    df = pd.read_csv(LOCAL_CSV_PATH, names=erp_columns, header=None)
    print("[DEBUG] Columns in ERP CSV:", df.columns.tolist())

    if "cShopifyOrderID" not in df.columns:
        print("[X] Missing cShopifyOrderID column.")
        return

    id_col = "cShopifyOrderID"
    order_ids = df[id_col].dropna().astype(str).unique().tolist()
    print(f"[→] Found {len(order_ids)} unique Shopify IDs")

    shopify_data = {}
    for i, batch in enumerate(chunk_list(order_ids, 50)):
        print(f"[→] Processing batch {i+1}: {len(batch)} orders")
        shopify_data.update(query_shopify_batch(batch))

    # === ENRICH ===
    enriched = []
    for _, row in df.iterrows():
        sid = str(row.get("cShopifyOrderID", "")).strip()
        match = shopify_data.get(sid)
        if match:
            row["shopify_status"] = match.get("shopify_status")
            row["shopify_location"] = match.get("shopify_location")
            row["shopify_order_id"] = match.get("shopify_order_id")
        else:
            row["shopify_status"] = "NOT FOUND"
            row["shopify_location"] = ""
            row["shopify_order_id"] = ""
        enriched.append(row)

    enriched_df = pd.DataFrame(enriched)
    output_file = os.path.join(LOCAL_DIR, "erp_aging_report_enriched.csv")
    enriched_df.to_csv(output_file, index=False)
    print(f"[✓] Enriched CSV written to: {output_file}")


if __name__ == "__main__":
    main()
