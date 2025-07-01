import os
import json
import time
import sqlite3
import pandas as pd
import requests
from datetime import datetime

# === CONFIGURATION ===
DB_PATH = "mad_recon.db"
SHOPIFY_ENDPOINT = "https://alo-yoga.myshopify.com/admin/api/2023-10/graphql.json"
SHOPIFY_HEADERS = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": "7904b3cc654fa017c25b62f8c16bc6fc"
}
BATCH_SIZE = 50  # Shopify GraphQL safe batch size
DEBUG_RESET = True  # Toggle to reset Shopify tables

# === SETUP ===
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# === RESET TABLES FOR CLEAN TEST RUN ===
if DEBUG_RESET:
    cursor.execute("DROP TABLE IF EXISTS shopify_orders")
    cursor.execute("DROP TABLE IF EXISTS shopify_parsed_orders")
    conn.commit()
    print("[✓] Dropped Shopify tables for clean test run.")

# === ENSURE TABLES EXIST ===
cursor.execute("""
CREATE TABLE IF NOT EXISTS shopify_orders (
    id TEXT PRIMARY KEY,
    timestamp TEXT,
    raw_json TEXT
)
""")

cursor.execute('''
CREATE TABLE IF NOT EXISTS shopify_parsed_orders (
    shopify_order_id TEXT,
    order_name TEXT,
    financial_status TEXT,
    fulfillment_status TEXT,
    fulfillment_location TEXT,
    timestamp TEXT
)
''')

# === FETCH UNIQUE SHOPIFY ORDER IDS FROM LATEST ERP TIMESTAMP ===
latest_ts_query = "SELECT MAX(timestamp) FROM erp_aging_data"
latest_ts = cursor.execute(latest_ts_query).fetchone()[0]

if not latest_ts:
    print("[!] No ERP data found in database.")
    conn.close()
    exit()

print(f"[✓] Latest ERP timestamp: {latest_ts}")

erp_query = """
SELECT DISTINCT cShopifyOrderID
FROM erp_aging_data
WHERE timestamp = ?
AND cShopifyOrderID IS NOT NULL
"""
order_ids = [row[0] for row in cursor.execute(erp_query, (latest_ts,)).fetchall()]
print(f"[✓] Total unique Shopify Order IDs to query: {len(order_ids)}")

# Skip IDs already processed
cursor.execute("SELECT id FROM shopify_orders")
processed_ids = set(row[0] for row in cursor.fetchall())
unprocessed_ids = [oid for oid in order_ids if f"gid://shopify/Order/{oid}" not in processed_ids]

print(f"[✓] Unprocessed IDs to enrich from Shopify: {len(unprocessed_ids)}")

def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

# === FETCH FROM SHOPIFY ===
for batch in chunk_list(unprocessed_ids, BATCH_SIZE):
    query_parts = []
    for i, oid in enumerate(batch):
        alias = f"order_{i}"
        gid = f"gid://shopify/Order/{oid}"
        query_parts.append(f'''
        {alias}: order(id: "{gid}") {{
            id
            name
            displayFinancialStatus
            fulfillments {{
                status
                location {{
                    name
                }}
            }}
        }}
        ''')

    query = f'''
    query {{
        {"".join(query_parts)}
    }}
    '''

    try:
        response = requests.post(
            SHOPIFY_ENDPOINT,
            headers=SHOPIFY_HEADERS,
            json={"query": query},
            timeout=20
        )
        print(f"[→] Shopify GraphQL batch call (IDs: {batch}) HTTP {response.status_code}")
        data = response.json()

        for alias, order_data in data.get("data", {}).items():
            if order_data:
                gid = order_data["id"]
                cursor.execute("""
                    INSERT OR REPLACE INTO shopify_orders (id, timestamp, raw_json)
                    VALUES (?, ?, ?)
                """, (gid, timestamp, json.dumps(order_data)))
                print(f"[DEBUG] Inserted Shopify order: {gid}")
        conn.commit()
        time.sleep(1)

    except Exception as e:
        print(f"[!] Shopify batch error: {e}")
        continue

print("[✓] Shopify enrichment complete.")

# === DEBUG: Confirm what was written ===
cursor.execute("SELECT COUNT(*) FROM shopify_orders")
print(f"[DEBUG] Total shopify_orders in DB: {cursor.fetchone()[0]}")

cursor.execute("SELECT DISTINCT timestamp FROM shopify_orders")
print(f"[DEBUG] Timestamps found in shopify_orders: {[row[0] for row in cursor.fetchall()]}")

# === PARSE SHOPIFY JSON TO FLAT TABLE ===
cursor.execute("SELECT id, timestamp, raw_json FROM shopify_orders WHERE timestamp = ?", (timestamp,))
rows = cursor.fetchall()
parsed_count = 0

for shopify_id, ts, raw_json in rows:
    try:
        parsed = json.loads(raw_json)
        gid = parsed.get("id", "")
        order_id = gid.split("/")[-1] if "gid://" in gid else gid
        order_name = parsed.get("name", "")
        financial_status = parsed.get("displayFinancialStatus", "")
        fulfillments = parsed.get("fulfillments", [])

        if not fulfillments:
            cursor.execute('''
                INSERT INTO shopify_parsed_orders
                (shopify_order_id, order_name, financial_status, fulfillment_status, fulfillment_location, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (order_id, order_name, financial_status, None, None, ts))
        else:
            for f in fulfillments:
                status = f.get("status")
                loc_name = f.get("location", {}).get("name", "")
                cursor.execute('''
                    INSERT INTO shopify_parsed_orders
                    (shopify_order_id, order_name, financial_status, fulfillment_status, fulfillment_location, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (order_id, order_name, financial_status, status, loc_name, ts))
        parsed_count += 1

    except Exception as e:
        print(f"[!] Failed to parse row for {shopify_id}: {e}")

conn.commit()
conn.close()
print(f"[✓] Parsed and inserted {parsed_count} Shopify orders into shopify_parsed_orders.")
