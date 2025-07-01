import sqlite3
import json

DB_PATH = r"C:\Users\andrew.beattie\AppData\Local\Programs\Python\Python313\Lib\Projects_2\mad_recon.db"

def parse_and_store_shopify_json():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Create parsed table (one row per location per order)
    cur.execute('''
    CREATE TABLE IF NOT EXISTS shopify_parsed_orders (
        shopify_order_id TEXT,
        order_name TEXT,
        financial_status TEXT,
        fulfillment_status TEXT,
        fulfillment_location TEXT,
        timestamp TEXT
    )
    ''')

    # Read from raw shopify table
    cur.execute("SELECT id, timestamp, raw_json FROM shopify_orders")
    rows = cur.fetchall()

    for shopify_id, timestamp, json_data in rows:
        try:
            parsed = json.loads(json_data)
            gid = parsed.get("id", "")
            order_id = gid.split("/")[-1] if "gid://" in gid else gid
            order_name = parsed.get("name", "")
            financial_status = parsed.get("displayFinancialStatus", "")
            fulfillments = parsed.get("fulfillments", [])

            if not fulfillments:
                # Insert a single row if no fulfillments exist
                cur.execute('''
                    INSERT INTO shopify_parsed_orders
                    (shopify_order_id, order_name, financial_status, fulfillment_status, fulfillment_location, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (order_id, order_name, financial_status, None, None, timestamp))
            else:
                for f in fulfillments:
                    fulfillment_status = f.get("status")
                    location_name = f.get("location", {}).get("name", "")
                    cur.execute('''
                        INSERT INTO shopify_parsed_orders
                        (shopify_order_id, order_name, financial_status, fulfillment_status, fulfillment_location, timestamp)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (order_id, order_name, financial_status, fulfillment_status, location_name, timestamp))

        except Exception as e:
            print(f"[!] Failed to parse row for {shopify_id}: {e}")

    conn.commit()
    conn.close()
    print("[✓] Parsed Shopify data saved to shopify_parsed_orders table.")

if __name__ == "__main__":
    parse_and_store_shopify_json()
