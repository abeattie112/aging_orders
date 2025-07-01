import requests
import sqlite3
import os

# --- CONFIGURATION ---
SHOP_NAME = "alo-yoga"
ACCESS_TOKEN = "7904b3cc654fa017c25b62f8c16bc6fc"  # Replace with your actual token

GRAPHQL_URL = f"https://{SHOP_NAME}.myshopify.com/admin/api/2024-04/graphql.json"

HEADERS = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": ACCESS_TOKEN
}

DB_NAME = "open_shopify.db"
CURSOR_FILE = "last_cursor.txt"
RESET_CURSOR = False  # Set True to start fresh

# --- GraphQL query function ---
def query_unfulfilled_lines(cursor=None):
    query = """
    query ($cursor: String) {
      orders(first: 250, after: $cursor, query: "created_at:>=2025-01-01 (fulfillment_status:unfulfilled OR fulfillment_status:partial)") {
        edges {
          cursor
          node {
            id
            name
            createdAt
            cancelledAt
            closedAt
            displayFinancialStatus
            metafield(namespace: "FDM4", key: "fdm4_order_number") {
              value
            }
            fulfillmentOrders(first: 10) {
              edges {
                node {
                  id
                  assignedLocation {
                    name
                  }
                  lineItems(first: 20) {
                    edges {
                      node {
                        id
                        lineItem {
                          id
                          name
                          sku
                          quantity
                        }
                        remainingQuantity
                      }
                    }
                  }
                }
              }
            }
          }
        }
        pageInfo {
          hasNextPage
        }
      }
    }
    """
    variables = {"cursor": cursor}
    response = requests.post(GRAPHQL_URL, headers=HEADERS, json={"query": query, "variables": variables}, timeout=20)
    
    if response.status_code != 200:
        print("Error:", response.text)
        return None

    return response.json()

# --- Setup database ---
def setup_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Create table if it doesn't exist — no DROP
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS unfulfilled_lines (
            order_name TEXT,
            order_id TEXT,
            created_at TEXT,
            fdm4_order_number TEXT,
            fulfillment_order_id TEXT,
            assigned_location TEXT,
            line_item_id TEXT,
            line_item_name TEXT,
            sku TEXT,
            ordered_quantity INTEGER,
            quantity_assigned INTEGER,
            PRIMARY KEY (order_id, line_item_id)
        )
    """)
    conn.commit()
    conn.close()

def clear_existing_data():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM unfulfilled_lines")
    conn.commit()
    conn.close()

def insert_data(lines):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    for line in lines:
        cursor.execute("""
            INSERT OR REPLACE INTO unfulfilled_lines (
                order_name, order_id, created_at, fdm4_order_number,
                fulfillment_order_id, assigned_location,
                line_item_id, line_item_name, sku,
                ordered_quantity, quantity_assigned
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            line["Order Name"],
            line["Order ID"],
            line["Created At"],
            line["FDM4 Order Number"],
            line["Fulfillment Order ID"],
            line["Assigned Location"],
            line["Line Item ID"],
            line["Line Item Name"],
            line["SKU"],
            line["Ordered Quantity"],
            line["Quantity Assigned to Fulfillment"]
        ))
    conn.commit()
    conn.close()

# --- Cursor file management ---
def save_cursor(cursor_value):
    with open(CURSOR_FILE, "w") as f:
        f.write(cursor_value)

def load_cursor():
    if os.path.exists(CURSOR_FILE):
        with open(CURSOR_FILE, "r") as f:
            return f.read().strip()
    return None

def clear_cursor():
    if os.path.exists(CURSOR_FILE):
        os.remove(CURSOR_FILE)
        print("[↩] Cursor file cleared. Starting fresh.")

# --- Main execution ---
def main():
    setup_db()

    cursor = load_cursor()
    if not cursor:
        print("[↩] No existing cursor — starting fresh and clearing table.")
        clear_existing_data()
    else:
        print(f"[↩] Existing cursor found: {cursor} — continuing without clearing existing rows.")

    if RESET_CURSOR:
        clear_cursor()
        cursor = None
        print("[↩] Cursor reset requested — table cleared and starting from beginning.")

    if cursor:
        print(f"[↩] Resuming from saved cursor: {cursor}")
    else:
        print("[↩] Starting from the beginning (fresh cursor)")

    page_count = 1
    total_orders = 0
    total_lines = 0
    has_next = True

    while has_next:
        print(f"\n[Batch {page_count}] Fetching orders...")

        data = query_unfulfilled_lines(cursor)
        if not data:
            print("No data returned. Stopping.")
            break

        if "errors" in data:
            print("GraphQL errors:", data["errors"])
            break

        if "data" not in data:
            print("Unexpected response format:", data)
            break

        orders = data["data"]["orders"]["edges"]
        num_orders_in_batch = len(orders)
        total_orders += num_orders_in_batch

        lines_in_batch = 0
        batch_lines = []

        for order_edge in orders:
            order = order_edge["node"]

            # Skip voided financial status
            if order["displayFinancialStatus"].lower() == "voided":
                continue

            # Skip canceled orders
            if order["cancelledAt"]:
                continue

            # Skip archived (closed) orders
            if order["closedAt"]:
                continue

            # Skip fully or partially refunded orders
            if order["displayFinancialStatus"].lower() in ["refunded", "partially_refunded"]:
                continue

            fdm4_order_number = None
            meta = order.get("metafield")
            if meta and meta.get("value"):
                fdm4_order_number = meta["value"]

            gid_order = order["id"].split("/")[-1]
            for fo_edge in order["fulfillmentOrders"]["edges"]:
                fo = fo_edge["node"]
                fo_id = fo["id"].split("/")[-1]
                location = fo["assignedLocation"]
                for li_edge in fo["lineItems"]["edges"]:
                    li = li_edge["node"]

                    # Skip fully fulfilled or removed lines
                    if li["remainingQuantity"] == 0:
                        continue

                    line_item = li["lineItem"]
                    line_item_id = line_item["id"].split("/")[-1]
                    batch_lines.append({
                        "Order Name": order["name"],
                        "Order ID": gid_order,
                        "Created At": order["createdAt"],
                        "FDM4 Order Number": fdm4_order_number,
                        "Fulfillment Order ID": fo_id,
                        "Assigned Location": location["name"] if location else "N/A",
                        "Line Item ID": line_item_id,
                        "Line Item Name": line_item["name"],
                        "SKU": line_item["sku"],
                        "Ordered Quantity": line_item["quantity"],
                        "Quantity Assigned to Fulfillment": li["remainingQuantity"],
                    })
                    lines_in_batch += 1

        total_lines += lines_in_batch

        # Insert batch immediately
        insert_data(batch_lines)

        print(f"[Batch {page_count}] Orders: {num_orders_in_batch}, Lines this batch: {lines_in_batch}, Total lines so far: {total_lines}")

        has_next = data["data"]["orders"]["pageInfo"]["hasNextPage"]
        if has_next:
            cursor = orders[-1]["cursor"]
            save_cursor(cursor)
            page_count += 1
        else:
            # Remove cursor file on completion
            if os.path.exists(CURSOR_FILE):
                os.remove(CURSOR_FILE)
            print("\n[✓] All pages completed. Cursor file cleared.")

    print(f"\n[✓] Inserted total of {total_lines} line items into DB.")
    print(f"[✓] Total orders processed: {total_orders}")

if __name__ == "__main__":
    main()
