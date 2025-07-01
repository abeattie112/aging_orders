import sqlite3
import pandas as pd

# === CONFIG ===
CSV_PATH = r"C:\Users\andrew.beattie\AppData\Local\Programs\Python\Python313\Lib\Projects_2\shop_erp_recon\aging_orders\pick_aging_report.csv"
DB_PATH = r"C:\Users\andrew.beattie\AppData\Local\Programs\Python\Python313\Lib\Projects_2\shop_erp_recon\aging_orders\pick_aging.db"
TABLE_NAME = "picked_aging_report"

# === Load and prepare data ===
df = pd.read_csv(CSV_PATH)

# Select and rename columns
df = df[[ 
    "Order Id", 
    "Order Name", 
    "Product Id", 
    "Location Id", 
    "Order Shipment Line Units in Status"
]].copy()

df.rename(columns={
    "Order Id": "order_id",
    "Order Name": "order_name",
    "Product Id": "product_id",
    "Location Id": "location_id",
    "Order Shipment Line Units in Status": "order_shipment_line_units"
}, inplace=True)

# === Load into SQLite ===
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Drop table if exists
cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")

# Save new table
df.to_sql(TABLE_NAME, conn, index=False)

conn.commit()
conn.close()

print(f"[✓] Loaded data into table: {TABLE_NAME}")
