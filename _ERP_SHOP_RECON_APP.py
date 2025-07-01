import streamlit as st
import sqlite3
import pandas as pd
import os

# Path to SQLite database
DB_PATH = "../../mad_recon.db"

# App layout
st.set_page_config(page_title="MAD Recon Viewer", layout="wide")
st.title("🧮 MAD Recon Data Explorer")

# Check database existence
if not os.path.exists(DB_PATH):
    st.error("Database file 'mad_recon.db' not found.")
    st.stop()

# Connect to DB
conn = sqlite3.connect(DB_PATH)

# Layout tabs
tab1, tab2 = st.tabs(["ERP Aging Report", "Shopify Data"])

with tab1:
    st.header("ERP Aging Report")
    try:
        erp_df = pd.read_sql_query("SELECT * FROM erp_aging_data ORDER BY timestamp DESC", conn)
        st.dataframe(erp_df, use_container_width=True)
    except Exception as e:
        st.error(f"Failed to load ERP data: {e}")

with tab2:
    st.header("Shopify Orders (Parsed)")
    try:
        shopify_df = pd.read_sql_query("SELECT * FROM shopify_parsed_orders ORDER BY timestamp DESC", conn)
        st.dataframe(shopify_df, use_container_width=True)
    except Exception as e:
        st.error(f"Failed to load Shopify parsed data: {e}")

conn.close()
