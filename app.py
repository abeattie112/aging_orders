import streamlit as st
import sqlite3
import pandas as pd

# === CONFIGURATION ===
DB_PATH = r"C:\Users\andrew.beattie\AppData\Local\Programs\Python\Python313\Lib\Projects_2\pick_aging.db"

# === View mapping ===
VIEW_MAPPING = {
    "Aging vs ERP": "picked_aging_merged_with_erp",
    "Missing in ERP": "picked_aging_merged_missing_in_erp",
    "Shipped From Different Location": "picked_aging_partial_shipped_only",
    "Orders Open Needing Resolution": "picked_aging_remaining_open",
    "Orders to be Cancelled in OMS": "picked_aging_fully_canceled"
}

# === Streamlit UI ===
st.set_page_config(page_title="ERP & Aging Explorer", layout="wide")
st.title("🗂️ ERP & Aging Views Explorer")

selected_view_label = st.selectbox("Select View", options=list(VIEW_MAPPING.keys()))
selected_view_name = VIEW_MAPPING[selected_view_label]

# === Load data from DB ===
@st.cache_data
def load_data(view_name):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(f"SELECT * FROM {view_name}", conn)
    conn.close()
    return df

# === Display data ===
st.subheader(f"View: {selected_view_label}")
df = load_data(selected_view_name)

st.write(f"**Total rows:** {len(df)}")
st.dataframe(df, use_container_width=True)

# Optionally allow CSV download
csv = df.to_csv(index=False).encode("utf-8")
st.download_button("Download CSV", csv, f"{selected_view_name}.csv", "text/csv")

