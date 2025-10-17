# dashboard_app.py
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pymongo import MongoClient

# ----------------------------
# CONNECT TO MONGODB
# ----------------------------
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "popups_analytics"
COLLECTION = "stall_kpis"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
data = list(db[COLLECTION].find())
df = pd.DataFrame(data)

# ----------------------------
# DASHBOARD UI
# ----------------------------
st.set_page_config(page_title="Pop-Up Market Dashboard", layout="wide")
st.title("🏪 Pop-Up Market Analytics Dashboard")
st.subheader("Stall KPIs Overview")

# Show raw data
st.write("### Raw Data")
st.dataframe(df)

# ----------------------------
# KPIs summary
# ----------------------------
st.write("### Key Metrics")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Stalls", df['stall_id'].nunique())
col2.metric("Total Transactions", df['num_transactions'].sum())
col3.metric("Total Sales", df['total_sales'].sum())
col4.metric("Total Visitors", df['total_visitors'].sum())

# ----------------------------
# Charts
# ----------------------------
st.write("### Sales per Stall")
plt.figure(figsize=(10,5))
sns.barplot(x='stall_id', y='total_sales', data=df)
plt.xticks(rotation=45)
st.pyplot(plt)

st.write("### Avg Transaction Amount per Stall")
plt.figure(figsize=(10,5))
sns.barplot(x='stall_id', y='avg_tx_amount', data=df)
plt.xticks(rotation=45)
st.pyplot(plt)

st.write("### Total Visitors per Stall")
plt.figure(figsize=(10,5))
sns.barplot(x='stall_id', y='total_visitors', data=df)
plt.xticks(rotation=45)
st.pyplot(plt)