import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
from pymongo import MongoClient, DESCENDING, ASCENDING
# MongoDB Configuration
DB_NAME = st.secrets['db_name']
WAREHOUSE_INTERVAL = st.secrets.warehouse_interval
WAREHOUSE_INTERVAL_COLLECTION = [f'{interval}_data' for interval in WAREHOUSE_INTERVAL]

@st.cache_resource
def initialize_mongo_client(db_name=DB_NAME):
    client = MongoClient(**st.secrets["mongo"])
    return client[db_name]

@st.cache_data
def fetch_index_return(symbol):
    collection = initialize_mongo_client()[WAREHOUSE_INTERVAL_COLLECTION[0]]
    if not WAREHOUSE_INTERVAL:
        raise ValueError("warehouse_interval is empty in st.secrets")

    # Fetch data from MongoDB for the specified symbol and date range
    df = pd.DataFrame(list(collection.find(
        {"symbol": symbol, "date": {"$gte": pd.to_datetime("2024-01-01T00:00:00Z")}}, 
        {"_id": 0}
    )))

    # Ensure the DataFrame is sorted by date
    df = df.sort_values(by="date")

    # Calculate cumulative returns based on the first available 'close' value
    df['cumulative_return'] = (df['close'] / df['close'].iloc[0]) - 1

    return df
    
st.set_page_config(page_title="Stock Prediction Dashboard", layout="wide")
st.sidebar.success("Select a page to view")

client =  MongoClient(**st.secrets["mongo"])
symbols = client[st.secrets['db_name']][st.secrets.processed_collection_name].distinct("symbol")
st.title("Stock Prediction Dashboard - Not yet implemented :construction:")
st.header("Earning Line Chart YTD")

# Line chart
line_chart = go.Figure()

for symbol in ['QQQ','SPY']:
    data = fetch_index_return(symbol)['cumulative_return']
    line_chart.add_trace(go.Scatter(x=np.arange(1, len(data)), 
                                    y=data, 
                                    mode='lines+markers', 
                                    name=symbol,
                                    marker_line_color="rgba(0,0,0,0.7)",
                                    opacity=0.7))

st.plotly_chart(line_chart)

col1, col2, col3 = st.columns(3)
with col1:
    st.subheader("Buy Signal")
    st.write(np.random.choice(symbols, 2))
with col2:
    st.subheader("Hold Signal")
    st.write(np.random.choice(symbols, 2))
with col3:
    st.subheader("Sell Signal")
    st.write(np.random.choice(symbols, 2))