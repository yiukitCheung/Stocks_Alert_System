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
WAREHOUSE_INTERVAL_COLLECTION = '1d_data'
PROCESSED_COLLECTION = st.secrets.processed_collection_name

@st.cache_resource
def initialize_mongo_client(db_name=DB_NAME):
    client = MongoClient(**st.secrets["mongo"])
    return client[db_name]

@st.cache_data
def fetch_index_return(symbol):
    collection = initialize_mongo_client()[WAREHOUSE_INTERVAL_COLLECTION]
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
    
    # Define the query to fetch stocks where the close price is above both the 13EMA and 169EMA for intervals 3D, 6D, and 13D
    query = {
        "date": {"$gte": pd.to_datetime("2024-10-11T00:00:00Z")},
        "interval": {"$in": ["1D", "3D", "6D", "13D"]},
        "$expr": {
            "$and": [
                {"$gt": ["$close", "$13EMA"]},
                {"$gt": ["$close", "$169EMA"]}
            ]
        }
    }

    # Execute the query and project only the symbol field
    results = initialize_mongo_client()[PROCESSED_COLLECTION].distinct("symbol", query)
    
    st.subheader("Buy Signal")
    if not results:
        st.write("No results found")
    else:
        # Display the symbols in a styled format
        st.markdown("""
            <style>
                .buy-container {
                    display: flex;
                    flex-wrap: wrap;
                    gap: 10px;
                }
                .buy-badge {
                    background-color: #4CAF50;
                    color: white;
                    padding: 8px 12px;
                    border-radius: 5px;
                    font-size: 16px;
                    text-align: center;
                }
            </style>
        """, unsafe_allow_html=True)

        st.markdown('<div class="buy-container">', unsafe_allow_html=True)
        for symbol in results:
            st.markdown(f'<div class="buy-badge">{symbol}</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
        
with col2:
# Define the query to fetch stocks where the close price is above both the 13EMA and 169EMA for intervals 3D, 6D, and 13D
    query = {
        "date": {"$gte": pd.to_datetime("2024-10-11T00:00:00Z")},
        "interval": {"$in": ["1D","3D","6D", "13D"]},
        "$expr": {
            "$and": [
                {"$lt": ["$close", "$13EMA"]},
                {"$gt": ["$close", "$169EMA"]}
            ]
        }
    }

    # Execute the query and project only the symbol field
    results = initialize_mongo_client()[PROCESSED_COLLECTION].distinct("symbol", query)
    
    st.subheader("Hold Signal")
    if not results:
        st.write("No results found")
    else:
        # Display the symbols in a styled format
        st.markdown("""
            <style>
                .symbol-container {
                    display: flex;
                    flex-wrap: wrap;
                    gap: 10px;
                }
                .symbol-badge {
                    background-color: #A9A9A9;
                    color: white;
                    padding: 8px 12px;
                    border-radius: 5px;
                    font-size: 16px;
                    text-align: center;
                }
            </style>
        """, unsafe_allow_html=True)

        st.markdown('<div class="symbol-container">', unsafe_allow_html=True)
        for symbol in results:
            st.markdown(f'<div class="symbol-badge">{symbol}</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
        
with col3:
    # Define the query to fetch stocks where the close price is above both the 13EMA and 169EMA for intervals 3D, 6D, and 13D
    query = {
        "date": {"$gte": pd.to_datetime("2024-10-11T00:00:00Z")},
        "interval": {"$in": ["1D", "3D"]},
        "$expr": {
            "$or": [
                {"$lt": ["$close", "$13EMA"]},
                {"$lt": ["$close", "$169EMA"]}
            ]
        }
    }

    # Execute the query and project only the symbol field
    results = initialize_mongo_client()[PROCESSED_COLLECTION].distinct("symbol", query)
    st.subheader("Sell Signal")
    if not results:
        st.write("No results found")
    else:
        # Display the symbols in a styled format
        st.markdown("""
            <style>
                .sell-container {
                    display: flex;
                    flex-wrap: wrap;
                    gap: 10px;
                }
                .sell-badge {
                    background-color: #FF0000;
                    color: white;
                    padding: 8px 12px;
                    border-radius: 5px;
                    font-size: 16px;
                    text-align: center;
                }
            </style>
        """, unsafe_allow_html=True)

        st.markdown('<div class="sell-container">', unsafe_allow_html=True)
        for symbol in results:
            st.markdown(f'<div class="sell-badge">{symbol}</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)