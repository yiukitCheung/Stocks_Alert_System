import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
from pymongo import MongoClient, DESCENDING, ASCENDING
import sys, os
# Ensure the correct path to the 'data' directory

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'data'))
from utils.trading_strategy import DailyTradingStrategy, Pick_Stock

# MongoDB Configuration
DB_NAME = st.secrets['db_name']
WAREHOUSE_INTERVAL = st.secrets.warehouse_interval
WAREHOUSE_INTERVAL_COLLECTION = '1d_data'
PROCESSED_COLLECTION = st.secrets.processed_collection_name
ALERT_COLLECTION = st.secrets.alert_collection_name

def initialize_mongo_client():
    client = MongoClient(**st.secrets["mongo"])
    return client

@st.cache_data
def fetch_index_return(symbol):
    collection = initialize_mongo_client()[DB_NAME][WAREHOUSE_INTERVAL_COLLECTION]
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
    df['cumulative_return'] = df['close'].pct_change().fillna(0).add(1).cumprod()
    return df

def fetch_portfolio_return(trades_data):
    trades_data = pd.DataFrame(trades_data)
    
def get_trade_data(data_collection, alert_collection):
    stock_candidates = Pick_Stock(alert_collection).run()
    trades_history = DailyTradingStrategy(data_collection, alert_collection, stock_candidates)
    trades_history.execute_critical_trades()
    return trades_history.get_trades()

@st.cache_data
def compute_metrics(filtered_trades):
    # Verify required columns are present
    if 'profit/loss' not in filtered_trades.columns or 'total_asset' not in filtered_trades.columns:
        raise ValueError("The DataFrame must contain 'profit/loss' and 'total_asset' columns.")

    # Clean and convert profit/loss column to numeric decimal
    profit = filtered_trades['profit/loss'].str.replace('%', '').astype(float) / 100
    # Calculate the number of winning and losing trades
    win_trades = (profit > 0).sum()
    loss_trades = (profit <= 0).sum()

    # Calculate final trade profit rate
    final_trade_profit_rate = (filtered_trades['total_asset'].iloc[-1] - 10000) / 100

    return [win_trades, loss_trades, final_trade_profit_rate]
    
st.set_page_config(page_title="Stock Prediction Dashboard", layout="wide")
st.sidebar.success("Select a page to view")

client =  MongoClient(**st.secrets["mongo"])
symbols = client[st.secrets['db_name']][st.secrets.processed_collection_name].distinct("symbol")
st.title("Stock Prediction Dashboard")
st.header("Earning Line Chart YTD")

# Initialize the figure for the line chart
line_chart = go.Figure()

# Loop through the symbols (e.g., 'QQQ', 'SPY') to fetch index returns and add them to the plot
for symbol in ['QQQ', 'SPY']:
    data = fetch_index_return(symbol)[['cumulative_return', 'date']]
    line_chart.add_trace(go.Scatter(
        x=data['date'], 
        y=data['cumulative_return'],  # Ensure you use 'cumulative_return' as the y-axis
        mode='lines+markers', 
        name=symbol,
        marker_line_color="rgba(0,0,0,0.7)",
        opacity=0.7
    ))

# Initialize MongoDB client and fetch the necessary data
client = initialize_mongo_client()
symbols = client[st.secrets['db_name']][st.secrets['processed_collection_name']].distinct("symbol")

# Fetch trade data
df_trades = get_trade_data(client[DB_NAME][PROCESSED_COLLECTION],
                        client[DB_NAME][ALERT_COLLECTION])

# Compute metrics (e.g., win rate, loss rate, final profit rate)
win_trades, loss_trades, final_trade_profit_rate = compute_metrics(df_trades)

# Create columns in Streamlit for display (if using Streamlit)
col1, col2, col3 = st.columns(3, gap='medium')

# Ensure 'Exit_date' is in datetime format
df_trades['Exit_date'] = pd.to_datetime(df_trades['Exit_date'])

# Compute cumulative return of trades
df_trades['cumulative_return'] = df_trades['total_asset'].pct_change().fillna(0).add(1).cumprod()

# Add the cumulative return of the trades to the line chart
line_chart.add_trace(go.Scatter(
    x=df_trades['Exit_date'],
    y=df_trades['cumulative_return'],
    mode='lines+markers',
    name='Portfolio',
    marker_line_color="rgba(0,0,0,0.7)",
    opacity=0.7
))

# CSS styling for metric containers
st.markdown("""
    <style>
    .metric-container {
        background-color: #f5f5f5;
        border-radius: 8px;
        padding: 20px;
        box-shadow: 0px 2px 4px rgba(0, 0, 0, 0.1);
        display: flex;
        justify-content: center;
        align-items: center;
        flex-direction: column;
        text-align: center;
        height: 100%;
    }
    .metric-label {
        font-weight: bold;
        font-size: 24px;
        margin: 0;
    }
    .metric-value {
        font-size: 24px;
        font-weight: bold;
        margin-top: 8px;
    }
    </style>
""", unsafe_allow_html=True)

# Determine the color of the profit value based on its sign
profit_color = "green" if final_trade_profit_rate > 0 else "red"

with col1:
    st.markdown(f"""
        <div class="metric-container">
            <h3 class="metric-label">Final Trade Profit</h3>
            <p class="metric-value" style="color: {profit_color};">{final_trade_profit_rate}%</p>
        </div>
    """, unsafe_allow_html=True)
        
with col2:
    st.markdown(f"""
        <div class="metric-container">
            <h3 class="metric-label">Win Trades</h3>
            <p class="metric-value" style="color: green;">{win_trades}</p>
        </div>
    """, unsafe_allow_html=True)
        
with col3:
    st.markdown(f"""
        <div class="metric-container">
            <h3 class="metric-label">Loss Trades</h3>
            <p class="metric-value" style="color: red;">{loss_trades}</p>
        </div>
    """, unsafe_allow_html=True)

st.plotly_chart(line_chart)

col1, col2, col3 = st.columns(3)

with col1:
    query = {
        "date": {"$gte": pd.to_datetime("2024-10-11T00:00:00Z")},
        "interval": {"$in": [1,3,6,13]},
        "$expr": {
            "$and": [
                {"$gt": ["$close", "$13EMA"]},
                {"$gt": ["$close", "$169EMA"]}
            ]
        }
    }
    results = initialize_mongo_client()[DB_NAME][PROCESSED_COLLECTION].distinct("symbol", query)

    st.subheader("Buy Signal")
    if not results:
        st.write("No results found")
    else:
        # Add Buy specific styles
        st.markdown("""
            <style>
                .buy-container {
                    display: flex;
                    flex-wrap: wrap;
                    gap: 10px;
                }
                .buy-badge {
                    background-color: #4CAF50 !important;
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
    query = {
        "date": {"$gte": pd.to_datetime("2024-10-11T00:00:00Z")},
        "interval": {"$in": [1,3,6,13]},
        "$expr": {
            "$and": [
                {"$lt": ["$close", "$13EMA"]},
                {"$gt": ["$close", "$169EMA"]}
            ]
        }
    }
    results = initialize_mongo_client()[DB_NAME][PROCESSED_COLLECTION].distinct("symbol", query)

    st.subheader("Hold Signal")
    if not results:
        st.write("No results found")
    else:
        # Add Hold specific styles
        st.markdown("""
            <style>
                .hold-container {
                    display: flex;
                    flex-wrap: wrap;
                    gap: 10px;
                }
                .hold-badge {
                    background-color: #A9A9A9 !important;
                    color: white;
                    padding: 8px 12px;
                    border-radius: 5px;
                    font-size: 16px;
                    text-align: center;
                }
            </style>
        """, unsafe_allow_html=True)

        st.markdown('<div class="hold-container">', unsafe_allow_html=True)
        for symbol in results:
            st.markdown(f'<div class="hold-badge">{symbol}</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

with col3:
    query = {
        "date": {"$gte": pd.to_datetime("2024-10-11T00:00:00Z")},
        "interval": {"$in": [1,3]},
        "$expr": {
            "$or": [
                {"$lt": ["$close", "$13EMA"]},
                {"$lt": ["$close", "$169EMA"]}
            ]
        }
    }
    results = initialize_mongo_client()[DB_NAME][PROCESSED_COLLECTION].distinct("symbol", query)

    st.subheader("Sell Signal")
    if not results:
        st.write("No results found")
    else:
        # Add Sell specific styles
        st.markdown("""
            <style>
                .sell-container {
                    display: flex;
                    flex-wrap: wrap;
                    gap: 10px;
                }
                .sell-badge {
                    background-color: #FF0000 !important;
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