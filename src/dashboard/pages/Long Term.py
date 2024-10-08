import pandas as pd
import pymongo
import plotly.graph_objects as go
import plotly.subplots as sp
import streamlit as st
import os, sys
# Ensure the correct path to the 'data' directory
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'data'))
from utils.trading_strategy import TradingStrategy

# MongoDB Configuration
DB_NAME = st.secrets['db_name']
PROCESSED_COLLECTION_NAME = st.secrets.processed_collection_name
STREAMING_COLLECTIONS = [f'{interval}_stock_datastream' for interval in st.secrets.streaming_interval]
LIVE_ALERT_COLLECTION = st.secrets.live
BATCH_ALERT_COLLECTION = st.secrets.batch
DESIRED_STREAMING_INTERVAL = st.secrets.streaming_interval

def connect_to_mongo(db_name=DB_NAME):
    client = pymongo.MongoClient(**st.secrets["mongo"])
    return client[db_name]

def fetch_stock_data(collection, stock_symbol):
    warehouse_interval = st.secrets.warehouse_interval
    if not warehouse_interval:
        raise ValueError("warehouse_interval is empty in st.secrets")
    return pd.DataFrame(list(collection.find({"symbol": stock_symbol,
                                            "interval": warehouse_interval[0]}, 
                                            {"_id": 0}))).sort_values(by=['date'])

@st.cache_data
def execute_trades(filtered_df):
    trades_history = TradingStrategy(filtered_df)
    trades_history.execute_trades()
    return trades_history.get_trades()

@st.cache_data
def compute_metrics(filtered_trades):
    win_trades = len(filtered_trades[filtered_trades['profit'] > 0])
    loss_trades = len(filtered_trades[filtered_trades['profit'] <= 0])
    final_trade_profit_rate = (round(filtered_trades['total_asset'].iloc[-1]) - 10000) / 100
    return [win_trades, loss_trades, final_trade_profit_rate]

def create_figure(filtered_df, filtered_trades, show_macd=False):
    win_trades, loss_trades, final_trade_profit_rate = compute_metrics(filtered_trades)
    
    col1, col2, col3 = st.columns(3, gap='medium')
    st.markdown("""
        <style>
        .metric-container {
            display: flex;
            justify-content: center;
            align-items: center;
            height: 100%;
        }
        </style>
    """, unsafe_allow_html=True)
    
    with col1:
        st.markdown('<div class="metric-container">', unsafe_allow_html=True)
        st.metric(label="Final Trade Profit", value=f"{final_trade_profit_rate}%")
        st.markdown('</div>', unsafe_allow_html=True)
        
    with col2:
        st.markdown('<div class="metric-container">', unsafe_allow_html=True)
        st.metric(label="Win Trades", value=win_trades)
        st.markdown('</div>', unsafe_allow_html=True)
        
    with col3:
        st.markdown('<div class="metric-container">', unsafe_allow_html=True)
        st.metric(label="Loss Trades", value=loss_trades)
        st.markdown('</div>', unsafe_allow_html=True)

    row_height = [0.65, 0.15, 0.25] if show_macd else [0.8, 0.2]
    row = 3 if show_macd else 2
    
    fig = sp.make_subplots(rows=row, cols=1, shared_xaxes=True, vertical_spacing=0.1, row_heights=row_height)

    fig.add_trace(go.Candlestick(
        x=filtered_df['date'],
        open=filtered_df['open'],
        high=filtered_df['high'],
        low=filtered_df['low'],
        close=filtered_df['close'],
        name='price'), row=1, col=1)

    for ema in ['144EMA', '169EMA', '13EMA', '8EMA']:
        fig.add_trace(go.Scatter(x=filtered_df['date'], y=filtered_df[ema], mode="lines", name=ema), row=1, col=1)

    for trade_type, color in [('Entry', 'rgba(0,255,0,0.7)'), ('Exit', 'rgba(255,0,0,0.7)')]:
        fig.add_trace(go.Scatter(
            x=filtered_trades[f'{trade_type}_date'],
            y=filtered_trades[f'{trade_type}_price'],
            mode="markers",
            customdata=filtered_trades,
            marker_symbol="diamond-dot",
            marker_size=8,
            marker_line_width=2,
            marker_line_color="rgba(0,0,0,0.7)",
            marker_color=color,
            hovertemplate=f"{trade_type} Time: %{{customdata[1]}}<br>{trade_type} Price: %{{y:.2f}}<br>Total Asset: %{{customdata[5]:.3f}}",
            name=f"{trade_type}s"), row=1, col=1)

    if show_macd:
        fig.add_trace(go.Scatter(x=filtered_df['date'], y=filtered_df['MACD'], mode='lines', name='MACD'), row=2, col=1)
        fig.add_trace(go.Scatter(x=filtered_df['date'], y=filtered_df['MACD_SIGNAL'], mode='lines', name='MACD signal'), row=2, col=1)
    
    fig.add_trace(go.Scatter(x=filtered_trades['Exit_date'], y=filtered_trades['total_asset'], mode='lines', name='Profit', line=dict(color='green')), 
                row=row, col=1)

    fig.update_xaxes(title_text="Date", row=3, col=1)
    fig.update_yaxes(title_text="Profit/Loss", row=3, col=1)
    fig.update_layout(xaxis_rangeslider_visible=False, autosize=False, showlegend=False, height=800, width=1200)

    return fig

def static_analysis_page():
    st.title("Stock Analysis Dashboard")
    st.sidebar.header("Stock Selector")
    
    processed_collection = connect_to_mongo()[PROCESSED_COLLECTION_NAME]
    stock_selector = st.sidebar.selectbox('Select Stock', options=sorted(processed_collection.distinct("symbol")), index=0)
    
    processed_df = fetch_stock_data(processed_collection, stock_selector)
    filtered_trades = execute_trades(processed_df)
    
    show_macd = st.sidebar.checkbox("Show MACD", value=False)
    fig = create_figure(processed_df, filtered_trades,show_macd)

    st.plotly_chart(fig, use_container_width=True)
if __name__ == "__main__":
    static_analysis_page()