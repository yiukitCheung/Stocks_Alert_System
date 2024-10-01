import streamlit as st
import json
import pandas as pd
import plotly.graph_objects as go
from pymongo import MongoClient
import threading

# MongoDB initialization
def initialize_mongo_client(mongo_uri, db_name):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    return db

# Function to get current date
def get_current_date():
    current_date = pd.to_datetime('today').normalize()
    if current_date.weekday() == 5:  # Saturday
        current_date -= pd.Timedelta(days=1)
    elif current_date.weekday() == 6:  # Sunday
        current_date -= pd.Timedelta(days=2)
    elif current_date.weekday() == 0 and pd.to_datetime('today').hour < 9:  # Monday before 9:30
        current_date -= pd.Timedelta(days=3)
    elif pd.to_datetime('today').hour < 9:  # Weekday before 9:30
        current_date -= pd.Timedelta(days=1)
    return current_date.replace(hour=9, minute=30).strftime('%Y-%m-%d %H:%M')

# Fetch stock data for a specific interval and symbol
def fetch_data(db, collection_name, symbol, current_date):
    return db[collection_name].find(
        {
            "symbol": symbol, 
            "datetime": {"$gte": pd.to_datetime(current_date)}
        }
    )

# Fetch stock alerts based on symbol and interval
def fetch_alerts(db, collection_name, symbol, interval, current_date, alert_type=None):
    query = {
        "symbol": symbol, 
        "interval": interval,
        "datetime": {"$gte": pd.to_datetime(current_date)}
    }
    if alert_type:
        query[alert_type] = True
    return db[collection_name].find(query)

# Plot candlestick chart for a specific interval
def plot_candlestick_chart(filtered_df, filtered_live_alerts, stock_selector, interval):
    candlestick_chart = go.Figure(data=[go.Candlestick(
        x=filtered_df.index, 
        open=filtered_df['open'], 
        high=filtered_df['high'], 
        low=filtered_df['low'], 
        close=filtered_df['close']
    )])
    
    # Add annotations for live alerts
    for _, alert in filtered_live_alerts.iterrows():
        if alert['datetime'] in filtered_df.index:
            candlestick_chart.add_annotation(
                x=alert['datetime'],
                y=filtered_df.loc[alert['datetime'], 'close'],
                text="Bullish Engulfing",
                showarrow=True,
                arrowhead=1
            )
    # Update layout
    candlestick_chart.update_layout(
        xaxis_rangeslider_visible=False,
        showlegend=False,
        title=f'{stock_selector} Candlestick Chart ({interval})'
    )
    return candlestick_chart

def listen_to_mongodb_changes(db, update_callback):
    pipeline = [{'$match': {'operationType': 'insert'}}]  # Only watch for insert operations
    with db.watch(pipeline) as stream:  # Watch at the database level
        for change in stream:
            # Check if the change is in one of the time series collections you are interested in
            ns = change['ns']['coll']
            if ns in ['5m_stock_datastream', '30m_stock_datastream', '60m_stock_datastream']:
                update_callback(ns)  # Trigger update for the respective interval

# Main Streamlit app
def live_alert_page():
    st.set_page_config(layout="wide", 
                    page_title="Stock Data Visualization", 
                    initial_sidebar_state="expanded")
    st.sidebar.header("Stock Selector")

    # MongoDB setup
    mongo_uri = "mongodb://localhost:27017/"
    db_name = "streaming_data"
    collection_names = ['5m_stock_datastream', '30m_stock_datastream', '60m_stock_datastream']

    db = initialize_mongo_client(mongo_uri, db_name)

    options = sorted(db[collection_names[0]].distinct("symbol"))
    current_date = get_current_date()

    st.title(f'Live Alert Tracking Dashboard')
    stock_selector = st.sidebar.selectbox('Select Stock', options=options, index=0)
    
    # Create placeholders for each interval's chart
    chart_placeholders = {
        '5m': st.empty(),
        '30m': st.empty(),
        '60m': st.empty()
    }

    interval_topic_map = {
        '5m': '5m_stock_datastream',
        '30m': '30m_stock_datastream',
        '60m': '60m_stock_datastream'
    }

    # Update chart for a specific interval
    def update_chart(interval):
        selected_topic = interval_topic_map[interval]
        filtered_query = fetch_data(db, selected_topic, stock_selector, current_date)
        filtered_live_alerts_query = fetch_alerts(db, 'stock_live_alert', stock_selector, interval, current_date, alert_type="bullish_engulfer")

        filtered_df = pd.DataFrame(list(filtered_query))
        filtered_live_alerts = pd.DataFrame(list(filtered_live_alerts_query))

        if not filtered_df.empty:
            filtered_df['datetime'] = pd.to_datetime(filtered_df['datetime'])
            filtered_df.set_index('datetime', inplace=True)
            candlestick_chart = plot_candlestick_chart(filtered_df, filtered_live_alerts, stock_selector, interval)
            chart_placeholders[interval].plotly_chart(candlestick_chart)
        else:
            chart_placeholders[interval].write(f"No data available for {stock_selector} in {interval} interval")

    # Start listening to MongoDB Change Streams for each interval
    for interval in ['5m', '30m', '60m']:
        threading.Thread(target=listen_to_mongodb_changes, args=(db, interval_topic_map[interval], lambda interval=interval: update_chart(interval)), daemon=True).start()

    # Initial chart rendering for each interval
    for interval in ['5m', '30m', '60m']:
        update_chart(interval)

if __name__ == '__main__':
    live_alert_page()