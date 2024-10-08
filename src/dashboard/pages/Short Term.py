
import pandas as pd
import pymongo
import plotly.graph_objects as go
import streamlit as st
import time
from datetime import datetime

# MongoDB Configuration
DB_NAME = st.secrets['db_name']
PROCESSED_COLLECTION_NAME = st.secrets.processed_collection_name
STREAMING_COLLECTIONS = [f'{interval}_stock_datastream' for interval in st.secrets.streaming_interval]
LIVE_ALERT_COLLECTION = st.secrets.live
BATCH_ALERT_COLLECTION = st.secrets.batch
DESIRED_STREAMING_INTERVAL = st.secrets.streaming_interval
WINDOW_SIZE = st.secrets.window_size

def initialize_mongo_client(db_name=DB_NAME):
    client = pymongo.MongoClient(**st.secrets["mongo"])
    return client[db_name]

@st.cache_data
def get_current_date():
    current_date = pd.to_datetime('today').normalize()
    if current_date.weekday() in [5, 6]:  # Saturday or Sunday
        current_date -= pd.Timedelta(days=current_date.weekday() - 4)
    elif current_date.weekday() == 0 and pd.to_datetime('today').hour < 9:  # Monday before 9:30
        current_date -= pd.Timedelta(days=3)
    elif pd.to_datetime('today').hour < 7:  # Weekday before 9:30
        current_date -= pd.Timedelta(days=1)
    return (current_date.replace(hour=7, minute=30) - pd.Timedelta(days=WINDOW_SIZE))

def fetch_data(db, collection_name, symbol, current_date):
    query = {"symbol": symbol, "datetime": {"$gte": current_date}}
    return db[collection_name].find(query)

def fetch_alerts(db, collection_name, symbol, interval, current_date, alert_type=None):
    query = {"symbol": symbol, "interval":interval, "datetime": {"$gte": current_date}}
    if alert_type:
        query["alert_type"] = {"$in": alert_type}
    return db[collection_name].find(query)

def plot_candlestick_chart(filtered_df, filtered_live_alerts, filtered_batch_alerts, stock_selector, interval):
    candlestick_chart = go.Figure(data=[go.Candlestick(
        x=filtered_df.index, 
        open=filtered_df['open'], 
        high=filtered_df['high'], 
        low=filtered_df['low'], 
        close=filtered_df['close']
    )])
    for _, alert in filtered_live_alerts.iterrows():
        if alert['datetime'] in filtered_df.index:
            alert_text = {
                'bullish_engulfer': "Bullish Engulfing",
                'bearish_engulfer': "Bearish Engulfing",
                'bullish_382': "Hammer"
            }.get(alert['alert_type'], "")
            candlestick_chart.add_annotation(
                x=alert['datetime'],
                y=filtered_df.loc[alert['datetime'], 'close'],
                text=alert_text,
                showarrow=True,
                arrowhead=1
            )
    for _, row in filtered_batch_alerts.iterrows():
        end_date = row['datetime'] + pd.Timedelta(minutes=60)
        row['datetime'] = row['datetime'].strftime('%Y-%m-%d %H:%M:%S')
        end_date = end_date.strftime('%Y-%m-%d %H:%M:%S')
        for line_type, color in [('support', 'blue'), ('resistance', 'red')]:
            if line_type in row:
                candlestick_chart.add_trace(go.Scatter(
                    x=[row['datetime'], end_date], 
                    y=[row[line_type], row[line_type]], 
                    mode='lines', 
                    line=dict(color=color, width=1, dash='dash'), 
                    name=line_type.capitalize()
                ))
    candlestick_chart.update_layout(xaxis_rangeslider_visible=False, showlegend=False, title=f'{stock_selector} Candlestick Chart ({interval})')
    return candlestick_chart

def live_alert_page():
    st.sidebar.header("Stock Selector")
    db = initialize_mongo_client()
    options = sorted(db[STREAMING_COLLECTIONS[0]].distinct("symbol"))
    current_date = get_current_date()
    st.write(current_date)

    st.title('Live Alert Tracking Dashboard')
    stock_selector = st.sidebar.selectbox('Select Stock', options=options, index=0)
    
    chart_placeholders = {interval: st.empty() for interval in DESIRED_STREAMING_INTERVAL}
    
    def update_chart(interval):
        # Fetch the data from MongoDB
        selected_topic = f"{interval}_stock_datastream"
        filtered_query = fetch_data(db, selected_topic, stock_selector, current_date)
        filtered_live_alerts_query = fetch_alerts(db, LIVE_ALERT_COLLECTION, stock_selector, interval, current_date, alert_type=["bullish_engulfer","bearish_engulfer","bullish_382"])
        filtered_batch_alerts_query = fetch_alerts(db, BATCH_ALERT_COLLECTION, stock_selector, interval, current_date)
        
        # Convert the query results to a DataFrame
        filtered_df = pd.DataFrame(list(filtered_query))
        filtered_live_alerts = pd.DataFrame(list(filtered_live_alerts_query))
        filtered_batch_alerts = pd.DataFrame(list(filtered_batch_alerts_query))
        if not filtered_df.empty:
            filtered_df['datetime'] = pd.to_datetime(filtered_df['datetime'])
            filtered_df.set_index('datetime', inplace=True)
            candlestick_chart = plot_candlestick_chart(filtered_df, filtered_live_alerts, filtered_batch_alerts, stock_selector, interval)
            chart_placeholders[interval].plotly_chart(candlestick_chart, use_container_width=True)
        else:
            chart_placeholders[interval].write(f"No data available for {stock_selector} in {interval} interval")
            
    while True:
        for interval in DESIRED_STREAMING_INTERVAL:  
            update_chart(interval)
        time.sleep(60)

if __name__ == "__main__":
    live_alert_page()