
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

def fetch_data(db, collection_name, symbol):
    query = {"symbol": symbol}
    return db[collection_name].find(query)

def fetch_alerts(db, collection_name, symbol, interval, alert_type=None):
    query = {"symbol": symbol, "interval":interval}
    if alert_type:
        query["alert_type"] = {"$in": alert_type}
    return db[collection_name].find(query)

def plot_candlestick_chart(filtered_df, filtered_live_alerts, filtered_batch_alerts, stock_selector, interval):
    
    candlestick_chart = go.Figure(data=[go.Candlestick(
        x=filtered_df.index.astype(str), 
        open=filtered_df['open'], 
        high=filtered_df['high'], 
        low=filtered_df['low'], 
        close=filtered_df['close']
    )])
    
    
    # for _, alert in filtered_live_alerts.iterrows():
    #     if alert['datetime'] in filtered_df.index:
    #         alert_text = {
    #             'bullish_engulfer': "Bullish Engulfing",
    #             'bearish_engulfer': "Bearish Engulfing",
    #             'bullish_382': "Hammer"
    #         }.get(alert['alert_type'], "")
    #         candlestick_chart.add_annotation(
    #             x=alert['datetime'].strftime('%Y-%m-%d %H:%M:%S'),
    #             y=filtered_df.loc[alert['datetime'], 'close'],
    #             text=alert_text,
    #             showarrow=True,
    #             arrowhead=1
    #         )
    # for _, row in filtered_batch_alerts.iterrows():
    #     end_date = row['datetime'] + pd.Timedelta(minutes=60)
    #     row['datetime'] = row['datetime'].strftime('%Y-%m-%d %H:%M:%S')
    #     end_date = end_date.strftime('%Y-%m-%d %H:%M:%S')
    #     for line_type, color in [('support', 'blue'), ('resistance', 'red')]:
    #         if line_type in row:
    #             candlestick_chart.add_trace(go.Scatter(
    #                 x=[row['datetime'], end_date], 
    #                 y=[row[line_type], row[line_type]], 
    #                 mode='lines', 
    #                 line=dict(color=color, width=1, dash='dash'), 
    #                 name=line_type.capitalize()
    #             ))
                
    candlestick_chart.update_layout(xaxis_rangeslider_visible=False,
                                showlegend=False, 
                                xaxis_type='category',
                                title=f'{stock_selector} Candlestick Chart ({interval})',
                                xaxis=dict(
                                    showticklabels=False,
                                    title='')
                                )
    return candlestick_chart


def update_charts(db, stock_selector, interval):
    # Prepare data for the selected stock and interval
    selected_topic = f"{interval}_stock_datastream"
    filtered_query = fetch_data(db, selected_topic, stock_selector)
    filtered_live_alerts_query = fetch_alerts(db, LIVE_ALERT_COLLECTION, stock_selector, interval, alert_type=["bullish_engulfer", "bearish_engulfer", "bullish_382"])
    filtered_batch_alerts_query = fetch_alerts(db, BATCH_ALERT_COLLECTION, stock_selector, interval)
    
    # Convert the query results to a DataFrame
    filtered_df = pd.DataFrame(list(filtered_query))
    filtered_live_alerts = pd.DataFrame(list(filtered_live_alerts_query))
    filtered_batch_alerts = pd.DataFrame(list(filtered_batch_alerts_query))
    
    # Convert the 'datetime' field to datetime
    if not filtered_df.empty:
        filtered_df['datetime'] = pd.to_datetime(filtered_df['datetime'])
        filtered_df.set_index('datetime', inplace=True)
    else:
        st.warning("No data available for the selected interval and symbol.")
        return

    # Plot the Chart
    candlestick_chart = plot_candlestick_chart(filtered_df, filtered_live_alerts, filtered_batch_alerts, stock_selector, interval)
    # Display the chart
    st.plotly_chart(candlestick_chart, use_container_width=True)
    if 'support' not in filtered_batch_alerts.columns and 'resistance' not in filtered_batch_alerts.columns:
        st.warning("No support/resistance data available for the selected interval and symbol.")
    elif ('support' not in filtered_batch_alerts.columns ) or ('resistance' not in filtered_batch_alerts.columns):
        # Plot one histogram for the alerts
        histogram_fig = go.Figure()
        # Add support histogram
        if not 'support' not in filtered_batch_alerts.columns:
            histogram_fig.add_trace(go.Histogram(
                x=filtered_batch_alerts['support'],
                name='Support',
                marker_color='blue',
                opacity=0.5,
                xbins=dict(size=0.5)  # Adjust bin width to be thinner
            ))
        # Add resistance histogram
        if not 'resistance' not in filtered_batch_alerts.columns:
            histogram_fig.add_trace(go.Histogram(
                x=filtered_batch_alerts['resistance'],
                name='Resistance',
                marker_color='red',
                opacity=0.5,
                xbins=dict(size=0.5)  # Adjust bin width to be thinner
            ))
        # Update layout for better visualization
        histogram_fig.update_layout(
            barmode='overlay',
            title='Support/ Resistance Histogram',
            xaxis_title='Price',
            yaxis_title='Count'
        )
        # Plot the combined histogram
        st.plotly_chart(histogram_fig, use_container_width=True)    
    
    else:
        # Combine histograms for support and resistance
        histogram_fig = go.Figure()
        # Add support histogram
        histogram_fig.add_trace(go.Histogram(
            x=filtered_batch_alerts['support'],
            name='Support',
            marker_color='green',
            opacity=0.5,
            xbins=dict(size=0.5)  # Adjust bin width to be thinner
        ))

        # Add resistance histogram
        histogram_fig.add_trace(go.Histogram(
            x=filtered_batch_alerts['resistance'],
            name='Resistance',
            marker_color='red',
            opacity=0.5,
            bingroup=10,
            xbins=dict(size=0.5)  # Adjust bin width to be thinner
        ))

        # Update layout for better visualization
        histogram_fig.update_layout(
            barmode='overlay',
            title='Support and Resistance Histogram',
            xaxis_title='Price',
            yaxis_title='Count'
        )

        # Plot the combined histogram
        st.plotly_chart(histogram_fig, use_container_width=True)
if __name__ == "__main__":
    # Set page config
    st.set_page_config(layout="wide")
    # Initialize MongoDB client
    db = initialize_mongo_client()
    # Get the list of stocks
    options = sorted(db[STREAMING_COLLECTIONS[0]].distinct("symbol"))
    intervals = DESIRED_STREAMING_INTERVAL
    # Streamlit UI
    st.sidebar.header("Selector")
    st.title('Live Alert Tracking Dashboard')
    stock_selector = st.sidebar.selectbox('Select Stock', options=options, index=0)
    intervals_selector = st.sidebar.selectbox('Select Interval', options=intervals, index=0)
    # Create a placeholder for the chart
    chart_placeholder = st.empty()
    # Continuously update the chart every minute
    while True:
        # Update the chart based on user selection
        if stock_selector and intervals_selector:
            with chart_placeholder.container():
                update_charts(db, stock_selector, intervals_selector)
        # Refresh the data every 60 seconds
        time.sleep(60)