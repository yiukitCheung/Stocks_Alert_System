
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

def fetch_alerts(db, collection_name, symbol, interval, date, alert_type=None):
    query = {"symbol": symbol, "interval": interval, "datetime": {"$gte": date}}
    if alert_type:
        query["alert_type"] = {"$in": alert_type}
    
    # Fetch the latest document by sorting 'datetime' in descending order
    return db[collection_name].find(query, sort=[("datetime", -1)])

def plot_candlestick_chart(filtered_df,filtered_live_alerts, stock_selector, interval):
    
    candlestick_chart = go.Figure(data=[go.Candlestick(
        x=filtered_df.index.astype(str), 
        open=filtered_df['open'], 
        high=filtered_df['high'], 
        low=filtered_df['low'], 
        close=filtered_df['close']
    )])
    
        
    for _, alert in filtered_live_alerts.iterrows():
        if alert['datetime'] in filtered_df.index:
            color = 'green' if alert['alert_type'] in ['bullish_engulfer', 'bullish_382'] else 'red' if alert['alert_type'] == 'bearish_engulfer' else 'black'
            hover_text = f"Alert Type: {alert['alert_type']}<br>Date: {alert['datetime'].strftime('%Y-%m-%d %H:%M:%S')}"
            candlestick_chart.add_annotation(
                x=alert['datetime'].strftime('%Y-%m-%d %H:%M:%S'),
                y=1.05,
                xref='x',
                yref='paper',
                showarrow=False,
                text='',
                hovertext=hover_text,
                font=dict(color=color),
                bgcolor=color,
                bordercolor=color,
                borderwidth=2
            )
    
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
                
    candlestick_chart.update_layout(
        xaxis_rangeslider_visible=False,
        showlegend=False,
        xaxis_type='category',
        title={
            'text': f'{stock_selector}',
            'x': 0.5,  # Center the title
            'xanchor': 'center',  # Anchor the title at the center
            'yanchor': 'top',  # Anchor the title at the top
            'font': {
                'size': 24,  # Set the font size
                'color': 'black',  # Set the font color
                'family': 'Arial, sans-serif'  # Set the font family
            }
        },
        xaxis=dict(
            showticklabels=False,
            title=''
        ),
        yaxis=dict(
            title='Price'
        ),
        width=1200,
        height=700
        )
    return candlestick_chart

def plot_support_resistance_histogram(filtered_batch_alerts):
    if 'support' in filtered_batch_alerts.columns or 'resistance' in filtered_batch_alerts.columns:
        histogram_fig = go.Figure()

        if 'support' in filtered_batch_alerts.columns:
            histogram_fig.add_trace(go.Histogram(
                x=filtered_batch_alerts['support'],
                name='Support',
                marker_color='blue',
                opacity=0.5,
                xbins=dict(size=0.5)
            ))

        if 'resistance' in filtered_batch_alerts.columns:
            histogram_fig.add_trace(go.Histogram(
                x=filtered_batch_alerts['resistance'],
                name='Resistance',
                marker_color='red',
                opacity=0.5,
                xbins=dict(size=0.5)
            ))

        histogram_fig.update_layout(
            barmode='overlay',
            xaxis_title='Price',
            yaxis_title='Count',
            showlegend=False,
            title={
            'text': 'Support/Resistance Levels Strength',
            'x': 0.5,  # Center the title
            'xanchor': 'center',  # Anchor the title at the center
            'yanchor': 'top',  # Anchor the title at the top
            'font': {
                'size': 18,  # Set the font size
                'color': 'black',  # Set the font color
                'family': 'Arial, sans-serif'  # Set the font family
            }
            },
            xaxis=dict(
                showticklabels=True,
                title='Price'
            ),
            yaxis=dict(
                title='Strength'
            )
        )
        st.plotly_chart(histogram_fig, use_container_width=True,showlegend=False)

    else:
        st.warning("No support/resistance data available for the selected interval and symbol.")
            
def plot_live_page(db, stock_selector, interval):
    # Prepare data for the selected stock and interval
    selected_topic = f"{interval}_stock_datastream"
    filtered_query = fetch_data(db, selected_topic, stock_selector)
    filtered_live_alerts_query = fetch_alerts(db, LIVE_ALERT_COLLECTION, stock_selector, interval, date=pd.to_datetime('today').normalize(),
                                            alert_type=["bullish_engulfer", "bearish_engulfer", "bullish_382"])
    filtered_batch_alerts_query = fetch_alerts(db, BATCH_ALERT_COLLECTION, stock_selector, interval,date=get_current_date())
    
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
    candlestick_chart = plot_candlestick_chart(filtered_df, filtered_live_alerts, stock_selector, interval)
    # Display the chart
    st.plotly_chart(candlestick_chart, use_container_width=True)
    # Display the lastest alerts
    if not filtered_live_alerts.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            
            only_time = filtered_live_alerts['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
            only_time = only_time.head(1).values[0]
                
            st.markdown(f"""
            <div style="
                text-align: center; 
                font-size: 36px; 
                font-weight: bold; 
                color: #4CAF50; 
                border: 2px solid #4CAF50; 
                padding: 10px; 
                border-radius: 10px;
                background-color: #f9f9f9;
            ">
            {only_time}
            </div>
            """, unsafe_allow_html=True)
            
        with col2: 
            alert_type = filtered_live_alerts['alert_type'].values[0]
            
            # Determine the color and message based on the alert type
            if alert_type == 'bullish_engulfer' or alert_type == 'bullish_382':
                alert_message = f"{alert_type.replace('_', ' ').capitalize()} &#x1F7E2;"  # Green Circle Emoji
                alert_color = "#4CAF50"  # Green color
            elif alert_type == 'bearish_engulfer':
                alert_message = f"{alert_type.replace('_', ' ').capitalize()} &#x1F534;"  # Red Circle Emoji
                alert_color = "#FF5733"  # Red color
            st.markdown(f"""
            <div style="
                text-align: center; 
                font-size: 36px; 
                font-weight: bold; 
                color: {alert_color}; 
                border: 2px solid {alert_color}; 
                padding: 10px; 
                border-radius: 10px;
                background-color: #f9f9f9;
            ">
            {alert_message}
            </div>
            """, unsafe_allow_html=True)
    else:
        
        col1, col2 = st.columns(2)
        with col1:
                    st.markdown(f"""
            <div style="
                text-align: center; 
                font-size: 36px; 
                font-weight: bold; 
                color: #808080; 
                border: 2px solid #808080; 
                padding: 10px; 
                border-radius: 10px;
                background-color: #f9f9f9;
            ">
            No Alert Time Available
            </div>
            """, unsafe_allow_html=True)
            
        with col2: 
            st.markdown(f"""
            <div style="
                text-align: center; 
                font-size: 36px; 
                font-weight: bold; 
                color: #808080;
                border: 2px solid #808080; 
                padding: 10px; 
                border-radius: 10px;
                background-color: #f9f9f9;
            ">
            No Alert Yet
            </div>
            """, unsafe_allow_html=True)
            
    # Display the support/resistance histogram
    plot_support_resistance_histogram(filtered_batch_alerts)
    
            
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
    st.title(f'Live Alert Tracking Dashboard')
    stock_selector = st.sidebar.selectbox('Select Stock', options=options, index=0)
    intervals_selector = st.sidebar.selectbox('Select Interval', options=intervals, index=0)
    # Create a placeholder for the chart
    chart_placeholder = st.empty()
    # Continuously update the chart every minute
    while True:
        # Update the chart based on user selection
        if stock_selector and intervals_selector:
            with chart_placeholder.container():
                plot_live_page(db, stock_selector, intervals_selector)
        # Refresh the data every 60 seconds
        time.sleep(60)