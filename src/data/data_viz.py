import pandas as pd
import pymongo
import plotly.graph_objects as go
import plotly.subplots as sp
import streamlit as st
from utils.alert_strategy import Alert
from utils.trading_strategy import TradingStrategy
from utils.features_engineering import add_features
import time
class mongo_config:
    @staticmethod
    def read_config():
        config = {}
        with open('mongo.properties') as fh:
            for line in fh:
                line = line.strip()
                if len(line) != 0 and line[0] != "#":
                    parameter, value = line.strip().split('=', 1)
                    config[parameter] = value.strip()
        return config
# Constants
MONGO_URI = mongo_config.read_config()['mongo_uri']
DB_NAME = "historic_price"
COLLECTION_NAME = "daily_stock_price"
STREAMING_DB_NAME = "streaming_data"
STREAMING_COLLECTIONS = ['5m_stock_datastream', '30m_stock_datastream', '60m_stock_datastream']


def connect_to_mongo(db_uri=MONGO_URI, db_name=DB_NAME, collection_name=COLLECTION_NAME):
    client = pymongo.MongoClient(db_uri)
    db = client[db_name]
    collection = db[collection_name]
    return collection


def fetch_stock_data(collection, stock_symbol):
    filtered_query = collection.find({"symbol": stock_symbol}, {"_id": 0})
    return pd.DataFrame(list(filtered_query))

@st.cache_data
def add_technical_features(filtered_df):
    filtered_df = add_features(filtered_df).apply()
    filtered_df = Alert(filtered_df).add_alert()
    return filtered_df

@st.cache_data
def execute_trades(filtered_df):
    trades_history = TradingStrategy(filtered_df)
    trades_history.execute_trades()
    return trades_history.get_trades()

@ st.cache_data
def compute_metrics(filtered_trades):
    win_trades = len(filtered_trades[filtered_trades['profit'] > 0])
    loss_trades = len(filtered_trades[filtered_trades['profit'] <= 0])
    final_trade_profit_rate = (round(filtered_trades['total_asset'].iloc[-1]) - 10000) / 100
    return [win_trades, loss_trades, final_trade_profit_rate]

def create_figure(filtered_df,filtered_trades, show_macd=False):
    win_trades, loss_trades, final_trade_profit_rate = compute_metrics(filtered_trades)
    
    # Display KPIs horizontally
    col1, col2, col3 = st.columns(3, gap='medium')
    
    # Add CSS to center the content
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
    
    collection = connect_to_mongo()
    stock_selector = st.sidebar.selectbox('Select Stock', options=sorted(collection.distinct("symbol")), index=0)
    
    filtered_df = fetch_stock_data(collection, stock_selector)
    filtered_df = filtered_df.sort_values(by=['date'])
    filtered_df = add_technical_features(filtered_df)
    filtered_trades = execute_trades(filtered_df)
    
    # Add a checkbox to toggle the MACD plot
    show_macd = st.sidebar.checkbox("Show MACD", value=False)
    
    fig = create_figure(filtered_df, filtered_trades, show_macd)

    st.plotly_chart(fig, use_container_width=True)

def initialize_mongo_client(mongo_uri=MONGO_URI, db_name=STREAMING_DB_NAME):
    client = pymongo.MongoClient(mongo_uri)
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
    return current_date.replace(hour=7, minute=30).strftime('%Y-%m-%d %H:%M')

def fetch_data(db, collection_name, symbol, current_date):
    return db[collection_name].find({"symbol": symbol, "datetime": {"$gte": pd.to_datetime(current_date)}})

def fetch_alerts(db, collection_name, symbol, interval, current_date, alert_type=None):
    if alert_type:
        query = {"symbol": symbol, "datetime": {"$gte": pd.to_datetime(current_date)}, "alert_type": {"$in": alert_type}}
    else:
        query = {"symbol": symbol, "datetime": {"$gte": pd.to_datetime(current_date)}}
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
            if alert['alert_type'] == 'bullish_engulfer':
                candlestick_chart.add_annotation(
                    x=alert['datetime'],
                    y=filtered_df.loc[alert['datetime'], 'close'],
                    text="Bullish Engulfing",
                    showarrow=True,
                    arrowhead=1
                )
            elif alert['alert_type'] == 'bearish_engulfer':
                candlestick_chart.add_annotation(
                    x=alert['datetime'],
                    y=filtered_df.loc[alert['datetime'], 'close'],
                    text="Bearish Engulfing",
                    showarrow=True,
                    arrowhead=1
                )
            elif alert['alert_type'] == 'bullish_382':
                candlestick_chart.add_annotation(
                    x=alert['datetime'],
                    y=filtered_df.loc[alert['datetime'], 'close'],
                    text="Hammer",
                    showarrow=True,
                    arrowhead=1
                )
    # Add support lines
    if 'support' in filtered_batch_alerts.columns:
        for _, row in filtered_batch_alerts.iterrows():
            end_date = row['datetime'] + pd.Timedelta(minutes=60)
            row['datetime'] = row['datetime'].strftime('%Y-%m-%d %H:%M:%S')
            end_date = end_date.strftime('%Y-%m-%d %H:%M:%S')
            candlestick_chart.add_trace(go.Scatter(x=[row['datetime'], end_date], 
                                    y=[row['support'], row['support']], 
                                    mode='lines', 
                                    line=dict(color='blue', width=1, dash='dash'), 
                                    name='Support'))

    # Add resistance lines
    if 'resistance' in filtered_batch_alerts.columns:
        for _, row in filtered_batch_alerts.iterrows():
            end_date = row['datetime'] + pd.Timedelta(minutes=60)
            row['datetime'] = row['datetime'].strftime('%Y-%m-%d %H:%M:%S')
            end_date = end_date.strftime('%Y-%m-%d %H:%M:%S')
            candlestick_chart.add_trace(go.Scatter(x=[row['datetime'], end_date], 
                                    y=[row['resistance'], row['resistance']], 
                                    mode='lines', 
                                    line=dict(color='red', width=1, dash='dash'), 
                                    name='Resistance'))
            
    candlestick_chart.update_layout(xaxis_rangeslider_visible=False, showlegend=False, title=f'{stock_selector} Candlestick Chart ({interval})')
    return candlestick_chart

def live_alert_page():
    st.sidebar.header("Stock Selector")

    db = initialize_mongo_client()
    options = sorted(db[STREAMING_COLLECTIONS[0]].distinct("symbol"))
    current_date = get_current_date()
    st.write(f"Current Date: {current_date}")
    st.title('Live Alert Tracking Dashboard')
    stock_selector = st.sidebar.selectbox('Select Stock', options=options, index=0)
    
    chart_placeholders = {interval: st.empty() for interval in ['5m', '30m', '60m']}
    interval_topic_map = {interval: f'{interval}_stock_datastream' for interval in ['5m', '30m', '60m']}

    def update_chart(interval):
        selected_topic = interval_topic_map[interval]
        filtered_query = fetch_data(db, selected_topic, stock_selector, current_date)
        filtered_live_alerts_query = fetch_alerts(db, 
                                                'stock_live_alert',
                                                stock_selector, 
                                                interval, 
                                                current_date, 
                                                alert_type=["bullish_engulfer","bearish_engulfer","bullish_382"])
        
        filtered_batch_alerts_query = fetch_alerts(db, 
                                                'stock_batch_alert',
                                                stock_selector, 
                                                interval, 
                                                current_date)
        
        filtered_df = pd.DataFrame(list(filtered_query))
        filtered_live_alerts = pd.DataFrame(list(filtered_live_alerts_query))
        filtered_batch_alerts = pd.DataFrame(list(filtered_batch_alerts_query))
        if not filtered_df.empty:
            filtered_df['datetime'] = pd.to_datetime(filtered_df['datetime'])
            filtered_df.set_index('datetime', inplace=True)
            candlestick_chart = plot_candlestick_chart(filtered_df, 
                                                    filtered_live_alerts,
                                                    filtered_batch_alerts,
                                                    stock_selector, 
                                                    interval)
            chart_placeholders[interval].plotly_chart(candlestick_chart)
        else:
            chart_placeholders[interval].write(f"No data available for {stock_selector} in {interval} interval")
            
    # Update the charts for all intervals
    while True:
        for interval in ['5m', '30m', '60m']:  
            # Update the chart every 1 minute
            update_chart(interval)
        time.sleep(60)
        

def main():
    st.set_page_config(page_title="Stock Analysis Dashboard", layout="wide")
    
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Go to", ["live_alert_page", "static_analysis_page"])
    
    if page == "live_alert_page":
        live_alert_page()
    elif page == "static_analysis_page":    
        static_analysis_page()
        
if __name__ == "__main__":
    main()
