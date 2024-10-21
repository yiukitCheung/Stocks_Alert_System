import pandas as pd
import pymongo
import plotly.graph_objects as go
import streamlit as st
import plotly.subplots as sp

# MongoDB Configuration
DB_NAME = st.secrets['db_name']
PROCESSED_COLLECTION_NAME = st.secrets.processed_collection_name
ALERT_COLLECTION_NAME = st.secrets.alert_collection_name
def connect_to_mongo(db_name=DB_NAME):
    client = pymongo.MongoClient(**st.secrets["mongo"])
    return client[db_name]


def fetch_stock_data(collection, stock_symbol, interval):
    warehouse_interval = st.secrets.warehouse_interval
    if not warehouse_interval:
        raise ValueError("warehouse_interval is empty in st.secrets")
    return pd.DataFrame(list(collection.find({"symbol": stock_symbol,
                                            "interval": interval}, 
                                            {"_id": 0}))).sort_values(by=['date'])

def fetch_alert_data(collection, stock_symbol,interval):
    if not interval:
        raise ValueError("warehouse_interval is empty in st.secrets")
    query = collection.find({"symbol": stock_symbol,
                            "interval": interval},
                            {"_id": 0})

    return pd.DataFrame(list(query)).sort_values(by=['date'])     

def create_figure(filtered_df, show_macd=False):

    row_height = [0.8, 0.2] if show_macd else [1]
    row = 2 if show_macd else 1
    # Calculate the date range for the last 6 months
    end_date = filtered_df['date'].max() + pd.DateOffset(days=20)
    start_date = end_date - pd.DateOffset(months=6)
    
    # Calculate the price range for the last 6 months
    price_range = filtered_df[(filtered_df['date'] >= start_date) & (filtered_df['date'] <= end_date)]
    min_price = price_range['low'].min() * 0.95 
    max_price = price_range['high'].max() * 1.05
    
    fig = sp.make_subplots(rows=row, cols=1, shared_xaxes=True, vertical_spacing=0.1, row_heights=row_height)

    fig.add_trace(go.Candlestick(
        x=filtered_df['date'],
        open=filtered_df['open'],
        high=filtered_df['high'],
        low=filtered_df['low'],
        close=filtered_df['close'],
        name='price'), row=1, col=1)
    fig.update_xaxes(range=[start_date, end_date],title_text="Date", row=3, col=1)
    
    for ema in ['144EMA', '169EMA', '13EMA', '8EMA']:
        fig.add_trace(go.Scatter(x=filtered_df['date'], y=filtered_df[ema], mode="lines", name=ema), row=1, col=1)
        
    fig.update_xaxes(range=[start_date, end_date],title_text="Date", row=1, col=1)
    fig.update_yaxes(range=[min_price, max_price], title_text="Price", row=1, col=1)
    
    # for trade_type, color in [('Entry', 'rgba(0,255,0,0.7)'), ('Exit', 'rgba(255,0,0,0.7)')]:
    #     fig.add_trace(go.Scatter(
    #         x=filtered_trades[f'{trade_type}_date'],
    #         y=filtered_trades[f'{trade_type}_price'],
    #         mode="markers",
    #         customdata=filtered_trades,
    #         marker_symbol="diamond-dot",
    #         marker_size=8,
    #         marker_line_width=2,
    #         marker_line_color="rgba(0,0,0,0.7)",
    #         marker_color=color,
    #         hovertemplate=f"{trade_type} Time: %{{customdata[1]}}<br>{trade_type} Price: %{{y:.2f}}<br>Total Asset: %{{customdata[5]:.3f}}",
    #         name=f"{trade_type}s"), row=1, col=1)

    if show_macd:
        fig.add_trace(go.Scatter(x=filtered_df['date'], y=filtered_df['MACD'], mode='lines', name='MACD'), row=2, col=1)
        fig.add_trace(go.Scatter(x=filtered_df['date'], y=filtered_df['MACD_SIGNAL'], mode='lines', name='MACD signal'), row=2, col=1)
        
    # Calculate the MACD start and end date
    macd_start_date = filtered_df['date'].min()
    macd_end_date = filtered_df['date'].max()
    
    fig.update_xaxes(range=[start_date, end_date],title_text="Date", row=2, col=1)
    fig.update_yaxes(range=[macd_start_date, macd_end_date], title_text="MACD", row=2, col=1)
    # fig.add_trace(go.Scatter(x=filtered_trades['Exit_date'], y=filtered_trades['total_asset'], mode='lines', name='Profit', line=dict(color='green')), 
    #             row=row, col=1)\
    # fig.update_yaxes(title_text="Profit/Loss", row=3, col=1)
    
    fig.update_layout(xaxis_rangeslider_visible=False, autosize=False, showlegend=False, height=1000, width=1200)

    return fig

def display_alerts(alert_df):
    # Display Alert
    today_alert = alert_df[alert_df['date'] == alert_df['date'].max()]

    # Function to map alert values to color and message
    def get_alert_color_and_message(alert_type, value):
        alert_mappings = {
            'velocity_alert': {
                'velocity_maintained': ('green', 'Velocity Maintained'),
                'velocity_weak': ('red', 'Velocity Weakened'),
                'velocity_loss': ('red', 'Velocity Loss'),
                'velocity_negotiating': ('red', 'Velocity Negotiating')
            },
            'candle_alert': {
                'bullish_engulf': ('green', 'Bullish Engulfing'),
                'bearish_engulf': ('red', 'Bearish Engulfing'),
                'hammer': ('green', 'Hammer Pattern'),
                'inverse_hammer': ('red', 'Inverse Hammer Pattern')
            },
            'macd_alert': {
                'bullish_macd': ('green', 'MACD Bullish Crossover'),
                'bearish_macd': ('red', 'MACD Bearish Crossover')
            }
        }
    
        # Default to grey color if value or alert type is not recognized
        color, message = alert_mappings.get(alert_type, {}).get(value, ('grey', 'Unknown Alert'))
        return color, message
    
    # Main function to display alerts
    def display_alerts(today_alert):        
        # Loop through the relevant alert columns (e.g., velocity, candle, MACD)
        for column in ['velocity_alert', 'candle_alert', 'macd_alert']:
            alert_value = today_alert[column].values[0] if not today_alert[column].empty else None
            if pd.notna(alert_value):  # Only display if alert has a valid value
                alert_color, alert_message = get_alert_color_and_message(column, alert_value)
            
                # Display the alert with color-coded dot and message
                st.markdown(f"""
                    <div style="text-align: center;">
                    <span style="font-size:50px; color:{alert_color}">●</span>
                    <div style="font-size:16px; font-weight:bold; margin-top:10px; color:{alert_color};">{alert_message}</div>
                    </div>
                """, unsafe_allow_html=True)
                
    # Display the alerts            
    display_alerts(today_alert)
    
def static_analysis_page(processed_collection, alert_collection):
    # Set the page title and layout
    st.set_page_config(page_title="Stock Analysis Dashboard", layout="wide")
    st.markdown("<h1 style='text-align: center;'>Stock Analysis Dashboard</h1>", unsafe_allow_html=True)
    
    # Add a sidebar
    # Create a dropdown to select the stock
    stock_selector = st.sidebar.selectbox('Select Stock', options=sorted(processed_collection.distinct("symbol")), index=0)
    # Create a dropdown to select the interval
    default_interval = st.secrets['velocity_dict'][stock_selector]
    interval_selector = st.sidebar.selectbox('Optimal Interval/ Select Interval', 
                                    options=sorted(processed_collection.distinct("interval")), 
                                    index=sorted(processed_collection.distinct("interval")).\
                                        index(default_interval) if default_interval in processed_collection.distinct("interval")\
                                            else 0)
    
    alert_df = fetch_alert_data(alert_collection, stock_selector, interval_selector)
    # Add an update button
    if st.sidebar.button("Update Data"):
        # Refetch the latest data when the button is clicked
        processed_df = fetch_stock_data(processed_collection, stock_selector, interval_selector)
        st.success("Data updated successfully!")
    else:
        # Display the existing data if the button is not clicked
        processed_df = fetch_stock_data(processed_collection, stock_selector, interval_selector)
        
    # Add a checkbox to show the MACD
    show_macd = st.sidebar.checkbox("Show MACD", value=False)
    
    # Create the figure
    fig = create_figure(processed_df, show_macd)
    
    col1, col2 = st.columns([5, 1], vertical_alignment="top")
    with col1:
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Display the alerts
        display_alerts(alert_df)
    
if __name__ == "__main__":
    # Connect to MongoDB and fetch the processed collection
    processed_collection = connect_to_mongo()[PROCESSED_COLLECTION_NAME]
    alert_collection = connect_to_mongo()[ALERT_COLLECTION_NAME]
    static_analysis_page(processed_collection, alert_collection)