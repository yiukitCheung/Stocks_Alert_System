import pandas as pd
import pymongo
import plotly.graph_objects as go
import streamlit as st
import os, sys
import plotly.subplots as sp
# Ensure the correct path to the 'data' directory
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'data'))
from utils.trading_strategy import TradingStrategy

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

def fetch_alert_data(collection, stock_symbol):
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
# def compute_metrics(filtered_trades):
#     win_trades = len(filtered_trades[filtered_trades['profit'] > 0])
#     loss_trades = len(filtered_trades[filtered_trades['profit'] <= 0])
#     final_trade_profit_rate = (round(filtered_trades['total_asset'].iloc[-1]) - 10000) / 100
#     return [win_trades, loss_trades, final_trade_profit_rate]

def create_figure(filtered_df, filtered_trades, show_macd=False):
    # win_trades, loss_trades, final_trade_profit_rate = compute_metrics(filtered_trades)
    col1, col2, col3 = st.columns(3, gap='medium')

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
    # profit_color = "green" if final_trade_profit_rate > 0 else "red"

    # with col1:
    #     st.markdown(f"""
    #         <div class="metric-container">
    #             <h3 class="metric-label">Final Trade Profit</h3>
    #             <p class="metric-value" style="color: {profit_color};">{final_trade_profit_rate}%</p>
    #         </div>
    #     """, unsafe_allow_html=True)
            
    # with col2:
    #     st.markdown(f"""
    #         <div class="metric-container">
    #             <h3 class="metric-label">Win Trades</h3>
    #             <p class="metric-value" style="color: green;">{win_trades}</p>
    #         </div>
    #     """, unsafe_allow_html=True)
            
    # with col3:
    #     st.markdown(f"""
    #         <div class="metric-container">
    #             <h3 class="metric-label">Loss Trades</h3>
    #             <p class="metric-value" style="color: red;">{loss_trades}</p>
    #         </div>
    #     """, unsafe_allow_html=True)
        
    row_height = [0.65, 0.15, 0.25] if show_macd else [0.8, 0.2]
    row = 3 if show_macd else 2
    # Calculate the date range for the last 6 months
    end_date = filtered_df['date'].max()
    start_date = end_date - pd.DateOffset(months=6)

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
    
    # fig.add_trace(go.Scatter(x=filtered_trades['Exit_date'], y=filtered_trades['total_asset'], mode='lines', name='Profit', line=dict(color='green')), 
    #             row=row, col=1)

    fig.update_xaxes(range=[start_date, end_date],title_text="Date", row=3, col=1)
    # fig.update_yaxes(title_text="Profit/Loss", row=3, col=1)
    fig.update_layout(xaxis_rangeslider_visible=False, autosize=False, showlegend=False, height=1000, width=1200)

    return fig

def static_analysis_page():
    # Set the page title and layout
    st.set_page_config(page_title="Stock Analysis Dashboard", layout="wide")
    st.title("Stock Analysis Dashboard")
    
    # Add a sidebar
    # Connect to MongoDB and fetch the processed collection
    processed_collection = connect_to_mongo()[PROCESSED_COLLECTION_NAME]
    alert_collection = connect_to_mongo()[ALERT_COLLECTION_NAME]
    # Create a dropdown to select the stock
    stock_selector = st.sidebar.selectbox('Select Stock', options=sorted(processed_collection.distinct("symbol")), index=0)
    # Create a dropdown to select the interval
    default_interval = st.secrets['velocity_dict'][stock_selector]
    interval = st.sidebar.selectbox('Optimal Interval/ Select Interval', 
                                    options=sorted(processed_collection.distinct("interval")), 
                                    index=sorted(processed_collection.distinct("interval")).\
                                        index(default_interval) if default_interval in processed_collection.distinct("interval")\
                                            else 0)
    
    # Add an update button
    if st.sidebar.button("Update Data"):
        # Refetch the latest data when the button is clicked
        processed_df = fetch_stock_data(processed_collection, stock_selector, interval)
        # alert_df = fetch_alert_data(alert_collection, stock_selector)
        # filtered_trades = execute_trades(alert_df)
        st.success("Data updated successfully!")
    else:
        # Display the existing data if the button is not clicked
        processed_df = fetch_stock_data(processed_collection, stock_selector, interval)
        # alert_df = fetch_alert_data(alert_collection, stock_selector)
        # filtered_trades = execute_trades(alert_df)
        
    # Add a checkbox to show the MACD
    show_macd = st.sidebar.checkbox("Show MACD", value=False)
    
    # Create the figure
    fig = create_figure(processed_df, None, show_macd)
    st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    static_analysis_page()