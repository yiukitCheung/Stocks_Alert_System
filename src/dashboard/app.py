import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
from pymongo import MongoClient, DESCENDING, ASCENDING

st.set_page_config(page_title="Stock Prediction Dashboard", layout="wide")
st.sidebar.success("Select a page to view")

client =  MongoClient(**st.secrets["mongo"])
symbols = client[st.secrets['db_name']][st.secrets.processed_collection_name].distinct("symbol")
st.title("Stock Prediction Dashboard - Not yey implemented :construction:")
st.header("Earning Line Chart YTD")
line_chart = go.Figure()
line_chart.add_trace(go.Scatter(x=np.arange(1, 11), y=np.random.randint(1, 100, 10), mode='lines+markers', name='Trading',marker_line_color="rgba(0,0,0,0.7)",))
line_chart.add_trace(go.Scatter(x=np.arange(1, 11), y=np.random.randint(1, 100, 10), mode='lines+markers', name='SPY'))
line_chart.add_trace(go.Scatter(x=np.arange(1, 11), y=np.random.randint(1, 100, 10), mode='lines+markers', name='QQQ'))
line_chart.add_trace(go.Scatter(x=np.arange(1, 11), y=np.random.randint(1, 100, 10), mode='lines+markers', name='TLT'))

st.plotly_chart(line_chart)
st.header("Considered Stocks")
st.write(symbols)
col1, col2, col3 = st.columns(3)
with col1:
    st.subheader("Buy Signal")
    st.write(np.random.choice(symbols, 2))
with col2:
    st.subheader("Hold Signal")
    st.write(np.random.choice(symbols, 2))
with col3:
    st.subheader("Sell Signal")
    st.write(np.random.choice(symbols, 2))