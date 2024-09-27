import pandas as pd
import pymongo
import plotly.graph_objects as go
import plotly.subplots as sp

from utils.alert_strategy import Alert
from utils.trading_strategy import TradingStrategy
from utils.features_engineering import add_features
import streamlit as st
import pandas as pd

class StockDataViz:
    def __init__(self, db_uri="mongodb://localhost:27017/", 
                db_name="local", 
                collection_name="daily_stock_price"):
        
        # Connect to MongoDB
        self.client = pymongo.MongoClient(db_uri)
        # Select the database
        self.db = self.client[db_name]
        # Select the collection
        self.collection = self.db[collection_name]

    def run(self):
        # Initialize Streamlit app
        st.title('Stock Data Visualization')

        # Dropdown for stock selection
        stock_selector = st.selectbox(
            'Select Stock',
            options=self.collection.distinct("symbol"),
            index=0
        )

        # Fetch specific stock
        filtered_query = self.collection.find({"symbol": f"{stock_selector}"})
        # Convert to pandas dataframe
        filtered_df = pd.DataFrame(list(filtered_query))   
        # Add technical features
        filtered_df = add_features(filtered_df).apply()

        # Add Alerts
        filtered_df = Alert(filtered_df).add_alert()

        # Sandbox Testing
        trades_history = TradingStrategy(filtered_df)
        trades_history.execute_trades()
        filtered_trades = trades_history.get_trades()

        # Create figure with subplots
        fig = sp.make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.1, row_heights=[0.65, 0.15, 0.25])

        # Candlestick chart
        fig.add_trace(go.Candlestick(
            x=filtered_df['date'],
            open=filtered_df['open'],
            high=filtered_df['high'],
            low=filtered_df['low'],
            close=filtered_df['close'],
            name='price'), row=1, col=1)

        # Add EMA traces as lines
        fig.add_trace(go.Scatter(x=filtered_df['date'], y=filtered_df['144EMA'], 
                                mode="lines", name="EMA 144"), row=1, col=1)
        fig.add_trace(go.Scatter(x=filtered_df['date'], y=filtered_df['169EMA'],
                                mode="lines", name="EMA 169"), row=1, col=1)
        fig.add_trace(go.Scatter(x=filtered_df['date'], y=filtered_df['13EMA'],
                                mode="lines", name="EMA 13"), row=1, col=1)
        fig.add_trace(go.Scatter(x=filtered_df['date'], y=filtered_df['8EMA'],
                                mode="lines", name="EMA 8"), row=1, col=1)

        # Add Buy / Sell Annotations
        fig.add_trace(go.Scatter(
                        x=filtered_trades.Entry_date,
                        y=filtered_trades.Entry_price,
                        mode="markers",
                        customdata=filtered_trades,
                        marker_symbol="diamond-dot",
                        marker_size=8,
                        marker_line_width=2,
                        marker_line_color="rgba(0,0,0,0.7)",
                        marker_color="rgba(0,255,0,0.7)",
                        hovertemplate="Entry Time: %{customdata[1]}<br>" +\
                            "Entry Price: %{y:.2f}<br>" +\
                            "Total Asset: %{customdata[5]:.3f}",
                        name="Entries"), row=1, col=1)

        fig.add_trace(go.Scatter(
                        x=filtered_trades.Exit_date,
                        y=filtered_trades.Exit_price,
                        mode="markers",
                        customdata=filtered_trades,
                        marker_symbol="diamond-dot",
                        marker_size=8,
                        marker_line_width=2,
                        marker_line_color="rgba(0,0,0,0.7)",
                        marker_color="rgba(255,0,0,0.7)",
                        hovertemplate="Exit Time: %{customdata[1]}<br>" +\
                            "Exit Price: %{y:.2f}<br>" +\
                            "Total Asset: %{customdata[5]:.3f}",
                        name="Exits"), row=1, col=1)

        # Add MACD subplot
        fig.add_trace(go.Scatter(x=filtered_df['date'], y=filtered_df['MACD'], 
                                mode='lines', name='MACD'), row=2, col=1)
        fig.add_trace(go.Scatter(x=filtered_df['date'], y=filtered_df['MACD_SIGNAL'],
                                mode='lines', name='MACD signal'), row=2, col=1)

        # Add Profit subplot with conditional coloring
        fig.add_trace(go.Scatter(x=filtered_trades['Exit_date'], y=filtered_trades['total_asset'], 
                                mode='lines', name='Profit', line=dict(color='green')),
                    row=3, col=1)

        # Set x and y axis titles
        fig.update_xaxes(title_text="Date", row=3, col=1)
        fig.update_yaxes(title_text="Profit/Loss", row=3, col=1)

        # Layout settings
        fig.update_layout(
            xaxis_rangeslider_visible=False,
            autosize=False,
            showlegend=False,
            height=800,
            width=1200,
        )

        # Display the figure in Streamlit
        st.plotly_chart(fig, use_container_width=True)

# To run the app
if __name__ == "__main__":
    app = StockDataViz()
    app.run()