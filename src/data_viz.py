import pandas as pd
import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
import pymongo
import plotly.graph_objects as go
import plotly.subplots as sp

from alert_strategy import Alert
from features_engineering import add_features
from trading_strategy import TradingStrategy

# Fetch data from database

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
# Select the database
db = client["stock_data_db"]
# Select the collection
collection = db["stock_data"]
# Fetch all data
data=collection.find({})
# Convert to pandas dataframe
df = pd.DataFrame(list(data))

# Initialize Dash app
app = dash.Dash(__name__)

# Create figure with subplots
fig = sp.make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.1, row_heights=[0.65, 0.15, 0.25])

# Define the layout of the Dash app
app.layout = html.H1([
app.layout = html.H1([
    dcc.Dropdown(
        id='stock-selector',
        options=[{'label': symbol, 'value': symbol} for symbol in df['symbol'].unique()],
        value=df['symbol'].unique()[0]
    ),
    dcc.Graph(id='Sandbox Testing', figure=fig)
    ]
)

# Callback to update the graph based on selected stock and selected range
@app.callback(
    Output('Sandbox Testing', 'figure'),
    [Input('stock-selector', 'value'), Input('Sandbox Testing', 'relayoutData')]
    Output('Sandbox Testing', 'figure'),
    [Input('stock-selector', 'value'), Input('Sandbox Testing', 'relayoutData')]
)
def update_chart(selected_stock, relayout_data):
    
    # Filter the data based on the selected stock
    filtered_df = pd.DataFrame(df[df['symbol'] == selected_stock].iloc[0]['price_data'])    
    
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

    # Add Profit subplot
    fig.add_trace(go.Scatter(x=filtered_trades['Exit_date'], y=filtered_trades['total_asset'], 
                            mode='lines', name='Profit'), row=3, col=1)

    # Layout settings
    fig.update_layout(
        xaxis_rangeslider_visible=False,
        autosize=False,
        height=800,
        height=800,
    )

    # If a new x-axis range is selected or zoomed
    if relayout_data and 'xaxis.range[0]' in relayout_data and 'xaxis.range[1]' in relayout_data:
        x_start = relayout_data['xaxis.range[0]']
        x_end = relayout_data['xaxis.range[1]']

        # Filter the data based on the selected x-axis range
        filtered_data = filtered_df[(filtered_df['date'] >= x_start) & (filtered_df['date'] <= x_end)]
        trades_filtered = filtered_trades[(filtered_trades['Exit_date'] >= x_start) & (filtered_trades['Exit_date'] <= x_end)]
        
        # Calculate the new y-axis range for the candlestick chart
        y_min = filtered_data['low'].min()
        y_max = filtered_data['high'].max()

        # Calculate the new y-axis range for the MACD chart
        y_min_macd = filtered_data[['MACD', 'MACD_SIGNAL']].min().min()
        y_max_macd = filtered_data[['MACD', 'MACD_SIGNAL']].max().max()

        # Calculate the new y-axis range for the Profit chart
        y_min_profit = trades_filtered['total_asset'].min()
        y_max_profit = trades_filtered['total_asset'].max()

        # Update the figure with new y-axis and x-axis ranges
        fig.update_xaxes(range=[x_start, x_end], row=1, col=1)
        fig.update_xaxes(range=[x_start, x_end], row=2, col=1)  
        fig.update_xaxes(range=[x_start, x_end], row=3, col=1) 
        
        fig.update_yaxes(range=[y_min, y_max], row=1, col=1)
        fig.update_yaxes(range=[y_min_macd, y_max_macd], row=2, col=1)
        fig.update_yaxes(range=[y_min_profit, y_max_profit], row=3, col=1)

    return fig

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True, port=8051)